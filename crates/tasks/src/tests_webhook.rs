//! Behaviour at the durable service/queue boundary. Written for external review;
//! do not execute until the reviewer explicitly authorizes the test stage.
use super::*;
use pioneer_protocol::{TaskCancelScope, TaskRunInputEncoding, TaskRunTriggerInput};
use sea_orm::{ColumnTrait, QueryFilter};

fn webhook_params() -> TaskCreateParams {
    let mut params = create_params(TaskTriggerSpec::External {
        source: "webhook".into(),
        event_type: None,
        filter: None,
    });
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    spec.prompt.instructions =
        vec!["Interpret each incoming body using the saved objective.".into()];
    spec.prompt.output_instructions = Some("Return the result for this request.".into());
    spec.prompt.input = Some(TaskAgentInput {
        text: Some("saved template".into()),
        variables: Vec::new(),
        attachments: Vec::new(),
        references: Vec::new(),
    });
    params.agent_spec = Some(spec);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_webhook_owner".into());
    params.created_by_thread_id = params.owner_id.clone();
    params
}

fn payload(body: &str) -> TaskRunTriggerInput {
    TaskRunTriggerInput {
        content_type: Some("text/plain; charset=utf-8".into()),
        encoding: TaskRunInputEncoding::Utf8,
        body: body.into(),
    }
}

async fn create(
    runtime: &TaskRuntime,
    params: TaskCreateParams,
) -> pioneer_protocol::TaskCreateResponse {
    let mut context = task_create_context_for(&params);
    context.webhook_secret_ref = Some("webhook-test-secret-reference".into());
    runtime
        .service()
        .create_task(context, params)
        .await
        .unwrap()
}

async fn read(runtime: &TaskRuntime, id: &str) -> pioneer_protocol::TaskGetResponse {
    runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id: id.into() })
        .await
        .unwrap()
}

async fn post(
    runtime: &TaskRuntime,
    trigger: &pioneer_protocol::TaskTrigger,
    body: &str,
) -> TaskRun {
    runtime
        .service()
        .accept_webhook(
            &trigger.id,
            trigger.webhook_secret_ref.as_deref().unwrap(),
            payload(body),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn webhook_waits_then_accepts_independent_concurrent_inputs_into_fifo_queue() {
    let runtime = runtime().await;
    let created = create(&runtime, webhook_params()).await;
    assert!(created.run.is_none());
    assert_eq!(created.task.status, TaskStatus::Waiting);
    assert_eq!(created.trigger.id.len(), 21);
    assert!(
        created
            .trigger
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
    );
    assert_eq!(
        created.task.lifecycle_policy.as_ref().unwrap().completion,
        pioneer_protocol::TaskCompletionBehavior::KeepActiveForRecurring
    );
    assert_eq!(
        created.task.lifecycle_policy.as_ref().unwrap().attachment,
        TaskAttachmentMode::Detached
    );
    assert_eq!(
        created.task.delivery_policy.as_ref().unwrap().mode,
        TaskDeliveryMode::Thread
    );
    assert_eq!(
        created
            .task
            .delivery_policy
            .as_ref()
            .unwrap()
            .thread_id
            .as_deref(),
        Some("thr_webhook_owner")
    );
    let (left, right) = tokio::join!(
        post(&runtime, &created.trigger, " first\n"),
        post(&runtime, &created.trigger, "second\0")
    );
    assert_ne!(left.id, right.id);
    let duplicate = post(&runtime, &created.trigger, " first\n").await;
    assert_ne!(left.id, duplicate.id);
    let after = read(&runtime, &created.task.id).await;
    assert_eq!(after.runs.len(), 3);
    assert_eq!(
        after.runs.iter().map(|r| r.run_number).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(after.triggers[0].id, created.trigger.id);
    for accepted in [&left, &right, &duplicate] {
        let durable = after.runs.iter().find(|r| r.id == accepted.id).unwrap();
        assert_eq!(durable.trigger_input, accepted.trigger_input);
        let occurrence = runtime
            .service()
            .store()
            .get_task_occurrence_contract_by_run(&accepted.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(occurrence.status, TaskOccurrenceStatus::Queued);
    }
    let base = after
        .agent_specs
        .iter()
        .find(|spec| spec.run_id.is_none())
        .unwrap();
    assert_eq!(
        base.prompt.input.as_ref().unwrap().text.as_deref(),
        Some("saved template")
    );
    assert_eq!(
        after
            .agent_specs
            .iter()
            .filter(|spec| spec.run_id.is_some())
            .count(),
        3
    );
    let store = runtime.service().store();
    // Competing dispatchers cannot admit run 2 while run 1 owns the only slot.
    let first = &after.runs[0];
    let second = &after.runs[1];
    let (claim_first, claim_second) = tokio::join!(
        store.claim_task_run_for_dispatch(&first.id, first.created_at),
        store.claim_task_run_for_dispatch(&second.id, second.created_at),
    );
    assert!(claim_first.unwrap().is_some());
    assert!(claim_second.unwrap().is_none());
    store
        .append_task_event(
            TaskEventPayload::RunCancelled {
                task_id: first.task_id.clone(),
                run_id: first.id.clone(),
                reason: Some("release queue slot".into()),
                cancelled_at: first.created_at + 1,
            },
            first.created_at + 1,
        )
        .await
        .unwrap();
    assert!(
        store
            .claim_task_run_for_dispatch(&second.id, second.created_at + 1)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn webhook_rejects_paused_cancelled_and_finish_requested_tasks_and_resume_keeps_capability() {
    for operation in ["pause", "cancel", "finish"] {
        let runtime = runtime().await;
        let created = create(&runtime, webhook_params()).await;
        let service = runtime.service();
        assert!(
            service
                .accept_webhook(
                    &created.trigger.id,
                    "wrong-secret-reference",
                    payload("unauthorized")
                )
                .await
                .is_err()
        );
        match operation {
            "pause" => {
                service
                    .pause_task(
                        TaskMutationContext::default(),
                        TaskPauseParams {
                            task_id: created.task.id.clone(),
                            reason: None,
                        },
                    )
                    .await
                    .unwrap();
            }
            "cancel" => {
                service
                    .cancel_task(
                        TaskMutationContext::default(),
                        TaskCancelParams {
                            task_id: created.task.id.clone(),
                            reason: None,
                            scope: TaskCancelScope::TaskOnly,
                        },
                    )
                    .await
                    .unwrap();
            }
            "finish" => {
                service
                    .finish_task(
                        TaskMutationContext::user(TEST_PRINCIPAL_ID),
                        pioneer_protocol::TaskFinishParams {
                            task_id: created.task.id.clone(),
                            reason: None,
                        },
                    )
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            service
                .accept_webhook(
                    &created.trigger.id,
                    created.trigger.webhook_secret_ref.as_deref().unwrap(),
                    payload("must not enter queue")
                )
                .await
                .is_err()
        );
        assert!(read(&runtime, &created.task.id).await.runs.is_empty());
        if operation == "pause" {
            service
                .resume_task(
                    TaskMutationContext::default(),
                    TaskResumeParams {
                        task_id: created.task.id.clone(),
                        reason: None,
                    },
                )
                .await
                .unwrap();
            let resumed = read(&runtime, &created.task.id).await;
            assert_eq!(
                resumed.triggers[0].webhook_secret_ref,
                created.trigger.webhook_secret_ref
            );
            assert_eq!(resumed.triggers[0].id, created.trigger.id);
            post(&runtime, &created.trigger, "after resume").await;
        }
    }
}

#[tokio::test]
async fn accepted_webhook_survives_runtime_restart_and_retry_preserves_exact_input() {
    let runtime = runtime().await;
    let mut params = webhook_params();
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 2,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(5),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    let created = create(&runtime, params).await;
    let accepted = post(
        &runtime,
        &created.trigger,
        "\nopaque request with no envelope\n",
    )
    .await;
    let store = runtime.service().store();
    drop(runtime);
    let restarted = TaskRuntime::new(store.clone());
    let recovered = read(&restarted, &created.task.id).await;
    assert_eq!(recovered.runs[0].trigger_input, accepted.trigger_input);
    assert_eq!(recovered.runs[0].status, TaskRunStatus::Queued);
    assert_eq!(recovered.triggers[0].id, created.trigger.id);
    // Traverse ordinary claim/execution/terminal orchestration, which creates
    // a new retry TaskRun and reuses the occurrence's saved delivery policy.
    store
        .claim_task_run_for_dispatch(&accepted.id, accepted.created_at)
        .await
        .unwrap()
        .unwrap();
    store
        .claim_task_run_execution_for_dispatch(
            &accepted.id,
            accepted.executor_kind,
            "webhook-test",
            accepted.created_at,
            accepted.created_at + 60,
        )
        .await
        .unwrap()
        .unwrap();
    persist_mock_agent_execution(store.clone(), &accepted).await;
    let handle = TaskExecutionHandle::new(
        store,
        restarted.event_bus(),
        accepted.task_id.clone(),
        accepted.id.clone(),
    );
    handle.mark_started(accepted.created_at).await.unwrap();
    handle
        .fail_run(
            Some(TaskError {
                recovery_diagnostic: None,
                code: "test_failure".into(),
                message: "retry this run".into(),
                class: TaskErrorClass::Internal,
                details: None,
                failed_run_id: Some(accepted.id.clone()),
            }),
            accepted.created_at + 1,
        )
        .await
        .unwrap();
    let after = read(&restarted, &created.task.id).await;
    let retry = after
        .runs
        .iter()
        .find(|run| run.retry_of_run_id.as_deref() == Some(accepted.id.as_str()))
        .unwrap();
    assert_eq!(retry.trigger_input, accepted.trigger_input);
    assert_eq!(retry.attempt_number, 2);
    assert_eq!(retry.run_group_id, accepted.run_group_id);
    assert!(after.task.finish_request().is_none());
    assert!(!after.task.status.is_terminal());
}

#[tokio::test]
async fn webhook_scheduler_drains_two_requests_at_limit_one_then_waits_with_delivery() {
    let runtime = runtime().await;
    let starts = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    runtime
        .register_executor(Arc::new(SlowAgentExecutor {
            store: runtime.service().store(),
            starts: starts.clone(),
            release: release.clone(),
        }))
        .await;
    let created = create(&runtime, webhook_params()).await;
    let first = post(&runtime, &created.trigger, "first scheduler request").await;
    let second = post(&runtime, &created.trigger, "second scheduler request").await;
    assert_eq!(
        runtime.process_due_once(second.created_at).await.unwrap(),
        1
    );
    timeout(Duration::from_secs(5), async {
        while starts.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let in_flight = read(&runtime, &created.task.id).await;
    assert_eq!(in_flight.runs[0].status, TaskRunStatus::Starting);
    assert_eq!(in_flight.runs[1].status, TaskRunStatus::Queued);
    assert_eq!(
        runtime.process_due_once(second.created_at).await.unwrap(),
        0
    );
    release.notify_one();
    timeout(Duration::from_secs(5), async {
        while read(&runtime, &created.task.id).await.runs[0].status != TaskRunStatus::Succeeded {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        runtime.process_due_once(second.created_at).await.unwrap(),
        1
    );
    timeout(Duration::from_secs(5), async {
        while starts.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.notify_one();
    let after = timeout(Duration::from_secs(5), async {
        loop {
            let after = read(&runtime, &created.task.id).await;
            if after.task.status == TaskStatus::Waiting
                && after
                    .runs
                    .iter()
                    .all(|run| run.status == TaskRunStatus::Succeeded)
            {
                break after;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(after.triggers[0].status, TaskTriggerStatus::Active);
    assert_eq!(after.runs[0].trigger_input, first.trigger_input);
    assert_eq!(after.runs[1].trigger_input, second.trigger_input);
    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: TEST_WORKSPACE_ID.into(),
            task_id: Some(created.task.id.clone()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap();
    assert_eq!(deliveries.deliveries.len(), 2);
    assert!(
        deliveries
            .deliveries
            .iter()
            .all(|delivery| delivery.target_thread_id.as_deref() == Some("thr_webhook_owner"))
    );
    post(&runtime, &created.trigger, "next request").await;
}

#[tokio::test]
async fn cancellation_reprepares_when_a_webhook_run_arrives_after_its_snapshot() {
    for starting in [false, true] {
        let runtime = runtime().await;
        let created = create(&runtime, webhook_params()).await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        runtime.service().store().set_delivery_commit_gate_for_test(
            pioneer_crud::TaskDeliveryCommitTestKind::Cancellation,
            entered.clone(),
            release.clone(),
        );
        let service = runtime.service();
        let task_id = created.task.id.clone();
        let cancellation = tokio::spawn(async move {
            service
                .cancel_task(
                    TaskMutationContext::default(),
                    TaskCancelParams {
                        task_id,
                        reason: Some("stop accepting webhook work".into()),
                        scope: TaskCancelScope::TaskOnly,
                    },
                )
                .await
        });
        timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        // The original cancellation plan has no runs. The accepted request must
        // still be included, whether it stays Queued or wins a dispatch claim.
        let accepted = post(
            &runtime,
            &created.trigger,
            "accepted during cancellation preparation",
        )
        .await;
        if starting {
            assert!(
                runtime
                    .service()
                    .store()
                    .claim_task_run_for_dispatch(&accepted.id, accepted.created_at)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        release.notify_one();
        let cancelled = timeout(Duration::from_secs(5), cancellation)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.task.status, TaskStatus::Cancelled);
        let after = read(&runtime, &created.task.id).await;
        assert_eq!(after.runs.len(), 1);
        assert_eq!(after.runs[0].status, TaskRunStatus::Cancelled);
        assert_eq!(after.runs[0].trigger_input, accepted.trigger_input);
        let occurrence = runtime
            .service()
            .store()
            .get_task_occurrence_contract_by_run(&accepted.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(occurrence.status, TaskOccurrenceStatus::Cancelled);
        assert!(
            runtime
                .service()
                .accept_webhook(
                    &created.trigger.id,
                    created.trigger.webhook_secret_ref.as_deref().unwrap(),
                    payload("after cancellation"),
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn webhook_defaults_and_admission_class_do_not_change_generic_external_or_overrides() {
    let runtime = runtime().await;
    for webhook in [true, false] {
        for explicit in [false, true] {
            let mut params = webhook_params();
            if !webhook {
                params.trigger.spec = TaskTriggerSpec::External {
                    source: "reserved-other-source".into(),
                    event_type: None,
                    filter: None,
                };
            }
            if explicit {
                params.lifecycle_policy = Some(TaskLifecyclePolicy {
                    attachment: TaskAttachmentMode::Detached,
                    on_parent_cancel: TaskParentTerminalAction::KeepRunning,
                    on_parent_failure: TaskParentTerminalAction::KeepRunning,
                    completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
                });
                params.delivery_policy = Some(TaskDeliveryPolicy {
                    mode: TaskDeliveryMode::None,
                    thread_target: None,
                    thread_id: None,
                    webhook_url: None,
                    include_result: false,
                    format: pioneer_protocol::TaskDeliveryFormat::Summary,
                });
            }
            let expected_lifecycle = params.lifecycle_policy.clone();
            let expected_delivery = params.delivery_policy.clone();
            let mut context = task_create_context_for(&params);
            context.webhook_secret_ref = webhook.then(|| "webhook-default-test".into());
            let created = runtime
                .service()
                .create_task(context, params)
                .await
                .unwrap();
            let lifecycle = created.task.lifecycle_policy.as_ref().unwrap();
            let delivery = created.task.delivery_policy.as_ref().unwrap();
            if explicit {
                assert_eq!(Some(lifecycle), expected_lifecycle.as_ref());
                assert_eq!(Some(delivery), expected_delivery.as_ref());
            } else if webhook {
                assert_eq!(
                    lifecycle.completion,
                    pioneer_protocol::TaskCompletionBehavior::KeepActiveForRecurring
                );
                assert_eq!(delivery.mode, TaskDeliveryMode::Thread);
                assert_eq!(
                    delivery.thread_target,
                    Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
                );
            } else {
                assert_eq!(
                    lifecycle.completion,
                    pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun
                );
                assert_eq!(delivery.mode, TaskDeliveryMode::None);
                assert!(delivery.thread_id.is_none());
            }
            let lease = pioneer_entity::execution_admission_lease::Entity::find_by_id(format!(
                "quota:task:{}",
                created.task.id
            ))
            .one(&runtime.service().store().database_connection())
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                lease.operation_class,
                if webhook {
                    "scheduled_task"
                } else {
                    "queued_task"
                }
            );
            assert_eq!(
                lease.quota_bucket,
                if webhook { "scheduled" } else { "queued" }
            );
        }
    }
}

#[tokio::test]
async fn webhook_admission_does_not_decode_historical_inputs_or_run_specific_specs() {
    let runtime = runtime().await;
    let created = create(&runtime, webhook_params()).await;
    let store = runtime.service().store();
    for _ in 0..8 {
        post(
            &runtime,
            &created.trigger,
            &"historical request".repeat(4096),
        )
        .await;
    }
    runtime
        .service()
        .update_task(
            TaskMutationContext::default(),
            TaskUpdateParams {
                task_id: created.task.id.clone(),
                instructions: Some(vec!["Use the current saved webhook instructions.".into()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let database = store.database_connection();
    // Unrelated old payloads are deliberately unreadable. A full aggregate
    // reader fails here; accepting the next request must not depend on them.
    database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task_run SET trigger_input_json='not-json' WHERE task_id=?",
            [created.task.id.clone().into()],
        ))
        .await
        .unwrap();
    database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task_agent_spec SET prompt_json='not-json' WHERE task_id=? AND run_id IS NOT NULL",
            [created.task.id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(store.get_task(&created.task.id).await.is_err());
    let accepted = post(&runtime, &created.trigger, "new independent body").await;
    assert_eq!(accepted.run_number, 9);
    assert_eq!(
        store
            .get_task_run(&accepted.id)
            .await
            .unwrap()
            .unwrap()
            .trigger_input,
        Some(payload("new independent body"))
    );
    let frozen = pioneer_entity::task_agent_spec::Entity::find()
        .filter(pioneer_entity::task_agent_spec::Column::RunId.eq(accepted.id))
        .one(&database)
        .await
        .unwrap()
        .unwrap();
    let prompt: TaskAgentPrompt = serde_json::from_str(&frozen.prompt_json).unwrap();
    assert_eq!(
        prompt.instructions,
        vec!["Use the current saved webhook instructions."]
    );
}

async fn admit_webhook_for_terminal(runtime: &TaskRuntime, run: &TaskRun) {
    let store = runtime.service().store();
    store
        .claim_task_run_for_dispatch(&run.id, run.created_at)
        .await
        .unwrap()
        .unwrap();
    store
        .claim_task_run_execution_for_dispatch(
            &run.id,
            run.executor_kind,
            "webhook-terminal-race",
            run.created_at,
            run.created_at + 60,
        )
        .await
        .unwrap()
        .unwrap();
    persist_mock_agent_execution(store.clone(), run).await;
    TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    )
    .mark_started(run.created_at)
    .await
    .unwrap();
}

#[tokio::test]
async fn terminal_commit_reprepares_after_webhook_admission_without_losing_result_or_retry_number()
{
    for retry in [false, true] {
        let runtime = runtime().await;
        let mut params = webhook_params();
        if retry {
            params.retry_policy = Some(TaskRetryPolicy {
                max_attempts: 2,
                backoff: TaskRetryBackoffKind::Fixed,
                initial_delay_seconds: Some(5),
                max_delay_seconds: None,
                retry_on: vec![TaskErrorClass::Internal],
            });
        }
        let created = create(&runtime, params).await;
        let first = post(&runtime, &created.trigger, "first body").await;
        admit_webhook_for_terminal(&runtime, &first).await;
        let store = runtime.service().store();
        let terminal = if retry {
            TaskEventPayload::RunFailed {
                task_id: first.task_id.clone(),
                run_id: first.id.clone(),
                error: Some(TaskError {
                    recovery_diagnostic: None,
                    code: "test_failure".into(),
                    message: "retry accepted body".into(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: Some(first.id.clone()),
                }),
                completed_at: first.created_at + 1,
            }
        } else {
            TaskEventPayload::RunCompleted {
                task_id: first.task_id.clone(),
                run_id: first.id.clone(),
                result: Some(TaskResult {
                    summary: Some("first request completed".into()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(first.id.clone()),
                }),
                completed_at: first.created_at + 1,
            }
        };
        let prepared = store
            .prepare_task_terminal_transition(terminal)
            .await
            .unwrap();
        let second = post(
            &runtime,
            &created.trigger,
            "second accepted while first completes",
        )
        .await;
        let outcome = store
            .commit_task_terminal_transition(prepared)
            .await
            .unwrap();
        assert_eq!(
            outcome.status,
            pioneer_crud::TaskTerminalCommitStatus::Applied
        );
        let after = read(&runtime, &created.task.id).await;
        assert_eq!(after.runs.len(), if retry { 3 } else { 2 });
        assert_eq!(
            after.runs[0].status,
            if retry {
                TaskRunStatus::Failed
            } else {
                TaskRunStatus::Succeeded
            }
        );
        assert_eq!(after.runs[1].id, second.id);
        assert_eq!(after.runs[1].trigger_input, second.trigger_input);
        assert_eq!(after.runs[1].status, TaskRunStatus::Queued);
        if retry {
            assert!(outcome.retry_scheduled);
            assert_eq!(after.runs[2].run_number, 3);
            assert_eq!(
                after.runs[2].retry_of_run_id.as_deref(),
                Some(first.id.as_str())
            );
            assert_eq!(after.runs[2].trigger_input, first.trigger_input);
        } else {
            assert_eq!(
                after.runs[0].result.as_ref().unwrap().summary.as_deref(),
                Some("first request completed")
            );
            let deliveries = runtime
                .service()
                .list_deliveries(TaskDeliveriesParams {
                    workspace_id: TEST_WORKSPACE_ID.into(),
                    task_id: Some(first.task_id.clone()),
                    run_id: Some(first.id),
                    statuses: vec![],
                    limit: Some(10),
                })
                .await
                .unwrap();
            assert_eq!(deliveries.deliveries.len(), 1);
            assert_eq!(
                deliveries.deliveries[0].target_thread_id.as_deref(),
                Some("thr_webhook_owner")
            );
            assert_eq!(
                deliveries.deliveries[0]
                    .result_snapshot
                    .as_ref()
                    .unwrap()
                    .summary
                    .as_deref(),
                Some("first request completed")
            );
        }
    }
}

#[tokio::test]
async fn queued_webhook_does_not_allow_terminal_commit_to_adopt_changed_policy() {
    let runtime = runtime().await;
    let created = create(&runtime, webhook_params()).await;
    let first = post(&runtime, &created.trigger, "protected run").await;
    admit_webhook_for_terminal(&runtime, &first).await;
    let store = runtime.service().store();
    let prepared = store
        .prepare_task_terminal_transition(TaskEventPayload::RunCompleted {
            task_id: first.task_id.clone(),
            run_id: first.id.clone(),
            result: None,
            completed_at: first.created_at + 1,
        })
        .await
        .unwrap();
    post(&runtime, &created.trigger, "new queue member").await;
    runtime
        .service()
        .update_task(
            TaskMutationContext::default(),
            TaskUpdateParams {
                task_id: created.task.id.clone(),
                delivery_policy: Some(TaskDeliveryPolicy {
                    mode: TaskDeliveryMode::None,
                    thread_target: None,
                    thread_id: None,
                    webhook_url: None,
                    include_result: false,
                    format: pioneer_protocol::TaskDeliveryFormat::Summary,
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let error = store
        .commit_task_terminal_transition(prepared)
        .await
        .unwrap_err();
    assert!(
        error
            .downcast_ref::<pioneer_crud::TaskTerminalConflict>()
            .is_some()
    );
    assert_eq!(
        store.get_task_run(&first.id).await.unwrap().unwrap().status,
        TaskRunStatus::Running
    );
}

fn completed_webhook(run: &TaskRun, at: i64, summary: &str) -> TaskEventPayload {
    TaskEventPayload::RunCompleted {
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        result: Some(TaskResult {
            summary: Some(summary.into()),
            data: None,
            artifacts: vec![],
            completed_by_run_id: Some(run.id.clone()),
        }),
        completed_at: at,
    }
}

fn failed_webhook(run: &TaskRun, at: i64) -> TaskEventPayload {
    TaskEventPayload::RunFailed {
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        error: Some(TaskError {
            recovery_diagnostic: None,
            code: "parallel_failure".into(),
            message: "Retry this request independently".into(),
            class: TaskErrorClass::Internal,
            details: None,
            failed_run_id: Some(run.id.clone()),
        }),
        completed_at: at,
    }
}

fn parallel_webhook_params() -> TaskCreateParams {
    let mut params = webhook_params();
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 2,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 2,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(5),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    params
}

#[tokio::test]
async fn parallel_webhook_terminal_commits_preserve_both_results_inputs_and_deliveries() {
    // Preserve S1 success/success, then exercise sibling retry/success and two
    // prepared failures whose retries must receive fresh, distinct numbers.
    // The last variant admits the retry before committing its prepared sibling.
    for scenario in 0..=3 {
        let admit_retry = scenario == 3;
        let failures = if admit_retry { 1 } else { scenario };
        let runtime = runtime().await;
        let created = create(&runtime, parallel_webhook_params()).await;
        let first = post(&runtime, &created.trigger, "first parallel body").await;
        let second = post(&runtime, &created.trigger, "second parallel body").await;
        admit_webhook_for_terminal(&runtime, &first).await;
        admit_webhook_for_terminal(&runtime, &second).await;
        let before = read(&runtime, &created.task.id).await;
        assert!(
            before
                .runs
                .iter()
                .all(|run| run.status == TaskRunStatus::Running)
        );
        let trigger = &before.triggers[0];
        let first_at = trigger.updated_at + 10;
        let second_at = trigger.updated_at + 20;
        let store = runtime.service().store();
        let first_prepared = store
            .prepare_task_terminal_transition(if failures > 0 {
                failed_webhook(&first, first_at)
            } else {
                completed_webhook(&first, first_at, "first parallel result")
            })
            .await
            .unwrap();
        let second_prepared = store
            .prepare_task_terminal_transition(if failures > 1 {
                failed_webhook(&second, second_at)
            } else {
                completed_webhook(&second, second_at, "second parallel result")
            })
            .await
            .unwrap();
        let first_outcome = store
            .commit_task_terminal_transition(first_prepared)
            .await
            .unwrap();
        assert_eq!(
            first_outcome.status,
            pioneer_crud::TaskTerminalCommitStatus::Applied
        );
        let between = read(&runtime, &created.task.id).await;
        assert_eq!(&between.triggers[0], trigger);
        assert_eq!(
            between.task.status,
            if failures > 0 {
                TaskStatus::Queued
            } else {
                TaskStatus::Running
            }
        );
        if admit_retry {
            let retry = between.runs.iter().find(|run| run.run_number == 3).unwrap();
            let previous = store
                .get_task_occurrence_contract_by_run(&first.id)
                .await
                .unwrap()
                .unwrap();
            // Use the scheduler's ordinary retry transition and atomic claim:
            // this transfers the same logical occurrence away from the source.
            let mut occurrence = previous.retry(retry.attempt_number).unwrap();
            occurrence.run_id = retry.id.clone();
            let (claimed, _) = store
                .claim_task_retry_for_dispatch_with_event(
                    &retry.id,
                    retry.ready_at.unwrap(),
                    occurrence,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(claimed.status, TaskRunStatus::Starting);
            assert_eq!(claimed.trigger_input, first.trigger_input);
            assert!(
                store
                    .get_task_occurrence_contract_by_run(&first.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let rebound = store
                .get_task_occurrence_contract_by_run(&retry.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(rebound.occurrence_id, previous.occurrence_id);
            assert_eq!(rebound.run_id, retry.id);
            assert_eq!(rebound.task_id, previous.task_id);
            assert_eq!(rebound.trigger_id, previous.trigger_id);
            assert_eq!(rebound.occurrence_key, previous.occurrence_key);
            assert_eq!(
                rebound.action_idempotency_key,
                previous.action_idempotency_key
            );
            assert_eq!(rebound.execution_generation, previous.execution_generation);
            assert_eq!(rebound.retry_attempt, retry.attempt_number);
        }
        let second_outcome = store
            .commit_task_terminal_transition(second_prepared)
            .await
            .unwrap();
        assert_eq!(
            second_outcome.status,
            pioneer_crud::TaskTerminalCommitStatus::Applied
        );
        assert_eq!(first_outcome.retry_scheduled, failures > 0);
        assert_eq!(second_outcome.retry_scheduled, failures > 1);
        let after = read(&runtime, &created.task.id).await;
        assert_eq!(
            after.task.status,
            if failures > 0 {
                TaskStatus::Queued
            } else {
                TaskStatus::Waiting
            }
        );
        assert_eq!(after.runs.len(), 2 + failures);
        assert_eq!(
            after
                .runs
                .iter()
                .map(|run| run.run_number)
                .collect::<Vec<_>>(),
            (1..=(2 + failures) as i64).collect::<Vec<_>>()
        );
        let deliveries = runtime
            .service()
            .list_deliveries(TaskDeliveriesParams {
                workspace_id: TEST_WORKSPACE_ID.into(),
                task_id: Some(first.task_id.clone()),
                run_id: None,
                statuses: vec![],
                limit: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(deliveries.deliveries.len(), 2 - failures);
        for (index, accepted, completed_at, summary, outcome) in [
            (0, &first, first_at, "first parallel result", &first_outcome),
            (
                1,
                &second,
                second_at,
                "second parallel result",
                &second_outcome,
            ),
        ] {
            let run = after.runs.iter().find(|run| run.id == accepted.id).unwrap();
            let retrying = index < failures;
            assert_eq!(
                run.status,
                if retrying {
                    TaskRunStatus::Failed
                } else {
                    TaskRunStatus::Succeeded
                }
            );
            assert_eq!(run.completed_at, Some(completed_at));
            assert_eq!(run.trigger_input, accepted.trigger_input);
            if retrying {
                assert!(outcome.delivery_id.is_none());
                let retry = after
                    .runs
                    .iter()
                    .find(|run| run.retry_of_run_id.as_deref() == Some(accepted.id.as_str()))
                    .unwrap();
                assert_eq!(retry.run_number, 3 + index as i64);
                assert_eq!(retry.parent_run_id.as_deref(), Some(accepted.id.as_str()));
                assert_eq!(retry.run_group_id, accepted.run_group_id);
                assert_eq!(retry.attempt_number, 2);
                assert_eq!(
                    retry.status,
                    if admit_retry {
                        TaskRunStatus::Starting
                    } else {
                        TaskRunStatus::Queued
                    }
                );
                assert_eq!(retry.trigger_input, accepted.trigger_input);
            } else {
                assert!(outcome.delivery_id.is_some());
                assert_eq!(
                    run.result.as_ref().unwrap().summary.as_deref(),
                    Some(summary)
                );
                let delivery = deliveries
                    .deliveries
                    .iter()
                    .find(|delivery| delivery.run_id == accepted.id)
                    .unwrap();
                assert_eq!(Some(&delivery.id), outcome.delivery_id.as_ref());
                assert_eq!(delivery.mode, TaskDeliveryMode::Thread);
                assert_eq!(
                    delivery.target_thread_id.as_deref(),
                    Some("thr_webhook_owner")
                );
                assert_eq!(
                    delivery
                        .result_snapshot
                        .as_ref()
                        .unwrap()
                        .summary
                        .as_deref(),
                    Some(summary)
                );
            }
        }
        if failures == 0 {
            assert_ne!(first_outcome.delivery_id, second_outcome.delivery_id);
        }
    }
}

#[tokio::test]
async fn webhook_terminal_does_not_adopt_retry_with_changed_source_input() {
    let runtime = runtime().await;
    let created = create(&runtime, parallel_webhook_params()).await;
    let first = post(&runtime, &created.trigger, "retry source body").await;
    let second = post(&runtime, &created.trigger, "successful sibling body").await;
    admit_webhook_for_terminal(&runtime, &first).await;
    admit_webhook_for_terminal(&runtime, &second).await;
    let store = runtime.service().store();
    let failure = store
        .prepare_task_terminal_transition(failed_webhook(&first, first.created_at + 10))
        .await
        .unwrap();
    let success = store
        .prepare_task_terminal_transition(completed_webhook(
            &second,
            second.created_at + 20,
            "sibling result",
        ))
        .await
        .unwrap();
    store
        .commit_task_terminal_transition(failure)
        .await
        .unwrap();
    // An invalid stored successor is not proof of ordinary sibling progress.
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task_run SET trigger_input_json=? WHERE retry_of_run_id=?",
            [
                serde_json::to_string(&payload("unrelated body"))
                    .unwrap()
                    .into(),
                first.id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let error = store
        .commit_task_terminal_transition(success)
        .await
        .unwrap_err();
    assert!(
        error
            .downcast_ref::<pioneer_crud::TaskTerminalConflict>()
            .is_some()
    );
    assert_eq!(
        store
            .get_task_run(&second.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskRunStatus::Running
    );
    assert!(
        runtime
            .service()
            .list_deliveries(TaskDeliveriesParams {
                workspace_id: TEST_WORKSPACE_ID.into(),
                task_id: Some(first.task_id),
                run_id: None,
                statuses: vec![],
                limit: Some(10),
            })
            .await
            .unwrap()
            .deliveries
            .is_empty()
    );
}

#[tokio::test]
async fn webhook_terminal_reconciles_started_sibling_but_rejects_unbacked_activity() {
    for progress in 0..=2 {
        let start_sibling = progress > 0;
        let runtime = runtime().await;
        let created = create(&runtime, parallel_webhook_params()).await;
        let first = post(&runtime, &created.trigger, "completing body").await;
        admit_webhook_for_terminal(&runtime, &first).await;
        let second = post(&runtime, &created.trigger, "next execution body").await;
        let before = read(&runtime, &created.task.id).await;
        assert_eq!(before.task.status, TaskStatus::Queued);
        let store = runtime.service().store();
        let prepared = store
            .prepare_task_terminal_transition(completed_webhook(
                &first,
                first.created_at + 10,
                "completed before sibling",
            ))
            .await
            .unwrap();
        if start_sibling {
            // The ordinary claim/mark_started path changes aggregate Queued to
            // Running without changing the trigger, queue watermark or A.
            admit_webhook_for_terminal(&runtime, &second).await;
            if progress == 2 {
                // Its activity event still explains the shared label if the
                // sibling has already completed by the time A commits.
                let sibling_terminal = store
                    .prepare_task_terminal_transition(completed_webhook(
                        &second,
                        second.created_at + 20,
                        "sibling completed first",
                    ))
                    .await
                    .unwrap();
                store
                    .commit_task_terminal_transition(sibling_terminal)
                    .await
                    .unwrap();
            }
        } else {
            store
                .database_connection()
                .execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    "UPDATE task SET status='running' WHERE id=?",
                    [first.task_id.clone().into()],
                ))
                .await
                .unwrap();
        }
        let outcome = store.commit_task_terminal_transition(prepared).await;
        let after = read(&runtime, &created.task.id).await;
        assert_eq!(after.triggers, before.triggers);
        assert_eq!(after.runs.len(), 2);
        assert_eq!(after.runs[0].trigger_input, first.trigger_input);
        assert_eq!(after.runs[1].trigger_input, second.trigger_input);
        assert_eq!(
            after
                .runs
                .iter()
                .map(|run| run.run_number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let deliveries = runtime
            .service()
            .list_deliveries(TaskDeliveriesParams {
                workspace_id: TEST_WORKSPACE_ID.into(),
                task_id: Some(first.task_id),
                run_id: None,
                statuses: vec![],
                limit: Some(10),
            })
            .await
            .unwrap();
        if start_sibling {
            let outcome = outcome.unwrap();
            assert_eq!(
                outcome.status,
                pioneer_crud::TaskTerminalCommitStatus::Applied
            );
            assert_eq!(
                after.task.status,
                if progress == 2 {
                    TaskStatus::Waiting
                } else {
                    TaskStatus::Running
                }
            );
            assert_eq!(after.runs[0].status, TaskRunStatus::Succeeded);
            assert_eq!(
                after.runs[1].status,
                if progress == 2 {
                    TaskRunStatus::Succeeded
                } else {
                    TaskRunStatus::Running
                }
            );
            assert_eq!(
                after.runs[0].result.as_ref().unwrap().summary.as_deref(),
                Some("completed before sibling")
            );
            assert_eq!(
                deliveries.deliveries.len(),
                if progress == 2 { 2 } else { 1 }
            );
            let delivery = deliveries
                .deliveries
                .iter()
                .find(|delivery| delivery.run_id == first.id)
                .unwrap();
            assert_eq!(delivery.mode, TaskDeliveryMode::Thread);
            assert_eq!(Some(&delivery.id), outcome.delivery_id.as_ref());
            assert_eq!(
                delivery
                    .result_snapshot
                    .as_ref()
                    .unwrap()
                    .summary
                    .as_deref(),
                Some("completed before sibling")
            );
            assert_eq!(
                delivery.target_thread_id.as_deref(),
                Some("thr_webhook_owner")
            );
        } else {
            assert!(
                outcome
                    .unwrap_err()
                    .downcast_ref::<pioneer_crud::TaskTerminalConflict>()
                    .is_some()
            );
            assert_eq!(after.runs[0].status, TaskRunStatus::Running);
            assert_eq!(after.runs[1].status, TaskRunStatus::Queued);
            assert!(deliveries.deliveries.is_empty());
        }
    }
}
