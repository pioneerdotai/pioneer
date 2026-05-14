use crate::{
    TaskCreateContext, TaskExecutionContext, TaskExecutionHandle, TaskExecutor,
    TaskExecutorRecoveryOutcome, TaskExecutorStartOutcome, TaskMutationContext, TaskRuntime,
    TaskRuntimeResult, TaskWaitContext, WriteLockDecision,
};
use async_trait::async_trait;
use migration::{Migrator, MigratorTrait};
use pioneer_crud::CrudStore;
use pioneer_protocol::{
    TaskAgentPrompt, TaskAgentSpecInput, TaskAgentToolPolicy, TaskAgentWriteMode,
    TaskAttachmentMode, TaskCancelParams, TaskConcurrencyConflictPolicy, TaskConcurrencyPolicy,
    TaskCreateParams, TaskDeliveriesParams, TaskDeliveryMode, TaskDeliveryPolicy,
    TaskDeliveryStatus, TaskDetachParams, TaskError, TaskErrorClass, TaskEventPayload,
    TaskEventsParams, TaskExecutorKind, TaskLifecyclePolicy, TaskOwnerKind,
    TaskParentTerminalAction, TaskPauseParams, TaskRescheduleParams, TaskResult, TaskResumeParams,
    TaskRetryBackoffKind, TaskRetryPolicy, TaskRun, TaskRunStatus, TaskStatus, TaskTriggerInput,
    TaskTriggerSpec, TaskTriggerStatus, TaskValue, TaskWaitMode, TaskWaitParams,
};
use sea_orm::Database;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

#[derive(Default)]
struct CompletingSystemExecutor;

#[async_trait]
impl TaskExecutor for CompletingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .complete_run(
                Some(TaskResult {
                    summary: Some(format!("completed run {}", run.run_number)),
                    data: Some(TaskValue::Integer(run.run_number)),
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }

    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
    }
}

#[derive(Default)]
struct FailingSystemExecutor;

#[async_trait]
impl TaskExecutor for FailingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .fail_run(
                Some(TaskError {
                    code: "test_failure".to_owned(),
                    message: "test failure".to_owned(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }

    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
    }
}

#[derive(Default)]
struct CancellationFailingSystemExecutor;

#[async_trait]
impl TaskExecutor for CancellationFailingSystemExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }

    async fn start_run(
        &self,
        _context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        handle.mark_started(run.created_at).await?;
        handle
            .fail_run(
                Some(TaskError {
                    code: "child_turn_cancelled".to_owned(),
                    message: "task cancelled".to_owned(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: Some(run.id.clone()),
                }),
                run.created_at,
            )
            .await?;
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        _run_id: &str,
        _reason: &str,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        Ok(())
    }
}

async fn runtime() -> TaskRuntime {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("sqlite memory database should connect");
    Migrator::up(&connection, None)
        .await
        .expect("migration should apply");
    TaskRuntime::new(Arc::new(CrudStore::new(connection)))
}

fn create_params(spec: TaskTriggerSpec) -> TaskCreateParams {
    TaskCreateParams {
        workspace_id: "ws_tasks".to_owned(),
        owner_kind: TaskOwnerKind::Workspace,
        owner_id: Some("ws_tasks".to_owned()),
        created_by_thread_id: None,
        created_by_turn_id: None,
        parent_task_id: None,
        executor_kind: TaskExecutorKind::System,
        title: "Task".to_owned(),
        goal: "Do the task".to_owned(),
        priority: 0,
        trigger: TaskTriggerInput { spec },
        agent_spec: None,
        lifecycle_policy: None,
        delivery_policy: None,
        retry_policy: None,
        timeout_policy: None,
        concurrency_policy: None,
        metadata: None,
    }
}

fn agent_spec(max_depth: i64) -> TaskAgentSpecInput {
    TaskAgentSpecInput {
        agent_role: None,
        agent_nickname: None,
        model: Some("test-model".to_owned()),
        model_provider: Some("openai".to_owned()),
        prompt: TaskAgentPrompt {
            goal: "Do agent work".to_owned(),
            instructions: Vec::new(),
            input: None,
            output_instructions: None,
        },
        context_policy: None,
        tool_policy: None,
        result_contract: None,
        depth: 0,
        max_depth,
    }
}

#[tokio::test]
async fn immediate_create_uses_scheduler_and_creates_one_queued_run() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");

    assert_eq!(response.task.status, TaskStatus::Queued);
    let run = response.run.expect("immediate task should have a run");
    assert_eq!(run.status, TaskRunStatus::Queued);

    let created_again = runtime
        .process_due_once(i64::MAX / 4)
        .await
        .expect("scheduler should be idempotent");
    assert_eq!(created_again, 0);
}

#[tokio::test]
async fn duplicate_scheduler_wakeups_do_not_create_duplicate_one_shot_runs() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 10,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");

    let (left, right) = tokio::join!(runtime.process_due_once(10), runtime.process_due_once(10));
    let created = left.expect("left scheduler pass should succeed")
        + right.expect("right scheduler pass should succeed");
    assert_eq!(created, 1);

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
}

#[tokio::test]
async fn scheduled_create_does_not_fire_before_due_time() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");

    assert_eq!(response.task.status, TaskStatus::Scheduled);
    assert!(response.run.is_none());
    let created = runtime
        .process_due_once(3_999_999_999)
        .await
        .expect("scheduler should process");
    assert_eq!(created, 0);
}

#[tokio::test]
async fn scheduled_trigger_fires_when_due() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 10,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");

    let created = runtime
        .process_due_once(10)
        .await
        .expect("scheduler should fire due trigger");
    assert_eq!(created, 1);
    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
}

#[tokio::test]
async fn interval_trigger_recomputes_next_fire_at() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 10,
                interval_anchor_at: Some(4_000_000_000),
            }),
        )
        .await
        .expect("interval task should create");

    let created = runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("scheduler should fire interval trigger");
    assert_eq!(created, 1);
    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
    assert!(
        task.triggers[0]
            .next_fire_at
            .is_some_and(|value| value > 4_000_000_000)
    );
}

#[tokio::test]
async fn interval_task_fires_repeatedly_after_terminal_runs_and_stays_active() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 10,
                interval_anchor_at: Some(4_000_000_000),
            }),
        )
        .await
        .expect("interval task should create");

    assert_eq!(
        runtime
            .process_due_once(4_000_000_000)
            .await
            .expect("first interval fire should succeed"),
        1
    );
    assert_eq!(
        runtime
            .process_due_once(4_000_000_010)
            .await
            .expect("second interval fire should succeed"),
        1
    );

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 2);
    assert_eq!(task.task.status, TaskStatus::Scheduled);
    assert_eq!(task.triggers[0].status, TaskTriggerStatus::Active);
    assert!(
        task.triggers[0]
            .next_fire_at
            .is_some_and(|next| next > 4_000_000_010)
    );
}

#[tokio::test]
async fn recurring_due_trigger_with_active_serial_run_skips_fire_and_moves_next_fire_forward() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(4_000_000_000),
    });
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let response = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect("interval task should create");

    assert_eq!(
        runtime
            .process_due_once(4_000_000_000)
            .await
            .expect("first interval fire should create one run"),
        1
    );
    let after_first = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after first fire");
    assert_eq!(after_first.runs.len(), 1);
    assert_eq!(after_first.task.status, TaskStatus::Queued);
    assert_eq!(after_first.triggers[0].next_fire_at, Some(4_000_000_010));

    assert_eq!(
        runtime
            .process_due_once(4_000_000_010)
            .await
            .expect("overlapped interval fire should be skipped"),
        0
    );
    let after_overlap = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after overlap skip");
    assert_eq!(after_overlap.runs.len(), 1);
    assert_eq!(after_overlap.task.status, TaskStatus::Queued);
    assert_eq!(after_overlap.triggers[0].status, TaskTriggerStatus::Active);
    assert_eq!(after_overlap.triggers[0].next_fire_at, Some(4_000_000_020));

    assert_eq!(
        runtime
            .process_due_once(4_000_000_010)
            .await
            .expect("repeated scheduler pass before next fire should stay idle"),
        0
    );
    let after_repeat = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read after repeated pass");
    assert_eq!(after_repeat.runs.len(), 1);
    assert_eq!(after_repeat.triggers[0].next_fire_at, Some(4_000_000_020));
}

#[tokio::test]
async fn failed_run_schedules_retry_and_defers_terminal_delivery_until_exhausted() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(FailingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Immediate);
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_retry_owner".to_owned());
    params.created_by_thread_id = Some("thr_retry_owner".to_owned());
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 2,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(5),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::OwnerThread,
        thread_id: None,
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::Summary,
    });

    let response = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect("retry task should create");

    let after_first = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after first failure");
    assert_eq!(after_first.task.status, TaskStatus::Queued);
    assert_eq!(after_first.runs.len(), 2);
    let first_run = after_first
        .runs
        .iter()
        .find(|run| run.attempt_number == 1)
        .expect("first run should exist");
    let retry_run = after_first
        .runs
        .iter()
        .find(|run| run.attempt_number == 2)
        .expect("retry run should exist");
    assert_eq!(retry_run.status, TaskRunStatus::Queued);
    assert_eq!(
        retry_run.retry_of_run_id.as_deref(),
        Some(first_run.id.as_str())
    );
    let retry_ready_at = retry_run.ready_at.expect("retry should have ready time");
    assert_eq!(retry_ready_at, first_run.created_at.saturating_add(5));

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id.clone()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read after first failure");
    assert!(deliveries.deliveries.is_empty());

    assert_eq!(
        runtime
            .process_due_once(retry_ready_at)
            .await
            .expect("retry attempt should dispatch"),
        1
    );
    let after_second = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read after retry exhaustion");
    assert_eq!(after_second.task.status, TaskStatus::Failed);
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id.clone(),
            after_sequence: None,
        })
        .await
        .expect("task events should read");
    assert!(
        events
            .events
            .iter()
            .any(|event| matches!(event.payload, TaskEventPayload::RunRetryExhausted { .. }))
    );

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read after exhaustion");
    assert_eq!(deliveries.deliveries.len(), 1);
}

#[tokio::test]
async fn write_locks_block_conflicting_agent_runs_and_recover_release_terminal_locks() {
    let runtime = runtime().await;

    let mut first = create_params(TaskTriggerSpec::Immediate);
    first.executor_kind = TaskExecutorKind::Agent;
    let mut first_spec = agent_spec(3);
    first_spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
        write_mode: TaskAgentWriteMode::ScopedWrite,
        allowed_paths: vec!["src".to_owned()],
        network_access: false,
    });
    first.agent_spec = Some(first_spec);
    first.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let first = runtime
        .service()
        .create_task(TaskCreateContext::default(), first)
        .await
        .expect("first agent task should create");
    let first_run_id = first.run.expect("first run should exist").id;

    let mut second = create_params(TaskTriggerSpec::Immediate);
    second.executor_kind = TaskExecutorKind::Agent;
    let mut second_spec = agent_spec(3);
    second_spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
        write_mode: TaskAgentWriteMode::ScopedWrite,
        allowed_paths: vec!["src/lib.rs".to_owned()],
        network_access: false,
    });
    second.agent_spec = Some(second_spec);
    second.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs: 1,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    let second = runtime
        .service()
        .create_task(TaskCreateContext::default(), second)
        .await
        .expect("second agent task should create");
    let second_run_id = second.run.expect("second run should exist").id;

    let first_decision = runtime
        .service()
        .acquire_write_locks_for_run(first_run_id.as_str(), 100)
        .await
        .expect("first run should acquire lock");
    assert!(matches!(first_decision, WriteLockDecision::Acquired(_)));

    let second_decision = runtime
        .service()
        .acquire_write_locks_for_run(second_run_id.as_str(), 101)
        .await
        .expect("second run should queue on conflict");
    assert!(matches!(second_decision, WriteLockDecision::Queued));

    let second_events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: second.task.id.clone(),
            after_sequence: None,
        })
        .await
        .expect("second task events should read");
    assert!(second_events.events.iter().any(|event| {
        matches!(
            event.payload,
            TaskEventPayload::WriteLockBlocked { ref conflicts, .. } if conflicts.len() == 1
        )
    }));

    let completed = runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id: first.task.id.clone(),
                run_id: first_run_id.clone(),
                result: None,
                completed_at: 200,
            },
            200,
        )
        .await
        .expect("first run completion should append");
    runtime.service().publish_and_wake(vec![completed]).await;
    runtime
        .service()
        .recover_retry_and_lock_state(201)
        .await
        .expect("lock recovery should release terminal run lock");

    let second_decision = runtime
        .service()
        .acquire_write_locks_for_run(second_run_id.as_str(), 202)
        .await
        .expect("second run should acquire after release");
    assert!(matches!(second_decision, WriteLockDecision::Acquired(_)));
}

#[tokio::test]
async fn pause_excludes_due_trigger_and_resume_restores_future_fire() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");

    let paused = runtime
        .service()
        .pause_task(
            TaskMutationContext::default(),
            TaskPauseParams {
                task_id: response.task.id.clone(),
                reason: Some("hold".to_owned()),
            },
        )
        .await
        .expect("pause should succeed");
    assert_eq!(paused.triggers[0].status, TaskTriggerStatus::Paused);
    assert_eq!(
        runtime
            .process_due_once(4_000_000_000)
            .await
            .expect("paused due scan should succeed"),
        0
    );

    let resumed = runtime
        .service()
        .resume_task(
            TaskMutationContext::default(),
            TaskResumeParams {
                task_id: response.task.id.clone(),
                reason: Some("continue".to_owned()),
            },
        )
        .await
        .expect("resume should succeed");
    assert_eq!(resumed.triggers[0].status, TaskTriggerStatus::Active);
    assert_eq!(resumed.triggers[0].next_fire_at, Some(4_000_000_000));
}

#[tokio::test]
async fn cron_trigger_can_fire_repeatedly_with_timezone() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Cron {
                cron_expr: "0 9 * * *".to_owned(),
                timezone: "UTC".to_owned(),
            }),
        )
        .await
        .expect("cron task should create");
    let first_fire = response
        .trigger
        .next_fire_at
        .expect("cron should have initial fire");
    assert_eq!(
        runtime
            .process_due_once(first_fire)
            .await
            .expect("first cron fire should succeed"),
        1
    );
    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    let second_fire = task.triggers[0]
        .next_fire_at
        .expect("cron should advance after terminal run");
    assert!(second_fire > first_fire);
    assert_eq!(
        runtime
            .process_due_once(second_fire)
            .await
            .expect("second cron fire should succeed"),
        1
    );
}

#[tokio::test]
async fn terminal_run_enqueues_owner_thread_delivery_from_normalized_result() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
    });
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_owner".to_owned());
    params.created_by_thread_id = Some("thr_owner".to_owned());

    let response = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect("scheduled task should create");
    runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("scheduled task should fire");

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    assert_eq!(deliveries.deliveries.len(), 1);
    let delivery = &deliveries.deliveries[0];
    assert_eq!(delivery.mode, TaskDeliveryMode::OwnerThread);
    assert_eq!(delivery.status, TaskDeliveryStatus::Pending);
    assert_eq!(delivery.target_thread_id.as_deref(), Some("thr_owner"));
    assert_eq!(
        delivery
            .result_snapshot
            .as_ref()
            .and_then(|result| result.summary.as_deref()),
        Some("completed run 1")
    );
}

#[tokio::test]
async fn cancel_task_cancels_pending_deliveries() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(4_000_000_000),
    });
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_owner".to_owned());
    params.created_by_thread_id = Some("thr_owner".to_owned());
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::OwnerThread,
        thread_id: None,
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::Summary,
    });
    let response = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect("interval task should create");
    runtime
        .process_due_once(4_000_000_000)
        .await
        .expect("interval task should fire");

    runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("stop scheduled work".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("cancel should succeed");

    let deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: "ws_tasks".to_owned(),
            task_id: Some(response.task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .expect("deliveries should read");
    assert_eq!(
        deliveries.deliveries[0].status,
        TaskDeliveryStatus::Cancelled
    );
}

#[tokio::test]
async fn invalid_cron_timezone_is_rejected() {
    let runtime = runtime().await;
    let error = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Cron {
                cron_expr: "0 8 * * *".to_owned(),
                timezone: "Not/AZone".to_owned(),
            }),
        )
        .await
        .expect_err("invalid timezone should fail");
    assert!(format!("{error:#}").contains("invalid timezone"));
}

#[tokio::test]
async fn invalid_interval_is_rejected() {
    let runtime = runtime().await;
    let error = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 0,
                interval_anchor_at: None,
            }),
        )
        .await
        .expect_err("invalid interval should fail");
    assert!(format!("{error:#}").contains("interval_seconds must be positive"));
}

#[tokio::test]
async fn cron_trigger_computes_next_fire_in_timezone() {
    let next = crate::TaskTriggerCalculator::initial_next_fire_at(
        &TaskTriggerSpec::Cron {
            cron_expr: "0 9 * * *".to_owned(),
            timezone: "UTC".to_owned(),
        },
        1_700_000_000,
    )
    .expect("cron should compute")
    .expect("cron should have next fire");
    assert!(next > 1_700_000_000);
}

#[tokio::test]
async fn cron_trigger_computes_moscow_morning_fire_in_utc() {
    let next = crate::TaskTriggerCalculator::initial_next_fire_at(
        &TaskTriggerSpec::Cron {
            cron_expr: "0 7 * * *".to_owned(),
            timezone: "Europe/Moscow".to_owned(),
        },
        1_778_752_618,
    )
    .expect("cron should compute")
    .expect("cron should have next fire");
    assert_eq!(next, 1_778_817_600);
}

#[tokio::test]
async fn scheduled_agent_task_requires_self_contained_prompt_contract() {
    let runtime = runtime().await;

    let mut missing_prompt = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
    });
    missing_prompt.executor_kind = TaskExecutorKind::Agent;
    missing_prompt.agent_spec = Some(agent_spec(3));
    let error = runtime
        .service()
        .create_task(TaskCreateContext::default(), missing_prompt)
        .await
        .expect_err("scheduled agent task should reject empty prompt");
    assert!(format!("{error:#}").contains("self-contained executor instructions"));

    let mut missing_output = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
    });
    missing_output.executor_kind = TaskExecutorKind::Agent;
    let mut spec = agent_spec(3);
    spec.prompt.instructions = vec![
        "Use currently available runtime capabilities by capability, not stale tool names."
            .to_owned(),
        "If required data is unavailable, report a clear failure.".to_owned(),
    ];
    missing_output.agent_spec = Some(spec);
    let error = runtime
        .service()
        .create_task(TaskCreateContext::default(), missing_output)
        .await
        .expect_err("scheduled agent task should reject missing output contract");
    assert!(format!("{error:#}").contains("output instructions"));

    let mut valid = create_params(TaskTriggerSpec::Cron {
        cron_expr: "0 7 * * *".to_owned(),
        timezone: "Europe/Moscow".to_owned(),
    });
    valid.executor_kind = TaskExecutorKind::Agent;
    let mut spec = agent_spec(3);
    spec.prompt.instructions = vec![
        "Use currently available runtime capabilities by capability, not stale tool names."
            .to_owned(),
        "If required data is unavailable, report a clear failure.".to_owned(),
    ];
    spec.prompt.output_instructions =
        Some("Return concise markdown with result fields or explicit failure reason.".to_owned());
    valid.agent_spec = Some(spec);
    runtime
        .service()
        .create_task(TaskCreateContext::default(), valid)
        .await
        .expect("scheduled agent task should accept a durable prompt contract");
}

#[tokio::test]
async fn lifecycle_defaults_attach_only_immediate_parent_turn_tasks() {
    let runtime = runtime().await;
    let mut immediate = create_params(TaskTriggerSpec::Immediate);
    immediate.created_by_thread_id = Some("thread_a".to_owned());
    immediate.created_by_turn_id = Some("turn_a".to_owned());
    let immediate = runtime
        .service()
        .create_task(TaskCreateContext::default(), immediate)
        .await
        .expect("immediate task should create");
    assert_eq!(
        immediate.task.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Attached
    );

    let scheduled = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");
    assert_eq!(
        scheduled.task.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Detached
    );
}

#[tokio::test]
async fn max_depth_is_enforced_before_child_events_are_appended() {
    let runtime = runtime().await;
    let mut root = create_params(TaskTriggerSpec::Manual {
        allowed_actor: None,
    });
    root.executor_kind = TaskExecutorKind::Agent;
    root.agent_spec = Some(agent_spec(1));
    let root = runtime
        .service()
        .create_task(TaskCreateContext::default(), root)
        .await
        .expect("root task should create");

    let mut child = create_params(TaskTriggerSpec::Manual {
        allowed_actor: None,
    });
    child.executor_kind = TaskExecutorKind::Agent;
    child.parent_task_id = Some(root.task.id.clone());
    child.agent_spec = Some(agent_spec(1));
    let error = runtime
        .service()
        .create_task(TaskCreateContext::default(), child)
        .await
        .expect_err("child beyond max depth should fail");
    assert!(format!("{error:#}").contains("exceeds max depth"));

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: root.task.id,
            after_sequence: None,
        })
        .await
        .expect("events should read");
    assert!(
        events
            .events
            .iter()
            .all(|event| !matches!(event.payload, TaskEventPayload::DepthLimitExceeded { .. }))
    );
}

#[tokio::test]
async fn reschedule_updates_trigger_and_wakes_scheduler() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");

    let rescheduled = runtime
        .service()
        .reschedule_task(
            TaskMutationContext::default(),
            TaskRescheduleParams {
                task_id: response.task.id.clone(),
                trigger: TaskTriggerInput {
                    spec: TaskTriggerSpec::ScheduledAt {
                        scheduled_at: 10,
                        timezone: Some("UTC".to_owned()),
                    },
                },
            },
        )
        .await
        .expect("task should reschedule");
    assert_eq!(rescheduled.trigger.next_fire_at, Some(10));

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert_eq!(task.runs.len(), 1);
}

#[tokio::test]
async fn cancel_task_is_idempotent_and_cancels_trigger_and_runs() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run_id = response.run.expect("run should exist").id;

    let first = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("stop".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("cancel should succeed");
    let second = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: response.task.id.clone(),
                reason: Some("stop again".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("second cancel should be idempotent");
    assert_eq!(first.task.status, TaskStatus::Cancelled);
    assert_eq!(second.task.status, TaskStatus::Cancelled);

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id,
        })
        .await
        .expect("task should read");
    assert!(task.triggers.iter().all(|trigger| {
        matches!(
            trigger.status,
            TaskTriggerStatus::Cancelled | TaskTriggerStatus::Exhausted
        )
    }));
    assert_eq!(
        task.runs
            .iter()
            .find(|run| run.id == run_id)
            .expect("run should exist")
            .status,
        TaskRunStatus::Cancelled
    );
}

#[tokio::test]
async fn cancel_attached_subtree_cancels_detaches_and_keeps_by_policy() {
    let runtime = runtime().await;
    let root = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("root task should create");

    let mut attached_cancel = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
    });
    attached_cancel.parent_task_id = Some(root.task.id.clone());
    attached_cancel.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let attached_cancel = runtime
        .service()
        .create_task(TaskCreateContext::default(), attached_cancel)
        .await
        .expect("attached cancel child should create");

    let mut attached_detach = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
    });
    attached_detach.parent_task_id = Some(root.task.id.clone());
    attached_detach.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Detach,
        on_parent_failure: TaskParentTerminalAction::Detach,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let attached_detach = runtime
        .service()
        .create_task(TaskCreateContext::default(), attached_detach)
        .await
        .expect("attached detach child should create");

    let mut detached_keep = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
    });
    detached_keep.parent_task_id = Some(root.task.id.clone());
    detached_keep.lifecycle_policy = Some(TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Detached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let detached_keep = runtime
        .service()
        .create_task(TaskCreateContext::default(), detached_keep)
        .await
        .expect("detached child should create");

    let cancelled = runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: root.task.id.clone(),
                reason: Some("parent stopped".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("subtree cancel should succeed");

    assert!(
        cancelled
            .cancelled_tasks
            .iter()
            .any(|task| task.id.as_str() == root.task.id.as_str())
    );
    assert!(
        cancelled
            .cancelled_tasks
            .iter()
            .any(|task| task.id.as_str() == attached_cancel.task.id.as_str())
    );
    assert!(
        cancelled
            .detached_tasks
            .iter()
            .any(|task| task.id.as_str() == attached_detach.task.id.as_str())
    );
    assert!(
        cancelled
            .kept_tasks
            .iter()
            .any(|task| task.id.as_str() == detached_keep.task.id.as_str())
    );

    let attached_cancel_state = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: attached_cancel.task.id,
        })
        .await
        .expect("cancel child should read")
        .task;
    let attached_detach_state = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: attached_detach.task.id,
        })
        .await
        .expect("detach child should read")
        .task;
    let detached_keep_state = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: detached_keep.task.id,
        })
        .await
        .expect("kept child should read")
        .task;

    assert_eq!(attached_cancel_state.status, TaskStatus::Cancelled);
    assert_eq!(attached_detach_state.status, TaskStatus::Scheduled);
    assert_eq!(
        attached_detach_state.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Detached
    );
    assert_eq!(detached_keep_state.status, TaskStatus::Scheduled);
}

#[tokio::test]
async fn detach_task_updates_attachment_without_cancelling() {
    let runtime = runtime().await;
    let mut params = create_params(TaskTriggerSpec::ScheduledAt {
        scheduled_at: 4_000_000_000,
        timezone: Some("UTC".to_owned()),
    });
    params.lifecycle_policy = Some(pioneer_protocol::TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: pioneer_protocol::TaskParentTerminalAction::Cancel,
        on_parent_failure: pioneer_protocol::TaskParentTerminalAction::Cancel,
        completion: pioneer_protocol::TaskCompletionBehavior::CompleteOnTerminalRun,
    });
    let response = runtime
        .service()
        .create_task(TaskCreateContext::default(), params)
        .await
        .expect("task should create");
    let detached = runtime
        .service()
        .detach_task(
            TaskMutationContext::default(),
            TaskDetachParams {
                task_id: response.task.id,
            },
        )
        .await
        .expect("detach should succeed");

    assert_eq!(detached.task.status, TaskStatus::Scheduled);
    assert_eq!(
        detached.task.lifecycle_policy.unwrap().attachment,
        TaskAttachmentMode::Detached
    );
}

#[tokio::test]
async fn wait_wakes_from_event_bus_on_terminal_event() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("scheduled task should create");

    let service = runtime.service();
    let task_id = response.task.id.clone();
    let waiter = tokio::spawn({
        let service = service.clone();
        let task_id = task_id.clone();
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids: vec![task_id],
                        run_ids: Vec::new(),
                        timeout_ms: Some(5_000),
                        return_completed: true,
                        return_pending: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("wait should complete")
        }
    });

    service
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id,
                reason: Some("test cancellation".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("task should cancel");

    let waited = waiter.await.expect("waiter should join");
    assert_eq!(waited.cancelled.len(), 1);
    assert!(!waited.timed_out);
}

#[tokio::test]
async fn wait_classifies_terminal_run_even_before_task_terminal_event() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let run_id = response.run.expect("run should exist").id;

    let waiter = tokio::spawn({
        let service = runtime.service();
        let run_id = run_id.clone();
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids: Vec::new(),
                        run_ids: vec![run_id],
                        timeout_ms: Some(5_000),
                        return_completed: true,
                        return_pending: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("wait should complete")
        }
    });

    let appended = runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id: response.task.id,
                run_id,
                result: None,
                completed_at: 42,
            },
            42,
        )
        .await
        .expect("run completion event should append");
    runtime.service().publish_and_wake(vec![appended]).await;

    let waited = waiter.await.expect("waiter should join");
    assert_eq!(waited.completed.len(), 1);
    assert!(waited.pending.is_empty());
}

#[tokio::test]
async fn wait_timeout_returns_partial_pending_state() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("task should create");

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![response.task.id],
                run_ids: Vec::new(),
                timeout_ms: Some(5),
                return_completed: true,
                return_pending: true,
                ..Default::default()
            },
        )
        .await
        .expect("wait should return timeout");
    assert!(waited.timed_out);
    assert_eq!(waited.pending.len(), 1);
}

#[tokio::test]
async fn wait_return_pending_false_still_waits_on_internal_pending_state() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("task should create");

    let waited = runtime
        .service()
        .wait_tasks(
            TaskWaitContext::default(),
            TaskWaitParams {
                task_ids: vec![response.task.id],
                run_ids: Vec::new(),
                timeout_ms: Some(5),
                return_completed: true,
                return_pending: false,
                ..Default::default()
            },
        )
        .await
        .expect("wait should return timeout");
    assert!(waited.timed_out);
    assert_eq!(waited.total_count, 1);
    assert_eq!(waited.pending_count, 1);
    assert!(waited.pending.is_empty());
}

#[tokio::test]
async fn wait_any_terminal_returns_after_first_target_finishes() {
    let runtime = runtime().await;
    let first = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("first task should create");
    let second = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: Some("UTC".to_owned()),
            }),
        )
        .await
        .expect("second task should create");

    let waiter = tokio::spawn({
        let service = runtime.service();
        let task_ids = vec![first.task.id.clone(), second.task.id.clone()];
        async move {
            service
                .wait_tasks(
                    TaskWaitContext::default(),
                    TaskWaitParams {
                        task_ids,
                        run_ids: Vec::new(),
                        timeout_ms: Some(5_000),
                        mode: TaskWaitMode::AnyTerminal,
                        return_completed: true,
                        return_pending: true,
                    },
                )
                .await
                .expect("wait should complete")
        }
    });

    runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: first.task.id,
                reason: Some("test cancellation".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .expect("first task should cancel");

    let waited = waiter.await.expect("waiter should join");
    assert!(!waited.timed_out);
    assert_eq!(waited.total_count, 2);
    assert_eq!(waited.terminal_count, 1);
    assert_eq!(waited.pending_count, 1);
    assert_eq!(waited.cancelled.len(), 1);
}

#[tokio::test]
async fn cancellation_class_failure_projects_as_cancelled_without_task_failed() {
    let runtime = runtime().await;
    runtime
        .register_executor(Arc::new(CancellationFailingSystemExecutor))
        .await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");

    runtime
        .process_due_once(i64::MAX / 4)
        .await
        .expect("scheduler should run");

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: response.task.id.clone(),
        })
        .await
        .expect("task should read");
    assert_eq!(task.task.status, TaskStatus::Cancelled);
    assert_eq!(task.runs.last().unwrap().status, TaskRunStatus::Cancelled);

    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
        })
        .await
        .expect("events should read");
    assert!(
        events
            .events
            .iter()
            .all(|event| event.event_type != pioneer_protocol::constants::events::TASK_FAILED)
    );
}

#[tokio::test]
async fn projector_does_not_regress_cancelled_task_after_late_failure_events() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;

    for event in [
        TaskEventPayload::RunCancelled {
            task_id: task_id.clone(),
            run_id: run_id.clone(),
            reason: Some("parent cancelled".to_owned()),
            cancelled_at: 10,
        },
        TaskEventPayload::TaskCancelled {
            task_id: task_id.clone(),
            reason: Some("parent cancelled".to_owned()),
            completed_at: 10,
        },
        TaskEventPayload::RunFailed {
            task_id: task_id.clone(),
            run_id: run_id.clone(),
            error: Some(TaskError {
                code: "child_turn_cancelled".to_owned(),
                message: "task cancelled".to_owned(),
                class: TaskErrorClass::Cancelled,
                details: None,
                failed_run_id: Some(run_id.clone()),
            }),
            completed_at: 11,
        },
        TaskEventPayload::TaskFailed {
            task_id: task_id.clone(),
            error: Some(TaskError {
                code: "child_turn_cancelled".to_owned(),
                message: "task cancelled".to_owned(),
                class: TaskErrorClass::Cancelled,
                details: None,
                failed_run_id: Some(run_id),
            }),
            completed_at: 11,
        },
    ] {
        runtime
            .service()
            .append_event(event, 11)
            .await
            .expect("event should append");
    }

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should read");
    assert_eq!(task.task.status, TaskStatus::Cancelled);
    assert_eq!(task.runs.last().unwrap().status, TaskRunStatus::Cancelled);
}

#[tokio::test]
async fn projector_replay_run_started_after_terminal_run_is_noop() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;

    runtime
        .service()
        .append_event(
            TaskEventPayload::RunCompleted {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                result: Some(TaskResult {
                    summary: Some("done".to_owned()),
                    data: None,
                    artifacts: Vec::new(),
                    completed_by_run_id: Some(run_id.clone()),
                }),
                completed_at: 10,
            },
            10,
        )
        .await
        .expect("run completed should append");
    runtime
        .service()
        .append_event(
            TaskEventPayload::RunStarted {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                started_at: 11,
            },
            11,
        )
        .await
        .expect("late run started should append");
    runtime
        .service()
        .append_event(
            TaskEventPayload::RunFailed {
                task_id: task_id.clone(),
                run_id: run_id.clone(),
                error: Some(TaskError {
                    code: "late_failure".to_owned(),
                    message: "late failure".to_owned(),
                    class: TaskErrorClass::Internal,
                    details: None,
                    failed_run_id: Some(run_id.clone()),
                }),
                completed_at: 12,
            },
            12,
        )
        .await
        .expect("late run failed should append");

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should read");
    assert_eq!(task.runs.last().unwrap().status, TaskRunStatus::Succeeded);
    assert_eq!(task.task.status, TaskStatus::Queued);
}

#[tokio::test]
async fn projector_replay_task_queued_after_task_cancelled_is_noop() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .expect("task should create");
    let task_id = response.task.id.clone();
    let run_id = response.run.expect("run should exist").id;

    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskCancelled {
                task_id: task_id.clone(),
                reason: Some("stop".to_owned()),
                completed_at: 20,
            },
            20,
        )
        .await
        .expect("task cancelled should append");
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskQueued {
                task_id: task_id.clone(),
                run_id: Some(run_id),
            },
            21,
        )
        .await
        .expect("late queued should append");

    let task = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id })
        .await
        .expect("task should read");
    assert_eq!(task.task.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn event_bus_filters_committed_events_by_workspace_and_root() {
    let runtime = runtime().await;
    let mut subscription = runtime.event_bus().subscribe(crate::TaskEventFilter {
        workspace_id: Some("ws_tasks".to_owned()),
        task_ids: Vec::new(),
        run_ids: Vec::new(),
        root_task_id: None,
        parent_task_id: None,
    });
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("task should create");

    let event = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("subscription should receive")
        .expect("event bus should be open");
    assert_eq!(event.workspace_id.as_deref(), Some("ws_tasks"));
    assert_eq!(event.task_id, response.task.id);
}

#[tokio::test]
async fn startup_reconciliation_emits_recovered_event_for_dormant_active_trigger() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("manual task should create");

    runtime.start().await.expect("runtime should start");
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
        })
        .await
        .expect("events should read");

    assert!(events.events.iter().any(|event| {
        matches!(
            event.payload,
            pioneer_protocol::TaskEventPayload::TaskRecovered { .. }
        )
    }));
}

#[tokio::test]
async fn startup_reconciliation_is_idempotent() {
    let runtime = runtime().await;
    let response = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .expect("manual task should create");

    runtime.start().await.expect("runtime should start");
    runtime.start().await.expect("runtime should start twice");
    let events = runtime
        .service()
        .get_task_events(TaskEventsParams {
            task_id: response.task.id,
            after_sequence: None,
        })
        .await
        .expect("events should read");

    let recovered_count = events
        .events
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                pioneer_protocol::TaskEventPayload::TaskRecovered { .. }
            )
        })
        .count();
    assert_eq!(recovered_count, 1);
}
