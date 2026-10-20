use super::*;

const FIRST: i64 = 4_000_000_000;

fn interval(policy: TaskTriggerCatchUpPolicy) -> TaskTriggerSpec {
    TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(FIRST),
        catch_up_policy: Some(policy),
    }
}

fn cron(policy: TaskTriggerCatchUpPolicy) -> TaskTriggerSpec {
    TaskTriggerSpec::Cron {
        cron_expr: "* * * * *".to_owned(),
        timezone: "UTC".to_owned(),
        catch_up_policy: Some(policy),
    }
}

async fn task(runtime: &TaskRuntime, task_id: &str) -> pioneer_protocol::TaskGetResponse {
    runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams {
            task_id: task_id.to_owned(),
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn skip_missed_live_interval_cron_and_one_shot_create_and_dispatch_once() {
    let skip = TaskTriggerCatchUpPolicy::skip_missed();
    for spec in [
        interval(skip),
        cron(skip),
        TaskTriggerSpec::ScheduledAt {
            scheduled_at: FIRST,
            timezone: None,
            catch_up_policy: Some(skip),
        },
    ] {
        for delay in [0, 2] {
            let runtime = runtime().await;
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            let created = runtime
                .service()
                .create_task(TaskCreateContext::default(), create_params(spec.clone()))
                .await
                .unwrap();
            let first = created.trigger.next_fire_at.unwrap();
            assert_eq!(runtime.process_due_once(first - 1).await.unwrap(), 0);
            assert_eq!(runtime.process_due_once(first + delay).await.unwrap(), 1);
            let saved = task(&runtime, &created.task.id).await;
            assert_eq!(saved.runs.len(), 1);
            assert_eq!(saved.runs[0].status, TaskRunStatus::Succeeded);
            assert_eq!(saved.triggers[0].last_fire_at, Some(first));
            assert_eq!(runtime.process_due_once(first + delay).await.unwrap(), 0);
            assert_eq!(task(&runtime, &created.task.id).await.runs, saved.runs);
            let events = runtime
                .service()
                .get_task_events(TaskEventsParams {
                    task_id: created.task.id,
                    after_sequence: None,
                    limit: None,
                })
                .await
                .unwrap();
            assert_eq!(
                events
                    .events
                    .iter()
                    .filter(|event| matches!(event.payload, TaskEventPayload::RunCreated { .. }))
                    .count(),
                1
            );
            assert_eq!(
                events
                    .events
                    .iter()
                    .filter(|event| matches!(event.payload, TaskEventPayload::RunStarting { .. }))
                    .count(),
                1
            );
            assert!(events.events.iter().any(|event| matches!(&event.payload, TaskEventPayload::TaskRescheduled { reason: TaskRescheduleReason::TriggerFired, trigger, .. } if trigger.last_fire_at == Some(first))));
        }
    }
}

#[tokio::test]
async fn skip_missed_keeps_firing_on_successive_live_ticks() {
    let skip = TaskTriggerCatchUpPolicy::skip_missed();
    for (spec, period) in [(interval(skip), 10), (cron(skip), 60)] {
        let runtime = runtime().await;
        runtime
            .register_executor(Arc::new(CompletingSystemExecutor))
            .await;
        let created = runtime
            .service()
            .create_task(TaskCreateContext::default(), create_params(spec))
            .await
            .unwrap();
        let first = created.trigger.next_fire_at.unwrap();
        assert_eq!(runtime.process_due_once(first - 1).await.unwrap(), 0);
        for index in 0..3 {
            let fire_at = first + index * period;
            assert_eq!(runtime.process_due_once(fire_at + 2).await.unwrap(), 1);
            assert_eq!(runtime.process_due_once(fire_at + 2).await.unwrap(), 0);
            let saved = task(&runtime, &created.task.id).await;
            assert_eq!(saved.runs.len(), (index + 1) as usize);
            assert!(
                saved
                    .runs
                    .iter()
                    .all(|run| run.status == TaskRunStatus::Succeeded)
            );
            assert_eq!(saved.triggers[0].last_fire_at, Some(fire_at));
            assert_eq!(saved.triggers[0].next_fire_at, Some(fire_at + period));
            assert_eq!(saved.triggers[0].status, TaskTriggerStatus::Active);
        }
    }
}

#[tokio::test]
async fn skip_missed_restart_discards_backlog_but_arms_the_next_occurrence() {
    let skip = TaskTriggerCatchUpPolicy::skip_missed();
    for (spec, period) in [(interval(skip), 10), (cron(skip), 60)] {
        let original = runtime().await;
        let created = original
            .service()
            .create_task(TaskCreateContext::default(), create_params(spec))
            .await
            .unwrap();
        let first = created.trigger.next_fire_at.unwrap();
        assert_eq!(original.process_due_once(first - 1).await.unwrap(), 0);
        let runtime = TaskRuntime::new(original.service().store());
        drop(original);
        runtime
            .register_executor(Arc::new(CompletingSystemExecutor))
            .await;
        let latest = first + 3 * period;
        assert_eq!(runtime.process_due_once(latest + 2).await.unwrap(), 0);
        let skipped = task(&runtime, &created.task.id).await;
        assert!(skipped.runs.is_empty());
        assert_eq!(skipped.triggers[0].last_fire_at, Some(latest));
        assert_eq!(skipped.triggers[0].next_fire_at, Some(latest + period));
        assert_eq!(runtime.process_due_once(latest + 2).await.unwrap(), 0);
        assert_eq!(
            runtime.process_due_once(latest + period + 2).await.unwrap(),
            1
        );
        assert_eq!(
            runtime.process_due_once(latest + period + 2).await.unwrap(),
            0
        );
        let saved = task(&runtime, &created.task.id).await;
        assert_eq!(saved.runs.len(), 1);
        assert_eq!(saved.runs[0].status, TaskRunStatus::Succeeded);
        assert_eq!(saved.triggers[0].last_fire_at, Some(latest + period));
        assert_eq!(saved.triggers[0].next_fire_at, Some(latest + 2 * period));
    }
}

#[tokio::test]
async fn skip_missed_cold_one_shot_is_exhausted_without_a_run() {
    let runtime = runtime().await;
    let created = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: FIRST,
                timezone: None,
                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
            }),
        )
        .await
        .unwrap();
    assert_eq!(runtime.process_due_once(FIRST + 2).await.unwrap(), 0);
    assert_eq!(runtime.process_due_once(FIRST + 2).await.unwrap(), 0);
    let saved = task(&runtime, &created.task.id).await;
    assert!(saved.runs.is_empty());
    assert_eq!(saved.triggers[0].last_fire_at, Some(FIRST));
    assert_eq!(saved.triggers[0].next_fire_at, None);
    assert_eq!(saved.triggers[0].status, TaskTriggerStatus::Exhausted);
}

#[tokio::test]
async fn terminal_status_refresh_keeps_the_next_skip_missed_deadline_armed() {
    let runtime = runtime().await;
    let created = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(interval(TaskTriggerCatchUpPolicy::skip_missed())),
        )
        .await
        .unwrap();
    assert_eq!(runtime.process_due_once(FIRST).await.unwrap(), 1);
    let run = task(&runtime, &created.task.id).await.runs.remove(0);
    let store = runtime.service().store();
    store
        .claim_task_run_for_dispatch(&run.id, FIRST)
        .await
        .unwrap()
        .unwrap();
    let handle =
        TaskExecutionHandle::new(store, runtime.event_bus(), created.task.id.clone(), run.id);
    handle.mark_started(FIRST).await.unwrap();
    // The first sweep armed FIRST + 10. Terminal refresh changes updated_at
    // after that deadline but leaves the scheduled occurrence untouched.
    handle.complete_run(None, FIRST + 11).await.unwrap();
    let refreshed = task(&runtime, &created.task.id).await;
    assert_eq!(refreshed.triggers[0].updated_at, FIRST + 11);
    assert_eq!(refreshed.triggers[0].next_fire_at, Some(FIRST + 10));
    assert_eq!(runtime.process_due_once(FIRST + 12).await.unwrap(), 1);
    assert_eq!(runtime.process_due_once(FIRST + 12).await.unwrap(), 0);
    let saved = task(&runtime, &created.task.id).await;
    assert_eq!(saved.runs.len(), 2);
    assert_eq!(saved.triggers[0].last_fire_at, Some(FIRST + 10));
}

#[tokio::test]
async fn saturated_trigger_obeys_catch_up_before_creating_a_bounded_queue() {
    for (policy, new_runs, last, next) in [
        (
            TaskTriggerCatchUpPolicy::skip_missed(),
            0,
            FIRST + 30,
            FIRST + 40,
        ),
        (
            TaskTriggerCatchUpPolicy::run_once_for_latest_missed(),
            1,
            FIRST + 30,
            FIRST + 40,
        ),
        (
            TaskTriggerCatchUpPolicy::run_all_missed(4),
            1,
            FIRST + 10,
            FIRST + 20,
        ),
    ] {
        let runtime = runtime().await;
        let mut params = create_params(interval(policy));
        params.concurrency_policy = Some(TaskConcurrencyPolicy {
            key: None,
            max_parallel_runs: 1,
            on_conflict: TaskConcurrencyConflictPolicy::Queue,
        });
        let created = runtime
            .service()
            .create_task(TaskCreateContext::default(), params)
            .await
            .unwrap();
        assert_eq!(runtime.process_due_once(FIRST).await.unwrap(), 1);
        let initial = task(&runtime, &created.task.id).await.runs.remove(0);
        let store = runtime.service().store();
        store
            .claim_task_run_for_dispatch(&initial.id, FIRST)
            .await
            .unwrap()
            .unwrap();
        let handle = TaskExecutionHandle::new(
            store,
            runtime.event_bus(),
            created.task.id.clone(),
            initial.id,
        );
        handle.mark_started(FIRST).await.unwrap();
        assert_eq!(
            runtime.process_due_once(FIRST + 35).await.unwrap(),
            new_runs
        );
        let saved = task(&runtime, &created.task.id).await;
        assert_eq!(saved.runs.len(), 1 + new_runs);
        assert_eq!(saved.runs[0].status, TaskRunStatus::Running);
        assert_eq!(saved.triggers[0].last_fire_at, Some(last));
        assert_eq!(saved.triggers[0].next_fire_at, Some(next));
        assert_eq!(runtime.process_due_once(FIRST + 35).await.unwrap(), 0);
        assert_eq!(
            task(&runtime, &created.task.id).await.runs.len(),
            1 + new_runs
        );
        if new_runs == 0 {
            // The next live skip_missed fire must queue behind the occupied
            // serial slot; a second fire coalesces into that queued work.
            assert_eq!(runtime.process_due_once(FIRST + 42).await.unwrap(), 1);
            assert_eq!(runtime.process_due_once(FIRST + 52).await.unwrap(), 0);
            let queued = task(&runtime, &created.task.id).await;
            assert_eq!(queued.runs.len(), 2);
            assert_eq!(queued.runs[1].status, TaskRunStatus::Queued);
            assert_eq!(queued.triggers[0].next_fire_at, Some(FIRST + 60));
            handle.complete_run(None, FIRST + 53).await.unwrap();
            runtime
                .register_executor(Arc::new(CompletingSystemExecutor))
                .await;
            assert_eq!(runtime.process_due_once(FIRST + 53).await.unwrap(), 1);
            assert_eq!(
                task(&runtime, &created.task.id).await.runs[1].status,
                TaskRunStatus::Succeeded
            );
        }
    }
}

#[tokio::test]
async fn competing_schedulers_materialize_and_dispatch_one_live_occurrence() {
    let first = runtime().await;
    let second = TaskRuntime::new(first.service().store());
    let created = first
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(interval(TaskTriggerCatchUpPolicy::skip_missed())),
        )
        .await
        .unwrap();
    first.process_due_once(FIRST - 1).await.unwrap();
    second.process_due_once(FIRST - 1).await.unwrap();
    let (left, right) = tokio::join!(
        first.process_due_once(FIRST + 2),
        second.process_due_once(FIRST + 2)
    );
    assert_eq!(left.unwrap() + right.unwrap(), 1);
    assert_eq!(task(&first, &created.task.id).await.runs.len(), 1);
    first
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    assert_eq!(first.process_due_once(FIRST + 2).await.unwrap(), 1);
    assert_eq!(second.process_due_once(FIRST + 2).await.unwrap(), 0);
    let saved = task(&first, &created.task.id).await;
    assert_eq!(saved.runs.len(), 1);
    assert_eq!(saved.runs[0].status, TaskRunStatus::Succeeded);
}

#[tokio::test]
async fn same_deadline_reschedule_rejects_a_stale_trigger_plan_atomically() {
    let runtime = runtime().await;
    let created = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(interval(TaskTriggerCatchUpPolicy::skip_missed())),
        )
        .await
        .unwrap();
    let old = created.trigger;
    runtime.process_due_once(FIRST - 1).await.unwrap();
    runtime
        .service()
        .reschedule_task(
            TaskMutationContext::default(),
            TaskRescheduleParams {
                task_id: created.task.id.clone(),
                trigger: TaskTriggerInput {
                    spec: interval(TaskTriggerCatchUpPolicy::run_once_for_latest_missed()),
                },
            },
        )
        .await
        .unwrap();
    let current = task(&runtime, &created.task.id).await;
    assert_eq!(current.triggers[0].next_fire_at, old.next_fire_at);
    let mut stale = old.clone();
    stale.last_fire_at = Some(FIRST);
    stale.next_fire_at = Some(FIRST + 10);
    let result = runtime
        .service()
        .store()
        .append_due_trigger_task_events(
            &old,
            FIRST,
            vec![TaskEventPayload::TaskRescheduled {
                task_id: created.task.id.clone(),
                trigger: stale,
                rescheduled_at: FIRST,
                reason: TaskRescheduleReason::MissedFireSkipped,
            }],
            Vec::new(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(task(&runtime, &created.task.id).await, current);
    assert_eq!(runtime.process_due_once(FIRST + 2).await.unwrap(), 1);
}

#[tokio::test]
async fn pause_resume_rearms_skip_missed_future_schedule() {
    let runtime = runtime().await;
    let created = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(interval(TaskTriggerCatchUpPolicy::skip_missed())),
        )
        .await
        .unwrap();
    runtime.process_due_once(FIRST - 1).await.unwrap();
    runtime
        .service()
        .pause_task(
            TaskMutationContext::default(),
            TaskPauseParams {
                task_id: created.task.id.clone(),
                reason: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(runtime.process_due_once(FIRST + 35).await.unwrap(), 0);
    assert!(task(&runtime, &created.task.id).await.runs.is_empty());
    // Service APIs use wall time, still before FIRST. Resume recomputes the
    // future schedule and its immediate scheduler sweep arms that snapshot.
    runtime
        .service()
        .resume_task(
            TaskMutationContext::default(),
            TaskResumeParams {
                task_id: created.task.id.clone(),
                reason: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        task(&runtime, &created.task.id).await.triggers[0].next_fire_at,
        Some(FIRST)
    );
    assert_eq!(runtime.process_due_once(FIRST + 2).await.unwrap(), 1);
    assert_eq!(runtime.process_due_once(FIRST + 2).await.unwrap(), 0);
}
