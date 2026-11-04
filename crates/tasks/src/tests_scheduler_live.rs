use super::*;
use crate::executor::TaskExecutorRegistry;
use crate::reconciliation::TaskStartupReconciler;
use crate::scheduler::TaskScheduler;
use sea_orm::{ColumnTrait, QueryFilter};

#[tokio::test]
async fn due_run_page_filters_future_work_before_limit_and_rotates() {
    let runtime = runtime().await;
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(
            runtime
                .service()
                .create_task(
                    TaskCreateContext::default(),
                    create_params(TaskTriggerSpec::Immediate),
                )
                .await
                .unwrap()
                .run
                .unwrap(),
        );
    }
    runs.sort_by(|left, right| left.id.cmp(&right.id));
    let now = runs[0].created_at;
    let db = runtime.service().store().database_connection();
    // Fixture repair uses the scoped serialized database path, with FK disabled
    // as in the production runtime. This early ID must not consume LIMIT.
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(runs[0].id.clone()))
        .col_expr(
            pioneer_entity::task_run::Column::ReadyAt,
            sea_orm::sea_query::Expr::value(
                chrono::DateTime::from_timestamp(now + 1000, 500_000_000)
                    .unwrap()
                    .fixed_offset(),
            ),
        )
        .exec(&db)
        .await
        .unwrap();
    let store = runtime.service().store().with_maintenance_access();
    assert_eq!(
        store.next_queued_task_ready_deadline(now).await.unwrap(),
        Some(now + 1001)
    );
    assert!(
        !store
            .list_scheduler_due_task_runs(now + 1000, None, 10)
            .await
            .unwrap()
            .iter()
            .any(|run| run.id == runs[0].id)
    );
    assert!(
        store
            .list_scheduler_due_task_runs(now + 1001, None, 10)
            .await
            .unwrap()
            .iter()
            .any(|run| run.id == runs[0].id)
    );
    let first = store
        .list_scheduler_due_task_runs(now, None, 1)
        .await
        .unwrap();
    assert_eq!(first[0].id, runs[1].id);
    let next = store
        .list_scheduler_due_task_runs(now, Some(&first[0].id), 1)
        .await
        .unwrap();
    assert_eq!(next[0].id, runs[2].id);
    assert!(
        store
            .list_scheduler_due_task_runs(now, Some(&next[0].id), 1)
            .await
            .unwrap()
            .is_empty()
    );
    // The raw page still occupies LIMIT and carries its locator when domain
    // conversion fails. The strict compatibility API must refuse incompleteness.
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(runs[1].id.clone()))
        .col_expr(
            pioneer_entity::task_run::Column::TriggerInputJson,
            sea_orm::sea_query::Expr::value("invalid JSON"),
        )
        .exec(&db)
        .await
        .unwrap();
    assert!(
        store
            .list_scheduler_due_task_runs(now, None, 1)
            .await
            .is_err()
    );
    let partial = store
        .list_scheduler_due_task_runs_partial(now, None, 1)
        .await
        .unwrap();
    assert_eq!(partial.scanned, 1);
    assert_eq!(partial.last.as_deref(), Some(runs[1].id.as_str()));
    assert!(partial.records.is_empty());
    assert_eq!(partial.failures[0].id, runs[1].id);
    assert_eq!(
        store
            .list_scheduler_due_task_runs(now, partial.last.as_deref(), 1)
            .await
            .unwrap()[0]
            .id,
        runs[2].id
    );
}

#[tokio::test]
async fn live_recovery_audits_dormant_triggers_without_reloading_future_triggers() {
    let runtime = runtime().await;
    let store = runtime.service().store().clone();
    let bus = runtime.event_bus();
    let executors = Arc::new(TaskExecutorRegistry::new());
    let scheduler = TaskScheduler::new(store.clone(), bus.clone(), executors.clone());
    let reconciler = TaskStartupReconciler::new(store, bus, executors, scheduler.handle());
    let future = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: None,
                catch_up_policy: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(reconciler.reconcile(10).await.unwrap().active_triggers, 1);
    let mut trigger = future.trigger;
    trigger.next_fire_at = None;
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRescheduled {
                task_id: future.task.id,
                trigger,
                rescheduled_at: 11,
                reason: TaskRescheduleReason::UserRequested,
            },
            11,
        )
        .await
        .unwrap();
    assert_eq!(reconciler.reconcile(11).await.unwrap().active_triggers, 1);
    let another = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_001,
                timezone: None,
                catch_up_policy: None,
            }),
        )
        .await
        .unwrap();
    let live = reconciler.reconcile(12).await.unwrap();
    assert_eq!(
        live.active_triggers, 1,
        "only the dormant row belongs to live trigger audit"
    );
    assert_eq!(
        runtime
            .service()
            .store()
            .get_task(&another.task.id)
            .await
            .unwrap()
            .unwrap()
            .triggers[0]
            .next_fire_at,
        Some(4_000_000_001)
    );
}

#[tokio::test]
async fn lag_rebuild_arms_a_foreign_replacement_schedule() {
    let runtime = runtime().await;
    let bus = runtime.event_bus();
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors.register(Arc::new(CompletingSystemExecutor)).await;
    let scheduler = TaskScheduler::new(runtime.service().store(), bus.clone(), executors);
    let task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Interval {
                interval_seconds: 10,
                interval_anchor_at: Some(4_000_000_000),
                catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
            }),
        )
        .await
        .unwrap();
    scheduler.process_due_once(10).await.unwrap();
    let mut replacement = task.trigger;
    replacement.next_fire_at = Some(4_000_000_010);
    replacement.spec = TaskTriggerSpec::Interval {
        interval_seconds: 20,
        interval_anchor_at: Some(4_000_000_010),
        catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
    };
    // Simulate a foreign writer: commit a replacement without publishing a hint.
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRescheduled {
                task_id: task.task.id.clone(),
                trigger: replacement,
                rescheduled_at: 11,
                reason: TaskRescheduleReason::UserRequested,
            },
            11,
        )
        .await
        .unwrap();
    let other = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Manual {
                allowed_actor: None,
            }),
        )
        .await
        .unwrap();
    for at in 12..1040 {
        let event = runtime
            .service()
            .append_event(
                TaskEventPayload::TaskRecovered {
                    task_id: other.task.id.clone(),
                    run_id: None,
                    message: format!("wake {at}"),
                    recovered_at: at,
                },
                at,
            )
            .await
            .unwrap();
        bus.publish(event).await;
    }
    scheduler.process_due_once(1040).await.unwrap();
    assert_eq!(
        scheduler.process_due_once(4_000_000_010).await.unwrap(),
        1,
        "lag must reconstruct and arm the replacement while still in the future"
    );
}

// Primary recovery regression: an early domain decode failure must not restart
// startup discovery or hide rows beyond the startup quantum. A future trigger
// distinguishes the startup all-active read from the live dormant-only audit.
#[tokio::test]
async fn poisoned_recovery_preserves_discovery_and_reaches_unfinished_tail() {
    for raw_decode in [false, true] {
        let runtime = runtime().await;
        let mut created = Vec::new();
        for _ in 0..1025 {
            created.push(
                runtime
                    .service()
                    .create_task(
                        TaskCreateContext::default(),
                        create_params(TaskTriggerSpec::Immediate),
                    )
                    .await
                    .unwrap(),
            );
        }
        created.sort_by(|a, b| a.run.as_ref().unwrap().id.cmp(&b.run.as_ref().unwrap().id));
        let poison = &created[0];
        let tail = created.last().unwrap().run.as_ref().unwrap();
        runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::ScheduledAt {
                    scheduled_at: 4_000_000_000,
                    timezone: None,
                    catch_up_policy: None,
                }),
            )
            .await
            .unwrap();
        let db = runtime.service().store().database_connection();
        if raw_decode {
            pioneer_entity::task_run::Entity::update_many()
                .filter(
                    pioneer_entity::task_run::Column::Id.eq(poison
                        .run
                        .as_ref()
                        .unwrap()
                        .id
                        .clone()),
                )
                .col_expr(
                    pioneer_entity::task_run::Column::TriggerInputJson,
                    sea_orm::sea_query::Expr::value("invalid JSON"),
                )
                .exec(&db)
                .await
                .unwrap();
        } else {
            pioneer_entity::task::Entity::update_many()
                .filter(pioneer_entity::task::Column::Id.eq(poison.task.id.clone()))
                .col_expr(
                    pioneer_entity::task::Column::ConcurrencyPolicyJson,
                    sea_orm::sea_query::Expr::value("invalid JSON"),
                )
                .exec(&db)
                .await
                .unwrap();
        }
        let bus = runtime.event_bus();
        let executors = Arc::new(TaskExecutorRegistry::new());
        executors.register(Arc::new(CompletingSystemExecutor)).await;
        let scheduler =
            TaskScheduler::new(runtime.service().store(), bus.clone(), executors.clone());
        let reconciler = TaskStartupReconciler::new(
            runtime.service().store(),
            bus,
            executors,
            scheduler.handle(),
        );
        let now = chrono::Utc::now().timestamp();
        assert!(reconciler.reconcile(now).await.is_err());
        let live = reconciler.reconcile(now + 1).await.unwrap();
        assert_eq!(
            live.active_triggers, 0,
            "future triggers must not be rediscovered after a row failure"
        );
        let events = runtime
            .service()
            .store()
            .get_task_events(&tail.task_id, None)
            .await
            .unwrap();
        assert!(
            events.events.iter().any(|event| matches!(&event.payload,
        TaskEventPayload::TaskRecovered { run_id: Some(id), .. } if id == &tail.id)),
            "the unfinished tail beyond the startup page must be recovered"
        );
        assert!(
            reconciler.reconcile(now + 6).await.is_err(),
            "the exact poison remains a timed retry"
        );
        let live = reconciler.reconcile(now + 7).await.unwrap();
        assert_eq!(
            live.active_triggers, 0,
            "retry failure must not restart startup discovery"
        );
    }
}

#[tokio::test]
async fn due_trigger_noop_page_and_row_failure_do_not_hide_trigger_or_run_tail() {
    for raw_decode in [false, true] {
        let runtime = runtime().await;
        let now = chrono::Utc::now().timestamp() + 100;
        let db = runtime.service().store().database_connection();
        let mut prefix = Vec::new();
        for _ in 0..1024 {
            let task = runtime
                .service()
                .create_task(
                    TaskCreateContext::default(),
                    create_params(TaskTriggerSpec::Immediate),
                )
                .await
                .unwrap();
            // A stale active one-shot with an existing run is a legitimate no-op.
            // Keep those runs future-ready so they cannot fill the due-run page.
            pioneer_entity::task_run::Entity::update_many()
                .filter(pioneer_entity::task_run::Column::Id.eq(task.run.unwrap().id))
                .col_expr(
                    pioneer_entity::task_run::Column::ReadyAt,
                    sea_orm::sea_query::Expr::value(
                        chrono::DateTime::from_timestamp(now + 1000, 0)
                            .unwrap()
                            .fixed_offset(),
                    ),
                )
                .exec(&db)
                .await
                .unwrap();
            pioneer_entity::task_trigger::Entity::update_many()
                .filter(pioneer_entity::task_trigger::Column::Id.eq(task.trigger.id.clone()))
                .col_expr(
                    pioneer_entity::task_trigger::Column::Status,
                    sea_orm::sea_query::Expr::value("active"),
                )
                .col_expr(
                    pioneer_entity::task_trigger::Column::NextFireAt,
                    sea_orm::sea_query::Expr::value(
                        chrono::DateTime::from_timestamp(now - 10, 0)
                            .unwrap()
                            .fixed_offset(),
                    ),
                )
                .exec(&db)
                .await
                .unwrap();
            prefix.push(task.task.id);
        }
        // Source-read failure belongs to one trigger, and must not abort due runs.
        if raw_decode {
            pioneer_entity::task_trigger::Entity::update_many()
                .filter(pioneer_entity::task_trigger::Column::TaskId.eq(prefix[0].clone()))
                .col_expr(
                    pioneer_entity::task_trigger::Column::SpecJson,
                    sea_orm::sea_query::Expr::value("invalid JSON"),
                )
                .exec(&db)
                .await
                .unwrap();
        } else {
            pioneer_entity::task::Entity::update_many()
                .filter(pioneer_entity::task::Column::Id.eq(prefix[0].clone()))
                .col_expr(
                    pioneer_entity::task::Column::ConcurrencyPolicyJson,
                    sea_orm::sea_query::Expr::value("invalid JSON"),
                )
                .exec(&db)
                .await
                .unwrap();
        }
        let healthy = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::ScheduledAt {
                    scheduled_at: now,
                    timezone: None,
                    catch_up_policy: None,
                }),
            )
            .await
            .unwrap();
        let due_run = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::Immediate),
            )
            .await
            .unwrap()
            .run
            .unwrap();
        let executors = Arc::new(TaskExecutorRegistry::new());
        executors.register(Arc::new(CompletingSystemExecutor)).await;
        let scheduler =
            TaskScheduler::new(runtime.service().store(), runtime.event_bus(), executors);
        assert!(scheduler.process_due_once(now).await.is_err());
        assert_eq!(
            runtime
                .service()
                .store()
                .get_task_run(&due_run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            TaskRunStatus::Succeeded,
            "a trigger failure must not suppress independent due-run dispatch"
        );
        let next = scheduler.process_due_once(now).await;
        if raw_decode {
            assert!(
                next.is_err(),
                "addressed poison remains visible while tail dispatch proceeds"
            );
        } else {
            assert_eq!(next.unwrap(), 1);
        }
        let task = runtime
            .service()
            .store()
            .get_task(&healthy.task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.runs.len(), 1);
        assert_eq!(task.runs[0].status, TaskRunStatus::Succeeded);
        // Wrap restores retries; poison did not disappear or become success.
        assert!(scheduler.process_due_once(now).await.is_err());
    }
}

#[tokio::test]
async fn due_trigger_keyset_visits_equal_timestamps_before_wrap() {
    let runtime = runtime().await;
    let now = chrono::Utc::now().timestamp() + 100;
    let mut ids = Vec::new();
    for _ in 0..3 {
        let task = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::ScheduledAt {
                    scheduled_at: now,
                    timezone: None,
                    catch_up_policy: None,
                }),
            )
            .await
            .unwrap();
        ids.push(task.trigger.id);
    }
    ids.sort();
    let store = runtime.service().store().with_maintenance_access();
    let mut cursor: Option<(i64, String)> = None;
    for id in ids {
        let page = store
            .list_due_active_task_triggers_page(
                now,
                cursor.as_ref().map(|(at, id)| (*at, id.as_str())),
                1,
            )
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id, id);
        cursor = Some((now, id));
    }
    assert!(
        store
            .list_due_active_task_triggers_page(
                now,
                cursor.as_ref().map(|(at, id)| (*at, id.as_str())),
                1
            )
            .await
            .unwrap()
            .is_empty()
    );
    // A new pass has no cursor and rediscovers retained rows.
    assert_eq!(
        store
            .list_due_active_task_triggers_page(now, None, 1)
            .await
            .unwrap()
            .len(),
        1
    );
}

// Persistent dormant-row failure must retain an error without suppressing
// independently owned recovery on either startup or a later live pass.
struct DormantIsolationExecutor(
    Arc<std::sync::atomic::AtomicUsize>,
    Option<Arc<std::sync::atomic::AtomicBool>>,
);
#[async_trait::async_trait]
impl TaskExecutor for DormantIsolationExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::System
    }
    async fn start_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorStartOutcome> {
        CompletingSystemExecutor
            .start_run(context, run, handle)
            .await
    }
    async fn cancel_run(
        &self,
        context: TaskExecutionContext,
        id: &str,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<()> {
        CompletingSystemExecutor
            .cancel_run(context, id, reason, handle)
            .await
    }
    async fn recover_run(
        &self,
        _context: TaskExecutionContext,
        _run: TaskRun,
        _handle: TaskExecutionHandle,
    ) -> TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .1
            .as_ref()
            .is_some_and(|fail| fail.load(Ordering::SeqCst))
        {
            anyhow::bail!("injected waiting-review recovery storage failure");
        }
        Ok(TaskExecutorRecoveryOutcome::LeftUnchanged)
    }
}
#[tokio::test]
async fn dormant_append_poison_keeps_error_while_healthy_inflight_recovery_proceeds() {
    use sea_orm::ConnectionTrait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let runtime = runtime().await;
    let poison = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: 4_000_000_000,
                timezone: None,
                catch_up_policy: None,
            }),
        )
        .await
        .unwrap();
    let mut trigger = poison.trigger;
    trigger.next_fire_at = None;
    runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRescheduled {
                task_id: poison.task.id.clone(),
                trigger,
                rescheduled_at: 10,
                reason: TaskRescheduleReason::UserRequested,
            },
            10,
        )
        .await
        .unwrap();
    let db = runtime.service().store().database_connection();
    db.execute_unprepared(&format!("CREATE TRIGGER test_dormant_append_refusal BEFORE INSERT ON task_event WHEN NEW.task_id='{}' BEGIN SELECT RAISE(FAIL,'dormant recovery append refused'); END", poison.task.id)).await.unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors
        .register(Arc::new(DormantIsolationExecutor(count.clone(), None)))
        .await;
    let bus = runtime.event_bus();
    let scheduler = TaskScheduler::new(runtime.service().store(), bus.clone(), executors.clone());
    let reconciler = TaskStartupReconciler::new(
        runtime.service().store(),
        bus.clone(),
        executors,
        scheduler.handle(),
    );
    let now = chrono::Utc::now().timestamp();
    for (index, offset) in [0, 6].into_iter().enumerate() {
        let healthy = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::Immediate),
            )
            .await
            .unwrap();
        let run = healthy.run.unwrap();
        TaskExecutionHandle::new(
            runtime.service().store(),
            bus.clone(),
            healthy.task.id.clone(),
            run.id.clone(),
        )
        .mark_started(now + offset)
        .await
        .unwrap();
        assert!(
            reconciler.reconcile(now + offset).await.is_err(),
            "dormant failure remains visible"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            index + 1,
            "healthy executor recovery must run on startup and live pass"
        );
        let execution = runtime
            .service()
            .store()
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap();
        assert!(execution.worker_id.is_some());
        assert!(
            execution
                .lease_until
                .is_some_and(|until| until > now + offset)
        );
        let history = runtime
            .service()
            .store()
            .get_task_events(&healthy.task.id, None)
            .await
            .unwrap();
        assert!(history.events.iter().any(|event| matches!(&event.payload, TaskEventPayload::TaskRecovered { run_id: Some(id), .. } if id == &run.id)));
    }
}

#[tokio::test]
async fn foreign_expired_cache_refresh_replaces_deadlines_without_rebuild() {
    for change in ["pause", "delete", "clear", "later"] {
        let runtime = runtime().await;
        let store = runtime.service().store();
        let executors = Arc::new(TaskExecutorRegistry::new());
        executors.register(Arc::new(CompletingSystemExecutor)).await;
        let scheduler = TaskScheduler::new(store.clone(), runtime.event_bus(), executors);
        let task = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::Interval {
                    interval_seconds: 10,
                    interval_anchor_at: Some(4_000_000_000),
                    catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
                }),
            )
            .await
            .unwrap();
        let inactive = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::Manual {
                    allowed_actor: None,
                }),
            )
            .await
            .unwrap();
        pioneer_entity::task_trigger::Entity::update_many()
            .filter(pioneer_entity::task_trigger::Column::Id.eq(inactive.trigger.id))
            .col_expr(
                pioneer_entity::task_trigger::Column::Status,
                sea_orm::sea_query::Expr::value("paused"),
            )
            .col_expr(
                pioneer_entity::task_trigger::Column::SpecJson,
                sea_orm::sea_query::Expr::value("invalid JSON"),
            )
            .exec(&store.database_connection())
            .await
            .unwrap();
        let hint = runtime
            .service()
            .append_event(
                TaskEventPayload::TaskRecovered {
                    task_id: inactive.task.id,
                    run_id: None,
                    message: "inactive history".into(),
                    recovered_at: 3_999_999_990,
                },
                3_999_999_990,
            )
            .await
            .unwrap();
        runtime.event_bus().publish(hint).await;
        scheduler.process_due_once(3_999_999_990).await.unwrap();
        let mut replacement = task.trigger.clone();
        replacement.next_fire_at = Some(4_000_000_040);
        replacement.spec = TaskTriggerSpec::Interval {
            interval_seconds: 20,
            interval_anchor_at: Some(4_000_000_040),
            catch_up_policy: Some(TaskTriggerCatchUpPolicy::skip_missed()),
        };
        match change {
            "pause" => replacement.status = TaskTriggerStatus::Paused,
            "clear" => replacement.next_fire_at = None,
            "delete" => {
                pioneer_entity::task_trigger::Entity::delete_by_id(task.trigger.id.clone())
                    .exec(&store.database_connection())
                    .await
                    .unwrap();
            }
            _ => {}
        }
        if change != "delete" {
            // Append commits durable state but deliberately does not publish to this bus.
            runtime
                .service()
                .append_event(
                    TaskEventPayload::TaskRescheduled {
                        task_id: task.task.id.clone(),
                        trigger: replacement,
                        rescheduled_at: 3_999_999_991,
                        reason: TaskRescheduleReason::UserRequested,
                    },
                    3_999_999_991,
                )
                .await
                .unwrap();
        }
        scheduler.process_due_once(4_000_000_000).await.unwrap();
        assert_eq!(
            scheduler.next_sleep_duration(4_000_000_000).await,
            if change == "later" {
                Duration::from_secs(40)
            } else {
                Duration::MAX
            },
            "{change}"
        );
        if change == "later" {
            assert_eq!(
                scheduler.process_due_once(4_000_000_040).await.unwrap(),
                1,
                "a future replacement observed by addressed refresh must arm its own occurrence"
            );
        } else {
            assert!(
                store
                    .get_task(&task.task.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .runs
                    .is_empty()
            );
        }
    }
    // The same addressed cache maintenance covers a queued run that a foreign
    // owner terminalized while its ready_at was still in the future.
    let runtime = runtime().await;
    let task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    let run = task.run.unwrap();
    let store = runtime.service().store();
    let db = store.database_connection();
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(run.id.clone()))
        .col_expr(
            pioneer_entity::task_run::Column::ReadyAt,
            sea_orm::sea_query::Expr::value(
                chrono::DateTime::from_timestamp(4_000_000_000, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
        )
        .exec(&db)
        .await
        .unwrap();
    let scheduler = TaskScheduler::new(
        store.clone(),
        Arc::new(crate::event_bus::TaskEventBus::new()),
        Arc::new(TaskExecutorRegistry::new()),
    );
    scheduler.process_due_once(3_999_999_990).await.unwrap();
    TaskExecutionHandle::new(
        store.clone(),
        runtime.event_bus(),
        task.task.id,
        run.id.clone(),
    )
    .complete_run(None, 3_999_999_991)
    .await
    .unwrap();
    // This scheduler has a separate bus: the durable completion cannot publish
    // a local hint, so only expired-cache addressed refresh can remove the entry.
    scheduler.process_due_once(4_000_000_000).await.unwrap();
    assert_eq!(
        scheduler.next_sleep_duration(4_000_000_000).await,
        Duration::MAX
    );
}

#[tokio::test]
async fn malformed_discovery_rows_preserve_executor_dispatch_and_terminal_repair() {
    // Keep the large composed setup/dispatch futures off the test thread stack.
    let runtime = Box::pin(runtime()).await;
    let store = runtime.service().store();
    let terminal = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::Immediate),
    ))
    .await
    .unwrap();
    let terminal_run = terminal.run.unwrap();
    let now = chrono::Utc::now().timestamp();
    let setup_executors = Arc::new(TaskExecutorRegistry::new());
    setup_executors
        .register(Arc::new(CompletingSystemExecutor))
        .await;
    Box::pin(
        TaskScheduler::new(store.clone(), runtime.event_bus(), setup_executors)
            .process_due_once(now),
    )
    .await
    .unwrap();
    store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE task_occurrence_contract SET status = 'recovering' WHERE run_id = ?",
            [terminal_run.id.clone().into()],
        ))
        .await
        .unwrap();
    let bad_run = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::Immediate),
    ))
    .await
    .unwrap()
    .run
    .unwrap();
    let bad_trigger = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::ScheduledAt {
            scheduled_at: now,
            timezone: None,
            catch_up_policy: None,
        }),
    ))
    .await
    .unwrap();
    let db = store.database_connection();
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(bad_run.id.clone()))
        .col_expr(
            pioneer_entity::task_run::Column::TriggerInputJson,
            sea_orm::sea_query::Expr::value("invalid JSON"),
        )
        .exec(&db)
        .await
        .unwrap();
    pioneer_entity::task_trigger::Entity::update_many()
        .filter(pioneer_entity::task_trigger::Column::Id.eq(bad_trigger.trigger.id.clone()))
        .col_expr(
            pioneer_entity::task_trigger::Column::SpecJson,
            sea_orm::sea_query::Expr::value("invalid JSON"),
        )
        .col_expr(
            pioneer_entity::task_trigger::Column::NextFireAt,
            sea_orm::sea_query::Expr::value(Option::<chrono::DateTime<chrono::FixedOffset>>::None),
        )
        .exec(&db)
        .await
        .unwrap();
    let inflight = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::Immediate),
    ))
    .await
    .unwrap();
    let inflight_run = inflight.run.unwrap();
    TaskExecutionHandle::new(
        store.clone(),
        runtime.event_bus(),
        inflight.task.id.clone(),
        inflight_run.id.clone(),
    )
    .mark_started(now)
    .await
    .unwrap();
    let healthy = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::Immediate),
    ))
    .await
    .unwrap()
    .run
    .unwrap();
    // Immediate runs use the creation clock for ready_at. Capture the pass
    // clock after fixture writes so a second boundary cannot leave them future-due.
    let now = chrono::Utc::now().timestamp();
    let count = Arc::new(AtomicUsize::new(0));
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors
        .register(Arc::new(DormantIsolationExecutor(count.clone(), None)))
        .await;
    let scheduler = TaskScheduler::new(store.clone(), runtime.event_bus(), executors.clone());
    let reconciler = TaskStartupReconciler::new(
        store.clone(),
        runtime.event_bus(),
        executors,
        scheduler.handle(),
    );
    assert!(Box::pin(reconciler.reconcile(now)).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let execution = store
        .load_execution_for_run(&inflight_run.id)
        .await
        .unwrap()
        .unwrap();
    assert!(execution.lease_until.is_some_and(|until| until > now));
    assert_eq!(
        store
            .get_task_occurrence_contract_by_run(&terminal_run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskOccurrenceStatus::Delivered
    );
    assert!(Box::pin(scheduler.process_due_once(now)).await.is_err());
    assert_eq!(
        store
            .get_task_run(&healthy.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskRunStatus::Succeeded
    );
    // A malformed exact hint and timed retry cannot suppress a healthy new run.
    let event = runtime
        .service()
        .append_event(
            TaskEventPayload::TaskRecovered {
                task_id: bad_run.task_id,
                run_id: Some(bad_run.id),
                message: "hint poison".into(),
                recovered_at: now + 6,
            },
            now + 6,
        )
        .await
        .unwrap();
    runtime.event_bus().publish(event).await;
    let later = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::Immediate),
    ))
    .await
    .unwrap()
    .run
    .unwrap();
    assert!(Box::pin(reconciler.reconcile(now + 6)).await.is_err());
    assert!(store.get_task_events(&later.task_id, None).await.unwrap().events.iter().any(|event|
        matches!(&event.payload, TaskEventPayload::TaskRecovered { run_id: Some(id), .. } if id == &later.id)));
    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "the valid lease prevents duplicate recovery"
    );
    assert!(
        Box::pin(scheduler.process_due_once(now + 6)).await.is_err(),
        "poison remains visible on live passes"
    );
    let startup_run = Box::pin(runtime.service().create_task(
        TaskCreateContext::default(),
        create_params(TaskTriggerSpec::Immediate),
    ))
    .await
    .unwrap()
    .run
    .unwrap();
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    assert!(
        Box::pin(runtime.start()).await.is_err(),
        "partial startup remains visible to the caller"
    );
    assert_eq!(
        store
            .get_task_run(&startup_run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskRunStatus::Succeeded,
        "startup must dispatch healthy work before returning a partial discovery error"
    );
}

// Actual scheduler dispatch under a growing eligible suffix. A first-page
// prefix remains legitimate no-op history; only a finite pass can return to
// the missed foreign Trigger inserted behind the retained cursor.
#[tokio::test]
async fn finite_due_trigger_pass_returns_behind_cursor_under_suffix_growth() {
    async fn noops(runtime: &TaskRuntime, at: i64, count: usize) {
        let db = runtime.service().store().database_connection();
        for _ in 0..count {
            let task = runtime
                .service()
                .create_task(
                    TaskCreateContext::default(),
                    create_params(TaskTriggerSpec::Immediate),
                )
                .await
                .unwrap();
            pioneer_entity::task_run::Entity::update_many()
                .filter(pioneer_entity::task_run::Column::Id.eq(task.run.unwrap().id))
                .col_expr(
                    pioneer_entity::task_run::Column::ReadyAt,
                    sea_orm::sea_query::Expr::value(
                        chrono::DateTime::from_timestamp(at + 10000, 0)
                            .unwrap()
                            .fixed_offset(),
                    ),
                )
                .exec(&db)
                .await
                .unwrap();
            pioneer_entity::task_trigger::Entity::update_many()
                .filter(pioneer_entity::task_trigger::Column::Id.eq(task.trigger.id))
                .col_expr(
                    pioneer_entity::task_trigger::Column::Status,
                    sea_orm::sea_query::Expr::value("active"),
                )
                .col_expr(
                    pioneer_entity::task_trigger::Column::NextFireAt,
                    sea_orm::sea_query::Expr::value(
                        chrono::DateTime::from_timestamp(at, 0)
                            .unwrap()
                            .fixed_offset(),
                    ),
                )
                .exec(&db)
                .await
                .unwrap();
        }
    }
    let runtime = runtime().await;
    // Dispatch uses an actual-clock ready_at fence; fixture construction
    // speed must not decide whether these injected quanta can claim the Run.
    let now = chrono::Utc::now().timestamp() - 5;
    noops(&runtime, now, 1025).await;
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors.register(Arc::new(CompletingSystemExecutor)).await;
    let scheduler = TaskScheduler::new(
        runtime.service().store(),
        Arc::new(crate::event_bus::TaskEventBus::new()),
        executors,
    );
    scheduler.process_due_once(now).await.unwrap();
    assert_eq!(
        scheduler.next_sleep_duration(now).await,
        Duration::ZERO,
        "an overdue cached backlog preserves base immediate scheduling between finite pages"
    );
    let missed = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::ScheduledAt {
                scheduled_at: now - 1,
                timezone: None,
                catch_up_policy: None,
            }),
        )
        .await
        .unwrap();
    for step in 1..=2 {
        noops(&runtime, now + step, 1024).await;
        scheduler.process_due_once(now + step).await.unwrap();
    }
    let runs = runtime
        .service()
        .store()
        .get_task(&missed.task.id)
        .await
        .unwrap()
        .unwrap()
        .runs;
    assert_eq!(runs.len(), 1);
    assert_eq!(
        runs[0].status,
        TaskRunStatus::Succeeded,
        "finite upper must allow actual behind-cursor dispatch despite a full newer suffix each quantum"
    );
}

#[tokio::test]
async fn finite_run_discovery_excludes_growing_suffix_until_wrap() {
    use sea_orm::{ActiveModelTrait, IntoActiveModel};
    let runtime = runtime().await;
    let store = runtime.service().store();
    let task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    let db = store.database_connection();
    let template = pioneer_entity::task_run::Entity::find_by_id(task.run.unwrap().id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    pioneer_entity::task_run::Entity::delete_by_id(template.id.clone())
        .exec(&db)
        .await
        .unwrap();
    // Test the real status and due projections; domain IDs are fixtures only,
    // and no fabricated occurrence/executor authority is asserted by this test.
    for status in [
        TaskRunStatus::Queued,
        TaskRunStatus::Starting,
        TaskRunStatus::Running,
        TaskRunStatus::WaitingReview,
    ] {
        let status_db = match status {
            TaskRunStatus::Queued => "queued",
            TaskRunStatus::Starting => "starting",
            TaskRunStatus::Running => "running",
            _ => "waiting_review",
        };
        for n in [100, 200] {
            let mut row = template.clone();
            row.id = format!("{n:021}");
            row.status = status_db.into();
            row.run_group_id = row.id.clone();
            row.run_number = n;
            row.ready_at = None;
            row.into_active_model().insert(&db).await.unwrap();
        }
        let (first, upper) = store
            .list_task_recovery_runs_finite(status, None, None, 1)
            .await
            .unwrap();
        let mut due_pass = if status == TaskRunStatus::Queued {
            let (page, upper) = store
                .list_scheduler_due_task_runs_finite(
                    chrono::Utc::now().timestamp() + 10000,
                    None,
                    None,
                    1,
                )
                .await
                .unwrap();
            Some((page.last, upper))
        } else {
            None
        };
        let mut missed = template.clone();
        missed.id = format!("{:021}", 50);
        missed.status = status_db.into();
        missed.run_group_id = missed.id.clone();
        missed.run_number = 50;
        missed.ready_at = None;
        missed.into_active_model().insert(&db).await.unwrap();
        let mut cursor = first.last;
        // Keep supplying a full suffix after the captured upper.
        for n in [300, 400] {
            let mut row = template.clone();
            row.id = format!("{n:021}");
            row.status = status_db.into();
            row.run_group_id = row.id.clone();
            row.run_number = n;
            row.ready_at = None;
            row.into_active_model().insert(&db).await.unwrap();
            let (page, _) = store
                .list_task_recovery_runs_finite(status, cursor.as_deref(), upper.as_deref(), 1)
                .await
                .unwrap();
            if let Some((after, upper)) = due_pass.as_mut() {
                let (due, _) = store
                    .list_scheduler_due_task_runs_finite(
                        chrono::Utc::now().timestamp() + 10000,
                        after.as_deref(),
                        upper.as_deref(),
                        1,
                    )
                    .await
                    .unwrap();
                assert!(
                    due.records
                        .iter()
                        .all(|row| Some(&row.id) <= upper.as_ref())
                );
                *after = due.last;
            }
            if let Some(last) = page.last {
                assert!(Some(&last) <= upper.as_ref());
                cursor = Some(last);
            } else {
                break;
            }
        }
        let (wrapped, _) = store
            .list_task_recovery_runs_finite(status, None, None, 1)
            .await
            .unwrap();
        assert_eq!(wrapped.records[0].id, format!("{:021}", 50));
        if status == TaskRunStatus::Queued {
            let (due, bound) = store
                .list_scheduler_due_task_runs_finite(
                    chrono::Utc::now().timestamp() + 10000,
                    None,
                    None,
                    1,
                )
                .await
                .unwrap();
            assert_eq!(due.records[0].id, format!("{:021}", 50));
            let (tail, _) = store
                .list_scheduler_due_task_runs_finite(
                    chrono::Utc::now().timestamp() + 10000,
                    due.last.as_deref(),
                    bound.as_deref(),
                    10,
                )
                .await
                .unwrap();
            assert!(
                tail.records
                    .iter()
                    .all(|row| Some(&row.id) <= bound.as_ref())
            );
        }
        for n in [50, 100, 200, 300, 400] {
            pioneer_entity::task_run::Entity::delete_by_id(format!("{n:021}"))
                .exec(&db)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn finite_dormant_recovery_pass_revisits_foreign_behind_cursor_trigger() {
    async fn dormant(runtime: &TaskRuntime, id: i64) -> String {
        let task = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::ScheduledAt {
                    scheduled_at: 4_000_000_000,
                    timezone: None,
                    catch_up_policy: None,
                }),
            )
            .await
            .unwrap();
        pioneer_entity::task_trigger::Entity::update_many()
            .filter(pioneer_entity::task_trigger::Column::Id.eq(task.trigger.id))
            .col_expr(
                pioneer_entity::task_trigger::Column::Id,
                sea_orm::sea_query::Expr::value(format!("{id:021}")),
            )
            .col_expr(
                pioneer_entity::task_trigger::Column::NextFireAt,
                sea_orm::sea_query::Expr::value(
                    Option::<chrono::DateTime<chrono::FixedOffset>>::None,
                ),
            )
            .exec(&runtime.service().store().database_connection())
            .await
            .unwrap();
        task.task.id
    }
    let runtime = runtime().await;
    let bus = Arc::new(crate::event_bus::TaskEventBus::new());
    let executors = Arc::new(TaskExecutorRegistry::new());
    let scheduler = TaskScheduler::new(runtime.service().store(), bus.clone(), executors.clone());
    let reconciler = TaskStartupReconciler::new(
        runtime.service().store(),
        bus,
        executors,
        scheduler.handle(),
    );
    reconciler.reconcile(10).await.unwrap();
    for id in 100..165 {
        dormant(&runtime, id).await;
    }
    reconciler.reconcile(11).await.unwrap();
    let missed = dormant(&runtime, 50).await;
    for step in 0..2 {
        for id in 300 + 64 * step..364 + 64 * step {
            dormant(&runtime, id).await;
        }
        reconciler.reconcile(12 + step).await.unwrap();
    }
    assert!(runtime.service().store().get_task_events(&missed,None).await.unwrap().events.iter().any(|event|
        matches!(&event.payload,TaskEventPayload::TaskRecovered { task_id,run_id:None,.. } if task_id==&missed)),
        "finite dormant pass must perform real recovery behind the cursor despite full suffix growth");
}

#[tokio::test]
async fn fractional_foreign_due_deadlines_keep_exact_upper_and_page_progress() {
    async fn foreign_trigger(runtime: &TaskRuntime, seconds: i64, nanos: u32) -> (String, String) {
        let task = runtime
            .service()
            .create_task(
                TaskCreateContext::default(),
                create_params(TaskTriggerSpec::ScheduledAt {
                    scheduled_at: seconds,
                    timezone: None,
                    catch_up_policy: None,
                }),
            )
            .await
            .unwrap();
        pioneer_entity::task_trigger::Entity::update_many()
            .filter(pioneer_entity::task_trigger::Column::Id.eq(&task.trigger.id))
            .col_expr(
                pioneer_entity::task_trigger::Column::NextFireAt,
                sea_orm::sea_query::Expr::value(
                    chrono::DateTime::from_timestamp(seconds, nanos)
                        .unwrap()
                        .fixed_offset(),
                ),
            )
            .exec(&runtime.service().store().database_connection())
            .await
            .unwrap();
        (task.task.id, task.trigger.id)
    }
    let runtime = runtime().await;
    let store = runtime.service().store();
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors.register(Arc::new(CompletingSystemExecutor)).await;
    let scheduler = TaskScheduler::new(
        store.clone(),
        Arc::new(crate::event_bus::TaskEventBus::new()),
        executors,
    );
    // Real dispatch claims use the actual clock. Keep injected process times
    // behind it so this test proves completion without waiting for wall time.
    let at = chrono::Utc::now().timestamp() - 100;
    scheduler.process_due_once(at - 1).await.unwrap();
    // Insert AFTER reconstruction, with a separate bus. The fractional row is
    // its own maximum and absent from the cache: rounding upper down loses it.
    let (single, _) = foreign_trigger(&runtime, at, 500_000_000).await;
    assert_eq!(scheduler.process_due_once(at + 1).await.unwrap(), 1);
    assert_eq!(
        store.get_task(&single).await.unwrap().unwrap().runs[0].status,
        TaskRunStatus::Succeeded
    );
    assert_eq!(scheduler.process_due_once(at + 2).await.unwrap(), 0);

    // Several fractional keys in ONE second, followed by a whole-second upper.
    // A truncated lower bound repeats its first row even when upper is correct.
    let mut expected = Vec::new();
    for (seconds, nanos) in [
        (at + 10, 100_000_000),
        (at + 10, 500_000_000),
        (at + 10, 900_000_000),
        (at + 11, 0),
    ] {
        expected.push(foreign_trigger(&runtime, seconds, nanos).await);
    }
    let mut after = None;
    let mut upper = None;
    for (_, id) in &expected {
        let (page, bound) = store
            .list_due_active_task_triggers_page_finite(
                at + 12,
                after.as_ref().map(
                    |(time, id): &(chrono::DateTime<chrono::FixedOffset>, String)| {
                        (*time, id.as_str())
                    },
                ),
                upper.as_ref().map(
                    |(time, id): &(chrono::DateTime<chrono::FixedOffset>, String)| {
                        (*time, id.as_str())
                    },
                ),
                1,
            )
            .await
            .unwrap();
        assert_eq!(page.scanned, 1);
        assert!(page.failures.is_empty());
        assert_eq!(&page.records[0].id, id);
        after = page.last;
        upper = bound;
    }
    let (eof, _) = store
        .list_due_active_task_triggers_page_finite(
            at + 12,
            after.as_ref().map(|(time, id)| (*time, id.as_str())),
            upper.as_ref().map(|(time, id)| (*time, id.as_str())),
            1,
        )
        .await
        .unwrap();
    assert_eq!(eof.scanned, 0);
    let (wrapped, _) = store
        .list_due_active_task_triggers_page_finite(at + 12, None, None, 1)
        .await
        .unwrap();
    assert_eq!(wrapped.records[0].id, expected[0].1);
    assert_eq!(scheduler.process_due_once(at + 12).await.unwrap(), 4);
    for (task, _) in expected {
        let runs = store.get_task(&task).await.unwrap().unwrap().runs;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, TaskRunStatus::Succeeded);
    }
    assert_eq!(scheduler.process_due_once(at + 13).await.unwrap(), 0);
}

#[tokio::test]
async fn empty_live_recovery_is_quiet_but_duplicate_event_mutations_still_wake() {
    async fn has_wake(bus: &crate::event_bus::TaskEventBus) -> bool {
        // Observe the actual retained Notify permit; no wall-clock SLA or
        // source-grep seam. Timeout polls a ready permit before its deadline.
        tokio::time::timeout(Duration::ZERO, bus.scheduler_notify().notified())
            .await
            .is_ok()
    }
    let runtime = runtime().await;
    let store = runtime.service().store();
    let bus = Arc::new(crate::event_bus::TaskEventBus::new());
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors.register(Arc::new(CompletingSystemExecutor)).await;
    let scheduler = TaskScheduler::new(store.clone(), bus.clone(), executors.clone());
    let reconciler =
        TaskStartupReconciler::new(store.clone(), bus.clone(), executors, scheduler.handle());
    let now = chrono::Utc::now().timestamp();
    reconciler.reconcile(now).await.unwrap();
    assert!(
        !has_wake(&bus).await,
        "empty reconstruction/queue bookkeeping is not a scheduling mutation"
    );
    reconciler.reconcile(now + 5).await.unwrap();
    assert!(
        !has_wake(&bus).await,
        "empty live pass must not restart scheduler polling"
    );
    let task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    let run = task.run.unwrap();
    assert!(
        reconciler
            .reconcile(now + 10)
            .await
            .unwrap()
            .recovered_events
            > 0
    );
    assert!(has_wake(&bus).await);
    let repeated = reconciler.reconcile(now + 15).await.unwrap();
    assert_eq!(
        repeated.recovered_events, 0,
        "the recovery message already exists"
    );
    assert!(
        has_wake(&bus).await,
        "possible occurrence synchronization still dirties/wakes its exact Task"
    );
    assert_eq!(scheduler.process_due_once(now + 15).await.unwrap(), 1);
    assert_eq!(
        store.get_task_run(&run.id).await.unwrap().unwrap().status,
        TaskRunStatus::Succeeded
    );
    let _ = has_wake(&bus).await; // committed dispatch/terminal events are real wakes
    reconciler.reconcile(now + 20).await.unwrap();
    assert!(!has_wake(&bus).await);

    // An unpublished future queue row is not an addressed local dependency.
    // Once a foreign writer moves ready_at earlier, indexed due discovery must
    // recover it without waiting for the old deadline or a local hint.
    let future = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap()
        .run
        .unwrap();
    let db = store.database_connection();
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(&future.id))
        .col_expr(
            pioneer_entity::task_run::Column::ReadyAt,
            sea_orm::sea_query::Expr::value(
                chrono::DateTime::from_timestamp(now + 3600, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
        )
        .exec(&db)
        .await
        .unwrap();
    reconciler.reconcile(now + 25).await.unwrap();
    assert!(
        !has_wake(&bus).await,
        "future-only live state does not mutate or wake"
    );
    assert!(!store.get_task_events(&future.task_id, None).await.unwrap().events.iter().any(|event|
        matches!(&event.payload, TaskEventPayload::TaskRecovered { run_id: Some(id), .. } if id == &future.id)));
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(&future.id))
        .col_expr(
            pioneer_entity::task_run::Column::ReadyAt,
            sea_orm::sea_query::Expr::value(
                chrono::DateTime::from_timestamp(now + 30, 0)
                    .unwrap()
                    .fixed_offset(),
            ),
        )
        .exec(&db)
        .await
        .unwrap();
    reconciler.reconcile(now + 30).await.unwrap();
    assert!(has_wake(&bus).await);
    assert!(store.get_task_events(&future.task_id, None).await.unwrap().events.iter().any(|event|
        matches!(&event.payload, TaskEventPayload::TaskRecovered { run_id: Some(id), .. } if id == &future.id)));
}

#[tokio::test]
async fn waiting_review_with_current_execution_lease_does_not_recover_or_wake() {
    let runtime = runtime().await;
    let store = runtime.service().store();
    let task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    let run = task.run.unwrap();
    let now = chrono::Utc::now().timestamp();
    TaskExecutionHandle::new(
        store.clone(),
        runtime.event_bus(),
        task.task.id,
        run.id.clone(),
    )
    .mark_started(now)
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors
        .register(Arc::new(DormantIsolationExecutor(calls.clone(), None)))
        .await;
    let bus = Arc::new(crate::event_bus::TaskEventBus::new());
    let scheduler = TaskScheduler::new(store.clone(), bus.clone(), executors.clone());
    let reconciler =
        TaskStartupReconciler::new(store.clone(), bus.clone(), executors, scheduler.handle());
    reconciler.reconcile(now).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let lease = store
        .load_execution_for_run(&run.id)
        .await
        .unwrap()
        .unwrap();
    assert!(lease.worker_id.is_some() && lease.lease_until.is_some_and(|at| at > now + 12));
    let _ = tokio::time::timeout(Duration::ZERO, bus.scheduler_notify().notified()).await;
    // Foreign/repair status changed before the old execution owner released its
    // lease. Review recovery cannot bypass that still-current ownership proof.
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(&run.id))
        .col_expr(
            pioneer_entity::task_run::Column::Status,
            sea_orm::sea_query::Expr::value("waiting_review"),
        )
        .exec(&store.database_connection())
        .await
        .unwrap();
    for at in [now + 6, now + 12] {
        reconciler.reconcile(at).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            tokio::time::timeout(Duration::ZERO, bus.scheduler_notify().notified())
                .await
                .is_err()
        );
    }
    // A normal heartbeat may shorten the wall-clock lease. It must wake the
    // parked owner and replace its cached lease without waiting for that lease.
    let db = store.database_connection();
    let mut wake = db.work_signal("tasks.lifecycle").subscribe();
    wake.begin_pass(&db).await.unwrap();
    store
        .heartbeat_execution(&lease.id, now + 12, Some(now + 20))
        .await
        .unwrap()
        .unwrap();
    assert!(futures_util::FutureExt::now_or_never(wake.wait(None)).is_some());
    reconciler.reconcile(now + 13).await.unwrap();
    assert_eq!(
        reconciler.next_wake_deadline(now + 13).await.unwrap(),
        Some(now + 20)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    wake.begin_pass(&db).await.unwrap();
    store
        .heartbeat_execution(&lease.id, now + 14, Some(now + 21))
        .await
        .unwrap()
        .unwrap();
    assert!(
        futures_util::FutureExt::now_or_never(wake.wait(None)).is_none(),
        "ordinary extension stays quiet"
    );
    store
        .heartbeat_execution(&lease.id, now + 14, Some(now + 20))
        .await
        .unwrap()
        .unwrap();
    reconciler.reconcile(now + 20).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "durable earlier lease releases recovery"
    );
}

#[tokio::test]
async fn task_commit_wakes_independent_runtime_subscribers_without_bus_delivery() {
    let runtime = runtime().await;
    let database = runtime.service().store().database_connection();
    // Separate facade instances share the physical runtime, not a TaskEventBus.
    let observer = pioneer_crud::CrudStore::new(database.clone());
    let signal = observer
        .database_connection()
        .work_signal("tasks.scheduler");
    let mut first = signal.subscribe();
    let mut second = signal.subscribe();
    first.begin_pass(&database).await.unwrap();
    second.begin_pass(&database).await.unwrap();
    runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        first.changed().await;
        second.changed().await;
    })
    .await
    .expect("each independent consumer must see the durable Task commit");
}

#[tokio::test]
async fn identical_actor_contract_upsert_does_not_reopen_scheduler_discovery() {
    let runtime = runtime().await;
    let task = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    let store = runtime.service().store();
    let contract = store
        .get_task_actor_contract(&task.task.id)
        .await
        .unwrap()
        .unwrap();
    let database = store.database_connection();
    let mut changes = database.work_signal("tasks.scheduler").subscribe();
    changes.begin_pass(&database).await.unwrap();
    let other_facade = pioneer_crud::CrudStore::new(database.clone());
    other_facade
        .upsert_task_actor_contract(&contract, task.task.created_at)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), changes.changed())
            .await
            .is_err(),
        "an immutable idempotent replay must not restart empty scheduler discovery"
    );
}

#[tokio::test]
async fn waiting_review_recovery_error_retries_without_another_recovery_event() {
    use std::sync::atomic::AtomicBool;
    let runtime = runtime().await;
    let store = runtime.service().store();
    let created = runtime
        .service()
        .create_task(
            TaskCreateContext::default(),
            create_params(TaskTriggerSpec::Immediate),
        )
        .await
        .unwrap();
    let run = created.run.unwrap();
    let now = run.created_at;
    TaskExecutionHandle::new(
        store.clone(),
        runtime.event_bus(),
        created.task.id,
        run.id.clone(),
    )
    .mark_started(now)
    .await
    .unwrap();
    // A durable waiting-review run with no current execution lease needs the
    // executor to reconstruct review work; no candidate/auto-accept clock exists.
    pioneer_entity::task_run::Entity::update_many()
        .filter(pioneer_entity::task_run::Column::Id.eq(&run.id))
        .col_expr(
            pioneer_entity::task_run::Column::Status,
            sea_orm::sea_query::Expr::value("waiting_review"),
        )
        .exec(&store.database_connection())
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::new(AtomicBool::new(true));
    let executors = Arc::new(TaskExecutorRegistry::new());
    executors
        .register(Arc::new(DormantIsolationExecutor(
            calls.clone(),
            Some(fail.clone()),
        )))
        .await;
    let bus = runtime.event_bus();
    let scheduler = TaskScheduler::new(store.clone(), bus.clone(), executors.clone());
    let reconciler = TaskStartupReconciler::new(store.clone(), bus, executors, scheduler.handle());
    assert!(reconciler.reconcile(now).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        reconciler.next_wake_deadline(now).await.unwrap(),
        Some(now + 5)
    );
    store
        .database_connection()
        .work_signal("tasks.lifecycle")
        .notify();
    reconciler.reconcile(now + 1).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "backoff cannot be bypassed by its own event"
    );
    assert!(reconciler.reconcile(now + 5).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let recovered_events = store
        .get_task_events(&run.task_id, None)
        .await
        .unwrap()
        .events
        .into_iter()
        .filter(|event| {
            matches!(&event.payload,
            TaskEventPayload::TaskRecovered { run_id: Some(id), .. } if id == &run.id)
        })
        .count();
    assert_eq!(
        recovered_events, 1,
        "the existing recovery event does not prove executor success"
    );
    assert_eq!(
        reconciler.next_wake_deadline(now + 5).await.unwrap(),
        Some(now + 10)
    );
    fail.store(false, Ordering::SeqCst);
    reconciler.reconcile(now + 10).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        store.get_task_run(&run.id).await.unwrap().unwrap().status,
        TaskRunStatus::WaitingReview
    );
    assert_eq!(reconciler.next_wake_deadline(now + 10).await.unwrap(), None);
}
