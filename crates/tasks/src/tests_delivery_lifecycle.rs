use super::*;
use pioneer_crud::{
    TaskDeliveryCommitTestKind as CommitKind, TaskDeliveryTransitionOutcome as Outcome,
};
use pioneer_protocol::{TaskDelivery, TaskDeliveryAttempt, TaskDeliveryAttemptStatus, TaskEvent};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};
use std::future::{Future, poll_fn};
use std::task::Poll;

const DELIVERY_TIME: i64 = 4_000_000_000;

async fn delivery_fixture() -> (TaskRuntime, TaskDelivery) {
    let runtime = runtime().await;
    let delivery = queue_delivery(&runtime).await;
    (runtime, delivery)
}

async fn queue_delivery(runtime: &TaskRuntime) -> TaskDelivery {
    runtime
        .register_executor(Arc::new(CompletingSystemExecutor))
        .await;
    let mut params = create_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(DELIVERY_TIME),
        catch_up_policy: None,
    });
    params.owner_kind = TaskOwnerKind::Thread;
    params.owner_id = Some("thr_owner".to_owned());
    params.created_by_thread_id = Some("thr_owner".to_owned());
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode: TaskDeliveryMode::Thread,
        thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread),
        thread_id: Some("thr_owner".to_owned()),
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::Summary,
    });
    let task = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap()
        .task;
    runtime.process_due_once(DELIVERY_TIME).await.unwrap();
    runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            task_id: Some(task.id),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap()
        .deliveries
        .remove(0)
}

fn gate(runtime: &TaskRuntime, kind: CommitKind) -> (Arc<Notify>, Arc<Notify>) {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    runtime.service().store().set_delivery_commit_gate_for_test(
        kind,
        entered.clone(),
        release.clone(),
    );
    (entered, release)
}

async fn reached(entered: &Notify) {
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("prepared operation must reach its gate");
}

async fn started(runtime: &TaskRuntime, id: &str, at: i64) -> (TaskDelivery, TaskDeliveryAttempt) {
    match runtime
        .background_control_service()
        .start_delivery(id, at)
        .await
        .unwrap()
    {
        Outcome::Applied(value) => value,
        Outcome::Superseded => panic!("fixture start lost ownership"),
    }
}

async fn cancel(runtime: &TaskRuntime, task_id: &str) -> pioneer_protocol::TaskCancelResponse {
    runtime
        .service()
        .cancel_task(
            TaskMutationContext::default(),
            TaskCancelParams {
                task_id: task_id.to_owned(),
                reason: Some("cancel_delivery_test".to_owned()),
                scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
            },
        )
        .await
        .unwrap()
}

#[derive(Debug, PartialEq)]
struct DurableSnapshot {
    deliveries: Vec<TaskDelivery>,
    attempts: Vec<TaskDeliveryAttempt>,
    events: Vec<TaskEvent>,
    authority_status: String,
}

async fn snapshot(runtime: &TaskRuntime, delivery: &TaskDelivery) -> DurableSnapshot {
    let service = runtime.service();
    let response = service
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: delivery.workspace_id.clone(),
            task_id: Some(delivery.task_id.clone()),
            run_id: Some(delivery.run_id.clone()),
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap();
    let events = service
        .get_task_events(TaskEventsParams {
            task_id: delivery.task_id.clone(),
            after_sequence: None,
            limit: Some(100),
        })
        .await
        .unwrap()
        .events;
    let authority_status = service
        .store()
        .database_connection()
        .query_one_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "SELECT status FROM task_delivery_authority WHERE delivery_id=?",
            [delivery.id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "status")
        .unwrap();
    DurableSnapshot {
        deliveries: response.deliveries,
        attempts: response.attempts,
        events,
        authority_status,
    }
}

async fn assert_no_wake(subscription: &mut crate::TaskEventSubscription) {
    // A single poll establishes absence after the losing call has returned.
    // No sleep or timing-dependent negative assertion is needed.
    poll_fn(|cx| {
        let mut recv = Box::pin(subscription.recv());
        assert!(
            matches!(recv.as_mut().poll(cx), Poll::Pending),
            "losing operation published a wake"
        );
        Poll::Ready(())
    })
    .await;
}

fn subscribe(runtime: &TaskRuntime, task_id: &str) -> crate::TaskEventSubscription {
    runtime.event_bus().subscribe(crate::TaskEventFilter {
        task_ids: vec![task_id.to_owned()],
        ..Default::default()
    })
}

async fn succeed(
    service: &crate::TaskService,
    delivery: TaskDelivery,
    attempt: TaskDeliveryAttempt,
    at: i64,
) -> TaskRuntimeResult<Outcome<TaskDelivery>> {
    service
        .complete_delivery(
            delivery,
            attempt,
            Some("turn_exact_receipt".to_owned()),
            None,
            Some(204),
            Some("committed_success_fingerprint".to_owned()),
            at,
        )
        .await
}

async fn fail(
    service: &crate::TaskService,
    delivery: TaskDelivery,
    attempt: TaskDeliveryAttempt,
    at: i64,
) -> TaskRuntimeResult<Outcome<TaskDelivery>> {
    service
        .fail_delivery(
            delivery,
            attempt,
            "delivery_test_failure".to_owned(),
            Some(503),
            Some("committed_failure_fingerprint".to_owned()),
            at,
        )
        .await
}

#[tokio::test]
async fn delivery_cancel_between_start_preparation_and_commit_creates_no_attempt() {
    let (runtime, delivery) = delivery_fixture().await;
    let (entered, release) = gate(&runtime, CommitKind::Start);
    let service = runtime.background_control_service();
    let id = delivery.id.clone();
    let operation = tokio::spawn(async move { service.start_delivery(&id, DELIVERY_TIME).await });
    reached(&entered).await;
    cancel(&runtime, &delivery.task_id).await;
    let before = snapshot(&runtime, &delivery).await;
    let mut subscription = subscribe(&runtime, &delivery.task_id);
    release.notify_one();
    assert!(matches!(
        operation.await.unwrap().unwrap(),
        Outcome::Superseded
    ));
    assert_eq!(snapshot(&runtime, &delivery).await, before);
    assert_eq!(before.deliveries[0].status, TaskDeliveryStatus::Cancelled);
    assert_eq!(before.deliveries[0].attempt_count, 0);
    assert!(before.attempts.is_empty());
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_cancel_before_failure_commit_preserves_cancelled_attempt_fields() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Finish);
    let service = runtime.background_control_service();
    let operation =
        tokio::spawn(async move { fail(&service, delivery, attempt, DELIVERY_TIME + 1).await });
    reached(&entered).await;
    cancel(&runtime, &queued.task_id).await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert!(matches!(
        operation.await.unwrap().unwrap(),
        Outcome::Superseded
    ));
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_eq!(before.authority_status, "cancelled");
    assert_eq!(before.attempts[0].status, TaskDeliveryAttemptStatus::Failed);
    assert_eq!(
        before.attempts[0].error.as_deref(),
        Some("cancel_delivery_test")
    );
    assert!(before.attempts[0].completed_at.is_some());
    assert!(before.attempts[0].http_status.is_none());
    assert!(before.attempts[0].response_fingerprint.is_none());
    assert!(before.deliveries[0].next_attempt_at.is_none());
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_cancel_before_success_commit_adds_no_receipt_or_event() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Finish);
    let service = runtime.background_control_service();
    let operation =
        tokio::spawn(async move { succeed(&service, delivery, attempt, DELIVERY_TIME + 1).await });
    reached(&entered).await;
    cancel(&runtime, &queued.task_id).await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert!(matches!(
        operation.await.unwrap().unwrap(),
        Outcome::Superseded
    ));
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert!(before.deliveries[0].delivered_at.is_none());
    assert!(before.deliveries[0].delivered_turn_id.is_none());
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_recovery_discovery_loses_to_cancellation_without_counting_it() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Recovery);
    let service = runtime.background_control_service();
    let operation = tokio::spawn(async move {
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 10)
            .await
    });
    reached(&entered).await;
    cancel(&runtime, &queued.task_id).await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert_eq!(operation.await.unwrap().unwrap().recovered, 0);
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_recovery_discovery_loses_to_success_and_retains_receipt() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Recovery);
    let service = runtime.background_control_service();
    let operation = tokio::spawn(async move {
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 10)
            .await
    });
    reached(&entered).await;
    assert!(matches!(
        succeed(
            &runtime.background_control_service(),
            delivery,
            attempt,
            DELIVERY_TIME + 302
        )
        .await
        .unwrap(),
        Outcome::Applied(_)
    ));
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert_eq!(operation.await.unwrap().unwrap().recovered, 0);
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_eq!(before.attempts[0].http_status, Some(204));
    assert_eq!(
        before.attempts[0].response_fingerprint.as_deref(),
        Some("committed_success_fingerprint")
    );
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_old_attempt_success_and_failure_cannot_change_the_next_attempt() {
    let (runtime, queued) = delivery_fixture().await;
    let (old_delivery, old_attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    assert!(matches!(
        fail(
            &runtime.background_control_service(),
            old_delivery.clone(),
            old_attempt.clone(),
            DELIVERY_TIME + 1
        )
        .await
        .unwrap(),
        Outcome::Applied(_)
    ));
    let (next, next_attempt) = started(&runtime, &queued.id, DELIVERY_TIME + 61).await;
    assert_eq!(next_attempt.attempt_number, old_attempt.attempt_number + 1);
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    assert!(matches!(
        succeed(
            &runtime.background_control_service(),
            old_delivery.clone(),
            old_attempt.clone(),
            DELIVERY_TIME + 62
        )
        .await
        .unwrap(),
        Outcome::Superseded
    ));
    assert!(matches!(
        fail(
            &runtime.background_control_service(),
            old_delivery,
            old_attempt,
            DELIVERY_TIME + 63
        )
        .await
        .unwrap(),
        Outcome::Superseded
    ));
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_eq!(before.attempts[0].http_status, Some(503));
    assert_eq!(before.attempts[0].completed_at, Some(DELIVERY_TIME + 1));
    assert_eq!(
        before.attempts[0].error.as_deref(),
        Some("delivery_test_failure")
    );
    assert_eq!(
        before.attempts[0].response_fingerprint.as_deref(),
        Some("committed_failure_fingerprint")
    );
    assert_eq!(before.deliveries[0], next);
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_competing_starts_issue_only_one_exact_attempt() {
    let (runtime, queued) = delivery_fixture().await;
    let (entered, release) = gate(&runtime, CommitKind::Start);
    let service = runtime.background_control_service();
    let id = queued.id.clone();
    let first = tokio::spawn(async move { service.start_delivery(&id, DELIVERY_TIME).await });
    reached(&entered).await;
    let (_, winner) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert!(matches!(first.await.unwrap().unwrap(), Outcome::Superseded));
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_eq!(before.attempts, vec![winner]);
    assert_eq!(before.deliveries[0].attempt_count, 1);
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_stale_cancellation_refreshes_after_worker_start_in_its_task_batch() {
    let (runtime, queued) = delivery_fixture().await;
    let (entered, release) = gate(&runtime, CommitKind::Cancellation);
    let service = runtime.service();
    let task_id = queued.task_id.clone();
    let operation = tokio::spawn(async move {
        service
            .cancel_task(
                TaskMutationContext::default(),
                TaskCancelParams {
                    task_id,
                    reason: Some("cancel_delivery_test".to_owned()),
                    scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
                },
            )
            .await
    });
    reached(&entered).await;
    let (_, active) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    release.notify_one();
    let response = operation.await.unwrap().unwrap();
    let state = snapshot(&runtime, &queued).await;
    assert_eq!(state.deliveries[0].status, TaskDeliveryStatus::Cancelled);
    assert_eq!(state.authority_status, "cancelled");
    assert_eq!(state.deliveries[0].attempt_count, active.attempt_number);
    assert_eq!(state.attempts[0].id, active.id);
    assert_eq!(state.attempts[0].status, TaskDeliveryAttemptStatus::Failed);
    assert_eq!(
        state.attempts[0].error.as_deref(),
        Some("cancel_delivery_test")
    );
    assert_eq!(response.cancelled_deliveries, state.deliveries);
    let cancellation = state
        .events
        .iter()
        .position(|event| matches!(event.payload, TaskEventPayload::DeliveryCancelled { .. }))
        .unwrap();
    let task_cancel = state
        .events
        .iter()
        .position(|event| matches!(event.payload, TaskEventPayload::TaskCancelled { .. }))
        .unwrap();
    assert!(cancellation < task_cancel);
    assert_eq!(
        state
            .events
            .iter()
            .filter(|event| matches!(event.payload, TaskEventPayload::DeliveryCancelled { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn delivery_stale_cancellation_loses_to_success_without_rewriting_attempt() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Cancellation);
    let service = runtime.service();
    let task_id = queued.task_id.clone();
    let operation = tokio::spawn(async move {
        service
            .cancel_task(
                TaskMutationContext::default(),
                TaskCancelParams {
                    task_id,
                    reason: Some("cancel_delivery_test".to_owned()),
                    scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
                },
            )
            .await
    });
    reached(&entered).await;
    succeed(
        &runtime.background_control_service(),
        delivery,
        attempt,
        DELIVERY_TIME + 1,
    )
    .await
    .unwrap();
    let before = snapshot(&runtime, &queued).await;
    release.notify_one();
    assert!(
        operation
            .await
            .unwrap()
            .unwrap()
            .cancelled_deliveries
            .is_empty()
    );
    let after = snapshot(&runtime, &queued).await;
    assert_eq!(after.deliveries, before.deliveries);
    assert_eq!(after.attempts, before.attempts);
    assert_eq!(after.authority_status, "delivered");
    assert!(
        !after
            .events
            .iter()
            .any(|event| matches!(event.payload, TaskEventPayload::DeliveryCancelled { .. }))
    );
    assert!(
        after
            .events
            .iter()
            .any(|event| matches!(event.payload, TaskEventPayload::TaskCancelled { .. }))
    );
}

#[tokio::test]
async fn delivery_stale_cancellation_after_retry_closes_the_current_attempt() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Cancellation);
    let service = runtime.service();
    let task_id = queued.task_id.clone();
    let operation = tokio::spawn(async move {
        service
            .cancel_task(
                TaskMutationContext::default(),
                TaskCancelParams {
                    task_id,
                    reason: Some("cancel_delivery_test".to_owned()),
                    scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
                },
            )
            .await
    });
    reached(&entered).await;
    fail(
        &runtime.background_control_service(),
        delivery,
        attempt,
        DELIVERY_TIME + 1,
    )
    .await
    .unwrap();
    let failed = snapshot(&runtime, &queued).await.attempts.remove(0);
    let (_, next) = started(&runtime, &queued.id, DELIVERY_TIME + 61).await;
    release.notify_one();
    operation.await.unwrap().unwrap();
    let state = snapshot(&runtime, &queued).await;
    assert_eq!(state.attempts[0], failed);
    assert_eq!(state.attempts[1].id, next.id);
    assert_eq!(state.attempts[1].status, TaskDeliveryAttemptStatus::Failed);
    assert_eq!(
        state.attempts[1].error.as_deref(),
        Some("cancel_delivery_test")
    );
    assert_eq!(state.deliveries[0].attempt_count, 2);
    assert_eq!(state.authority_status, "cancelled");
}

#[tokio::test]
async fn delivery_ordinary_retry_budget_exhaustion_and_recovery_remain_exact() {
    let (runtime, queued) = delivery_fixture().await;
    let service = runtime.background_control_service();
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let Outcome::Applied(pending) = fail(&service, delivery, attempt, DELIVERY_TIME + 1)
        .await
        .unwrap()
    else {
        panic!("failure not applied")
    };
    assert_eq!(pending.next_attempt_at, Some(DELIVERY_TIME + 61));
    assert!(matches!(
        service
            .start_delivery(&queued.id, DELIVERY_TIME + 60)
            .await
            .unwrap(),
        Outcome::Superseded
    ));
    started(&runtime, &queued.id, DELIVERY_TIME + 61).await;
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 362, 10)
            .await
            .unwrap()
            .recovered,
        1
    );
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 362, 10)
            .await
            .unwrap()
            .recovered,
        0
    );
    let recovered = snapshot(&runtime, &queued).await;
    assert_eq!(
        recovered.deliveries[0].next_attempt_at,
        Some(DELIVERY_TIME + 362)
    );
    assert_eq!(
        recovered.attempts[1].error.as_deref(),
        Some("task_delivery_recovered")
    );
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME + 362).await;
    let Outcome::Applied(failed) = fail(&service, delivery, attempt, DELIVERY_TIME + 363)
        .await
        .unwrap()
    else {
        panic!("exhaustion not applied")
    };
    assert_eq!(failed.attempt_count, queued.max_attempts);
    assert_eq!(failed.status, TaskDeliveryStatus::Failed);
    assert!(failed.next_attempt_at.is_none());
    let before = snapshot(&runtime, &queued).await;
    assert!(matches!(
        service
            .start_delivery(&queued.id, DELIVERY_TIME + 999)
            .await
            .unwrap(),
        Outcome::Superseded
    ));
    assert_eq!(snapshot(&runtime, &queued).await, before);
}

#[tokio::test]
async fn delivery_immutable_and_ownership_errors_are_not_superseded() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    cancel(&runtime, &queued.task_id).await;
    let before = snapshot(&runtime, &queued).await;
    let service = runtime.background_control_service();
    let mut altered = delivery.clone();
    altered.delivery_key.push_str("_redirected");
    assert!(
        fail(&service, altered, attempt.clone(), DELIVERY_TIME + 1)
            .await
            .is_err()
    );
    let mut altered = delivery.clone();
    altered.result_snapshot.as_mut().unwrap().summary = Some("different result".to_owned());
    assert!(
        succeed(&service, altered, attempt.clone(), DELIVERY_TIME + 1)
            .await
            .is_err()
    );
    let mut altered = delivery.clone();
    altered.target_thread_id = Some("different_thread".to_owned());
    assert!(
        fail(&service, altered, attempt.clone(), DELIVERY_TIME + 1)
            .await
            .is_err()
    );
    let mut unknown = attempt.clone();
    unknown.id = "unknown_attempt".to_owned();
    assert!(
        fail(&service, delivery.clone(), unknown, DELIVERY_TIME + 1)
            .await
            .is_err()
    );
    let mut wrong = attempt.clone();
    wrong.started_at += 1;
    assert!(
        fail(&service, delivery, wrong, DELIVERY_TIME + 1)
            .await
            .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
}

async fn sql(runtime: &TaskRuntime, statement: &str) {
    runtime
        .service()
        .store()
        .database_connection()
        .execute_raw(Statement::from_string(
            sea_orm::DbBackend::Sqlite,
            statement.to_owned(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn delivery_authority_changes_remain_errors_for_losing_workers() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    cancel(&runtime, &queued.task_id).await;
    sql(
        &runtime,
        "UPDATE task_delivery_authority SET disclosure_generation=disclosure_generation+1",
    )
    .await;
    let before = snapshot(&runtime, &queued).await;
    assert!(
        fail(
            &runtime.background_control_service(),
            delivery,
            attempt,
            DELIVERY_TIME + 1
        )
        .await
        .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
}

#[tokio::test]
async fn delivery_dependent_attempt_write_failure_rolls_back_event_and_projection() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    sql(&runtime, "CREATE TRIGGER reject_delivery_attempt_update BEFORE UPDATE ON task_delivery_attempt BEGIN SELECT RAISE(ABORT, 'test dependent attempt storage failure'); END").await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    assert!(
        succeed(
            &runtime.background_control_service(),
            delivery,
            attempt,
            DELIVERY_TIME + 1
        )
        .await
        .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
    sql(&runtime, "DROP TRIGGER reject_delivery_attempt_update").await;
}

#[tokio::test]
async fn delivery_cancellation_batch_rolls_back_if_its_dependent_write_fails() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    sql(&runtime, "CREATE TRIGGER reject_delivery_cancel_attempt BEFORE UPDATE ON task_delivery_attempt BEGIN SELECT RAISE(ABORT, 'test cancellation storage failure'); END").await;
    let before = snapshot(&runtime, &queued).await;
    let service = runtime.service();
    let mut subscription = subscribe(&runtime, &queued.task_id);
    assert!(
        service
            .cancel_task(
                TaskMutationContext::default(),
                TaskCancelParams {
                    task_id: queued.task_id.clone(),
                    reason: Some("cancel_delivery_test".to_owned()),
                    scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
                }
            )
            .await
            .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
    sql(&runtime, "DROP TRIGGER reject_delivery_cancel_attempt").await;
}

#[tokio::test]
async fn delivery_general_event_ingestion_keeps_strict_terminal_fsm() {
    let (runtime, queued) = delivery_fixture().await;
    let (mut delivery, mut attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    cancel(&runtime, &queued.task_id).await;
    let before = snapshot(&runtime, &queued).await;
    delivery.status = TaskDeliveryStatus::Pending;
    delivery.next_attempt_at = Some(DELIVERY_TIME + 60);
    delivery.last_error = Some("late_failure".to_owned());
    attempt.status = TaskDeliveryAttemptStatus::Failed;
    attempt.error = Some("late_failure".to_owned());
    attempt.completed_at = Some(DELIVERY_TIME + 1);
    assert!(
        runtime
            .service()
            .store()
            .append_task_event(
                TaskEventPayload::DeliveryFailed { delivery, attempt },
                DELIVERY_TIME + 1
            )
            .await
            .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
}

#[derive(Default)]
struct DeliveryRoutingObserver {
    writes: std::sync::Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
    reads: std::sync::Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
    queued: Notify,
    cancelled: Notify,
    watch_queue: std::sync::atomic::AtomicBool,
}

impl pioneer_sqlite::SqliteWriteObserver for DeliveryRoutingObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        let queued = matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. });
        let cancelled = matches!(event, pioneer_sqlite::SqliteWriteEvent::Cancelled { .. });
        self.writes.lock().unwrap().push(event);
        if queued && self.watch_queue.load(Ordering::SeqCst) {
            self.queued.notify_one();
        }
        if cancelled {
            self.cancelled.notify_one();
        }
    }
}
impl pioneer_sqlite::SqliteReadObserver for DeliveryRoutingObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.reads.lock().unwrap().push(event);
    }
}

impl DeliveryRoutingObserver {
    fn clear(&self) {
        self.writes.lock().unwrap().clear();
        self.reads.lock().unwrap().clear();
    }
}

async fn observed_delivery_fixture() -> (TaskRuntime, TaskDelivery, Arc<DeliveryRoutingObserver>) {
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&connection, None).await.unwrap();
    seed_task_test_workspace(&connection).await;
    let observer = Arc::new(DeliveryRoutingObserver::default());
    let executor =
        pioneer_sqlite::SqliteWriteExecutor::with_observer(connection.clone(), observer.clone());
    let database = pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(
        connection,
        executor,
        observer.clone(),
    );
    let runtime = TaskRuntime::new(Arc::new(CrudStore::new(database)));
    let delivery = queue_delivery(&runtime).await;
    (runtime, delivery, observer)
}

#[tokio::test]
async fn delivery_commit_uses_scoped_writer_and_maintenance_discovery() {
    let (runtime, queued, observer) = observed_delivery_fixture().await;
    observer.clear();
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let writes = observer.writes.lock().unwrap();
    assert!(!writes.is_empty());
    for event in writes.iter() {
        let class = match event {
            pioneer_sqlite::SqliteWriteEvent::Enqueued { class, .. }
            | pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. }
            | pioneer_sqlite::SqliteWriteEvent::Released { class, .. }
            | pioneer_sqlite::SqliteWriteEvent::Cancelled { class, .. } => class,
        };
        // This is the pre-existing Gateway background control scope.
        assert_eq!(*class, pioneer_sqlite::SqliteWriteClass::Critical);
    }
    assert_eq!(
        writes
            .iter()
            .filter(|event| matches!(event, pioneer_sqlite::SqliteWriteEvent::Acquired { .. }))
            .count(),
        1,
        "point reads and projection writes must share one physical writer transaction"
    );
    assert_eq!(
        writes
            .iter()
            .filter(|event| matches!(event, pioneer_sqlite::SqliteWriteEvent::Released { .. }))
            .count(),
        1
    );
    drop(writes);
    let reads = observer.reads.lock().unwrap();
    assert!(reads.iter().any(|event| matches!(
        event,
        pioneer_sqlite::SqliteReadEvent::OperationFinished {
            class: pioneer_sqlite::SqliteReadClass::Maintenance,
            ..
        }
    )));
    assert!(!reads.iter().any(|event| matches!(
        event,
        pioneer_sqlite::SqliteReadEvent::OperationFinished {
            class: pioneer_sqlite::SqliteReadClass::Interactive,
            ..
        }
    )));
}

#[tokio::test]
async fn delivery_cancelled_commit_wait_releases_queue_and_allows_next_delivery() {
    let (runtime, queued, observer) = observed_delivery_fixture().await;
    let (entered, release) = gate(&runtime, CommitKind::Start);
    let service = runtime.background_control_service();
    let id = queued.id.clone();
    let operation = tokio::spawn(async move { service.start_delivery(&id, DELIVERY_TIME).await });
    reached(&entered).await;
    let database = runtime.service().store().database_connection();
    let transaction = database.begin().await.unwrap();
    observer.clear();
    observer.watch_queue.store(true, Ordering::SeqCst);
    release.notify_one();
    reached(&observer.queued).await;
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    reached(&observer.cancelled).await;
    let cancellation = observer
        .writes
        .lock()
        .unwrap()
        .iter()
        .find_map(|event| {
            if let pioneer_sqlite::SqliteWriteEvent::Cancelled { class, queue, .. } = event {
                Some((*class, *queue))
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(cancellation.0, pioneer_sqlite::SqliteWriteClass::Critical);
    assert_eq!(
        cancellation.1,
        pioneer_sqlite::SqliteWriteQueueSnapshot::default()
    );
    transaction.rollback().await.unwrap();
    let before = snapshot(&runtime, &queued).await;
    assert_eq!(before.deliveries[0].status, TaskDeliveryStatus::Pending);
    assert!(before.attempts.is_empty());
    assert!(
        !before
            .events
            .iter()
            .any(|event| matches!(event.payload, TaskEventPayload::DeliveryStarted { .. }))
    );
    timeout(Duration::from_secs(5), async {
        started(&runtime, &queued.id, DELIVERY_TIME).await
    })
    .await
    .expect("cancelled queue entry must not retain writer or maintenance read permits");
}

#[tokio::test]
async fn delivery_recovery_rechecks_stuck_cutoff_inside_writer() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Recovery);
    let service = runtime.background_control_service();
    let operation = tokio::spawn(async move {
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 10)
            .await
    });
    reached(&entered).await;
    runtime
        .service()
        .store()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "UPDATE task_delivery SET updated_at=? WHERE id=?",
            [
                chrono::DateTime::from_timestamp(DELIVERY_TIME + 2, 0)
                    .unwrap()
                    .fixed_offset()
                    .into(),
                queued.id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert_eq!(operation.await.unwrap().unwrap().recovered, 0);
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_recovery_old_discovery_cannot_recover_the_next_attempt() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Recovery);
    let service = runtime.background_control_service();
    let operation = tokio::spawn(async move {
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 10)
            .await
    });
    reached(&entered).await;
    fail(
        &runtime.background_control_service(),
        delivery,
        attempt,
        DELIVERY_TIME + 2,
    )
    .await
    .unwrap();
    started(&runtime, &queued.id, DELIVERY_TIME + 302).await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert_eq!(operation.await.unwrap().unwrap().recovered, 0);
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_recovery_exhausts_the_existing_budget_without_increasing_it() {
    let (runtime, queued) = delivery_fixture().await;
    let service = runtime.background_control_service();
    let mut now = DELIVERY_TIME;
    for number in 1..=queued.max_attempts {
        let (delivery, attempt) = started(&runtime, &queued.id, now).await;
        assert_eq!(delivery.attempt_count, number);
        assert_eq!(attempt.attempt_number, number);
        now += 301;
        assert_eq!(
            service
                .recover_stuck_deliveries(now, 10)
                .await
                .unwrap()
                .recovered,
            1
        );
    }
    let final_state = snapshot(&runtime, &queued).await;
    assert_eq!(final_state.deliveries[0].status, TaskDeliveryStatus::Failed);
    assert_eq!(final_state.deliveries[0].attempt_count, queued.max_attempts);
    assert_eq!(final_state.deliveries[0].max_attempts, queued.max_attempts);
    assert!(final_state.deliveries[0].next_attempt_at.is_none());
    assert!(
        final_state
            .attempts
            .iter()
            .all(|attempt| attempt.error.as_deref() == Some("task_delivery_recovered"))
    );
    assert_eq!(
        service
            .recover_stuck_deliveries(now + 301, 10)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert_eq!(snapshot(&runtime, &queued).await, final_state);
}

#[tokio::test]
async fn delivery_actor_authority_changed_after_preparation_is_an_error() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let (entered, release) = gate(&runtime, CommitKind::Finish);
    let service = runtime.background_control_service();
    let operation =
        tokio::spawn(async move { fail(&service, delivery, attempt, DELIVERY_TIME + 1).await });
    reached(&entered).await;
    cancel(&runtime, &queued.task_id).await;
    sql(
        &runtime,
        "UPDATE task_actor_contract SET delivery_json='{}'",
    )
    .await;
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    release.notify_one();
    assert!(operation.await.unwrap().is_err());
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
}

#[tokio::test]
async fn delivery_missing_attempt_and_missing_receipt_are_errors_even_after_cancellation() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    cancel(&runtime, &queued.task_id).await;
    let service = runtime.background_control_service();
    let before = snapshot(&runtime, &queued).await;
    assert!(
        service
            .complete_delivery(
                delivery.clone(),
                attempt.clone(),
                None,
                None,
                None,
                None,
                DELIVERY_TIME + 1
            )
            .await
            .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
    sql(&runtime, "DELETE FROM task_delivery_attempt").await;
    let missing = snapshot(&runtime, &queued).await;
    assert!(
        fail(&service, delivery, attempt, DELIVERY_TIME + 1)
            .await
            .is_err()
    );
    assert_eq!(snapshot(&runtime, &queued).await, missing);
}

#[tokio::test]
async fn delivery_authority_review_selectors_use_bounded_ordered_indexes() {
    let (runtime, queued) = delivery_fixture().await;
    let database = runtime.service().store().database_connection();
    for (column, value, index) in [
        (
            "task_id",
            queued.task_id,
            "idx_task_agent_spec_task_updated",
        ),
        ("run_id", queued.run_id, "idx_task_agent_spec_run_updated"),
    ] {
        let plan = database.query_all_raw(Statement::from_sql_and_values(sea_orm::DbBackend::Sqlite,
            format!("EXPLAIN QUERY PLAN SELECT * FROM task_agent_spec WHERE {column}=? ORDER BY updated_at DESC LIMIT 1"), [value.into()],
        )).await.unwrap();
        let details: Vec<String> = plan
            .into_iter()
            .map(|row| row.try_get("", "detail").unwrap())
            .collect();
        assert!(
            details.iter().any(|detail| detail.contains(index)),
            "{details:?}"
        );
        assert!(
            !details.iter().any(|detail| detail.contains("TEMP B-TREE")),
            "{details:?}"
        );
    }
}

#[tokio::test]
async fn delivery_maintenance_and_interactive_commits_keep_their_own_scope_under_contention() {
    let (runtime, maintenance, observer) = observed_delivery_fixture().await;
    let interactive = queue_delivery(&runtime).await;
    let (maintenance_entered, maintenance_release) = gate(&runtime, CommitKind::Start);
    let service = runtime.maintenance_service();
    let id = maintenance.id.clone();
    let maintenance_operation =
        tokio::spawn(async move { service.start_delivery(&id, DELIVERY_TIME).await });
    reached(&maintenance_entered).await;
    let (interactive_entered, interactive_release) = gate(&runtime, CommitKind::Start);
    let service = runtime.service();
    let id = interactive.id.clone();
    let interactive_operation =
        tokio::spawn(async move { service.start_delivery(&id, DELIVERY_TIME).await });
    reached(&interactive_entered).await;
    let database = runtime.service().store().database_connection();
    let transaction = database.begin().await.unwrap();
    observer.clear();
    observer.watch_queue.store(true, Ordering::SeqCst);
    maintenance_release.notify_one();
    reached(&observer.queued).await;
    interactive_release.notify_one();
    // Notify is used only for readiness; inspect durable queue events to avoid
    // coalescing two notifications into an assumed second queue admission.
    timeout(Duration::from_secs(5), async {
        loop {
            let count = observer
                .writes
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, pioneer_sqlite::SqliteWriteEvent::Enqueued { .. }))
                .count();
            if count == 2 {
                break;
            }
            observer.queued.notified().await;
        }
    })
    .await
    .unwrap();
    transaction.rollback().await.unwrap();
    assert!(matches!(
        maintenance_operation.await.unwrap().unwrap(),
        Outcome::Applied(_)
    ));
    assert!(matches!(
        interactive_operation.await.unwrap().unwrap(),
        Outcome::Applied(_)
    ));
    let classes: Vec<_> = observer
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter_map(|event| {
            if let pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } = event {
                Some(*class)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(classes.len(), 2);
    assert!(classes.contains(&pioneer_sqlite::SqliteWriteClass::Maintenance));
    assert!(classes.contains(&pioneer_sqlite::SqliteWriteClass::Interactive));
    assert!(!classes.contains(&pioneer_sqlite::SqliteWriteClass::Critical));
    assert_eq!(snapshot(&runtime, &maintenance).await.attempts.len(), 1);
    assert_eq!(snapshot(&runtime, &interactive).await.attempts.len(), 1);
}

#[tokio::test]
async fn delivery_ordinary_success_is_terminal_and_duplicate_results_preserve_all_receipts() {
    let (runtime, queued) = delivery_fixture().await;
    let (delivery, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let service = runtime.background_control_service();
    let Outcome::Applied(result) = succeed(
        &service,
        delivery.clone(),
        attempt.clone(),
        DELIVERY_TIME + 1,
    )
    .await
    .unwrap() else {
        panic!("ordinary successful completion must be applied");
    };
    assert_eq!(result.status, TaskDeliveryStatus::Delivered);
    assert_eq!(
        result.delivered_turn_id.as_deref(),
        Some("turn_exact_receipt")
    );
    assert_eq!(result.delivered_at, Some(DELIVERY_TIME + 1));
    let before = snapshot(&runtime, &queued).await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    assert!(matches!(
        succeed(
            &service,
            delivery.clone(),
            attempt.clone(),
            DELIVERY_TIME + 2
        )
        .await
        .unwrap(),
        Outcome::Superseded
    ));
    assert!(matches!(
        fail(&service, delivery, attempt, DELIVERY_TIME + 3)
            .await
            .unwrap(),
        Outcome::Superseded
    ));
    assert!(matches!(
        service
            .start_delivery(&queued.id, DELIVERY_TIME + 4)
            .await
            .unwrap(),
        Outcome::Superseded
    ));
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
}

async fn recovery_snapshot(
    runtime: &TaskRuntime,
    id: &str,
) -> pioneer_crud::DeliveryRecoverySnapshot {
    runtime
        .service()
        .store()
        .with_maintenance_access()
        .task_delivery_recovery_snapshot(id)
        .await
        .unwrap()
        .unwrap()
}

async fn recovery_defer(
    runtime: &TaskRuntime,
    source: &pioneer_crud::DeliveryRecoverySnapshot,
    now: i64,
) -> bool {
    runtime
        .service()
        .store()
        .with_maintenance_access()
        .defer_task_delivery_recovery(source, &|| now)
        .await
        .unwrap()
}

async fn delete_exact_attempt(runtime: &TaskRuntime, attempt: &TaskDeliveryAttempt) {
    runtime
        .service()
        .store()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "DELETE FROM task_delivery_attempt WHERE id=?",
            [attempt.id.clone().into()],
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn recovery_missing_first_attempt_does_not_abort_or_change_domain_schedule() {
    let (runtime, poison) = delivery_fixture().await;
    let (_, missing) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &missing).await;
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 1).await;
    let before = recovery_snapshot(&runtime, &poison.id).await.delivery;
    assert_eq!(
        runtime
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
            .unwrap()
            .recovered,
        1
    );
    let after = recovery_snapshot(&runtime, &poison.id).await;
    assert_eq!(after.delivery, before);
    assert!(
        after.attempt.is_none(),
        "must not manufacture an exact attempt"
    );
    let retry = after.retry.unwrap();
    assert!(retry.expected_attempt_id.is_none());
    assert_eq!(retry.expected_attempt_count, 1);
    assert_eq!(retry.next_probe_at, DELIVERY_TIME + 306);
    assert_eq!(retry.attempts, 1);
    assert_eq!(
        runtime
            .service()
            .store()
            .get_task_delivery(&healthy.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskDeliveryStatus::Pending
    );
}

#[tokio::test]
async fn recovery_error_delay_survives_restart_and_due_source_duplicate_is_processed_once() {
    let (runtime, poison) = delivery_fixture().await;
    let (_, attempt) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &attempt).await;
    let source = recovery_snapshot(&runtime, &poison.id).await;
    assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 301).await);
    let initial = recovery_snapshot(&runtime, &poison.id).await;
    let restarted = TaskRuntime::new(runtime.service().store());
    assert_eq!(
        restarted
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 302, 64)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert_eq!(
        recovery_snapshot(&runtime, &poison.id).await.retry,
        initial.retry
    );
    assert_eq!(
        restarted
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 306, 64)
            .await
            .unwrap()
            .recovered,
        0
    );
    let retry = recovery_snapshot(&runtime, &poison.id).await.retry.unwrap();
    assert_eq!(
        retry.attempts, 2,
        "source/due duplicate consumes inputs but receives only one handler"
    );
    assert_eq!(retry.next_probe_at, DELIVERY_TIME + 316);
    assert_ne!(
        Some(retry.retry_token),
        initial.retry.map(|r| r.retry_token)
    );
}

#[tokio::test]
async fn recovery_raw_pages_pass_more_than_64_delayed_inputs_with_timestamp_ties() {
    let (runtime, queued) = delivery_fixture().await;
    let store = runtime.service().store().with_maintenance_access();
    // Discovery-only fixtures: keep the real task/run FK facts, but do not
    // pretend these cloned deliveries possess domain authority or attempts.
    let template = recovery_snapshot(&runtime, &queued.id).await.delivery;
    for number in 0..130 {
        let mut row: pioneer_entity::task_delivery::ActiveModel = template.clone().into();
        row.id = Set(format!("recovery_input_{number:03}"));
        row.delivery_key = Set(format!("recovery_input_key_{number:03}"));
        row.status = Set("delivering".to_owned());
        row.attempt_count = Set(1);
        row.next_attempt_at = Set(None);
        pioneer_entity::task_delivery::Entity::insert(row)
            .exec(&store.database_connection())
            .await
            .unwrap();
        let source = recovery_snapshot(&runtime, &format!("recovery_input_{number:03}")).await;
        assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 1_000).await);
    }
    let mut cursor = None;
    let mut ids = std::collections::BTreeSet::new();
    for count in [64, 64, 2] {
        let page = store
            .task_delivery_recovery_page(DELIVERY_TIME + 300, cursor.as_ref(), 64)
            .await
            .unwrap();
        assert_eq!(
            page.len(),
            count,
            "delayed rows must consume the raw input budget"
        );
        for row in &page {
            assert!(ids.insert(row.id.clone()));
        }
        cursor = page.last().cloned();
    }
    assert_eq!(ids.len(), 130);
    assert!(
        store
            .task_delivery_recovery_page(DELIVERY_TIME + 300, cursor.as_ref(), 64)
            .await
            .unwrap()
            .is_empty()
    );
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 1).await;
    let service = runtime.background_control_service();
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
            .unwrap()
            .recovered,
        1,
        "raw cursor must pass the delayed prefix and reach healthy current work"
    );
}

#[tokio::test]
async fn recovery_fair_source_retry_mix_leaves_each_source_some_budget() {
    let (runtime, poison) = delivery_fixture().await;
    let (_, missing) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &missing).await;
    let source = recovery_snapshot(&runtime, &poison.id).await;
    assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 301).await);
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 1).await;
    // One due retry and one raw source input. Duplicate poison still spends
    // that source input; advancing the cursor lets healthy progress next time.
    let service = runtime.background_control_service();
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 306, 2)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 306, 2)
            .await
            .unwrap()
            .recovered,
        1
    );
    assert_eq!(
        recovery_snapshot(&runtime, &poison.id)
            .await
            .retry
            .unwrap()
            .attempts,
        2
    );
}

#[tokio::test]
async fn recovery_retry_fences_same_timestamp_attempt_replacement_and_stale_token() {
    let (runtime, queued) = delivery_fixture().await;
    let (_, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let original = recovery_snapshot(&runtime, &queued.id).await;
    assert!(recovery_defer(&runtime, &original, DELIVERY_TIME + 301).await);
    let first_retry = recovery_snapshot(&runtime, &queued.id).await;
    sql(
        &runtime,
        "UPDATE task_delivery SET updated_at=updated_at, max_attempts=max_attempts",
    )
    .await;
    sql(&runtime, "UPDATE task_delivery_attempt SET status=status").await;
    assert_eq!(
        recovery_snapshot(&runtime, &queued.id).await.retry,
        first_retry.retry,
        "same-value writes preserve delay"
    );
    assert!(
        !recovery_defer(&runtime, &original, DELIVERY_TIME + 400).await,
        "an absence snapshot cannot overwrite a retry inserted later"
    );
    runtime
        .service()
        .store()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "UPDATE task_delivery_attempt SET id=? WHERE id=?",
            [
                "replacement_exact_attempt".into(),
                attempt.id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let replacement = recovery_snapshot(&runtime, &queued.id).await;
    assert_eq!(
        replacement.delivery.updated_at,
        original.delivery.updated_at
    );
    assert_eq!(
        replacement.delivery.attempt_count,
        original.delivery.attempt_count
    );
    assert!(replacement.retry.is_none());
    assert!(!recovery_defer(&runtime, &first_retry, DELIVERY_TIME + 400).await);
    assert!(recovery_defer(&runtime, &replacement, DELIVERY_TIME + 306).await);
    let new_retry = recovery_snapshot(&runtime, &queued.id).await.retry.unwrap();
    assert_eq!(
        new_retry.expected_attempt_id.as_deref(),
        Some("replacement_exact_attempt")
    );
    assert_ne!(
        new_retry.retry_token,
        first_retry.retry.unwrap().retry_token
    );
    assert!(!recovery_defer(&runtime, &replacement, DELIVERY_TIME + 500).await);
    assert_eq!(
        recovery_snapshot(&runtime, &queued.id).await.retry.unwrap(),
        new_retry
    );
}

#[tokio::test]
async fn recovery_verified_absence_is_invalidated_by_exact_attempt_insert() {
    let (runtime, queued) = delivery_fixture().await;
    let (_, attempt) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    let raw = recovery_snapshot(&runtime, &queued.id)
        .await
        .attempt
        .unwrap();
    delete_exact_attempt(&runtime, &attempt).await;
    let absent = recovery_snapshot(&runtime, &queued.id).await;
    assert!(recovery_defer(&runtime, &absent, DELIVERY_TIME + 301).await);
    pioneer_entity::task_delivery_attempt::Entity::insert(
        pioneer_entity::task_delivery_attempt::ActiveModel::from(raw),
    )
    .exec(&runtime.service().store().database_connection())
    .await
    .unwrap();
    assert!(
        recovery_snapshot(&runtime, &queued.id)
            .await
            .retry
            .is_none()
    );
    assert!(!recovery_defer(&runtime, &absent, DELIVERY_TIME + 302).await);
}

#[tokio::test]
async fn recovery_delivery_delete_reinsert_does_not_reuse_retry_token() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let source = recovery_snapshot(&runtime, &queued.id).await;
    assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 301).await);
    let old = recovery_snapshot(&runtime, &queued.id).await;
    runtime
        .service()
        .store()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "DELETE FROM task_delivery WHERE id=?",
            [queued.id.clone().into()],
        ))
        .await
        .unwrap();
    pioneer_entity::task_delivery::Entity::insert(
        pioneer_entity::task_delivery::ActiveModel::from(source.delivery),
    )
    .exec(&runtime.service().store().database_connection())
    .await
    .unwrap();
    let recreated = recovery_snapshot(&runtime, &queued.id).await;
    assert!(recreated.retry.is_none());
    assert!(recovery_defer(&runtime, &recreated, DELIVERY_TIME + 302).await);
    let new_retry = recovery_snapshot(&runtime, &queued.id).await.retry.unwrap();
    assert_ne!(
        new_retry.retry_token,
        old.retry.as_ref().unwrap().retry_token
    );
    assert!(!recovery_defer(&runtime, &old, DELIVERY_TIME + 600).await);
    assert_eq!(
        recovery_snapshot(&runtime, &queued.id).await.retry.unwrap(),
        new_retry
    );
}

#[tokio::test]
async fn recovery_panic_isolated_from_healthy_rows_and_persisted_as_error_retry() {
    let (runtime, poison) = delivery_fixture().await;
    started(&runtime, &poison.id, DELIVERY_TIME).await;
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 1).await;
    let (entered, release) = gate(&runtime, CommitKind::RecoveryPanic);
    let service = runtime.background_control_service();
    let operation = tokio::spawn(async move {
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
    });
    reached(&entered).await;
    release.notify_one();
    assert_eq!(operation.await.unwrap().unwrap().recovered, 1);
    let poison_state = recovery_snapshot(&runtime, &poison.id).await;
    assert_eq!(poison_state.delivery.status, "delivering");
    assert_eq!(poison_state.retry.unwrap().attempts, 1);
    assert_eq!(
        runtime
            .service()
            .store()
            .get_task_delivery(&healthy.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskDeliveryStatus::Pending
    );
}

#[tokio::test]
async fn recovery_attempt_write_failure_rolls_back_event_delivery_and_retry_cleanup() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let source = recovery_snapshot(&runtime, &queued.id).await;
    assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 301).await);
    let before = snapshot(&runtime, &queued).await;
    sql(&runtime, "CREATE TRIGGER reject_recovery_attempt BEFORE UPDATE ON task_delivery_attempt BEGIN SELECT RAISE(ABORT,'test recovery rollback'); END").await;
    let mut subscription = subscribe(&runtime, &queued.task_id);
    assert_eq!(
        runtime
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 306, 64)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert_no_wake(&mut subscription).await;
    let retry = recovery_snapshot(&runtime, &queued.id).await.retry.unwrap();
    assert_eq!(retry.attempts, 2);
    assert_eq!(retry.next_probe_at, DELIVERY_TIME + 316);
    sql(&runtime, "DROP TRIGGER reject_recovery_attempt").await;
    assert_eq!(
        runtime
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 316, 64)
            .await
            .unwrap()
            .recovered,
        1
    );
    assert!(
        recovery_snapshot(&runtime, &queued.id)
            .await
            .retry
            .is_none()
    );
}

#[tokio::test]
async fn recovery_large_terminal_history_has_empty_active_range() {
    let (runtime, queued) = delivery_fixture().await;
    let store = runtime.service().store().with_maintenance_access();
    let template = recovery_snapshot(&runtime, &queued.id).await.delivery;
    for number in 0..1_000 {
        let mut row: pioneer_entity::task_delivery::ActiveModel = template.clone().into();
        row.id = Set(format!("terminal_history_{number}"));
        row.delivery_key = Set(format!("terminal_history_key_{number}"));
        row.status = Set("cancelled".to_owned());
        row.next_attempt_at = Set(None);
        pioneer_entity::task_delivery::Entity::insert(row)
            .exec(&store.database_connection())
            .await
            .unwrap();
    }
    assert!(
        store
            .task_delivery_recovery_page(DELIVERY_TIME + 300, None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
            .unwrap()
            .recovered,
        0
    );
    assert!(
        store
            .due_task_delivery_recovery_retries(i64::MAX, 64)
            .await
            .unwrap()
            .is_empty()
    );
}

async fn physical_recovery_fixture() -> (
    TaskRuntime,
    TaskDelivery,
    Arc<DeliveryRoutingObserver>,
    tempfile::TempDir,
) {
    use sea_orm::ConnectOptions;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recovery.sqlite");
    let mut writer_options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    writer_options.max_connections(1);
    let writer = Database::connect(writer_options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    seed_task_test_workspace(&writer).await;
    let mut reader_options =
        ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    reader_options
        .max_connections(2)
        .map_sqlx_sqlite_opts(|options| {
            options
                .read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
    let reader = Database::connect(reader_options).await.unwrap();
    let observer = Arc::new(DeliveryRoutingObserver::default());
    let database = pioneer_sqlite::SqliteDatabase::from_executor_with_read_observer(
        reader,
        pioneer_sqlite::SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    database.validate_reader().await.unwrap();
    let runtime = TaskRuntime::new(Arc::new(CrudStore::new(database)));
    let queued = queue_delivery(&runtime).await;
    (runtime, queued, observer, directory)
}

#[tokio::test]
async fn recovery_retry_writes_are_maintenance_and_domain_commit_keeps_critical_scope() {
    let (runtime, queued, observer, _directory) = physical_recovery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let source = recovery_snapshot(&runtime, &queued.id).await;
    observer.clear();
    assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 301).await);
    let classes = observer
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(classes, [pioneer_sqlite::SqliteWriteClass::Maintenance]);
    observer.clear();
    assert_eq!(
        runtime
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 306, 64)
            .await
            .unwrap()
            .recovered,
        1
    );
    let classes = observer
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            pioneer_sqlite::SqliteWriteEvent::Acquired { class, .. } => Some(*class),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        classes,
        [pioneer_sqlite::SqliteWriteClass::Critical],
        "event+attempt+delivery+retry cleanup share one writer transaction"
    );
    let reads = observer.reads.lock().unwrap();
    assert!(reads.iter().any(|e| matches!(
        e,
        pioneer_sqlite::SqliteReadEvent::OperationFinished {
            class: pioneer_sqlite::SqliteReadClass::Maintenance,
            ..
        }
    )));
    assert!(!reads.iter().any(|e| matches!(
        e,
        pioneer_sqlite::SqliteReadEvent::OperationFinished {
            class: pioneer_sqlite::SqliteReadClass::Interactive,
            ..
        }
    )));
}

#[tokio::test]
async fn recovery_cancellation_aborts_queued_error_delay_and_releases_capacity() {
    let (runtime, queued, observer, _directory) = physical_recovery_fixture().await;
    let (_, missing) = started(&runtime, &queued.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &missing).await;
    let before = recovery_snapshot(&runtime, &queued.id).await;
    let blocker = runtime
        .service()
        .store()
        .database_connection()
        .begin()
        .await
        .unwrap();
    observer.clear();
    observer.watch_queue.store(true, Ordering::SeqCst);
    let service = runtime.background_control_service();
    let operation = tokio::spawn(async move {
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
    });
    reached(&observer.queued).await;
    operation.abort();
    assert!(operation.await.unwrap_err().is_cancelled());
    reached(&observer.cancelled).await;
    blocker.rollback().await.unwrap();
    let after = recovery_snapshot(&runtime, &queued.id).await;
    assert_eq!(after.delivery, before.delivery);
    assert!(
        after.retry.is_none(),
        "no uncommitted delay is claimed durable"
    );
    let queues = observer
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            pioneer_sqlite::SqliteWriteEvent::Cancelled { class, queue, .. } => {
                Some((*class, *queue))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(queues.contains(&(
        pioneer_sqlite::SqliteWriteClass::Maintenance,
        pioneer_sqlite::SqliteWriteQueueSnapshot::default()
    )));
    timeout(
        Duration::from_secs(5),
        runtime
            .background_control_service()
            .recover_stuck_deliveries(DELIVERY_TIME + 302, 64),
    )
    .await
    .unwrap()
    .unwrap();
}

fn pause_recovery_clock() -> tokio::task::JoinHandle<()> {
    tokio::time::pause();
    // SQLx waits on an external SQLite thread. Keep virtual time from advancing
    // automatically while it runs; tests advance it explicitly at boundaries.
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    })
}

async fn reject_retry_for(runtime: &TaskRuntime, delivery_id: &str) {
    // SQLite trigger DDL cannot bind parameters. Fixture IDs are quoted here.
    let id = delivery_id.replace('\'', "''");
    for event in ["INSERT", "UPDATE"] {
        sql(runtime, &format!("CREATE TRIGGER reject_recovery_retry_{event} BEFORE {event} ON task_delivery_recovery_retry WHEN NEW.delivery_id='{id}' BEGIN SELECT RAISE(ABORT,'test row-local bookkeeping failure'); END")).await;
    }
}

#[tokio::test]
async fn recovery_local_retry_insert_failure_first_source_candidate_still_repairs_next() {
    let (runtime, poison) = delivery_fixture().await;
    let (_, missing) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &missing).await;
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 1).await;
    reject_retry_for(&runtime, &poison.id).await;
    let report = runtime
        .background_control_service()
        .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
        .await
        .unwrap();
    assert_eq!(
        report,
        crate::TaskDeliveryRecoveryResult {
            selected: 2,
            recovered: 1,
            failed: 1,
            undurable: 1,
            cooling_down: true,
        }
    );
    assert_eq!(
        recovery_snapshot(&runtime, &healthy.id)
            .await
            .delivery
            .status,
        "pending"
    );
    let failed = recovery_snapshot(&runtime, &poison.id).await;
    assert!(failed.retry.is_none());
    assert!(failed.attempt.is_none());
    assert_eq!(failed.delivery.status, "delivering");
}

#[tokio::test]
async fn recovery_failed_bookkeeping_preserves_healthy_rows_and_uses_quantum_cooldown() {
    let (runtime, poison, observer, _directory) = physical_recovery_fixture().await;
    let before = queue_delivery(&runtime).await;
    started(&runtime, &before.id, DELIVERY_TIME).await;
    let (_, missing) = started(&runtime, &poison.id, DELIVERY_TIME + 1).await;
    delete_exact_attempt(&runtime, &missing).await;
    let after = queue_delivery(&runtime).await;
    started(&runtime, &after.id, DELIVERY_TIME + 2).await;
    reject_retry_for(&runtime, &poison.id).await;
    let service = runtime.background_control_service();
    let clock_guard = pause_recovery_clock();
    let report = service
        .recover_stuck_deliveries(DELIVERY_TIME + 302, 64)
        .await
        .unwrap();
    assert_eq!(
        report,
        crate::TaskDeliveryRecoveryResult {
            selected: 3,
            recovered: 2,
            failed: 1,
            undurable: 1,
            cooling_down: true,
        }
    );
    for healthy in [&before, &after] {
        let source = recovery_snapshot(&runtime, &healthy.id).await;
        assert_eq!(source.delivery.status, "pending");
        assert_eq!(source.attempt.unwrap().status, "failed");
    }
    let source = recovery_snapshot(&runtime, &poison.id).await;
    assert!(source.retry.is_none(), "failed bookkeeping is not durable");
    assert!(source.attempt.is_none());
    assert_eq!(source.delivery.status, "delivering");
    observer.clear();
    let skipped = service
        .recover_stuck_deliveries(DELIVERY_TIME + 303, 64)
        .await
        .unwrap();
    assert_eq!(skipped.selected, 0);
    assert!(
        skipped.cooling_down,
        "partial success must preserve cooldown"
    );
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
    tokio::time::advance(Duration::from_secs(5)).await;
    let again = service
        .recover_stuck_deliveries(DELIVERY_TIME + 307, 64)
        .await
        .unwrap();
    assert_eq!((again.selected, again.failed, again.undurable), (1, 1, 1));
    assert!(again.cooling_down);
    // Permanent A-only rejection remains visible; next quantum gets 10s.
    observer.clear();
    tokio::time::advance(Duration::from_secs(9)).await;
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 316, 64)
            .await
            .unwrap()
            .selected,
        0
    );
    assert!(observer.reads.lock().unwrap().is_empty());
    clock_guard.abort();
    tokio::time::resume();
}

#[tokio::test]
async fn recovery_multiple_local_snapshot_errors_use_one_cooldown_and_do_not_infer_attempt_absence()
{
    let (runtime, poison) = delivery_fixture().await;
    let (_, attempt) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    let another = queue_delivery(&runtime).await;
    started(&runtime, &another.id, DELIVERY_TIME + 1).await;
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 2).await;
    // SQLite dynamic typing permits a row-local decode error. Raw discovery
    // reads only id/updated_at and therefore still selects the healthy row.
    for id in [&poison.id, &another.id] {
        runtime
            .service()
            .store()
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
                "UPDATE task_delivery SET attempt_count=? WHERE id=?",
                ["invalid integer".into(), id.clone().into()],
            ))
            .await
            .unwrap();
    }
    let service = runtime.background_control_service();
    let clock_guard = pause_recovery_clock();
    let report = service
        .recover_stuck_deliveries(DELIVERY_TIME + 302, 64)
        .await
        .unwrap();
    assert_eq!(
        (
            report.selected,
            report.recovered,
            report.failed,
            report.undurable
        ),
        (3, 1, 2, 2)
    );
    assert!(report.cooling_down);
    assert_eq!(
        recovery_snapshot(&runtime, &healthy.id)
            .await
            .delivery
            .status,
        "pending"
    );
    for id in [&poison.id, &another.id] {
        runtime
            .service()
            .store()
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                sea_orm::DbBackend::Sqlite,
                "UPDATE task_delivery SET attempt_count=? WHERE id=?",
                [1_i64.into(), id.clone().into()],
            ))
            .await
            .unwrap();
        let source = recovery_snapshot(&runtime, id).await;
        assert!(
            source.retry.is_none(),
            "no unfenced retry with fabricated absence"
        );
        assert!(source.attempt.is_some());
    }
    assert_eq!(
        recovery_snapshot(&runtime, &poison.id)
            .await
            .attempt
            .unwrap()
            .id,
        attempt.id
    );
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 303, 64)
            .await
            .unwrap()
            .selected,
        0
    );
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 307, 64)
            .await
            .unwrap()
            .recovered,
        2,
        "two erroneous rows must not increase cooldown twice in one quantum"
    );
    clock_guard.abort();
    tokio::time::resume();
}

#[tokio::test]
async fn recovery_due_retry_local_failure_preserves_source_quota() {
    let (runtime, poison) = delivery_fixture().await;
    let (_, missing) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &missing).await;
    let source = recovery_snapshot(&runtime, &poison.id).await;
    assert!(recovery_defer(&runtime, &source, DELIVERY_TIME + 301).await);
    let original_retry = recovery_snapshot(&runtime, &poison.id).await.retry;
    let healthy = queue_delivery(&runtime).await;
    started(&runtime, &healthy.id, DELIVERY_TIME + 1).await;
    reject_retry_for(&runtime, &poison.id).await;
    let service = runtime.background_control_service();
    // Move source fairness cursor past A so the source quota selects B.
    assert_eq!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 1)
            .await
            .unwrap()
            .selected,
        1
    );
    let report = service
        .recover_stuck_deliveries(DELIVERY_TIME + 306, 2)
        .await
        .unwrap();
    assert_eq!(
        (
            report.selected,
            report.recovered,
            report.failed,
            report.undurable
        ),
        (2, 1, 1, 1)
    );
    assert!(report.cooling_down);
    let retained = recovery_snapshot(&runtime, &poison.id).await;
    assert_eq!(retained.retry, original_retry);
    assert_eq!(retained.retry.unwrap().attempts, 1);
    assert_eq!(
        recovery_snapshot(&runtime, &healthy.id)
            .await
            .delivery
            .status,
        "pending"
    );
}

#[tokio::test]
async fn recovery_database_unavailable_reports_discovery_error_and_limits_future_quanta() {
    let (runtime, _, observer, _directory) = physical_recovery_fixture().await;
    runtime
        .service()
        .store()
        .database_connection()
        .clone()
        .close()
        .await
        .unwrap();
    let service = runtime.background_control_service();
    assert!(
        service
            .recover_stuck_deliveries(DELIVERY_TIME + 301, 64)
            .await
            .is_err(),
        "closed physical pools must not be reported as a successful empty quantum"
    );
    observer.clear();
    let report = service
        .recover_stuck_deliveries(DELIVERY_TIME + 302, 64)
        .await
        .unwrap();
    assert_eq!(report.selected, 0);
    assert!(report.cooling_down);
    assert!(observer.reads.lock().unwrap().is_empty());
    assert!(observer.writes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn runtime_start_survives_row_local_recovery_bookkeeping_failure() {
    let (runtime, poison) = delivery_fixture().await;
    let (_, missing) = started(&runtime, &poison.id, DELIVERY_TIME).await;
    delete_exact_attempt(&runtime, &missing).await;
    reject_retry_for(&runtime, &poison.id).await;
    let stale_at = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp() - 301, 0)
        .unwrap()
        .fixed_offset();
    runtime
        .service()
        .store()
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DbBackend::Sqlite,
            "UPDATE task_delivery SET created_at=?,updated_at=? WHERE id=?",
            [stale_at.into(), stale_at.into(), poison.id.clone().into()],
        ))
        .await
        .unwrap();
    runtime
        .start()
        .await
        .expect("local recovery failure must not abort startup");
    let report = runtime
        .maintenance_service()
        .recover_stuck_deliveries(chrono::Utc::now().timestamp(), 64)
        .await
        .unwrap();
    assert!(
        report.cooling_down,
        "startup actually encountered the local undurable failure"
    );
    assert_eq!(report.selected, 0);
    assert!(
        recovery_snapshot(&runtime, &poison.id)
            .await
            .retry
            .is_none()
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn recovery_error_delay_uses_clock_after_writer_admission() {
    use std::sync::atomic::AtomicI64;
    let (runtime, queued, observer, _directory) = physical_recovery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let source = recovery_snapshot(&runtime, &queued.id).await;
    let blocker = runtime
        .service()
        .store()
        .database_connection()
        .begin()
        .await
        .unwrap();
    observer.clear();
    observer.watch_queue.store(true, Ordering::SeqCst);
    let clock = Arc::new(AtomicI64::new(DELIVERY_TIME + 301));
    let task_clock = clock.clone();
    let store = runtime.service().store().with_maintenance_access();
    let operation = tokio::spawn(async move {
        store
            .defer_task_delivery_recovery(&source, &|| task_clock.load(Ordering::SeqCst))
            .await
    });
    reached(&observer.queued).await;
    clock.store(DELIVERY_TIME + 401, Ordering::SeqCst);
    blocker.rollback().await.unwrap();
    assert!(operation.await.unwrap().unwrap());
    assert_eq!(
        recovery_snapshot(&runtime, &queued.id)
            .await
            .retry
            .unwrap()
            .next_probe_at,
        DELIVERY_TIME + 406
    );
}

#[tokio::test]
async fn recovery_stale_retry_snapshot_cannot_apply_event_or_delay_new_retry() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let old_source = recovery_snapshot(&runtime, &queued.id).await;
    let old_event = old_source.failure_event(DELIVERY_TIME + 301).unwrap();
    assert!(recovery_defer(&runtime, &old_source, DELIVERY_TIME + 301).await);
    let first_retry = recovery_snapshot(&runtime, &queued.id).await;
    assert!(recovery_defer(&runtime, &first_retry, DELIVERY_TIME + 306).await);
    let second_retry = recovery_snapshot(&runtime, &queued.id).await;
    assert_eq!(second_retry.delivery, first_retry.delivery);
    assert_ne!(
        second_retry.retry.as_ref().unwrap().retry_token,
        first_retry.retry.as_ref().unwrap().retry_token
    );
    let before = snapshot(&runtime, &queued).await;
    let current_retry = recovery_snapshot(&runtime, &queued.id).await.retry;
    let outcome = runtime
        .service()
        .store()
        .with_maintenance_reads_and_critical_writes()
        .recover_task_delivery(
            old_event,
            &first_retry,
            DELIVERY_TIME + 1,
            DELIVERY_TIME + 301,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, Outcome::Superseded));
    assert_eq!(snapshot(&runtime, &queued).await, before);
    assert!(!recovery_defer(&runtime, &old_source, DELIVERY_TIME + 700).await);
    assert!(!recovery_defer(&runtime, &first_retry, DELIVERY_TIME + 700).await);
    assert_eq!(
        recovery_snapshot(&runtime, &queued.id).await.retry,
        current_retry
    );
}

#[tokio::test]
async fn recovery_cancel_during_failure_deferral_wins_without_new_retry() {
    let (runtime, queued) = delivery_fixture().await;
    started(&runtime, &queued.id, DELIVERY_TIME).await;
    let source = recovery_snapshot(&runtime, &queued.id).await;
    let (entered, release) = gate(&runtime, CommitKind::RecoveryRetry);
    let store = runtime.service().store().with_maintenance_access();
    let operation = tokio::spawn(async move {
        store
            .defer_task_delivery_recovery(&source, &|| DELIVERY_TIME + 301)
            .await
    });
    reached(&entered).await;
    cancel(&runtime, &queued.task_id).await;
    release.notify_one();
    assert!(!operation.await.unwrap().unwrap());
    let cancelled = recovery_snapshot(&runtime, &queued.id).await;
    assert_eq!(cancelled.delivery.status, "cancelled");
    assert!(cancelled.retry.is_none());
}
