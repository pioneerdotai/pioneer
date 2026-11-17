use super::*;
use pioneer_observability::{
    live::{WaitKind, WorkState},
    scenario::{Correlation, accept_params as accept_correlation_params, inject_params},
};

pub(super) fn remote(trace: &str) -> Correlation {
    let mut params = serde_json::json!({"_pioneer_correlation":{
        "v":1,"trace":trace,"span":"2222222222222222","flags":1}});
    accept_correlation_params("thread/open", &mut params).0
}
fn trace(parent: &Correlation) -> Option<String> {
    parent.run(|| {
        let mut params = serde_json::json!({});
        inject_params("thread/open", &mut params);
        params["_pioneer_correlation"]["trace"]
            .as_str()
            .map(str::to_owned)
    })
}
pub(super) fn waits(trace: [u8; 16], kind: WaitKind) -> Vec<u64> {
    pioneer_observability::live::snapshot()
        .unwrap()
        .active
        .into_iter()
        .filter(|work| {
            work.context.trace_id == Some(trace)
                && matches!(work.state, WorkState::Waiting(wait) if wait == kind)
        })
        .map(|work| work.context.token)
        .collect()
}

#[tokio::test]
async fn committed_review_delivery_waits_preserve_lineage_and_retire_across_restart() {
    pioneer_observability::set_telemetry_enabled(true);
    let source = remote("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let unrelated = remote("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let runtime = runtime_with_review_config().await;
    let policy = TaskDeliveryPolicy {
        mode: TaskDeliveryMode::Thread,
        thread_target: Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread),
        thread_id: Some(TEST_PARENT_THREAD_ID.to_owned()),
        webhook_url: None,
        include_result: true,
        format: pioneer_protocol::TaskDeliveryFormat::Summary,
    };
    let (task, run, candidate) = source
        .scope(create_waiting_review_agent_task_with_delivery(
            &runtime,
            TaskAgentReviewPolicy::parent_agent_default(2),
            0,
            TaskResultCandidateStatus::PendingReview,
            Some(policy),
        ))
        .await;
    assert_eq!(waits([0xaa; 16], WaitKind::UserOrProvider).len(), 1);
    let parent = runtime
        .event_bus()
        .closing_candidate_parent(&run, &candidate);
    assert_eq!(
        trace(&parent).as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    let context = parent_accept_context_for_candidate(&runtime, &task, &candidate).await;
    let accepted = unrelated
        .scope(runtime.service().accept_task_result_candidate(
            context,
            accept_params(task.clone(), run.clone(), candidate),
        ))
        .await
        .unwrap();
    assert_eq!(accepted.run.status, TaskRunStatus::Succeeded);
    // Durable fanout must use the exact published decision event. The review
    // keeps its candidate lineage even though the accepting caller is unrelated.
    let events = runtime
        .service()
        .store()
        .get_task_events(&task, None)
        .await
        .unwrap()
        .events;
    let accepted_event = events
        .iter()
        .find(|event| {
            matches!(
                event.payload,
                TaskEventPayload::TaskResultCandidateAccepted { .. }
            )
        })
        .unwrap();
    let publication = runtime
        .event_bus()
        .closing_publication_parent(&task, accepted_event.sequence)
        .unwrap();
    assert_eq!(
        trace(&publication).as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    assert!(
        runtime
            .event_bus()
            .closing_publication_parent("other-task", accepted_event.sequence)
            .is_none()
    );
    assert!(
        runtime
            .event_bus()
            .closing_publication_parent(&task, i64::MAX)
            .is_none()
    );

    assert!(waits([0xaa; 16], WaitKind::UserOrProvider).is_empty());
    assert_eq!(waits([0xaa; 16], WaitKind::Timer).len(), 1);
    let mut deliveries = runtime
        .service()
        .list_deliveries(TaskDeliveriesParams {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            task_id: Some(task),
            run_id: Some(run.clone()),
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap()
        .deliveries;
    let pending = deliveries.remove(0);
    assert_eq!(pending.status, TaskDeliveryStatus::Pending);
    assert_eq!(
        trace(&runtime.event_bus().closing_delivery_parent(&pending.id, 0)).as_deref(),
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    );
    let service = runtime.background_control_service();
    let parent = runtime.event_bus().closing_delivery_parent(&pending.id, 0);
    let pioneer_crud::TaskDeliveryTransitionOutcome::Applied((first, first_attempt)) = parent
        .scope(service.start_delivery(&pending.id, pending.created_at))
        .await
        .unwrap()
    else {
        panic!("first delivery must start");
    };
    assert!(waits([0xaa; 16], WaitKind::Timer).is_empty());
    let parent = runtime
        .event_bus()
        .closing_delivery_parent(&first.id, first.attempt_count);
    let pioneer_crud::TaskDeliveryTransitionOutcome::Applied(retry) = parent
        .scope(service.fail_delivery(
            first.clone(),
            first_attempt.clone(),
            "retry".into(),
            None,
            None,
            pending.created_at + 1,
        ))
        .await
        .unwrap()
    else {
        panic!("first failure must schedule retry");
    };
    let retry_tokens = waits([0xaa; 16], WaitKind::Timer);
    assert_eq!(retry_tokens.len(), 1);
    let parent = runtime
        .event_bus()
        .closing_delivery_parent(&retry.id, retry.attempt_count);
    let pioneer_crud::TaskDeliveryTransitionOutcome::Applied((second, second_attempt)) = parent
        .scope(service.start_delivery(&retry.id, retry.next_attempt_at.unwrap()))
        .await
        .unwrap()
    else {
        panic!("retry must start");
    };
    assert!(waits([0xaa; 16], WaitKind::Timer).is_empty());
    let parent = runtime
        .event_bus()
        .closing_delivery_parent(&second.id, second.attempt_count);
    let pioneer_crud::TaskDeliveryTransitionOutcome::Applied(pending_again) = parent
        .scope(service.fail_delivery(
            second,
            second_attempt,
            "retry".into(),
            None,
            None,
            retry.next_attempt_at.unwrap() + 1,
        ))
        .await
        .unwrap()
    else {
        panic!("second failure must schedule retry");
    };
    let next_tokens = waits([0xaa; 16], WaitKind::Timer);
    assert_eq!(next_tokens.len(), 1);
    assert_ne!(next_tokens, retry_tokens);
    // A late first-attempt receipt must not retire the second pending wait.
    assert!(matches!(
        service
            .complete_delivery(
                first,
                first_attempt,
                Some("turn_exact_receipt".to_owned()),
                None,
                None,
                None,
                pending_again.updated_at + 1
            )
            .await
            .unwrap(),
        pioneer_crud::TaskDeliveryTransitionOutcome::Superseded
    ));
    assert_eq!(waits([0xaa; 16], WaitKind::Timer), next_tokens);
    runtime.shutdown().await;
    assert!(waits([0xaa; 16], WaitKind::Timer).is_empty());
    // Existing recovery data starts an independent local fragment, not the old
    // trace or a synthetic elapsed interval spanning shutdown.
    let recovered = TaskRuntime::new(runtime.service().store());
    unrelated.run(|| {
        recovered
            .event_bus()
            .observe_closing_delivery(&pending_again)
    });
    assert_eq!(waits([0xbb; 16], WaitKind::Timer).len(), 1);
    let parent = recovered
        .event_bus()
        .closing_delivery_parent(&pending_again.id, pending_again.attempt_count);
    assert_eq!(
        trace(&parent).as_deref(),
        Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    );
    let service = recovered.background_control_service();
    let pioneer_crud::TaskDeliveryTransitionOutcome::Applied((third, third_attempt)) = parent
        .scope(service.start_delivery(&pending_again.id, pending_again.next_attempt_at.unwrap()))
        .await
        .unwrap()
    else {
        panic!("recovered retry must start");
    };
    // At this TaskService boundary the worker supplies its exact receipt;
    // this test proves its durable transition, not creation of destination content.
    let pioneer_crud::TaskDeliveryTransitionOutcome::Applied(delivered) = parent
        .scope(service.complete_delivery(
            third,
            third_attempt,
            Some("turn_exact_receipt".to_owned()),
            None,
            None,
            None,
            pending_again.next_attempt_at.unwrap() + 1,
        ))
        .await
        .unwrap()
    else {
        panic!("current receipt must commit");
    };
    assert_eq!(delivered.status, TaskDeliveryStatus::Delivered);
    assert!(waits([0xbb; 16], WaitKind::Timer).is_empty());
    assert_eq!(
        recovered
            .service()
            .get_task(pioneer_protocol::TaskGetParams {
                task_id: accepted.task.id,
            })
            .await
            .unwrap()
            .runs
            .iter()
            .find(|value| value.id == run)
            .unwrap()
            .status,
        TaskRunStatus::Succeeded
    );
}

#[tokio::test]
async fn finite_recovery_observer_evicts_and_retires_without_owning_business_work() {
    pioneer_observability::set_telemetry_enabled(true);
    let source = remote("ffffffffffffffffffffffffffffffff");
    let runtime = runtime_with_review_config().await;
    let (task, run, candidate) = source
        .scope(create_waiting_review_agent_task(
            &runtime,
            2,
            0,
            TaskResultCandidateStatus::PendingReview,
        ))
        .await;
    let response = runtime
        .service()
        .get_task(pioneer_protocol::TaskGetParams { task_id: task })
        .await
        .unwrap();
    let run = response
        .runs
        .into_iter()
        .find(|value| value.id == run)
        .unwrap();
    let candidate = runtime
        .service()
        .store()
        .get_task_result_candidate(&candidate)
        .await
        .unwrap()
        .unwrap();
    let first = waits([0xff; 16], WaitKind::UserOrProvider);
    assert_eq!(first.len(), 1);
    // Synthetic recovery DTOs exercise only the documented sidecar budget;
    // this is not a claim that the database committed these extra runs.
    for index in 0..65 {
        let mut row = run.clone();
        row.id = format!("bounded-run-{index}");
        let mut result = candidate.clone();
        result.run_id = row.id.clone();
        result.id = format!("bounded-candidate-{index}");
        source.run(|| runtime.event_bus().observe_closing_review(&result, &row));
    }
    let active = waits([0xff; 16], WaitKind::UserOrProvider);
    assert_eq!(active.len(), 64);
    assert!(!active.contains(&first[0]));
    runtime.shutdown().await;
    assert!(waits([0xff; 16], WaitKind::UserOrProvider).is_empty());
    source.run(|| runtime.event_bus().observe_closing_review(&candidate, &run));
    assert!(waits([0xff; 16], WaitKind::UserOrProvider).is_empty());
}
