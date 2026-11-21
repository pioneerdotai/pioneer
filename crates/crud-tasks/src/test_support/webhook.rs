use crate::event_store::TaskEventStore;
use crate::task_event_context::TaskEventContextRepository;
use crate::task_projector::TaskOutputBinder;
use crate::test_support::task_projection::{sample_task, sample_task_run, sample_task_trigger};
use pioneer_protocol::*;

pub const AT: i64 = 1_700_000_000;

pub async fn webhook_fixture<B: TaskOutputBinder + Clone>(
    store: &TaskEventStore<B>,
) -> (Task, TaskTrigger) {
    let mut task = sample_task(AT);
    task.status = TaskStatus::Waiting;
    let mut trigger = sample_task_trigger(AT);
    trigger.spec = TaskTriggerSpec::External {
        source: "webhook".into(),
        event_type: None,
        filter: None,
    };
    trigger.next_fire_at = None;
    trigger.webhook_secret_ref = Some("webhook-fixture-secret-ref".into());
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::TriggerCreated {
                    trigger: trigger.clone(),
                },
            ],
            AT,
        )
        .await
        .unwrap();
    crate::task_actor_contract::upsert_task_actor_contract(
        &store.connection,
        &TaskActorContract {
            task_id: task.id.clone(),
            workspace_id: task.workspace_id.clone(),
            creator: PersistedActorRef::System,
            creator_presentation_snapshot: None,
            reviewer: TaskReviewerIntent::RuntimeAuto,
            execution_destination_thread_id: None,
            execution_route_id: None,
            execution_route_receipt_json: None,
            execution_route_expires_at_millis: None,
            delivery: TaskDeliveryActorContract {
                enabled: false,
                destination_thread_id: None,
                destination_user_id: None,
                destination_webhook_url_fingerprint: None,
                route_id: None,
                return_route_id: None,
                author_snapshot: None,
                route_receipt_json: None,
                disclosure_generation: 1,
                route_expires_at_millis: None,
            },
            launch: None,
            requested_identity_json: None,
            resolved_identity_id: None,
            resolved_profile_id: None,
            source_config_fingerprint: None,
            derived_child_launch_grant_json: None,
            creator_work_graph_root_execution_id: None,
            work_graph_root_execution_id: None,
            root_resource_scope_id: None,
            accounting_attribution: None,
            controller_principal_id: None,
            revision: 1,
        },
        AT,
    )
    .await
    .unwrap();
    (
        TaskEventContextRepository::new(&store.connection)
            .get_task_record(&task.id)
            .await
            .unwrap()
            .unwrap(),
        trigger,
    )
}

pub fn request(
    task: &Task,
    trigger: &TaskTrigger,
    number: i64,
) -> (TaskRun, TaskOccurrenceContract, Vec<TaskEventPayload>) {
    let mut run = sample_task_run(AT + number);
    run.id = format!("webhook_run_{number:08}");
    run.run_group_id = run.id.clone();
    run.task_id = task.id.clone();
    run.trigger_id = Some(trigger.id.clone());
    run.run_number = number;
    run.trigger_input = Some(TaskRunTriggerInput {
        content_type: Some("application/octet-stream".into()),
        encoding: TaskRunInputEncoding::Base64,
        body: "AP9cCg==".into(),
    });
    let occurrence = TaskOccurrenceContract {
        occurrence_id: run.id.clone(),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        trigger_id: Some(trigger.id.clone()),
        occurrence_key: format!("webhook:{}", run.id),
        execution_generation: number.try_into().unwrap(),
        agent_execution_id: None,
        work_graph_root_execution_id: None,
        root_resource_scope_id: None,
        status: TaskOccurrenceStatus::Queued,
        queue_position: Some(number.try_into().unwrap()),
        retry_attempt: 0,
        action_idempotency_key: format!("task:{}:{}", task.id, run.id),
        route_id: None,
        result_return_route_id: None,
        delivery_plan: None,
        terminal_reason: None,
    };
    let events = vec![
        TaskEventPayload::RunCreated {
            run: run.clone(),
            agent_spec: None,
        },
        TaskEventPayload::TaskQueued {
            task_id: task.id.clone(),
            run_id: Some(run.id.clone()),
        },
    ];
    (run, occurrence, events)
}
