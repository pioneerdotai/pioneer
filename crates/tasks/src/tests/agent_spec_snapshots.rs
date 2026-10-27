use super::*;
use pioneer_protocol::{
    TaskAgentContextMode, TaskAgentContextPolicy, TaskAgentResultContract, TaskAgentResultFormat,
    TaskAgentSpec, TaskGetResponse,
};

const FIRST: i64 = 4_000_000_000;

fn agent_params(trigger: TaskTriggerSpec) -> TaskCreateParams {
    let mut params = create_params(trigger);
    configure_agent_task(&mut params);
    let mut spec = agent_spec(3);
    // The service assigns depth 1 to tasks without a parent.
    spec.depth = 1;
    spec.agent_role = Some("researcher".into());
    spec.agent_nickname = Some("Snapshot worker".into());
    spec.prompt.instructions = vec!["Read the supplied input and report unavailable data.".into()];
    spec.prompt.output_instructions = Some("Return a concise result.".into());
    spec.prompt.input = Some(TaskAgentInput {
        text: Some("original input".into()),
        variables: Vec::new(),
        attachments: Vec::new(),
        references: Vec::new(),
    });
    spec.context_policy = Some(TaskAgentContextPolicy {
        mode: TaskAgentContextMode::LastNTurns,
        max_turns: Some(4),
        include_parent_summary: true,
        include_artifacts: true,
        custom_context: None,
    });
    spec.tool_policy = Some(TaskAgentToolPolicy {
        allowed_tools: vec!["read_file".into()],
        denied_tools: vec!["write_file".into()],
        write_mode: TaskAgentWriteMode::ReadOnly,
        allowed_paths: vec!["/workspace".into()],
        network_access: false,
    });
    spec.result_contract = Some(TaskAgentResultContract {
        format: TaskAgentResultFormat::Text,
        required: true,
        schema: None,
    });
    spec.review_policy = Some(TaskAgentReviewPolicy {
        mode: TaskAgentReviewMode::UserApproval,
        max_revision_rounds: 2,
        require_explicit_acceptance: true,
        reviewers: Vec::new(),
        resolution_strategy: TaskResultReviewResolutionStrategy::UserFinal,
    });
    params.agent_spec = Some(spec);
    params
}

fn interval_params(max_parallel_runs: u32) -> TaskCreateParams {
    let mut params = agent_params(TaskTriggerSpec::Interval {
        interval_seconds: 10,
        interval_anchor_at: Some(FIRST),
        catch_up_policy: None,
    });
    params.concurrency_policy = Some(TaskConcurrencyPolicy {
        key: None,
        max_parallel_runs,
        on_conflict: TaskConcurrencyConflictPolicy::Queue,
    });
    params
}

async fn read(runtime: &TaskRuntime, task_id: &str) -> TaskGetResponse {
    runtime
        .service()
        .store()
        .get_task(task_id)
        .await
        .unwrap()
        .unwrap()
}

fn snapshot<'a>(task: &'a TaskGetResponse, run_id: &str) -> &'a TaskAgentSpec {
    task.agent_specs
        .iter()
        .find(|spec| spec.run_id.as_deref() == Some(run_id))
        .expect("the exact run snapshot must remain available")
}

fn assert_configuration(spec: &TaskAgentSpec, input: &TaskAgentSpecInput) {
    assert_eq!(spec.agent_role, input.agent_role);
    assert_eq!(spec.agent_nickname, input.agent_nickname);
    assert_eq!(spec.model, input.model);
    assert_eq!(spec.model_provider, input.model_provider);
    assert_eq!(spec.prompt, input.prompt);
    assert_eq!(spec.context_policy, input.context_policy);
    assert_eq!(spec.tool_policy, input.tool_policy);
    assert_eq!(spec.permission_cap, input.permission_cap);
    assert_eq!(spec.security_cap, input.security_cap);
    assert_eq!(spec.result_contract, input.result_contract);
    assert_eq!(spec.review_policy, input.review_policy);
    assert_eq!(spec.depth, input.depth);
    assert_eq!(spec.max_depth, input.max_depth);
}

async fn start_run(runtime: &TaskRuntime, run: &TaskRun) -> TaskExecutionHandle {
    let store = runtime.service().store();
    store
        .claim_task_run_for_dispatch(&run.id, run.created_at)
        .await
        .unwrap()
        .unwrap();
    store
        .claim_task_run_execution_for_dispatch(
            &run.id,
            TaskExecutorKind::Agent,
            "snapshot-test",
            run.created_at,
            run.created_at + 60,
        )
        .await
        .unwrap()
        .unwrap();
    let handle = TaskExecutionHandle::new(
        store,
        runtime.event_bus(),
        run.task_id.clone(),
        run.id.clone(),
    );
    handle.mark_started(run.created_at).await.unwrap();
    handle
}

#[tokio::test]
async fn scheduled_siblings_preserve_snapshots_and_use_updated_base() {
    // One occupied slot exercises saturated enqueue; two slots exercise ordinary scheduling.
    for slots in [1, 2] {
        let runtime = runtime_with_review_config().await;
        let params = interval_params(slots);
        let original_input = params.agent_spec.clone().unwrap();
        let created = runtime
            .service()
            .create_task(task_create_context_for(&params), params)
            .await
            .unwrap();
        let base = created.agent_spec.unwrap();
        assert_eq!(runtime.process_due_once(FIRST).await.unwrap(), 1);
        let first = read(&runtime, &created.task.id).await.runs.remove(0);
        let _handle = start_run(&runtime, &first).await;
        let (_, child_turn_id) =
            record_active_task_execution_turn(&runtime, &first.task_id, &first.id).await;

        // Represent a historical snapshot newer than its base so source selection
        // cannot accidentally pass because of ties in created_at ordering.
        let initial = read(&runtime, &first.task_id).await;
        let first_spec_id = snapshot(&initial, &first.id).id.clone();
        let db = runtime.service().store().database_connection();
        pioneer_entity::task_agent_spec::Entity::update(
            pioneer_entity::task_agent_spec::ActiveModel {
                id: Set(first_spec_id),
                created_at: Set(chrono::DateTime::from_timestamp(FIRST, 0)
                    .unwrap()
                    .fixed_offset()),
                ..Default::default()
            },
        )
        .exec(&db)
        .await
        .unwrap();
        let before_update = read(&runtime, &first.task_id).await;
        let first_spec = snapshot(&before_update, &first.id).clone();
        assert_configuration(&first_spec, &original_input);
        assert_ne!(first_spec.id, base.id);
        assert_eq!(before_update.agent_specs.last().unwrap().id, first_spec.id);

        let instructions = vec!["Use the revised configuration for future runs.".to_owned()];
        runtime
            .service()
            .update_task(
                TaskMutationContext::default(),
                TaskUpdateParams {
                    task_id: first.task_id.clone(),
                    instructions: Some(instructions.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(runtime.process_due_once(FIRST + 10).await.unwrap(), 1);
        let saved = read(&runtime, &first.task_id).await;
        assert_eq!(saved.runs.len(), 2);
        assert_eq!(saved.runs[0].status, TaskRunStatus::Running);
        assert_eq!(saved.runs[1].status, TaskRunStatus::Queued);
        assert!(
            saved
                .task_run_turns
                .iter()
                .any(|turn| turn.turn_id == child_turn_id
                    && turn.status == TaskRunTurnStatus::InProgress)
        );
        assert_eq!(saved.agent_specs.len(), 3);
        assert_eq!(snapshot(&saved, &first.id), &first_spec);
        let current_base = saved
            .agent_specs
            .iter()
            .find(|spec| spec.run_id.is_none())
            .unwrap();
        assert_eq!(current_base.id, base.id);
        let mut updated_input = original_input;
        updated_input.prompt.instructions = instructions;
        assert_configuration(current_base, &updated_input);
        let sibling_spec = snapshot(&saved, &saved.runs[1].id);
        assert_ne!(sibling_spec.id, base.id);
        assert_ne!(sibling_spec.id, first_spec.id);
        assert_configuration(sibling_spec, &updated_input);
        assert_eq!(sibling_spec.task_id, first.task_id);
        assert_eq!(sibling_spec.created_at, base.created_at);
    }
}

#[tokio::test]
async fn scheduler_without_base_preserves_legacy_snapshot() {
    for slots in [1, 2] {
        let runtime = runtime_with_review_config().await;
        let params = interval_params(slots);
        let input = params.agent_spec.clone().unwrap();
        let created = runtime
            .service()
            .create_task(task_create_context_for(&params), params)
            .await
            .unwrap();
        assert_eq!(runtime.process_due_once(FIRST).await.unwrap(), 1);
        let initial = read(&runtime, &created.task.id).await;
        let first = &initial.runs[0];
        let _handle = start_run(&runtime, first).await;
        let first_spec = snapshot(&initial, &first.id).clone();
        // Fixture only: older versions could leave a task with just a run spec.
        let db = runtime.service().store().database_connection();
        pioneer_entity::task_agent_spec::Entity::delete_by_id(created.agent_spec.unwrap().id)
            .exec(&db)
            .await
            .unwrap();
        assert!(
            read(&runtime, &first.task_id)
                .await
                .agent_specs
                .iter()
                .all(|spec| spec.run_id.is_some())
        );
        assert_eq!(runtime.process_due_once(FIRST + 10).await.unwrap(), 1);
        let saved = read(&runtime, &first.task_id).await;
        assert_eq!(saved.agent_specs.len(), 2);
        assert!(saved.agent_specs.iter().all(|spec| spec.run_id.is_some()));
        assert_eq!(snapshot(&saved, &first.id), &first_spec);
        let sibling_spec = snapshot(&saved, &saved.runs[1].id);
        assert_ne!(sibling_spec.id, first_spec.id);
        assert_configuration(sibling_spec, &input);
    }
}

#[tokio::test]
async fn immediate_run_preserves_base_and_saved_snapshot_identity() {
    let runtime = runtime_with_review_config().await;
    let params = agent_params(TaskTriggerSpec::Immediate);
    let input = params.agent_spec.clone().unwrap();
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    let run = created.run.unwrap();
    let saved = read(&runtime, &run.task_id).await;
    assert_eq!(saved.agent_specs.len(), 2);
    let base = saved
        .agent_specs
        .iter()
        .find(|spec| spec.run_id.is_none())
        .unwrap();
    let run_spec = snapshot(&saved, &run.id);
    assert_ne!(base.id, run_spec.id);
    assert_configuration(base, &input);
    assert_configuration(run_spec, &input);
    assert_eq!(run_spec.created_at, base.created_at);
    assert_eq!(run_spec.updated_at, base.updated_at);
    let store = runtime.service().store();
    let events = store
        .get_task_events(&run.task_id, None)
        .await
        .unwrap()
        .events;
    let event = events
        .iter()
        .find(|event| matches!(event.payload, TaskEventPayload::RunCreated { .. }))
        .unwrap();
    let TaskEventPayload::RunCreated {
        agent_spec: Some(event_spec),
        ..
    } = &event.payload
    else {
        panic!("RunCreated must persist its snapshot");
    };
    assert_eq!(event_spec, run_spec);
    let replay = store
        .append_task_event(event.payload.clone(), run.created_at + 1)
        .await
        .unwrap();
    assert_eq!(replay.append_status, TaskEventAppendStatus::AlreadyExists);
    assert_eq!(
        read(&runtime, &run.task_id).await.agent_specs,
        saved.agent_specs
    );
}

#[tokio::test]
async fn retry_preserves_failed_snapshot_and_uses_its_configuration() {
    let runtime = runtime_with_review_config().await;
    let mut params = agent_params(TaskTriggerSpec::Immediate);
    params.retry_policy = Some(TaskRetryPolicy {
        max_attempts: 2,
        backoff: TaskRetryBackoffKind::Fixed,
        initial_delay_seconds: Some(5),
        max_delay_seconds: None,
        retry_on: vec![TaskErrorClass::Internal],
    });
    let input = params.agent_spec.clone().unwrap();
    let created = runtime
        .service()
        .create_task(task_create_context_for(&params), params)
        .await
        .unwrap();
    let run = created.run.unwrap();
    let handle = start_run(&runtime, &run).await;
    persist_mock_agent_execution(runtime.service().store(), &run).await;
    let before = read(&runtime, &run.task_id).await;
    let failed_spec = snapshot(&before, &run.id).clone();
    let instructions = vec!["Changed after this run was created.".to_owned()];
    runtime
        .service()
        .update_task(
            TaskMutationContext::default(),
            TaskUpdateParams {
                task_id: run.task_id.clone(),
                instructions: Some(instructions.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle
        .fail_run(
            Some(TaskError {
                recovery_diagnostic: None,
                code: "retryable".into(),
                message: "retryable failure".into(),
                class: TaskErrorClass::Internal,
                details: None,
                failed_run_id: Some(run.id.clone()),
            }),
            run.created_at + 10,
        )
        .await
        .unwrap();
    let saved = read(&runtime, &run.task_id).await;
    assert_eq!(saved.agent_specs.len(), 3);
    assert_eq!(snapshot(&saved, &run.id), &failed_spec);
    let retry = saved
        .runs
        .iter()
        .find(|candidate| candidate.retry_of_run_id.as_deref() == Some(run.id.as_str()))
        .unwrap();
    assert_eq!(retry.status, TaskRunStatus::Queued);
    let retry_spec = snapshot(&saved, &retry.id);
    let base = saved
        .agent_specs
        .iter()
        .find(|spec| spec.run_id.is_none())
        .unwrap();
    assert_ne!(retry_spec.id, base.id);
    assert_ne!(retry_spec.id, failed_spec.id);
    assert_eq!(base.prompt.instructions, instructions);
    assert_configuration(retry_spec, &input);
    assert_eq!(retry_spec.created_at, failed_spec.created_at);
    assert_eq!(retry_spec.updated_at, run.created_at + 10);
}
