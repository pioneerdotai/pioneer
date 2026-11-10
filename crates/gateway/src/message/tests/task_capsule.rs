use super::*;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

#[derive(Debug)]
struct CapsuleStepResult {
    items: Vec<TurnItem>,
    outputs: Vec<(String, serde_json::Value)>,
}

// Each step constructs a new Gateway/AgentManager over the same durable store.
// No requested-tool set, creator adapter or mutation cache survives the step.
fn capsule_step(
    workspace_manager: Arc<WorkspaceManager>,
    store: Arc<CrudStore>,
    workspace: &str,
    thread: &str,
    step: usize,
    calls: Vec<ProviderToolCall>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = CapsuleStepResult> + Send>> {
    capsule_step_with_rounds(
        workspace_manager,
        store,
        workspace,
        thread,
        step,
        vec![calls],
        capsule_review_config(),
    )
}

fn capsule_review_config() -> pioneer_tasks::TaskRuntimeConfig {
    crate::task_runtime_config_from_gateway_tasks_config(
        &pioneer_config::GatewayTasksConfig::default(),
    )
}

fn capsule_step_with_rounds(
    workspace_manager: Arc<WorkspaceManager>,
    store: Arc<CrudStore>,
    workspace: &str,
    thread: &str,
    step: usize,
    rounds: Vec<Vec<ProviderToolCall>>,
    review_config: pioneer_tasks::TaskRuntimeConfig,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = CapsuleStepResult> + Send>> {
    capsule_run_with_rounds(
        workspace_manager,
        store,
        workspace,
        thread,
        step,
        rounds,
        review_config,
        None,
    )
}

fn capsule_run_with_rounds(
    workspace_manager: Arc<WorkspaceManager>,
    store: Arc<CrudStore>,
    workspace: &str,
    thread: &str,
    step: usize,
    rounds: Vec<Vec<ProviderToolCall>>,
    review_config: pioneer_tasks::TaskRuntimeConfig,
    monitor: Option<(String, i64)>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = CapsuleStepResult> + Send>> {
    let workspace = workspace.to_owned();
    let thread = thread.to_owned();
    let calls = rounds.iter().flatten().cloned().collect::<Vec<_>>();
    Box::pin(async move {
        let mut provider = SequencedToolProvider::new(
            rounds.first().cloned().unwrap_or_default(),
            r#"<task_result>{"summary":"capsule step completed"}</task_result>"#,
        );
        if rounds.len() > 1 {
            provider = provider.with_tool_call_rounds(rounds);
        }
        let provider = Arc::new(provider);
        let processor = processor_with_review_config(
            Arc::new(pioneer_provider::ProviderRegistry::with_provider(
                "openai", provider,
            )),
            Arc::new(SessionManager::new()),
            workspace_manager,
            store.clone(),
            review_config,
        );
        processor
            .gateway_secrets
            .set_workspace_provider_api_key(&workspace, "openai", "fixture-key")
            .expect("capsule fixture provider should be configured");
        processor.set_gateway_listen_addr("[::1]:41234".parse().unwrap());
        processor.bind_task_bridge().await;
        let (owner_task_id, owner_run_id) = if let Some((task_id, fire_at)) = monitor {
            let before = store.get_task(&task_id).await.unwrap().unwrap();
            let previous = before
                .runs
                .iter()
                .map(|run| run.run_number)
                .max()
                .unwrap_or(0);
            assert!(
                before
                    .agent_specs
                    .iter()
                    .all(|spec| spec.review_policy.as_ref().is_none_or(|p| !p.is_enabled()))
            );
            processor
                .task_runtime
                .process_due_once(fire_at)
                .await
                .unwrap();
            let current = store.get_task(&task_id).await.unwrap().unwrap();
            let run = current
                .runs
                .iter()
                .max_by_key(|run| run.run_number)
                .unwrap();
            assert!(
                run.run_number > previous,
                "scheduler must admit a new monitor occurrence"
            );
            let run_id = run.id.clone();
            let final_run = timeout(Duration::from_secs(20), async {
                loop {
                    let run = store.get_task_run(&run_id).await.unwrap().unwrap();
                    if run.status.is_terminal() {
                        break run;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("monitor run finishes without requiring its own review");
            assert_eq!(final_run.status, pioneer_protocol::TaskRunStatus::Succeeded);
            (task_id, run_id)
        } else {
            let response = create_task_for_test(
                &processor,
                test_task_create_params(
                    &workspace,
                    &thread,
                    &format!("capsule_parent_turn_{step}"),
                    &format!("Capsule management step {step}"),
                    3,
                ),
            )
            .await
            .expect("start independent root execution");
            assert_eq!(
                wait_for_task_status(store.clone(), &response.task.id, TaskStatus::Completed).await,
                TaskStatus::Completed,
                "capsule step {step} must finish"
            );
            (response.task.id, response.run.unwrap().id)
        };
        // Keep the reconstructed runtime alive until any background worker launched
        // by this occurrence has submitted, after the originating turn completed.
        for call in &calls {
            let args: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
            let submitted_id = if call.name == "task_create" && args["background"] == true {
                store
                    .list_tasks(pioneer_protocol::TaskListParams {
                        workspace_id: workspace.clone(),
                        parent_task_id: Some(owner_task_id.clone()),
                        limit: Some(20),
                        ..Default::default()
                    })
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|task| task.title == args["title"].as_str().unwrap())
                    .map(|task| task.id)
            } else if call.name == "task_revise" {
                args["taskId"].as_str().map(str::to_owned)
            } else {
                None
            };
            if let Some(id) = submitted_id {
                let expected = if args["requireReview"] == true || call.name == "task_revise" {
                    TaskStatus::WaitingReview
                } else {
                    TaskStatus::Completed
                };
                assert_eq!(
                    wait_for_task_status(store.clone(), &id, expected).await,
                    expected
                );
            }
        }
        let lineage = wait_for_child_lineage_for_run(store.clone(), &owner_run_id).await;
        // The root task's run owns the execution turn, not its visible parent turn.
        let items = store
            .list_turn_items_by_type(&lineage.child_turn_id, "dynamic_tool_call")
            .await
            .expect("tool outcomes");
        if calls.iter().any(|call| {
            [
                "task_update",
                "task_pause",
                "task_reschedule",
                "task_resume",
                "task_cancel",
            ]
            .contains(&call.name.as_str())
        }) {
            let binding = pioneer_crud::load_agent_turn_response(
                &store.database_connection(),
                &lineage.child_turn_id,
            )
            .await
            .unwrap()
            .unwrap();
            let actions = pioneer_entity::agent_action::Entity::find()
                .filter(pioneer_entity::agent_action::Column::ExecutionId.eq(binding.execution_id))
                .filter(pioneer_entity::agent_action::Column::ActionKind.eq("control_task"))
                .all(&store.database_connection())
                .await
                .unwrap();
            let successful_controls = items.iter().filter(|item| matches!(item, TurnItem::DynamicToolCall { tool_name, success:Some(true), .. }
            if ["task_update", "task_pause", "task_reschedule", "task_resume", "task_cancel"].contains(&tool_name.as_str()))).count();
            assert_eq!(
                actions.len(),
                successful_controls,
                "each successful control has exactly one durable action"
            );
            for action in actions {
                let receipt = pioneer_crud::load_agent_action_receipt(
                    &store.database_connection(),
                    &action.id,
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(receipt.decision, "allowed");
                assert_eq!(action.status, "committed");
            }
        }
        // UI metadata deliberately hashes strings. Assert against the canonical
        // retained result that the model received, without changing UI storage.
        let mut outputs = Vec::new();
        for entry in store
            .list_turn_llm_context(&lineage.child_turn_id)
            .await
            .unwrap()
        {
            if entry.source != "tool_result_v2" {
                continue;
            }
            let name = entry.tool_name.expect("retained tool result has a name");
            let view: pioneer_protocol::ToolResultView =
                serde_json::from_str(&entry.payload).unwrap();
            if let pioneer_protocol::ToolResultView::Json { value, .. } = view {
                outputs.push((name, value));
            }
        }
        CapsuleStepResult { items, outputs }
    })
}

fn call(step: usize, name: &str, args: serde_json::Value) -> ProviderToolCall {
    ProviderToolCall {
        id: format!("capsule_{step}_{name}"),
        name: name.to_owned(),
        arguments: args.to_string(),
    }
}

fn assert_outcome(step: &CapsuleStepResult, name: &str, expected_success: bool) {
    let item = step
        .items
        .iter()
        .find(|item| {
            matches!(item,
                TurnItem::DynamicToolCall { tool_name, .. } if tool_name == name
            )
        })
        .unwrap_or_else(|| panic!("missing {name}: {step:?}"));
    let TurnItem::DynamicToolCall {
        success, status, ..
    } = item
    else {
        unreachable!()
    };
    assert_eq!(*success, Some(expected_success), "{name}: {item:?}");
    if expected_success {
        assert_eq!(*status, ToolCallStatus::Completed);
    }
}

fn tool_outputs(step: &CapsuleStepResult, name: &str) -> Vec<serde_json::Value> {
    step.outputs
        .iter()
        .filter(|(tool, _)| tool == name)
        .map(|(_, value)| value.clone())
        .collect()
}

#[test]
fn durable_task_management_survives_new_executions_and_runtime_reconstruction() {
    run_standard_stack_message_test("Task capsule continuity", async {
        let temp = tempfile::tempdir().unwrap();
        let connection = Database::connect(format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("capsule.sqlite3").display()
        ))
        .await
        .unwrap();
        let (manager, store, workspace) = setup_workspace_manager_with_connection(connection).await;
        let thread = "task_capsule_root";
        let items = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            0,
            vec![call(
                0,
                "task_create",
                json!({
                    "title":"Durable capsule monitor", "goal":"Check for changes",
                    "instructions":["Check for changes and report a concise result."],
                    "outputInstructions":"Return a short report.",
                    "trigger":{"kind":"cron", "cronExpr":"0 5 * * *", "timezone":"UTC"},
                    "deliveryPolicy":{"mode":"none","includeResult":false,"format":"summary"}
                }),
            )],
        )
        .await;
        assert_outcome(&items, "task_create", true);
        let tasks = store
            .list_tasks(pioneer_protocol::TaskListParams {
                workspace_id: workspace.clone(),
                limit: Some(20),
                ..Default::default()
            })
            .await
            .unwrap();
        let target = tasks
            .iter()
            .find(|t| t.title == "Durable capsule monitor")
            .unwrap()
            .id
            .clone();
        for (step, name, args) in [
            (1, "task_get", json!({"taskId":target})),
            (2, "task_list", json!({})),
            (3, "task_pause", json!({"taskId":target})),
            (
                4,
                "task_update",
                json!({"taskId":target,"title":"Updated capsule monitor"}),
            ),
            (
                5,
                "task_reschedule",
                json!({"taskId":target,"trigger":{"kind":"cron","cronExpr":"0 6 * * *","timezone":"UTC"}}),
            ),
            (6, "task_resume", json!({"taskId":target})),
        ] {
            let items = capsule_step(
                manager.clone(),
                store.clone(),
                &workspace,
                thread,
                step,
                vec![call(step, name, args)],
            )
            .await;
            assert_outcome(&items, name, true);
        }
        assert_eq!(
            store.get_task(&target).await.unwrap().unwrap().task.title,
            "Updated capsule monitor"
        );
        let revision = store
            .get_task(&target)
            .await
            .unwrap()
            .unwrap()
            .task
            .revision;
        let items = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            9,
            vec![call(
                9,
                "task_update",
                json!({"taskId":target,"title":"Must not overwrite","expectedRevision":revision-1}),
            )],
        )
        .await;
        assert_outcome(&items, "task_update", false);
        assert!(format!("{items:?}").contains("task_state_conflict"));
        assert_eq!(
            store
                .get_task(&target)
                .await
                .unwrap()
                .unwrap()
                .task
                .revision,
            revision
        );
        let items = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            10,
            vec![call(
                10,
                "task_update",
                json!({"taskId":target,"title":"Updated capsule monitor"}),
            )],
        )
        .await;
        assert_outcome(&items, "task_update", true);
        assert_eq!(
            store
                .get_task(&target)
                .await
                .unwrap()
                .unwrap()
                .task
                .revision,
            revision,
            "no-op update records its action without a duplicate state transition"
        );
        // Same owner, but a different capsule: observing or controlling it is forbidden.
        let items = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            "other_task_capsule",
            7,
            vec![
                call(7, "task_list", json!({})),
                call(7, "task_get", json!({"taskId":target})),
                call(7, "task_cancel", json!({"taskId":target})),
            ],
        )
        .await;
        assert_outcome(&items, "task_list", true);
        assert_outcome(&items, "task_get", false);
        assert_outcome(&items, "task_cancel", false);
        assert_eq!(
            store.get_task(&target).await.unwrap().unwrap().task.status,
            TaskStatus::Scheduled
        );
        let items = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            8,
            vec![call(8, "task_cancel", json!({"taskId":target}))],
        )
        .await;
        assert_outcome(&items, "task_cancel", true);
        assert_eq!(
            store.get_task(&target).await.unwrap().unwrap().task.status,
            TaskStatus::Cancelled
        );
        let items = capsule_step(
            manager,
            store,
            &workspace,
            thread,
            11,
            vec![call(
                11,
                "task_wait",
                json!({"taskIds":[target],"timeoutMs":1}),
            )],
        )
        .await;
        assert_outcome(&items, "task_wait", true);
    });
}

#[test]
fn task_wait_repeats_timed_out_observations_for_task_and_run_ids() {
    run_standard_stack_message_test("Repeated task wait", async {
        let temp = tempfile::tempdir().unwrap();
        let connection = Database::connect(format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("wait.sqlite3").display()
        ))
        .await
        .unwrap();
        let (manager, store, workspace) = setup_workspace_manager_with_connection(connection).await;
        let thread = "task_wait_repeat_root";
        let mut task_ids = Vec::new();
        let mut run_ids = Vec::new();
        for index in 0..2 {
            let title = format!("Wait target {index}");
            let items = capsule_step(
                manager.clone(),
                store.clone(),
                &workspace,
                thread,
                index,
                vec![call(
                    index,
                    "task_create",
                    json!({
                        "title": title,
                        "goal": "Remain queued for an observation test",
                        "instructions": ["Report when started."],
                        "outputInstructions": "Return a short report.",
                        "trigger": {"kind": "cron", "cronExpr": "0 5 * * *", "timezone": "UTC"},
                        "deliveryPolicy": {"mode": "none", "includeResult": false, "format": "summary"}
                    }),
                )],
            )
            .await;
            assert_outcome(&items, "task_create", true);
            let tasks = store
                .list_tasks(pioneer_protocol::TaskListParams {
                    workspace_id: workspace.clone(),
                    limit: Some(20),
                    ..Default::default()
                })
                .await
                .unwrap();
            let task = tasks.iter().find(|task| task.title == title).unwrap();
            task_ids.push(task.id.clone());
        }

        let future = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            2,
            vec![call(
                2,
                "task_wait",
                json!({"taskIds": [task_ids[0]], "timeoutMs": 1}),
            )],
        )
        .await;
        let future_output = tool_outputs(&future, "task_wait");
        assert_eq!(future_output.len(), 1);
        assert_eq!(future_output[0]["waitable"], false);
        assert_eq!(future_output[0]["nonWaitable"].as_array().unwrap().len(), 1);
        assert_eq!(future_output[0]["timedOut"], false);

        let timestamp = now_timestamp_secs();
        for task_id in &task_ids {
            let run_id = pioneer_protocol::generate_id(21);
            store
                .append_task_event(
                    TaskEventPayload::RunCreated {
                        run: TaskRun {
                            trigger_input: None,
                            id: run_id.clone(),
                            task_id: task_id.clone(),
                            trigger_id: None,
                            parent_run_id: None,
                            run_group_id: run_id.clone(),
                            attempt_number: 1,
                            retry_of_run_id: None,
                            ready_at: Some(timestamp),
                            run_number: 1,
                            status: TaskRunStatus::Queued,
                            executor_kind: TaskExecutorKind::Agent,
                            started_at: None,
                            completed_at: None,
                            heartbeat_at: None,
                            locked_by: None,
                            lock_expires_at: None,
                            result: None,
                            error: None,
                            created_at: timestamp,
                            updated_at: timestamp,
                        },
                        agent_spec: None,
                    },
                    timestamp,
                )
                .await
                .unwrap();
            run_ids.push(run_id);
        }

        for (step, arguments) in [
            (3, json!({"taskIds": task_ids, "timeoutMs": 1})),
            (4, json!({"runIds": run_ids, "timeoutMs": 1})),
        ] {
            let rounds = (0..4)
                .map(|round| vec![call(step * 10 + round, "task_wait", arguments.clone())])
                .collect();
            let items = capsule_step_with_rounds(
                manager.clone(),
                store.clone(),
                &workspace,
                thread,
                step,
                rounds,
                capsule_review_config(),
            )
            .await;
            let outputs = tool_outputs(&items, "task_wait");
            assert_eq!(
                outputs.len(),
                4,
                "every wait should reach the handler: {items:?}"
            );
            for output in outputs {
                assert_eq!(output["timedOut"], true);
                assert_eq!(output["pendingCount"], 2);
                assert!(output.get("recommendation").is_none());
                assert!(output.get("repeatedWait").is_none());
            }
        }
        for task_id in &task_ids {
            let state = store.get_task(task_id).await.unwrap().unwrap();
            assert_eq!(state.task.status, TaskStatus::Scheduled);
            assert_eq!(state.runs[0].status, TaskRunStatus::Queued);
        }

        store
            .append_task_event(
                TaskEventPayload::RunCompleted {
                    task_id: task_ids[0].clone(),
                    run_id: run_ids[0].clone(),
                    result: None,
                    completed_at: timestamp + 1,
                },
                timestamp + 1,
            )
            .await
            .unwrap();
        let completed = capsule_step(
            manager,
            store,
            &workspace,
            thread,
            5,
            vec![call(
                5,
                "task_wait",
                json!({"runIds": run_ids, "timeoutMs": 1}),
            )],
        )
        .await;
        let completed_output = tool_outputs(&completed, "task_wait");
        assert_eq!(completed_output.len(), 1);
        assert_eq!(completed_output[0]["terminalCount"], 1);
        assert_eq!(completed_output[0]["pendingCount"], 1);
        assert_eq!(completed_output[0]["timedOut"], false);
    });
}

async fn capsule_deliveries(
    store: &CrudStore,
    workspace: &str,
    task: &str,
) -> Vec<pioneer_protocol::TaskDelivery> {
    store
        .list_task_deliveries(pioneer_protocol::TaskDeliveriesParams {
            workspace_id: workspace.to_owned(),
            task_id: Some(task.to_owned()),
            run_id: None,
            statuses: Vec::new(),
            limit: Some(10),
        })
        .await
        .unwrap()
        .deliveries
}

#[test]
fn reviewed_background_survives_creator_and_is_reviewed_by_later_capsule_executions() {
    run_standard_stack_message_test("Detached capsule review", async {
        let temp = tempfile::tempdir().unwrap();
        let connection = Database::connect(format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("review.sqlite3").display()
        ))
        .await
        .unwrap();
        let (manager, store, workspace) = setup_workspace_manager_with_connection(connection).await;
        let thread = "review_capsule_root";
        let created = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            100,
            vec![call(
                100,
                "task_create",
                json!({"title":"Reviewed detached worker", "goal":"Return a capsule report",
                "background":true,"requireReview":true}),
            ), call(150, "task_create", json!({"title":"Second reviewed worker", "goal":"Return independent evidence", "background":true,"requireReview":true})),
            call(151, "task_create", json!({
                "title":"Recurring capsule monitor", "goal":"Review saved workers", "requireReview":false,
                "instructions":["Read saved worker task IDs and use task_get, task_result, task_revise and task_accept under current permissions."],
                "outputInstructions":"Report confirmed review decisions.",
                "trigger":{"kind":"interval","intervalSeconds":10,"intervalAnchorAt":4_000_000_000_i64},
                "concurrencyPolicy":{"maxParallelRuns":1,"onConflict":"queue"}
            }))],
        )
        .await;
        assert_outcome(&created, "task_create", true);
        let tasks = store
            .list_tasks(pioneer_protocol::TaskListParams {
                workspace_id: workspace.clone(),
                limit: Some(20),
                ..Default::default()
            })
            .await
            .unwrap();
        let task = tasks
            .iter()
            .find(|task| task.title == "Reviewed detached worker")
            .unwrap();
        assert_eq!(
            wait_for_task_status(store.clone(), &task.id, TaskStatus::WaitingReview).await,
            TaskStatus::WaitingReview
        );
        let original = store.get_task(&task.id).await.unwrap().unwrap();
        let policy = original.task.delivery_policy.as_ref().unwrap();
        assert_eq!(policy.mode, pioneer_protocol::TaskDeliveryMode::None);
        assert!(
            policy.thread_target.is_none()
                && policy.thread_id.is_none()
                && policy.webhook_url.is_none()
        );
        let actor = store
            .get_task_actor_contract(&task.id)
            .await
            .unwrap()
            .unwrap();
        assert!(!actor.delivery.enabled);
        assert!(
            actor.delivery.destination_thread_id.is_none()
                && actor.delivery.destination_user_id.is_none()
        );
        assert!(actor.delivery.route_id.is_none() && actor.delivery.route_receipt_json.is_none());
        assert!(
            actor.delivery.destination_webhook_url_fingerprint.is_none()
                && actor.delivery.return_route_id.is_none()
        );

        let candidate = original
            .result_candidates
            .iter()
            .find(|c| c.status == pioneer_protocol::TaskResultCandidateStatus::PendingReview)
            .unwrap();
        let worker_thread = candidate.thread_id.clone();
        let run = candidate.run_id.clone();
        let occurrence = store
            .get_task_occurrence_contract_by_run(&run)
            .await
            .unwrap()
            .unwrap();
        let plan = occurrence.delivery_plan.unwrap();
        assert_eq!(plan.policy.mode, pioneer_protocol::TaskDeliveryMode::None);
        assert!(plan.presentation_thread_id.is_none());
        assert!(occurrence.result_return_route_id.is_none());
        assert!(
            capsule_deliveries(&store, &workspace, &task.id)
                .await
                .is_empty()
        );
        let monitor_id = tasks
            .iter()
            .find(|t| t.title == "Recurring capsule monitor")
            .unwrap()
            .id
            .clone();
        let second_id = tasks
            .iter()
            .find(|t| t.title == "Second reviewed worker")
            .unwrap()
            .id
            .clone();
        let creations = tool_outputs(&created, "task_create");
        let creation = creations
            .iter()
            .find(|item| item["taskId"] == task.id)
            .unwrap();
        assert_eq!(creation["deliveryMode"], "none");
        assert_eq!(
            creation["waitRecommendation"],
            "do_not_call_task_wait_observe_detached_task"
        );

        assert_eq!(
            original
                .agent_specs
                .last()
                .unwrap()
                .review_policy
                .as_ref()
                .unwrap()
                .max_revision_rounds,
            5
        );
        // Reconstructed Gateway; neither creator binding nor its work graph is retained.
        let observed = capsule_run_with_rounds(
            manager.clone(),
            store.clone(),
            &workspace,
            thread,
            101,
            vec![vec![call(101, "task_get", json!({"taskId":task.id}))]],
            capsule_review_config(),
            Some((monitor_id.clone(), 4_000_000_000)),
        )
        .await;
        assert_outcome(&observed, "task_get", true);
        let outputs = tool_outputs(&observed, "task_get");
        assert_eq!(
            outputs[0]["reviewRequired"][0]["candidate"]["id"],
            candidate.id
        );
        assert!(
            outputs[0]["reviewRequired"][0]
                .get("reviewContent")
                .is_none()
        );
        assert!(outputs[0].get("operator").is_none());
        assert_eq!(
            outputs[0]["reviewRequired"][0]["remainingRevisionRounds"],
            5
        );
        let actions = outputs[0]["reviewRequired"][0]["allowedActions"]
            .as_array()
            .unwrap();
        assert!(actions.contains(&json!("task_accept")));
        assert!(actions.contains(&json!("task_revise")));

        let foreign = capsule_step(
            manager.clone(),
            store.clone(),
            &workspace,
            "foreign_review_capsule",
            102,
            vec![
                call(102, "task_result", json!({"candidateId":candidate.id})),
                call(
                    103,
                    "task_accept",
                    json!({"taskId":task.id,"runId":run,"candidateId":candidate.id}),
                ),
            ],
        )
        .await;
        assert_outcome(&foreign, "task_result", false);
        assert_outcome(&foreign, "task_accept", false);
        let revised = capsule_run_with_rounds(manager.clone(), store.clone(), &workspace, thread, 103,
            vec![vec![call(104,"task_result",json!({"candidateId":candidate.id}))],
                 vec![call(105,"task_revise",json!({"taskId":task.id,"runId":run,"candidateId":candidate.id,"feedback":"Include the complete evidence."}))]], capsule_review_config(), Some((monitor_id.clone(), 4_000_000_010))).await;
        assert_outcome(&revised, "task_result", true);
        assert_outcome(&revised, "task_revise", true);
        assert_eq!(
            wait_for_task_status(store.clone(), &task.id, TaskStatus::WaitingReview).await,
            TaskStatus::WaitingReview
        );
        let replacement = store.get_task(&task.id).await.unwrap().unwrap();
        let next = replacement
            .result_candidates
            .iter()
            .find(|c| c.status == pioneer_protocol::TaskResultCandidateStatus::PendingReview)
            .unwrap();
        assert_ne!(candidate.id, next.id);
        assert_eq!(next.thread_id, worker_thread);
        assert_eq!(next.round, 1);
        let accepted = capsule_run_with_rounds(
            manager,
            store.clone(),
            &workspace,
            thread,
            104,
            vec![
                vec![call(109, "task_get", json!({"taskId":task.id}))],
                vec![call(106, "task_result", json!({"candidateId":next.id}))],
                vec![call(
                    107,
                    "task_accept",
                    json!({"taskId":task.id,"runId":run,"candidateId":candidate.id}),
                )],
                vec![call(
                    108,
                    "task_accept",
                    json!({"taskId":task.id,"runId":run,"candidateId":next.id}),
                )],
                vec![call(152, "task_finish", json!({"taskId":monitor_id}))],
            ],
            capsule_review_config(),
            Some((monitor_id.clone(), 4_000_000_020)),
        )
        .await;
        assert_outcome(&accepted, "task_get", true);
        let observed = tool_outputs(&accepted, "task_get");
        assert_eq!(observed[0]["reviewRequired"][0]["candidate"]["id"], next.id);
        assert_eq!(
            observed[0]["reviewRequired"][0]["remainingRevisionRounds"],
            4
        );
        assert_outcome(&accepted, "task_result", true);
        assert_outcome(&accepted, "task_accept", false);
        // The old round remains rejected; the final decision targets only the replacement.
        let final_state = store.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(final_state.task.status, TaskStatus::Completed);
        assert_eq!(
            store
                .get_task(&second_id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::WaitingReview,
            "individual acceptance does not wait for all workers to be accepted"
        );
        let monitor = store.get_task(&monitor_id).await.unwrap().unwrap();
        assert_eq!(monitor.task.status, TaskStatus::Completed);
        assert_eq!(monitor.runs.len(), 3);

        assert!(final_state.task.result.is_some());
        assert!(
            capsule_deliveries(&store, &workspace, &task.id)
                .await
                .is_empty()
        );

        assert_eq!(
            final_state
                .result_candidates
                .iter()
                .find(|c| c.id == candidate.id)
                .unwrap()
                .status,
            pioneer_protocol::TaskResultCandidateStatus::Rejected
        );
        assert_eq!(
            final_state
                .result_candidates
                .iter()
                .find(|c| c.id == next.id)
                .unwrap()
                .status,
            pioneer_protocol::TaskResultCandidateStatus::Accepted
        );
        let decisions = &final_state.result_review_events;
        assert_ne!(
            decisions[0].reviewer_turn_id,
            decisions.last().unwrap().reviewer_turn_id
        );
        assert_ne!(decisions[0].reviewer, decisions.last().unwrap().reviewer);
    });
}

#[test]
fn background_creation_preserves_ordinary_completion_and_rejects_disabled_review() {
    run_standard_stack_message_test("Background review admission", async {
        for (enabled, background) in [(true, true), (true, false), (false, true), (false, false)] {
            for review in [None, Some(false)] {
                let (manager, store, workspace) = setup_workspace_manager().await;
                let mut config = capsule_review_config();
                config.review.enabled = enabled;
                // The globally disabled case must reject even with advanced admission enabled.
                config.review.allow_task_create_review_policy = !enabled;
                let mut ordinary_args = json!({"title":"Ordinary background", "goal":"Return a concise report", "background":background});
                if let Some(review) = review {
                    ordinary_args["requireReview"] = json!(review);
                }
                let mut rounds = vec![vec![call(200, "task_create", ordinary_args)]];
                if !enabled {
                    rounds.insert(0, vec![call(201, "task_create", json!({
                    "title":"Disabled reviewed background", "goal":"Return a report for review",
                    "background":background,"requireReview":true
                }))]);
                }
                let items = capsule_step_with_rounds(
                    manager,
                    store.clone(),
                    &workspace,
                    "background_admission_root",
                    200,
                    rounds,
                    config,
                )
                .await;
                let tasks = store
                    .list_tasks(pioneer_protocol::TaskListParams {
                        workspace_id: workspace,
                        limit: Some(20),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                assert!(
                    !tasks
                        .iter()
                        .any(|task| task.title == "Disabled reviewed background")
                );
                let ordinary = tasks
                    .iter()
                    .find(|task| task.title == "Ordinary background")
                    .unwrap();
                let response = store.get_task(&ordinary.id).await.unwrap().unwrap();
                assert_eq!(response.task.status, TaskStatus::Completed);
                let policy = response.task.delivery_policy.as_ref().unwrap();
                assert_eq!(
                    policy.mode,
                    if background {
                        pioneer_protocol::TaskDeliveryMode::Thread
                    } else {
                        pioneer_protocol::TaskDeliveryMode::None
                    }
                );
                if background {
                    assert_eq!(
                        policy.thread_target,
                        Some(pioneer_protocol::TaskDeliveryThreadTarget::OriginThread)
                    );
                    assert_eq!(
                        policy.thread_id.as_deref(),
                        Some("background_admission_root")
                    );
                }
                assert_eq!(
                    capsule_deliveries(&store, &response.task.workspace_id, &ordinary.id)
                        .await
                        .len(),
                    usize::from(background)
                );
                assert!(
                    response
                        .agent_specs
                        .iter()
                        .all(|spec| spec.review_policy.as_ref().is_none_or(|p| !p.is_enabled()))
                );
                assert_eq!(response.result_candidates.len(), 1);
                assert_eq!(
                    response.result_candidates[0].status,
                    pioneer_protocol::TaskResultCandidateStatus::Accepted
                );
                let outcomes = items
                    .items
                    .iter()
                    .filter_map(|item| match item {
                        TurnItem::DynamicToolCall {
                            tool_name, success, ..
                        } if tool_name == "task_create" => Some(*success),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    outcomes,
                    if enabled {
                        vec![Some(true)]
                    } else {
                        vec![Some(false), Some(true)]
                    }
                );
            }
        }
    });
}

#[test]
fn reviewed_background_explicit_delivery_is_frozen_and_queued_once_after_acceptance() {
    run_standard_stack_message_test("Explicit reviewed delivery", async {
        for include_result in [true, false] {
            let (manager, store, workspace) = setup_workspace_manager().await;
            let root = "explicit_delivery_root";
            let created = capsule_step(
                manager.clone(),
                store.clone(),
                &workspace,
                root,
                300,
                vec![call(
                    300,
                    "task_create",
                    json!({"title":"Explicit reviewed worker", "goal":"Return a report",
                    "background":true,"requireReview":true,
                    "deliveryPolicy":{"mode":"thread","threadTarget":"current_thread",
                        "includeResult":include_result,"format":"full_result"}}),
                )],
            )
            .await;
            assert_outcome(&created, "task_create", true);
            let creation = tool_outputs(&created, "task_create");
            assert_eq!(
                creation[0]["waitRecommendation"],
                "do_not_call_task_wait_observe_detached_task"
            );
            let tasks = store
                .list_tasks(pioneer_protocol::TaskListParams {
                    workspace_id: workspace.clone(),
                    limit: Some(20),
                    ..Default::default()
                })
                .await
                .unwrap();
            let task = tasks
                .iter()
                .find(|task| task.title == "Explicit reviewed worker")
                .unwrap();
            let state = store.get_task(&task.id).await.unwrap().unwrap();
            let candidate = state
                .result_candidates
                .iter()
                .find(|c| c.status == pioneer_protocol::TaskResultCandidateStatus::PendingReview)
                .unwrap();
            let policy = state.task.delivery_policy.as_ref().unwrap();
            assert_eq!(policy.mode, pioneer_protocol::TaskDeliveryMode::Thread);
            assert_eq!(
                policy.thread_target,
                Some(pioneer_protocol::TaskDeliveryThreadTarget::CurrentThread)
            );
            assert_eq!(policy.thread_id, state.task.created_by_thread_id);
            assert_ne!(policy.thread_id.as_deref(), Some(root));
            assert_eq!(policy.include_result, include_result);
            assert_eq!(
                policy.format,
                pioneer_protocol::TaskDeliveryFormat::FullResult
            );
            let occurrence = store
                .get_task_occurrence_contract_by_run(&candidate.run_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&occurrence.delivery_plan.unwrap().policy, policy);
            assert!(
                capsule_deliveries(&store, &workspace, &task.id)
                    .await
                    .is_empty()
            );
            let accepted = capsule_step_with_rounds(manager,store.clone(),&workspace,root,301,
                vec![vec![call(301,"task_result",json!({"candidateId":candidate.id}))],
                    vec![call(302,"task_accept",json!({"taskId":task.id,"runId":candidate.run_id,"candidateId":candidate.id}))],
                    vec![call(303,"task_accept",json!({"taskId":task.id,"runId":candidate.run_id,"candidateId":candidate.id}))]],
                capsule_review_config()).await;
            assert_outcome(&accepted, "task_accept", true);
            let decisions = tool_outputs(&accepted, "task_accept");
            assert_eq!(decisions.len(), 2);
            assert_eq!(decisions[1]["alreadyAccepted"], true);
            let deliveries = capsule_deliveries(&store, &workspace, &task.id).await;
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].target_thread_id, policy.thread_id);
            assert_eq!(deliveries[0].thread_target, policy.thread_target);
            assert_eq!(deliveries[0].result_snapshot.is_some(), include_result);
            if include_result {
                assert_eq!(deliveries[0].result_snapshot, candidate.result);
            }
            assert_eq!(
                store.get_task(&task.id).await.unwrap().unwrap().task.status,
                TaskStatus::Completed
            );
        }
    });
}

#[test]
fn future_task_creation_uses_the_same_explicit_review_choice_for_every_trigger() {
    run_standard_stack_message_test("Uniform future-task review", async {
        for trigger in [
            json!({"kind":"scheduled_at","scheduledAt":4_000_000_000_i64}),
            json!({"kind":"interval","intervalSeconds":10,"intervalAnchorAt":4_000_000_000_i64}),
            json!({"kind":"cron","cronExpr":"0 7 * * *","timezone":"UTC"}),
            json!({"kind":"external","source":"webhook"}),
        ] {
            for review in [None, Some(false), Some(true)] {
                let (manager, store, workspace) = setup_workspace_manager().await;
                let mut args = json!({
                    "title":"Future reviewed work", "goal":"Inspect saved input",
                    "instructions":["Inspect the current input and report evidence."],
                    "outputInstructions":"Return a concise report.", "trigger":trigger
                });
                if let Some(review) = review {
                    args["requireReview"] = json!(review);
                }
                let items = capsule_step(
                    manager,
                    store.clone(),
                    &workspace,
                    "uniform_review_root",
                    400,
                    vec![call(400, "task_create", args)],
                )
                .await;
                assert_outcome(&items, "task_create", true);
                let created = tool_outputs(&items, "task_create");
                let id = created[0]["taskId"].as_str().unwrap();
                let state = store.get_task(id).await.unwrap().unwrap();
                assert!(
                    state.runs.is_empty(),
                    "creation waits for the future trigger"
                );
                let policy = state.agent_specs.last().unwrap().review_policy.as_ref();
                assert_eq!(
                    policy.is_some_and(|policy| policy.is_enabled()),
                    review == Some(true)
                );
                if let Some(policy) = policy {
                    assert!(
                        policy.disable_timeout_acceptance && policy.require_explicit_acceptance
                    );
                }
                assert_eq!(
                    state.task.delivery_policy.as_ref().unwrap().mode,
                    pioneer_protocol::TaskDeliveryMode::Thread
                );
            }
        }
    });
}
