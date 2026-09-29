use super::*;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

// Each step constructs a new Gateway/AgentManager over the same durable store.
// No requested-tool set, creator adapter or mutation cache survives the step.
fn capsule_step(
    workspace_manager: Arc<WorkspaceManager>,
    store: Arc<CrudStore>,
    workspace: &str,
    thread: &str,
    step: usize,
    calls: Vec<ProviderToolCall>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<TurnItem>> + Send>> {
    capsule_step_with_rounds(
        workspace_manager,
        store,
        workspace,
        thread,
        step,
        vec![calls],
    )
}

fn capsule_step_with_rounds(
    workspace_manager: Arc<WorkspaceManager>,
    store: Arc<CrudStore>,
    workspace: &str,
    thread: &str,
    step: usize,
    rounds: Vec<Vec<ProviderToolCall>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Vec<TurnItem>> + Send>> {
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
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("test-model", "openai")),
            Arc::new(pioneer_provider::ProviderRegistry::with_provider(
                "openai", provider,
            )),
            Arc::new(SessionManager::new()),
            workspace_manager,
            store.clone(),
            test_gateway_secrets(),
            test_summary_config(),
            test_tool_loop_config(),
        ));
        processor.bind_task_bridge().await;
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
        let lineage =
            wait_for_child_lineage_for_run(store.clone(), &response.run.unwrap().id).await;
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
        items
    })
}

fn call(step: usize, name: &str, args: serde_json::Value) -> ProviderToolCall {
    ProviderToolCall {
        id: format!("capsule_{step}_{name}"),
        name: name.to_owned(),
        arguments: args.to_string(),
    }
}

fn assert_outcome(items: &[TurnItem], name: &str, expected_success: bool) {
    let item = items
        .iter()
        .find(|item| {
            matches!(item,
                TurnItem::DynamicToolCall { tool_name, .. } if tool_name == name
            )
        })
        .unwrap_or_else(|| panic!("missing {name}: {items:?}"));
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

fn wait_outputs(items: &[TurnItem]) -> Vec<serde_json::Value> {
    items
        .iter()
        .filter_map(|item| match item {
            TurnItem::DynamicToolCall {
                tool_name, storage, ..
            } if tool_name == "task_wait" => match storage {
                ToolStoragePayload::Metadata { metadata } => {
                    metadata.to_json().get("sanitizedResult").cloned()
                }
                ToolStoragePayload::Summary(summary) => {
                    summary.metadata.to_json().get("sanitizedResult").cloned()
                }
                _ => None,
            },
            _ => None,
        })
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
                json!({"taskIds":[target],"timeoutMs":0}),
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
        let future_output = wait_outputs(&future);
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
            )
            .await;
            let outputs = wait_outputs(&items);
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
        let completed_output = wait_outputs(&completed);
        assert_eq!(completed_output.len(), 1);
        assert_eq!(completed_output[0]["terminalCount"], 1);
        assert_eq!(completed_output[0]["pendingCount"], 1);
        assert_eq!(completed_output[0]["timedOut"], false);
    });
}
