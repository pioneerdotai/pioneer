use super::*;

#[test]
fn composer_history_failure_belongs_to_materialized_child() {
    run_standard_stack_message_test(
        "Composer child history failure",
        check_composer_history_failure_belongs_to_materialized_child(),
    );
}

async fn check_composer_history_failure_belongs_to_materialized_child() {
    for backend in [
        None,
        Some(("codex", CLIAgentRuntimeKind::Codex, "gpt-5")),
        Some(("claude", CLIAgentRuntimeKind::Claude, "claude-sonnet")),
    ] {
        let (tx, mut rx) = mpsc::channel(256);
        let sessions = Arc::new(SessionManager::new());
        let connection = register_authenticated_test_connection(sessions.as_ref(), tx).await;
        let threads = Arc::new(ThreadManager::new("test-model", "openai"));
        let (workspaces, store, workspace) = setup_workspace_manager().await;
        sessions
            .set_connection_workspace(connection, Some(workspace.clone()))
            .await;
        let provider = Arc::new(CaptureSummaryProvider::new("history recovered"));
        let cli = Arc::new(RecordingCliRuntimeSession::default());
        cli.enable_projected_mcp_metadata(store.clone()).await;
        let mut processor = MessageProcessor::new(
            threads.clone(),
            phase_13_provider_registry(provider.clone()),
            sessions,
            workspaces,
            store.clone(),
            test_gateway_secrets(),
            crate::message::summary::SummaryConfig {
                summary_model: None,
                summary_model_provider: None,
                title_model: None,
                title_model_provider: None,
            },
            test_tool_loop_config(),
        );
        if let Some((_, kind, _)) = backend {
            processor = with_enabled_test_cli_runtime_catalog(
                processor
                    .with_cli_runtime_manager_for_tests(test_cli_runtime_manager(cli.clone()))
                    .with_cli_mcp_readiness_override_for_tests(supported_test_cli_mcp_readiness(
                        kind,
                    )),
            );
        }
        let processor = Arc::new(processor);
        if backend.is_some() {
            processor
                .mark_cli_runtimes_ready_for_tests(&workspace)
                .await
                .unwrap();
            sync_test_cli_runtime_identities(&processor).await.unwrap();
        }
        processor.bind_task_bridge().await;
        processor.start_task_event_listener().await;
        let parent = "composer_history_parent";
        let started = threads
            .thread_start_seeded(
                connection,
                workspace.clone(),
                ThreadStartParams {
                    thread_id: parent.into(),
                    workspace_id: workspace.clone(),
                    // Keep automatic title generation out of provider-dispatch assertions.
                    name: Some("History preparation regression".into()),
                    model: Some("test-model".into()),
                    model_provider: Some("openai".into()),
                    sandbox: Some(SandboxMode::FullAccess),
                    mode: Some(ThreadMode::Agent),
                    origin_kind: Some(ThreadOriginKind::Collaborative),
                    sidebar_visibility: Some(ThreadSidebarVisibility::Visible),
                    visibility: None,
                    agent_nickname: None,
                    agent_role: None,
                },
                None,
                None,
            )
            .await
            .unwrap();
        store
            .upsert_thread_model(
                &started.response.thread,
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .unwrap();

        // Fail the actual history writer, not admission or a mocked executor.
        // This would reject turn/start with the old parent-side preparation.
        store
            .with_maintenance_access()
            .database_connection()
            .execute_unprepared(
                "CREATE TRIGGER reject_test_history BEFORE INSERT ON compaction_frozen_history
                 BEGIN SELECT RAISE(ABORT, 'injected history preparation failure'); END;",
            )
            .await
            .unwrap();

        // A second message must also be accepted without stopping its parent.
        for index in 0..3 {
            if index == 2 {
                // Removing only the injected failure must allow provider dispatch.
                store
                    .with_maintenance_access()
                    .database_connection()
                    .execute_unprepared("DROP TRIGGER reject_test_history")
                    .await
                    .unwrap();
            }
            let turn_id = format!("composer_history_message_{index}");
            let request_id = generate_test_request_id("history", &turn_id);
            let mut params = json!({
                "thread_id": parent,
                "turn_id": turn_id,
                "input": [{"type": "text", "text": "prepare history in my child"}],
                "mode": "Agent",
                "model": "test-model",
                "model_provider": "openai",
                "permission_profile": pioneer_protocol::TurnPermissionProfileSelection::full_access()
            });
            if let Some((runtime_id, runtime_kind, model)) = backend {
                params["model"] = json!(model);
                params["model_provider"] = serde_json::Value::Null;
                params["execution_backend"] = json!(AgentExecutionBackend::CLIAgentRuntime {
                    runtime_id: runtime_id.into(),
                    runtime_kind,
                });
            }
            let context = processor
                .session_manager
                .connection_context(connection)
                .await
                .unwrap();
            Arc::clone(&processor)
                .process_owned_request(
                    context,
                    json!({
                        "jsonrpc": "2.0", "id": request_id, "method": "turn/start", "params": params
                    })
                    .to_string(),
                )
                .await;
            let response = recv_response_by_id(&mut rx, &request_id).await;
            let accepted: TurnStartResponse = serde_json::from_value(response.result)
                .expect("history failure must not reject the Composer message");
            assert_eq!(accepted.turn.id, turn_id);
            assert_eq!(
                store
                    .get_turn(parent, &turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Completed
            );

            let tasks = processor
                .task_runtime
                .service()
                .list_tasks(TaskListParams {
                    workspace_id: workspace.clone(),
                    owner_kind: Some(TaskOwnerKind::Thread),
                    owner_id: Some(parent.into()),
                    limit: Some(10),
                    ..Default::default()
                })
                .await
                .unwrap();
            let task = tasks
                .tasks
                .iter()
                .find(|task| task.created_by_turn_id.as_deref() == Some(turn_id.as_str()))
                .unwrap();
            let run_id = wait_for_task_run_id(store.clone(), &task.id).await;
            let lineage = wait_for_child_lineage_for_run(store.clone(), &run_id).await;
            if index == 2 {
                if backend.is_some() {
                    assert_eq!(wait_for_cli_runtime_turn_starts(&cli, 1).await.len(), 1);
                } else {
                    timeout(Duration::from_secs(15), async {
                        while provider.snapshot_requests().is_empty() {
                            sleep(Duration::from_millis(25)).await;
                        }
                    })
                    .await
                    .expect("removing the history failure must restore provider dispatch");
                }
                assert!(
                    store
                        .get_task_run_conversation_snapshot(&run_id)
                        .await
                        .unwrap()
                        .is_some()
                );
                break;
            }
            timeout(Duration::from_secs(15), async {
                loop {
                    if let Some((_, child)) = store
                        .get_turn(&lineage.child_thread_id, &lineage.child_turn_id)
                        .await
                        .unwrap()
                        && child.status != TurnStatus::InProgress
                    {
                        assert_eq!(child.status, TurnStatus::Blocked);
                        break;
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("history failure must close the materialized child");
            assert_eq!(
                wait_for_run_status(store.clone(), &run_id, TaskRunStatus::Blocked).await,
                TaskRunStatus::Blocked,
                "the Task run must become terminal after its child is blocked"
            );
            assert!(
                store
                    .get_task_run_conversation_snapshot(&run_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                provider.snapshot_requests().is_empty(),
                "failed history must never reach the LLM"
            );
            assert!(
                cli.turn_starts.lock().await.is_empty(),
                "failed history must never activate a CLI turn"
            );
        }
    }
}
