//! Regressions use only the existing recording runtime and isolated stores.
use super::*;

const CLI_KINDS: [CLIAgentRuntimeKind; 3] = [
    CLIAgentRuntimeKind::Codex,
    CLIAgentRuntimeKind::Claude,
    CLIAgentRuntimeKind::OpenCode,
];

#[test]
fn cli_probe_failure_reaches_execution_and_never_repeats_an_accepted_turn() {
    run_gateway_message_test("cli-probe-isolation", || async {
        for kind in CLI_KINDS {
            for fail_operation in [false, true] {
                let mut harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
                let thread_id = "probe-isolation-thread";
                let turn_id = "probe-isolation-turn";
                seed_cli_runtime_skill_preflight_thread(&harness, thread_id).await;
                if !fail_operation {
                    harness
                        .processor
                        .invalidate_and_request_cli_runtime_warmup(&harness.workspace_id)
                        .await;
                    assert!(
                        harness
                            .processor
                            .cli_runtime_probe_snapshot(&harness.workspace_id, &harness.runtime_id)
                            .await
                            .unwrap()
                            .unwrap()
                            .models
                            .is_none(),
                        "first operation has no successful catalog"
                    );
                    let list_request_id = generate_test_request_id("probe", "no-models");
                    let list_request = json!({
                        "jsonrpc":"2.0", "id":list_request_id, "method": pioneer_protocol::constants::methods::CLI_RUNTIME_LIST_MODELS,
                        "params": { "workspace_id": harness.workspace_id, "runtime_id": harness.runtime_id },
                    }).to_string();
                    harness
                        .processor
                        .process_request_for_connection(harness.connection_id, &list_request)
                        .await;
                    let response = recv_response_by_id(&mut harness.rx, &list_request_id).await;
                    let catalog: pioneer_protocol::CLIRuntimeListModelsResponse =
                        serde_json::from_value(response.result).unwrap();
                    assert!(catalog.models.is_empty());
                    assert!(catalog.refreshed_at_unix_ms.is_none());
                    assert!(catalog.diagnostics.iter().any(
                        |diagnostic| diagnostic.code == "cli_runtime.model_catalog_unavailable"
                    ));
                }
                harness
                    .processor
                    .mark_cli_runtime_probe_failed_for_tests(
                        &harness.workspace_id,
                        &harness.runtime_id,
                    )
                    .await
                    .unwrap();
                if fail_operation {
                    *harness.cli_session.next_turn_start_error.lock().await =
                        Some("synthetic execution authorization failure".into());
                }
                let request_id = generate_test_request_id("probe", "isolation");
                let request = cli_runtime_turn_start_request_with_capabilities(
                    &request_id,
                    thread_id,
                    turn_id,
                    &harness.runtime_id,
                    kind,
                    Vec::new(),
                );
                harness
                    .processor
                    .process_request_for_connection(harness.connection_id, &request)
                    .await;
                let response = recv_response_by_id(&mut harness.rx, &request_id).await;
                let accepted: TurnStartResponse = serde_json::from_value(response.result)
                    .expect("admission must reach the recording runtime");
                assert_eq!(accepted.turn.id, turn_id);
                assert_eq!(
                    wait_for_cli_runtime_turn_starts(&harness.cli_session, 1)
                        .await
                        .len(),
                    1
                );
                if fail_operation {
                    assert_eq!(
                        wait_for_turn_status(
                            harness.crud_store.clone(),
                            thread_id,
                            turn_id,
                            TurnStatus::Blocked
                        )
                        .await,
                        TurnStatus::Blocked,
                        "execution error must remain a failure"
                    );
                    // Blocked is persisted before terminal session cleanup.
                    timeout(Duration::from_secs(5), async {
                        while harness.cli_session.closes.load(Ordering::SeqCst) == 0 {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .expect("failed execution should close its CLI session");
                } else {
                    timeout(Duration::from_secs(2), async {
                        loop {
                            if harness
                                .crud_store
                                .get_cli_runtime_turn_binding(turn_id)
                                .await
                                .unwrap()
                                .is_some_and(|binding| binding.native_turn_id.is_some())
                            {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                }
                let starts = harness.cli_session.factory_starts.load(Ordering::SeqCst);
                let closes = harness.cli_session.closes.load(Ordering::SeqCst);
                assert_eq!(closes, usize::from(fail_operation), "{kind:?} cleanup");
                harness
                    .processor
                    .mark_cli_runtime_probe_failed_for_tests(
                        &harness.workspace_id,
                        &harness.runtime_id,
                    )
                    .await
                    .unwrap();
                assert_eq!(harness.cli_session.turn_starts.lock().await.len(), 1);
                assert_eq!(
                    harness.cli_session.factory_starts.load(Ordering::SeqCst),
                    starts
                );
                assert_eq!(
                    harness.cli_session.closes.load(Ordering::SeqCst),
                    closes,
                    "probe failure must not close a session: {kind:?}"
                );
                let snapshot = harness
                    .processor
                    .cli_runtime_probe_snapshot(&harness.workspace_id, &harness.runtime_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(matches!(
                    snapshot.summary.status,
                    pioneer_protocol::RuntimeStatus::Error { .. }
                ));
            }
        }
    });
}

#[test]
fn cli_probe_failure_does_not_bypass_technical_mcp_admission() {
    run_gateway_message_test("cli-probe-mcp-admission", || async {
        for kind in CLI_KINDS {
            let mut harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
            seed_ready_fake_mcp_server(
                &harness.processor,
                harness.crud_store.as_ref(),
                &harness.workspace_id,
            )
            .await;
            seed_cli_runtime_skill_preflight_thread(&harness, "probe-mcp-thread").await;
            harness
                .processor
                .mark_cli_runtime_probe_failed_for_tests(&harness.workspace_id, &harness.runtime_id)
                .await
                .unwrap();
            let mut unavailable = supported_test_cli_mcp_readiness(kind);
            unavailable.supported = false;
            unavailable.strict_isolation = false;
            harness.processor = harness
                .processor
                .with_cli_mcp_readiness_override_for_tests(unavailable);
            let request_id = generate_test_request_id("probe", "mcp");
            let request = cli_runtime_turn_start_request_with_capabilities(
                &request_id,
                "probe-mcp-thread",
                "probe-mcp-turn",
                &harness.runtime_id,
                kind,
                // Select a real fixture tool so every provider enters MCP admission.
                vec![
                    serde_json::to_value(TurnCapability {
                        id: pioneer_protocol::mcp_tool_capability_key(
                            McpScopeKind::Workspace,
                            "resend",
                            "send",
                        ),
                        kind: TurnCapabilityKind::McpTool {
                            server_name: "resend".into(),
                            raw_tool_name: "send".into(),
                            scope_kind: McpScopeKind::Workspace,
                        },
                        label: None,
                    })
                    .unwrap(),
                ],
            );
            harness
                .processor
                .process_request_for_connection(harness.connection_id, &request)
                .await;
            let payload = recv_jsonrpc_payload_by_id(&mut harness.rx, &request_id).await;
            let error: JsonRpcErrorResponse = serde_json::from_str(&payload)
                .expect("unsupported MCP admission must return an error, not accept the turn");
            assert_public_error(
                &error,
                PublicErrorCode::Internal,
                PublicErrorStage::Admission,
            );
            assert_eq!(harness.cli_session.factory_starts.load(Ordering::SeqCst), 0);
            assert!(harness.cli_session.turn_starts.lock().await.is_empty());
        }
    });
}

#[test]
fn cli_probe_failure_uses_the_same_admission_for_durable_tasks() {
    run_gateway_message_test("cli-probe-task-admission", || async {
        for kind in CLI_KINDS {
            let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
            let processor = Arc::new(harness.processor);
            processor
                .mark_cli_runtime_probe_failed_for_tests(&harness.workspace_id, &harness.runtime_id)
                .await
                .unwrap();
            processor.bind_task_bridge().await;
            processor.start_task_event_listener().await;
            let model = match kind {
                CLIAgentRuntimeKind::Codex => "o4-mini",
                CLIAgentRuntimeKind::Claude => "claude-sonnet",
                CLIAgentRuntimeKind::OpenCode => "openai/gpt-5",
            };
            let params = detached_cli_task_create_params(
                &harness.workspace_id,
                "probe-task-parent",
                "probe-task-launch",
                &harness.runtime_id,
                kind,
                model,
                "one task execution",
            );
            ensure_task_create_parent_turn_for_test(&processor, &params)
                .await
                .unwrap();
            let result = create_task_for_test(&processor, params).await.unwrap();
            assert!(result.run.is_some());
            assert_eq!(
                wait_for_cli_runtime_turn_starts(&harness.cli_session, 1)
                    .await
                    .len(),
                1
            );
            processor
                .mark_cli_runtime_probe_failed_for_tests(&harness.workspace_id, &harness.runtime_id)
                .await
                .unwrap();
            assert_eq!(harness.cli_session.turn_starts.lock().await.len(), 1);
        }
    });
}

#[test]
fn cli_probe_failure_cannot_admit_a_disabled_or_deleted_configuration() {
    run_gateway_message_test("cli-probe-config-admission", || async {
        for kind in CLI_KINDS {
            for deleted in [false, true] {
                let mut harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
                seed_cli_runtime_skill_preflight_thread(&harness, "probe-config-thread").await;
                harness
                    .processor
                    .mark_cli_runtime_probe_failed_for_tests(
                        &harness.workspace_id,
                        &harness.runtime_id,
                    )
                    .await
                    .unwrap();
                let config = crate::isolated_test_app_config().unwrap();
                let name = config.gateway.settings_file_name.as_str();
                let path = harness.runtime_home.join(name);
                let mut settings = crate::settings::load_or_create_gateway_settings(
                    &path,
                    config.gateway.settings_version,
                    name,
                )
                .unwrap();
                let instances = if deleted {
                    Vec::new()
                } else {
                    vec![pioneer_protocol::GatewayCliRuntimeInstanceSettings {
                        compaction_model: None,
                        id: harness.runtime_id.clone(),
                        kind,
                        display_name: "Disabled CLI".into(),
                        nickname: "disabled-cli".into(),
                        enabled: false,
                        binary_path: "recording-runtime".into(),
                        home_path: harness.native_home.to_string_lossy().into_owned(),
                        shadow_home_path: None,
                    }]
                };
                settings
                    .set_cli_runtime_settings_for_tests(
                        pioneer_protocol::GatewayCliRuntimeSettings { instances },
                    )
                    .unwrap();
                crate::settings::save_gateway_settings(&path, &settings).unwrap();
                let request_id = generate_test_request_id("probe", "config");
                let request = cli_runtime_turn_start_request_with_capabilities(
                    &request_id,
                    "probe-config-thread",
                    "probe-config-turn",
                    &harness.runtime_id,
                    kind,
                    Vec::new(),
                );
                harness
                    .processor
                    .process_request_for_connection(harness.connection_id, &request)
                    .await;
                let _error = recv_error_by_id(&mut harness.rx, &request_id).await;
                assert_eq!(harness.cli_session.factory_starts.load(Ordering::SeqCst), 0);
                assert!(harness.cli_session.turn_starts.lock().await.is_empty());
            }
        }
    });
}
