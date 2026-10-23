use super::cli_task_tools::wait_for_task_cli_activation;
use super::*;
use crate::turn_mcp::result::CanonicalMcpToolResult;

// Native setup admits an ordinary Threads turn into a recording CLI backend.
// All assertions below exercise that CLI's frozen MCP facade, not setup handlers.
async fn start_threads_cli_fixture(
    kind: CLIAgentRuntimeKind,
) -> (
    CliRuntimeSkillPreflightHarness,
    Arc<MessageProcessor>,
    String,
) {
    let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
    *harness.cli_session.projected_mcp_store.lock().await = Some(harness.crud_store.clone());
    let processor = Arc::new(harness.processor.clone());
    processor
        .mark_cli_reasoning_model_ready_for_tests(
            &harness.workspace_id,
            &harness.runtime_id,
            "catalog-cli-choice",
        )
        .await
        .unwrap();
    processor.bind_agent_tool_bridges().await;
    let root_thread = format!("threads_seed_{kind:?}");
    let root_turn = format!("threads_seed_turn_{kind:?}");
    ensure_test_superuser_execution_turn(
        &processor,
        &harness.workspace_id,
        &root_thread,
        &root_turn,
    )
    .await;
    let mut permission = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
        pioneer_protocol::TurnPermissionMode::FullAccess,
        pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
    );
    permission.effective_policy.allowed_tools_restricted = true;
    permission.effective_policy.allowed_tools = pioneer_tools::THREADS_DOMAIN_TOOL_NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    permission.effective_policy.denied_tools = vec!["threads_children_list".to_owned()];
    let mut authority = crate::authorization::ExecutionAuthorizationContext::for_test(
        authenticated_test_superuser().as_ref(),
        &harness.workspace_id,
        &root_thread,
        &permission,
        None,
    );
    authority.bind_test_provider_authority(
        "openai",
        "test-model",
        processor
            .provider_registry
            .authority_fingerprint_for_workspace(&harness.workspace_id, "openai")
            .unwrap()
            .as_str(),
    );
    let mut encoded: JsonValue =
        serde_json::from_str(&authority.to_persisted_json().unwrap()).unwrap();
    encoded["grant_manifest"]["actions"]
        .as_array_mut()
        .unwrap()
        .retain(|action| !action.as_str().unwrap().starts_with("task_"));
    let authority = crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
        &encoded.to_string(),
    )
    .unwrap();
    processor
        .crud_store
        .set_turn_execution_authorization_context(
            &root_turn,
            &authority.to_persisted_json().unwrap(),
        )
        .await
        .unwrap();
    let mut security = pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
        harness._temp.path().to_str().unwrap(),
        pioneer_crud::utc_now().timestamp_millis(),
    );
    security.permission_profile = permission;
    processor
        .crud_store
        .set_turn_execution_security_snapshot(&root_turn, &security)
        .await
        .unwrap();
    let now = pioneer_crud::utc_now();
    processor
        .crud_store
        .upsert_turn_runtime_snapshot(pioneer_crud::NewTurnRuntimeSnapshot {
            turn_id: root_turn.clone(),
            thread_id: root_thread.clone(),
            workspace_id: harness.workspace_id.clone(),
            mode_json: serde_json::to_string(&ThreadMode::Agent).unwrap(),
            model: "test-model".to_owned(),
            provider_name: "openai".to_owned(),
            reasoning_effort: None,
            agent_skill_versions_json: None,
            hook_runtime_context_json: serde_json::to_string(
                &pioneer_agent::AgentTurnHookRuntimeContext::default(),
            )
            .unwrap(),
            workspace_skill_policies_json: "[]".to_owned(),
            input_json: "[]".to_owned(),
            capabilities_json: "[]".to_owned(),
            resolved_artifacts_json: "[]".to_owned(),
            runtime_environment_json: "{}".to_owned(),
            history_json: "[]".to_owned(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let tools = processor
        .materialize_agent_action_tools(pioneer_agent::TurnToolContext {
            workspace_id: harness.workspace_id.clone(),
            thread_id: root_thread,
            turn_id: root_turn.clone(),
        })
        .await
        .unwrap();
    let options = super::thread_launch_choices::call(
        &tools,
        "threads_start_options",
        "seed-options",
        json!({}),
    )
    .await;
    let option = options["threadCreationOptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["label"] == "Current collaboration")
        .unwrap();
    let created = super::thread_launch_choices::call(
        &tools,
        "thread_create",
        "seed-thread",
        json!({"optionId":option["id"]}),
    )
    .await;
    let target = created["targetOptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| {
            !options["targetOptions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|old| old["id"] == t["id"])
        })
        .unwrap();
    let identity = options["identities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["sourceKind"] == "cli_runtime_instance")
        .unwrap();
    super::thread_launch_choices::call(&tools, "threads_turn_start", "seed-cli-start", json!({
        "targetOptionId":target["id"], "input":[{"type":"text","text":"Inspect conversations using the permitted Threads tools."}],
        "launch":{"identity":{"kind":"exact","id":identity["id"]},
        "profile":{"kind":"model","providerId":format!("cli_runtime:{}", harness.runtime_id),"modelId":"catalog-cli-choice"},
        "reasoning":{"effort":"high"}}
    })).await;
    let outbox = pioneer_crud::load_agent_action_outbox(
        &processor.crud_store.database_connection(),
        crate::authorization::action_id_for_call("seed-cli-start").as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    let payload: JsonValue = serde_json::from_str(&outbox.payload_json).unwrap();
    let turn_id = payload["dispatch"]["turn_id"].as_str().unwrap().to_owned();
    let thread_id = payload["dispatch"]["thread_id"].as_str().unwrap();
    let admitted_turn = processor
        .crud_store
        .get_turn(thread_id, &turn_id)
        .await
        .unwrap()
        .unwrap()
        .1;
    let admitted_input = processor
        .crud_store
        .get_turn_inputs(&turn_id)
        .await
        .unwrap();
    // This fixture has no background outbox worker. Deliver the committed
    // start through the normal dispatcher before observing CLI activation.
    let (batch, admission_events) = crate::public_error::test_support::capture_events_async(
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64),
    )
    .await;
    let batch = batch.unwrap();
    assert!(batch.errors.is_empty(), "{:?}", batch.errors);
    assert_eq!(
        batch.delivered, 1,
        "seed Agent start must reach the CLI dispatcher: {admission_events:?}"
    );
    wait_for_recorded_cli_runtime_turn_start(&harness.cli_session).await;
    wait_for_task_cli_activation(&processor, &turn_id).await;
    let activated_turn = processor
        .crud_store
        .get_turn(thread_id, &turn_id)
        .await
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(activated_turn.author, admitted_turn.author);
    assert_eq!(
        activated_turn.permission_profile,
        admitted_turn.permission_profile
    );
    assert_eq!(
        processor
            .crud_store
            .get_turn_inputs(&turn_id)
            .await
            .unwrap(),
        admitted_input
    );
    (harness, processor, turn_id)
}

// A fresh facade for every delivery makes replay reach the durable Agent ledger,
// rather than succeeding only from the facade's in-memory request cache.
async fn invoke_threads_facade(
    processor: &MessageProcessor,
    turn_id: &str,
    call_id: &str,
    name: &str,
    arguments: JsonValue,
) -> anyhow::Result<CanonicalMcpToolResult> {
    use crate::cli_runtime::mcp::{
        coordinator::CliMcpCoordinator,
        facade::{
            CliMcpFacadeProjection, CliMcpFacadeRequestContext, CliMcpFacadeTool,
            CliMcpNoopProgressSink, CliMcpToolFacade,
        },
        grants::{CliMcpConnectionId, CliMcpGrantScope, CliMcpManifestHash},
        limits::CliMcpFacadeProjectionLimits,
    };
    use crate::cli_runtime::session_instance::CliSessionInstanceId;
    use pioneer_cli_mcp_bridge::{AttachRequest, BridgeGeneration};
    let binding = processor
        .crud_store
        .get_cli_runtime_turn_binding(turn_id)
        .await?
        .unwrap();
    let metadata = binding.mcp.as_ref().unwrap();
    let binding_workspace = binding.workspace_id.clone();
    let tools = processor
        .crud_store
        .list_turn_mcp_bindings(turn_id)
        .await?
        .iter()
        .map(|binding| crate::turn_mcp::native_tools::current_identity(&binding_workspace, binding))
        .collect::<Vec<_>>();
    let mut facade_tools = Vec::new();
    for tool in tools {
        let tool = tool.map_err(|e| anyhow::anyhow!("{e:?}"))?;
        facade_tools.push(CliMcpFacadeTool::new(
            tool.raw_tool_name,
            tool.description,
            tool.canonical_schema,
            json!({}),
        )?);
    }
    let projection =
        CliMcpFacadeProjection::new(facade_tools, CliMcpFacadeProjectionLimits::default())?;
    let coordinator = Arc::new(CliMcpCoordinator::default());
    let scope = CliMcpGrantScope::new(
        CliSessionInstanceId::unmanaged_for_test(
            CLIAgentRuntimeSessionKey::new(
                &binding.workspace_id,
                &binding.runtime_id,
                &binding.continuation_thread_id,
            )?,
            metadata.session_generation as u64,
        )?,
        CliMcpManifestHash::new(metadata.manifest_hash.clone()).unwrap(),
    );
    let issued = coordinator
        .issue_grant(scope.clone(), u64::MAX)
        .await
        .unwrap();
    let reservation = coordinator
        .stage_projection(&issued.grant_ref(), projection.fingerprint().clone())
        .await
        .unwrap();
    let bound = coordinator
        .attach(
            &AttachRequest {
                session_id: issued.bridge_session_id.clone(),
                generation: BridgeGeneration::new(metadata.session_generation as u64).unwrap(),
                nonce: issued.nonce.clone(),
            },
            &scope,
            CliMcpConnectionId::for_test(1),
        )
        .await
        .unwrap();
    let facade = CliMcpToolFacade::new(
        coordinator.clone(),
        Arc::new(processor.mcp_service.turn_mcp_invoker()),
        projection,
        Arc::new(CliMcpNoopProgressSink),
    );
    let mut context = CliMcpFacadeRequestContext {
        bound_grant: bound,
        projection_generation: reservation.generation,
        activation_generation: None,
    };
    facade.handle_message(&context, json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{
        "protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"threads-fixture","version":"1"}
    }})).await.unwrap();
    facade
        .handle_message(
            &context,
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await;
    let listed = facade
        .handle_message(
            &context,
            json!({"jsonrpc":"2.0","id":"list","method":"tools/list"}),
        )
        .await
        .unwrap();
    assert!(JsonValue::is_null(&listed["error"]), "{listed}");
    let turn = coordinator
        .reserve_turn(
            &context.bound_grant.grant_ref(),
            context.projection_generation,
            &binding.thread_id,
            turn_id,
        )
        .await
        .unwrap();
    coordinator
        .activate_turn(
            &context.bound_grant,
            turn.activation_generation,
            &binding.native_thread_id,
            binding.native_turn_id.as_deref().unwrap(),
        )
        .await
        .unwrap();
    context.activation_generation = Some(turn.activation_generation);
    let response = facade.handle_message(&context, json!({"jsonrpc":"2.0","id":call_id,"method":"tools/call","params":{"name":name,"arguments":arguments}})).await.unwrap();
    if !JsonValue::is_null(&response["error"]) {
        anyhow::bail!("{response}");
    }
    let result = &response["result"];
    Ok(CanonicalMcpToolResult {
        content: result["content"].clone(),
        structured_content: result.get("structuredContent").cloned(),
        is_error: result["isError"].as_bool().unwrap_or(false),
        duration_ms: 0,
        meta: result.get("_meta").cloned(),
    })
}

#[test]
fn cli_threads_facade_preserves_permissions_routes_launch_and_mutation_replay_for_all_providers() {
    run_standard_stack_message_test("CLI Threads facade integration", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, parent) = start_threads_cli_fixture(kind).await;
            let bindings = processor
                .crud_store
                .list_turn_mcp_bindings(&parent)
                .await
                .unwrap();
            assert_eq!(
                bindings
                    .iter()
                    .filter(|b| b.raw_tool_name == "threads_start_options")
                    .count(),
                1
            );
            assert!(
                !bindings
                    .iter()
                    .any(|b| b.raw_tool_name == "threads_children_list")
            );
            assert!(
                !bindings
                    .iter()
                    .any(|b| b.raw_tool_name.starts_with("task_"))
            );
            for name in pioneer_tools::THREADS_DOMAIN_TOOL_NAMES
                .iter()
                .filter(|name| **name != "threads_children_list")
            {
                let binding = bindings.iter().find(|b| b.raw_tool_name == *name).unwrap();
                let expected_callable = if kind == CLIAgentRuntimeKind::OpenCode {
                    format!("pioneer_{name}")
                } else {
                    format!("mcp__pioneer__{name}")
                };
                assert_eq!(binding.provider_callable_name, expected_callable);
                assert_eq!(binding.canonical_callable_name, *name);
                assert!(!binding.canonical_schema_fingerprint.is_empty());
                crate::turn_mcp::native_tools::current_identity(&harness.workspace_id, binding)
                    .unwrap();
            }
            let skills = processor
                .crud_store
                .find_turn_skill_bindings(&parent)
                .await
                .unwrap();
            assert_eq!(
                skills
                    .iter()
                    .map(|s| s.skill_slug.as_str())
                    .collect::<Vec<_>>(),
                ["threads"]
            );
            let instructions = harness.cli_session.turn_starts.lock().await[0]
                .elevated_instructions
                .text()
                .to_owned();
            assert!(instructions.contains("pioneer-system-threads"));
            assert!(!instructions.contains("Do not finish the parent turn while attached"));
            assert!(!instructions.contains("## Tasks"));
            assert!(instructions.contains("Authorized Pioneer Threads operations"));
            assert!(!instructions.contains("Authorized Pioneer task operations"));

            let authority = processor
                .load_turn_execution_authorization_context(&parent)
                .await
                .unwrap();
            assert!(!authority.grants_action(crate::authorization::ResourceAction::TaskCreate));
            assert!(!authority.grants_action(crate::authorization::ResourceAction::TaskRead));
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&parent)
                .await
                .unwrap()
                .unwrap();
            let restored = match processor.restore_cli_runtime_launch_spec(&binding).await {
                crate::message::turn_handlers::CliRuntimeLaunchSpecRestore::Ready(restored) => {
                    restored
                }
                crate::message::turn_handlers::CliRuntimeLaunchSpecRestore::Unavailable {
                    diagnostic,
                }
                | crate::message::turn_handlers::CliRuntimeLaunchSpecRestore::InvalidBinding {
                    diagnostic,
                } => panic!("{diagnostic}"),
            };
            assert_eq!(restored.elevated_instructions.text(), instructions);
            assert!(
                processor
                    .crud_store
                    .get_turn_runtime_snapshot(&parent)
                    .await
                    .unwrap()
                    .is_some(),
                "Threads-only execution needs its durable runtime snapshot too"
            );

            let listed = invoke_threads_facade(
                &processor,
                &parent,
                "threads-list",
                "threads_list",
                json!({"limit":10}),
            )
            .await
            .unwrap();
            assert!(!listed.is_error);
            let options = invoke_threads_facade(
                &processor,
                &parent,
                "threads-options",
                "threads_start_options",
                json!({}),
            )
            .await
            .unwrap();
            assert!(!options.is_error);
            let options = options.structured_content.unwrap();
            let creation = options["threadCreationOptions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|option| option["label"] == "New delegated root")
                .unwrap();
            let args = json!({"optionId":creation["id"]});
            let created = invoke_threads_facade(
                &processor,
                &parent,
                "threads-create",
                "thread_create",
                args.clone(),
            )
            .await
            .unwrap();
            assert!(!created.is_error, "{kind:?}: {created:?}");
            let created = created.structured_content.unwrap();
            let targets = created["targetOptions"].as_array().unwrap();
            let target = targets
                .iter()
                .find(|target| {
                    !options["targetOptions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|old| old["id"] == target["id"])
                })
                .unwrap();
            let replay = invoke_threads_facade(
                &processor,
                &parent,
                "threads-create",
                "thread_create",
                args.clone(),
            )
            .await
            .unwrap();
            assert!(!replay.is_error);
            assert_eq!(
                replay.structured_content.as_ref().unwrap()["receiptId"],
                created["receiptId"]
            );
            let create_id = crate::turn_mcp::native_tools::cli_task_call_id(
                &parent,
                "thread_create",
                "string:threads-create",
                &args,
            );
            let db = processor.crud_store.database_connection();
            let outbox = pioneer_crud::load_agent_action_outbox(
                &db,
                crate::authorization::action_id_for_call(&create_id).as_str(),
            )
            .await
            .unwrap()
            .unwrap();
            let created_payload: JsonValue = serde_json::from_str(&outbox.payload_json).unwrap();
            let thread_id = created_payload["created_thread_id"].as_str().unwrap();
            let thread = processor
                .crud_store
                .get_thread_model(thread_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                thread.sidebar_visibility,
                pioneer_protocol::ThreadSidebarVisibility::Visible
            );
            assert!(
                processor
                    .crud_store
                    .get_thread_model(thread_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .turns
                    .is_empty()
            );

            let send_args = json!({"targetOptionId":target["id"],"input":[{"type":"text","text":"Saved note, without running a model."}]});
            let sent = invoke_threads_facade(
                &processor,
                &parent,
                "threads-send",
                "thread_message_send",
                send_args.clone(),
            )
            .await
            .unwrap();
            assert!(!sent.is_error, "{sent:?}");
            let sent_again = invoke_threads_facade(
                &processor,
                &parent,
                "threads-send",
                "thread_message_send",
                send_args,
            )
            .await
            .unwrap();
            assert!(!sent_again.is_error);
            assert_eq!(
                sent.structured_content.as_ref().unwrap()["receiptId"],
                sent_again.structured_content.as_ref().unwrap()["receiptId"]
            );
            let saved = processor
                .crud_store
                .get_thread_model(thread_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                saved.turns.len(),
                1,
                "replayed send must have one durable message and no model turn"
            );
            assert_eq!(saved.turns[0].mode, ThreadMode::Message);
            let wrong_route = invoke_threads_facade(&processor, &parent, "threads-denied-route", "thread_message_send",
                json!({"targetOptionId":thread_id,"input":[{"type":"text","text":"Must not be written."}]})).await;
            assert!(
                wrong_route.as_ref().map_or(true, |result| result.is_error),
                "a readable thread ID must not become a write route"
            );
            assert_eq!(
                processor
                    .crud_store
                    .get_thread_model(thread_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .turns
                    .len(),
                1
            );
            let history = invoke_threads_facade(
                &processor,
                &parent,
                "threads-history",
                "threads_history_read",
                json!({"threadId":thread_id,"limit":10}),
            )
            .await
            .unwrap();
            assert!(!history.is_error, "{history:?}");
            assert!(
                !history.structured_content.unwrap()["events"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );

            let identity = options["identities"]
                .as_array()
                .unwrap()
                .iter()
                .find(|i| i["sourceKind"] == "cli_runtime_instance")
                .unwrap();
            let models = invoke_threads_facade(
                &processor,
                &parent,
                "threads-models",
                "threads_identity_odels_list",
                json!({"identityId":identity["id"]}),
            )
            .await
            .unwrap();
            assert!(!models.is_error, "{models:?}");
            let models = models.structured_content.unwrap();
            let executor = models["executors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|e| {
                    e["models"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|m| m["id"] == "catalog-cli-choice")
                })
                .unwrap();
            let start_args = json!({"targetOptionId":target["id"],"input":[{"type":"text","text":"Review the saved note."}],
                "launch":{"identity":{"kind":"exact","id":identity["id"]},
                "profile":{"kind":"model","providerId":executor["providerId"],"modelId":"catalog-cli-choice"},
                "reasoning":{"effort":"high"}}});
            let started = invoke_threads_facade(
                &processor,
                &parent,
                "threads-start",
                "threads_turn_start",
                start_args.clone(),
            )
            .await
            .unwrap();
            assert!(!started.is_error, "{started:?}");
            let start_id = crate::turn_mcp::native_tools::cli_task_call_id(
                &parent,
                "threads_turn_start",
                "string:threads-start",
                &start_args,
            );
            let action = crate::authorization::action_id_for_call(&start_id);
            let outbox = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap();
            let payload: JsonValue = serde_json::from_str(&outbox.payload_json).unwrap();
            let params: pioneer_protocol::TurnStartParams =
                serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
            assert_eq!(payload["dispatch"]["identity"]["id"], identity["id"]);
            assert_eq!(params.thread_id, thread_id);
            assert_eq!(
                params.execution_backend,
                Some(pioneer_protocol::AgentExecutionBackend::CLIAgentRuntime {
                    runtime_id: harness.runtime_id.clone(),
                    runtime_kind: kind,
                })
            );
            assert_eq!(params.model.as_deref(), Some("catalog-cli-choice"));
            assert_eq!(
                params.model_provider.as_deref(),
                executor["providerId"].as_str()
            );
            assert_eq!(params.reasoning.as_ref().unwrap().effort, "high");
            let replay = invoke_threads_facade(
                &processor,
                &parent,
                "threads-start",
                "threads_turn_start",
                start_args,
            )
            .await
            .unwrap();
            assert!(!replay.is_error, "{replay:?}");
            assert_eq!(
                started.structured_content.as_ref().unwrap()["receiptId"],
                replay.structured_content.as_ref().unwrap()["receiptId"]
            );
            assert_eq!(
                pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                    .await
                    .unwrap()
                    .unwrap()
                    .payload_json,
                outbox.payload_json
            );
            assert_eq!(
                processor
                    .crud_store
                    .get_thread_model(thread_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .turns
                    .len(),
                2,
                "replayed start must add only one model turn after the saved message"
            );
            assert!(
                processor
                    .crud_store
                    .list_tasks_created_by_turn(
                        &harness.workspace_id,
                        &processor
                            .crud_store
                            .get_cli_runtime_turn_binding(&parent)
                            .await
                            .unwrap()
                            .unwrap()
                            .thread_id,
                        &parent
                    )
                    .await
                    .unwrap()
                    .is_empty()
            );
            let denied = invoke_threads_facade(
                &processor,
                &parent,
                "denied-children",
                "threads_children_list",
                json!({"threadId":thread_id,"recursive":true,"limit":10}),
            )
            .await
            .unwrap_err();
            assert!(denied.to_string().contains("tool_unbound"));
        }
    });
}
