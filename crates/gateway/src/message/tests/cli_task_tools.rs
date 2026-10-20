use super::*;
use crate::turn_mcp::invoker::{TurnMcpInvocation, TurnMcpInvocationOrigin, TurnMcpInvoker};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use std::path::Path;

async fn start_task_cli_fixture(
    kind: CLIAgentRuntimeKind,
) -> (
    CliRuntimeSkillPreflightHarness,
    Arc<MessageProcessor>,
    String,
) {
    start_task_cli_fixture_with_task_config(kind, pioneer_tasks::TaskRuntimeConfig::default()).await
}

// Keep repeated fixture and invocation state machines off the enclosing test frame.
// These tests still run on the production Gateway worker stack.
fn start_task_cli_fixture_with_task_config(
    kind: CLIAgentRuntimeKind,
    task_config: pioneer_tasks::TaskRuntimeConfig,
) -> MessageFuture<
    'static,
    (
        CliRuntimeSkillPreflightHarness,
        Arc<MessageProcessor>,
        String,
    ),
> {
    message_future(async move {
        let mut harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
        harness.processor.task_runtime = Arc::new(pioneer_tasks::TaskRuntime::new_with_config(
            harness.crud_store.clone(),
            task_config,
        ));
        *harness.cli_session.projected_mcp_store.lock().await = Some(harness.crud_store.clone());
        let thread_id = format!("cli_tasks_{kind:?}").to_lowercase();
        let turn_id = format!("cli_tasks_turn_{kind:?}").to_lowercase();
        seed_cli_runtime_skill_preflight_thread(&harness, &thread_id).await;
        let processor = Arc::new(harness.processor.clone());
        processor.bind_agent_tool_bridges().await;
        let request = cli_runtime_turn_start_request_with_capabilities(
            &generate_test_request_id("cli-tasks", &format!("{kind:?}")),
            &thread_id,
            &turn_id,
            &harness.runtime_id,
            kind,
            Vec::new(),
        );
        processor
            .process_request_for_connection(harness.connection_id, &request)
            .await;
        wait_for_recorded_cli_runtime_turn_start(&harness.cli_session).await;
        wait_for_task_cli_activation(&processor, &turn_id).await;

        (harness, processor, turn_id)
    })
}

async fn wait_for_task_cli_activation(processor: &MessageProcessor, turn_id: &str) {
    timeout(Duration::from_secs(10), async {
        loop {
            if processor
                .crud_store
                .get_cli_runtime_turn_binding(turn_id)
                .await
                .unwrap()
                .is_some_and(|binding| binding.status == "running" && binding.mcp.is_some())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("CLI task fixture activation");
}

// Creation returns a durable TaskRun before its asynchronously admitted child
// necessarily exists. Observe that run's authoritative binding, rather than
// assuming nullable child ids in a creation response are already populated.
async fn wait_for_task_cli_child(
    processor: &MessageProcessor,
    run_id: &str,
    previous_turn_id: Option<&str>,
) -> pioneer_crud::CliRuntimeTurnBindingRecord {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(child) = processor
                .crud_store
                .get_latest_task_run_turn(run_id)
                .await
                .unwrap()
                && Some(child.turn_id.as_str()) != previous_turn_id
                && let Some(binding) = processor
                    .crud_store
                    .get_cli_runtime_turn_binding(&child.turn_id)
                    .await
                    .unwrap()
                && binding.status == "running"
                && binding.native_turn_id.is_some()
                && binding.mcp.is_some()
            {
                return binding;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Task run must activate its exact CLI child")
}

fn invoke_task<'a>(
    processor: &'a MessageProcessor,
    turn_id: &'a str,
    call_id: &'a str,
    name: &'a str,
    arguments: JsonValue,
) -> MessageFuture<
    'a,
    Result<CanonicalMcpToolResult, crate::turn_mcp::invoker::TurnMcpInvocationError>,
> {
    message_future(async move {
        let binding = processor
            .crud_store
            .get_cli_runtime_turn_binding(turn_id)
            .await
            .unwrap()
            .unwrap();
        let generation = binding.mcp.as_ref().unwrap().session_generation as u64;
        processor
            .mcp_service
            .turn_mcp_invoker()
            .invoke(
                TurnMcpInvocation {
                    workspace_id: binding.workspace_id,
                    thread_id: binding.thread_id,
                    turn_id: turn_id.to_owned(),
                    runtime_id: Some(binding.runtime_id),
                    session_generation: Some(generation),
                    provider_call_id: call_id.to_owned(),
                    canonical_callable_name: name.to_owned(),
                    arguments,
                    origin: TurnMcpInvocationOrigin::CliFacade,
                },
                tokio_util::sync::CancellationToken::new(),
            )
            .await
    })
}

#[test]
fn cli_task_runtime_snapshots_preserve_user_cancellation_for_all_providers() {
    run_standard_stack_message_test("CLI task snapshot cancellation", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (mut harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                processor
                    .crud_store
                    .get_turn_runtime_snapshot(&turn_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                processor
                    .crud_store
                    .get_turn_execution(&turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .executor_kind,
                pioneer_crud::TurnExecutorKind::CliRuntime
            );
            assert!(
                processor
                    .crud_store
                    .native_cancellation_context(&turn_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            for attempt in 0..2 {
                let request_id = generate_test_request_id("cancel", &format!("{attempt}-{kind:?}"));
                processor
                    .process_request_for_connection(
                        harness.connection_id,
                        &json!({
                            "jsonrpc":"2.0", "id":request_id, "method":"turn/cancel",
                            "params":{"thread_id":binding.thread_id, "turn_id":turn_id,
                                "reason":"Stop this CLI task parent"}
                        })
                        .to_string(),
                    )
                    .await;
                let response = recv_response_by_id(&mut harness.rx, &request_id).await;
                let cancelled: TurnCancelResponse =
                    serde_json::from_value(response.result).unwrap();
                assert_eq!(cancelled.turn.status, TurnStatus::Interrupted);
            }
            assert!(
                harness
                    .cli_session
                    .interrupts
                    .lock()
                    .await
                    .iter()
                    .any(|(thread, turn)| {
                        thread.as_deref() == Some(binding.native_thread_id.as_str())
                            && turn == &binding.native_turn_id
                    }),
                "{kind:?}: cancellation must reach the original provider turn"
            );
            assert_eq!(
                processor
                    .crud_store
                    .latest_cli_runtime_turn_attempt(&turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_crud::CliRuntimeTurnAttemptStatus::Interrupted
            );
            assert_eq!(harness.cli_session.turn_starts.lock().await.len(), 1);
        }
    });
}

#[test]
fn cli_foreground_child_turn_admits_the_selected_runtime_before_tool_projection() {
    run_standard_stack_message_test("CLI foreground child identity", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (mut harness, processor, parent_turn_id) = start_task_cli_fixture(kind).await;
            harness
                .cli_session
                .set_next_native_turn_id(format!("identity_child_{kind:?}"))
                .await;
            let created = invoke_task(
                &processor,
                &parent_turn_id,
                "identity-child",
                "task_create",
                json!({
                    "title":"Inspect the child scope", "goal":"Inspect the available context",
                    "outputInstructions":"Return findings."
                }),
            )
            .await
            .unwrap();
            assert!(!created.is_error, "{kind:?}: {created:?}");
            let result = created.structured_content.as_ref().unwrap();
            let child =
                wait_for_task_cli_child(&processor, result["runId"].as_str().unwrap(), None).await;
            let task_response = pioneer_crud::load_agent_turn_response(
                &processor.crud_store.database_connection(),
                &child.turn_id,
            )
            .await
            .unwrap()
            .unwrap();
            let cancelled = invoke_task(
                &processor,
                &parent_turn_id,
                "cancel-identity-child",
                "task_cancel",
                json!({"taskId":result["taskId"]}),
            )
            .await
            .unwrap();
            assert!(!cancelled.is_error, "{kind:?}: {cancelled:?}");
            assert_eq!(
                wait_for_turn_status(
                    processor.crud_store.clone(),
                    &child.thread_id,
                    &child.turn_id,
                    TurnStatus::Interrupted
                )
                .await,
                TurnStatus::Interrupted
            );

            // This is a new user-authored turn in the existing internal
            // thread, not a continuation of the Task's AgentExecution.
            open_persisted_child_for_test(
                &processor,
                harness.connection_id,
                &mut harness.rx,
                &harness.workspace_id,
                &child.thread_id,
                &format!("identity-open-{kind:?}"),
            )
            .await;
            let turn_id = format!("foreground_child_identity_{kind:?}");
            harness
                .cli_session
                .set_next_native_turn_id(format!("foreground_identity_{kind:?}"))
                .await;
            let request_id = generate_test_request_id("identity", &format!("{kind:?}"));
            processor
                .process_request_for_connection(
                    harness.connection_id,
                    &cli_runtime_turn_start_request_with_capabilities(
                        &request_id,
                        &child.thread_id,
                        &turn_id,
                        &harness.runtime_id,
                        kind,
                        Vec::new(),
                    ),
                )
                .await;
            let response = recv_response_by_id(&mut harness.rx, &request_id).await;
            let accepted: TurnStartResponse = serde_json::from_value(response.result).unwrap();
            assert_eq!(accepted.turn.id, turn_id);
            wait_for_task_cli_activation(&processor, &turn_id).await;

            let database = processor.crud_store.database_connection();
            let response = pioneer_crud::load_agent_turn_response(&database, &turn_id)
                .await
                .unwrap()
                .unwrap();
            let execution = pioneer_crud::load_agent_execution(&database, &response.execution_id)
                .await
                .unwrap()
                .unwrap();
            let identity = pioneer_crud::load_active_agent_identity_by_source(
                &database,
                &harness.workspace_id,
                pioneer_crud::SOURCE_CLI_RUNTIME_INSTANCE,
                &harness.runtime_id,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(execution.agent_identity_id, identity.id);
            assert_ne!(response.execution_id, task_response.execution_id);
            assert_eq!(
                response.execution_id,
                crate::message::agent_action_tools::root_agent_execution_id_for_turn(&turn_id)
            );
            assert!(execution.parent_execution_id.is_none() && execution.parent_task_id.is_none());
            assert_eq!(
                execution.home_root_thread_id,
                processor
                    .crud_store
                    .get_cli_runtime_turn_binding(&parent_turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .thread_id
            );
            let presentation = pioneer_crud::load_current_agent_presentation_snapshot(
                &database,
                &identity.id,
                identity.source_revision,
                &identity.source_fingerprint,
            )
            .await
            .unwrap()
            .unwrap();
            let author = processor
                .timeline_agent_author_for_turn(&child.thread_id, &turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(author.nickname, presentation.nickname);
            assert_eq!(author.cli_runtime_kind, Some(kind));
            let snapshot = author.agent.unwrap();
            assert_eq!(snapshot.agent_identity_id.as_str(), identity.id);
            assert_eq!(snapshot.agent_execution_id.as_str(), execution.id);
            assert_eq!(
                snapshot.identity_source_kind,
                pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance
            );

            let cancel_id = generate_test_request_id("identity-stop", &format!("{kind:?}"));
            processor.process_request_for_connection(harness.connection_id, &json!({
                "jsonrpc":"2.0", "id":cancel_id, "method":"turn/cancel",
                "params":{"thread_id":child.thread_id,"turn_id":turn_id,"reason":"fixture complete"}
            }).to_string()).await;
            let response = recv_response_by_id(&mut harness.rx, &cancel_id).await;
            let cancelled: TurnCancelResponse = serde_json::from_value(response.result).unwrap();
            assert_eq!(cancelled.turn.status, TurnStatus::Interrupted);
        }
    });
}

#[test]
fn cli_automatic_orchestration_skills_coexist_with_selected_user_names() {
    run_standard_stack_message_test("CLI orchestration skill name isolation", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            for user_name in ["tasks", "subagents"] {
                let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
                *harness.cli_session.projected_mcp_store.lock().await =
                    Some(harness.crud_store.clone());
                let user_path =
                    write_test_skill(&harness.user_root, user_name, "", "# user skill\n");
                std::fs::create_dir_all(user_path.join("references")).unwrap();
                std::fs::write(user_path.join("references/sentinel"), b"user reference").unwrap();
                let user_id = seed_cli_runtime_skill_installation(
                    &harness,
                    &format!("tests/{user_name}"),
                    "user",
                    &user_path,
                )
                .await;
                let thread_id = format!("cli_skill_names_{kind:?}_{user_name}").to_lowercase();
                let turn_id = format!("cli_skill_names_turn_{kind:?}_{user_name}").to_lowercase();
                seed_cli_runtime_skill_preflight_thread(&harness, &thread_id).await;
                let processor = Arc::new(harness.processor.clone());
                processor.bind_agent_tool_bridges().await;
                let request = cli_runtime_turn_start_request_with_capabilities(
                    &generate_test_request_id("cli-skill-names", &format!("{kind:?}-{user_name}")),
                    &thread_id,
                    &turn_id,
                    &harness.runtime_id,
                    kind,
                    vec![cli_runtime_skill_capability(
                        &format!("tests/{user_name}"),
                        "user",
                    )],
                );
                processor
                    .process_request_for_connection(harness.connection_id, &request)
                    .await;
                wait_for_recorded_cli_runtime_turn_start(&harness.cli_session).await;
                wait_for_task_cli_activation(&processor, &turn_id).await;
                let bindings = processor
                    .crud_store
                    .find_turn_skill_bindings(&turn_id)
                    .await
                    .unwrap();
                let snapshot = processor
                    .crud_store
                    .get_turn_runtime_snapshot(&turn_id)
                    .await
                    .unwrap()
                    .expect("CLI task capabilities must be durable before provider start");
                assert_eq!(snapshot.workspace_id, harness.workspace_id);
                assert_eq!(snapshot.thread_id, thread_id);
                assert_eq!(
                    crate::turn_runtime_snapshot::restored_execution_capabilities(
                        &snapshot.capabilities_json,
                    )
                    .unwrap(),
                    vec![pioneer_protocol::TurnCapability {
                        id: pioneer_protocol::skill_capability_key(&user_id),
                        kind: pioneer_protocol::TurnCapabilityKind::Skill {
                            skill_id: user_id.clone(),
                            pack_id: None,
                        },
                        label: Some(format!("tests/{user_name}")),
                    }]
                );
                assert_eq!(bindings.len(), 3);
                assert!(bindings.iter().any(|binding| binding.skill_id == user_id && binding.source_kind == "user"));
                for slug in ["tasks", "subagents"] {
                    assert!(
                        bindings
                            .iter()
                            .any(|binding| binding.source_kind == "system"
                                && binding.skill_owner.as_deref() == Some("pioneer")
                                && binding.skill_slug == slug
                                && binding.resolved_reason == "implicit")
                    );
                    let exported = harness
                        .native_home
                        .join("skills")
                        .join(format!("pioneer-system-{slug}"));
                    assert!(
                        std::fs::read_to_string(exported.join("SKILL.md"))
                            .unwrap()
                            .starts_with(&format!("---\nname: pioneer-system-{slug}\n"))
                    );
                    let canonical = Path::new(env!("CARGO_MANIFEST_DIR"))
                        .join("../../resources/skills/pioneer")
                        .join(slug);
                    for reference in std::fs::read_dir(canonical.join("references")).unwrap() {
                        let reference = reference.unwrap();
                        assert_eq!(
                            std::fs::read(exported.join("references").join(reference.file_name()))
                                .unwrap(),
                            std::fs::read(reference.path()).unwrap()
                        );
                    }
                }
                let exported_user = harness.native_home.join("skills").join(user_name);
                assert_eq!(
                    std::fs::read(exported_user.join("SKILL.md")).unwrap(),
                    std::fs::read(user_path.join("SKILL.md")).unwrap()
                );
                assert_eq!(
                    std::fs::read(exported_user.join("references/sentinel")).unwrap(),
                    b"user reference"
                );
                let starts = harness.cli_session.turn_starts.lock().await;
                let start = &starts[0];
                let instructions = start.instructions.elevated().unwrap().text();
                if kind == CLIAgentRuntimeKind::Claude {
                    assert!(instructions.contains(&format!("Selected skills:\n- {user_name}\n- pioneer-system-tasks\n- pioneer-system-subagents")),
                        "application order must retain the user's selected skill before automatic tasks/subagents");
                }
                for name in [
                    "pioneer-system-tasks",
                    "pioneer-system-subagents",
                    user_name,
                ] {
                    assert!(instructions.contains(name), "{kind:?}: {name}");
                    if kind != CLIAgentRuntimeKind::Claude {
                        assert!(start.input.as_array().unwrap().iter().any(|item| {
                            item["type"] == "skill"
                                && item["name"] == name
                                && item["path"]
                                    == harness
                                        .native_home
                                        .join("skills")
                                        .join(name)
                                        .join("SKILL.md")
                                        .display()
                                        .to_string()
                        }));
                    }
                }
                // Claude receives the same selected names/paths through the
                // existing managed plugin snapshot builder (covered in claude_session tests).
                drop(starts);
            }
        }
    });
}

#[test]
fn cli_attached_tasks_inherit_the_exact_selected_mcp_tool() {
    run_standard_stack_message_test("CLI task capability inheritance", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
            harness
                .cli_session
                .enable_projected_mcp_metadata(harness.crud_store.clone())
                .await;
            let processor = Arc::new(harness.processor.clone());
            processor.bind_agent_tool_bridges().await;
            seed_ready_fake_mcp_server(&processor, &harness.crud_store, &harness.workspace_id)
                .await;
            let thread_id = format!("cli_exact_capability_{kind:?}");
            let turn_id = format!("cli_exact_capability_turn_{kind:?}");
            seed_cli_runtime_skill_preflight_thread(&harness, &thread_id).await;
            let selected = pioneer_protocol::TurnCapability {
                id: "mcp-tool:workspace:resend:send".to_owned(),
                kind: pioneer_protocol::TurnCapabilityKind::McpTool {
                    server_name: "resend".to_owned(),
                    raw_tool_name: "send".to_owned(),
                    scope_kind: pioneer_protocol::McpScopeKind::Workspace,
                },
                label: Some("Selected tool".to_owned()),
            };
            let request = cli_runtime_turn_start_request_with_capabilities(
                &generate_test_request_id("exact-cap", &format!("{kind:?}")),
                &thread_id,
                &turn_id,
                &harness.runtime_id,
                kind,
                vec![serde_json::to_value(&selected).unwrap()],
            );
            processor
                .process_request_for_connection(harness.connection_id, &request)
                .await;
            wait_for_recorded_cli_runtime_turn_start(&harness.cli_session).await;
            wait_for_task_cli_activation(&processor, &turn_id).await;
            let snapshot = processor
                .crud_store
                .get_turn_runtime_snapshot(&turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                crate::turn_runtime_snapshot::restored_execution_capabilities(
                    &snapshot.capabilities_json
                )
                .unwrap(),
                vec![selected.clone()]
            );
            harness
                .cli_session
                .set_next_native_turn_id(format!("exact_child_{kind:?}"))
                .await;
            let child = invoke_task(&processor, &turn_id, "exact-child", "task_create", json!({
                "title":"Inspect with the selected tool", "goal":"Inspect the available context",
                "outputInstructions":"Return a concise report."
            })).await.unwrap();
            assert!(!child.is_error, "{kind:?}: {child:?}");
            let task_id = child.structured_content.as_ref().unwrap()["taskId"]
                .as_str()
                .unwrap();
            let actor = processor
                .crud_store
                .get_task_actor_contract(task_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                actor.launch.unwrap().execution.selected_capabilities,
                vec![selected],
                "inherit the selected tool, not the broader admitted server ceiling"
            );
            wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 2).await;
            let child_binding = wait_for_task_cli_child(
                &processor,
                child.structured_content.as_ref().unwrap()["runId"]
                    .as_str()
                    .unwrap(),
                None,
            )
            .await;
            let child_turn_id = child_binding.turn_id.as_str();
            let child_bindings = processor
                .crud_store
                .list_turn_mcp_bindings(child_turn_id)
                .await
                .unwrap();
            assert_eq!(
                child_bindings
                    .iter()
                    .filter(|binding| binding.server_name == "resend")
                    .map(|binding| binding.raw_tool_name.as_str())
                    .collect::<Vec<_>>(),
                vec!["send"]
            );
            let cancelled = invoke_task(
                &processor,
                &turn_id,
                "cancel-exact-child",
                "task_cancel",
                json!({"taskId":task_id}),
            )
            .await
            .unwrap();
            assert!(!cancelled.is_error, "{cancelled:?}");
        }
    });
}

#[test]
fn cli_tasks_share_authorization_schemas_skills_and_durable_replay_for_all_providers() {
    run_standard_stack_message_test("CLI task MCP contract", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            let bindings = processor
                .crud_store
                .list_turn_mcp_bindings(&turn_id)
                .await
                .unwrap();
            for definition in crate::turn_mcp::native_tools::NativeToolFamily::Tasks.definitions() {
                assert!(
                    bindings
                        .iter()
                        .any(|binding| binding.raw_tool_name == definition.spec.name)
                );
            }
            let skills = processor
                .crud_store
                .find_turn_skill_bindings(&turn_id)
                .await
                .unwrap();
            assert_eq!(
                skills.len(),
                2,
                "orchestration skills are automatic even without user attachments"
            );
            for slug in ["tasks", "subagents"] {
                assert!(
                    skills
                        .iter()
                        .any(|skill| skill.skill_owner.as_deref() == Some("pioneer")
                            && skill.skill_slug == slug)
                );
                let exported = harness
                    .native_home
                    .join("skills")
                    .join(format!("pioneer-system-{slug}"));
                let canonical = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../resources/skills/pioneer")
                    .join(slug);
                assert_eq!(
                    std::fs::read(exported.join("SKILL.md")).unwrap(),
                    std::fs::read_to_string(canonical.join("SKILL.md"))
                        .unwrap()
                        .replacen(
                            &format!("---\nname: {slug}\n"),
                            &format!("---\nname: pioneer-system-{slug}\n"),
                            1
                        )
                        .into_bytes()
                );
                for reference in std::fs::read_dir(canonical.join("references")).unwrap() {
                    let reference = reference.unwrap();
                    assert_eq!(
                        std::fs::read(exported.join("references").join(reference.file_name()))
                            .unwrap(),
                        std::fs::read(reference.path()).unwrap()
                    );
                }
            }
            let instructions = harness.cli_session.turn_starts.lock().await[0]
                .instructions
                .elevated()
                .unwrap()
                .text()
                .to_owned();
            assert!(instructions.contains("Do not finish the parent turn while attached"));
            assert!(instructions.contains("task_wait"));
            assert!(!instructions.contains("call `read_skill`"));

            // Inheritance works without a preliminary options call. Discard the
            // first response, recreate the invoker/handlers, and recover by receipt.
            let args = json!({"title":"Future CLI work", "goal":"Inspect current project status",
                "instructions":["Inspect the current workspace using available capabilities and report failures clearly."],
                "outputInstructions":"Return a concise status summary.",
                "trigger":{"kind":"cron", "cronExpr":"0 7 * * *", "timezone":"Europe/Moscow"},
                "idempotencyKey":" scheduled-from-cli "});
            let first = invoke_task(
                &processor,
                &turn_id,
                "provider-create-a",
                "task_create",
                args.clone(),
            )
            .await
            .unwrap();
            assert!(!first.is_error, "{first:?}");
            let task_id = first.structured_content.as_ref().unwrap()["taskId"]
                .as_str()
                .unwrap()
                .to_owned();
            drop(first);
            let replay = invoke_task(
                &processor,
                &turn_id,
                "provider-create-b",
                "task_create",
                args.clone(),
            )
            .await
            .unwrap();
            assert!(!replay.is_error, "{replay:?}");
            assert_eq!(
                replay.structured_content.as_ref().unwrap()["taskId"],
                task_id
            );
            let replay_payload = replay.structured_content.as_ref().unwrap();
            assert_eq!(replay_payload["alreadyCommitted"], true);
            assert!(replay_payload.get("status").is_none());
            assert!(replay_payload.get("title").is_none());
            assert!(replay_payload.get("waitable").is_none());
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                processor
                    .crud_store
                    .list_tasks_created_by_turn(&binding.workspace_id, &binding.thread_id, &turn_id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            let options = invoke_task(
                &processor,
                &turn_id,
                "options",
                "threads_start_options",
                json!({}),
            )
            .await
            .unwrap();
            assert!(!options.is_error);
            let mut changed = args;
            changed["goal"] = json!("different mutation");
            assert!(
                invoke_task(
                    &processor,
                    &turn_id,
                    "provider-create-c",
                    "task_create",
                    changed
                )
                .await
                .map(|result| result.is_error)
                .unwrap_or(true)
            );
            assert!(
                invoke_task(
                    &processor,
                    &turn_id,
                    "foreign-capsule",
                    "task_get",
                    json!({"taskId":"123456789012345678901"})
                )
                .await
                .map(|result| result.is_error)
                .unwrap_or(true)
            );
            for (name, arguments) in [
                ("task_list", json!({})),
                (
                    "task_update",
                    json!({"taskId":task_id, "title":"Updated future CLI work"}),
                ),
                ("task_pause", json!({"taskId":task_id})),
                (
                    "task_reschedule",
                    json!({"taskId":task_id, "trigger":{"kind":"cron", "cronExpr":"0 8 * * *", "timezone":"Europe/Moscow"}}),
                ),
                ("task_resume", json!({"taskId":task_id})),
                ("task_wait", json!({"taskIds":[task_id], "timeoutMs":1})),
                ("task_get", json!({"taskId":task_id})),
                ("task_cancel", json!({"taskId":task_id})),
            ] {
                let result = invoke_task(&processor, &turn_id, name, name, arguments)
                    .await
                    .unwrap();
                assert!(!result.is_error, "{name}: {result:?}");
            }
            assert_eq!(
                processor
                    .crud_store
                    .get_task(&task_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .status,
                pioneer_protocol::TaskStatus::Cancelled
            );
            let conflict = invoke_task(&processor, &turn_id, "reschedule-cancelled", "task_reschedule", json!({
                "taskId":task_id, "trigger":{"kind":"cron", "cronExpr":"0 9 * * *", "timezone":"Europe/Moscow"}
            })).await.unwrap();
            assert!(conflict.is_error);
            assert!(
                conflict.structured_content.as_ref().unwrap()["error"]
                    .as_str()
                    .unwrap()
                    .contains("task_state_conflict")
            );
        }
    });
}

#[test]
fn cli_parent_native_completion_continues_with_existing_attached_work() {
    run_standard_stack_message_test("CLI attached parent gate", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            *harness.cli_session.next_native_turn_id.lock().await = Some(format!("child_{kind:?}"));
            let child = invoke_task(&processor, &turn_id, "attached-create", "task_create", json!({
                "title":"Attached inspection", "goal":"Inspect the project and return evidence",
                "instructions":["Read the project and return findings; do not change files."], "outputInstructions":"Return findings with file references."
            })).await.unwrap();
            assert!(!child.is_error, "{child:?}");
            let child_id = child.structured_content.as_ref().unwrap()["taskId"]
                .as_str()
                .unwrap()
                .to_owned();
            wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 2).await;
            let child_binding = wait_for_task_cli_child(
                &processor,
                child.structured_content.as_ref().unwrap()["runId"]
                    .as_str()
                    .unwrap(),
                None,
            )
            .await;
            let child_turn_id = child_binding.turn_id.as_str();
            let parent_snapshot = processor
                .crud_store
                .get_turn_runtime_snapshot(&turn_id)
                .await
                .unwrap()
                .expect("admitted CLI parent runtime snapshot");
            assert!(
                crate::turn_runtime_snapshot::restored_execution_capabilities(
                    &parent_snapshot.capabilities_json,
                )
                .unwrap()
                .is_empty(),
                "implicit system skills must not widen inherited capabilities"
            );
            let child_adapter = processor
                .agent_action_binding(child_turn_id)
                .await
                .expect("CLI child adapter must be bound before provider activation");
            let child_response = pioneer_crud::load_agent_turn_response(
                &processor.crud_store.database_connection(),
                child_turn_id,
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                child_adapter.adapter.lock().await.execution_id().as_str(),
                child_response.execution_id
            );
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            let key = CLIAgentRuntimeSessionKey::new(
                &binding.workspace_id,
                &binding.runtime_id,
                &binding.continuation_thread_id,
            )
            .unwrap();
            let handle = harness.cli_manager.existing_session(&key).await.unwrap();
            let factory_starts_before = harness.cli_session.factory_starts.load(Ordering::SeqCst);

            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("continued_parent_{kind:?}"));
            let terminal = RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                native_thread_id: Some(binding.native_thread_id.clone()),
                native_turn_id: binding.native_turn_id.clone().unwrap(),
                status: "completed".to_owned(),
                native: None,
            });
            processor
                .handle_cli_runtime_timeline_event(handle.instance(), terminal.clone())
                .await;
            wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 3).await;
            wait_for_task_cli_activation(&processor, &turn_id).await;
            if kind == CLIAgentRuntimeKind::Claude {
                let continued = harness.cli_manager.existing_session(&key).await.unwrap();
                assert_eq!(
                    continued.instance(),
                    handle.instance(),
                    "skill restoration must preserve the parent process generation"
                );
                assert_eq!(
                    harness.cli_session.factory_starts.load(Ordering::SeqCst),
                    factory_starts_before,
                    "parent continuation must not invoke the factory again for identical skills"
                );
            }

            assert_eq!(
                processor
                    .crud_store
                    .get_turn(&binding.thread_id, &turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            let next = processor
                .crud_store
                .latest_cli_runtime_turn_attempt(&turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(next.attempt_index, 2);
            let continuation = harness
                .cli_session
                .turn_starts
                .lock()
                .await
                .last()
                .unwrap()
                .clone();
            assert!(continuation.input.to_string().contains(&child_id));
            assert!(
                continuation
                    .input
                    .to_string()
                    .contains("Do not repeat the original user request")
            );
            assert_eq!(
                processor
                    .crud_store
                    .list_tasks_created_by_turn(&binding.workspace_id, &binding.thread_id, &turn_id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
            processor
                .handle_cli_runtime_timeline_event(handle.instance(), terminal)
                .await;
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                3,
                "duplicate terminal events do not create another continuation"
            );
            let cancelled = invoke_task(
                &processor,
                &turn_id,
                "cancel-child",
                "task_cancel",
                json!({"taskId":child_id}),
            )
            .await
            .unwrap();
            assert!(!cancelled.is_error);
        }
    });
}

#[test]
fn completed_native_attempt_recovery_continues_unfinished_attached_tasks() {
    run_standard_stack_message_test("CLI completed-attempt task recovery", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("recovery_child_{kind:?}"));
            let created = invoke_task(&processor, &turn_id, "recovery-create", "task_create", json!({
                "title":"Recoverable attached work", "goal":"Inspect the project", "outputInstructions":"Return findings."
            })).await.unwrap();
            assert!(!created.is_error, "{created:?}");
            wait_for_task_cli_child(
                &processor,
                created.structured_content.as_ref().unwrap()["runId"]
                    .as_str()
                    .unwrap(),
                None,
            )
            .await;
            let task_id = created.structured_content.unwrap()["taskId"]
                .as_str()
                .unwrap()
                .to_owned();
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            let attempt = processor
                .crud_store
                .latest_cli_runtime_turn_attempt(&turn_id)
                .await
                .unwrap()
                .unwrap();
            processor
                .crud_store
                .mark_cli_runtime_turn_attempt_terminal(
                    &attempt.id,
                    pioneer_crud::CliRuntimeTurnAttemptStatus::Completed,
                    None,
                    chrono::Utc::now().fixed_offset(),
                )
                .await
                .unwrap();
            let (_, turn) = processor
                .crud_store
                .get_turn(&binding.thread_id, &turn_id)
                .await
                .unwrap()
                .unwrap();
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("recovered_parent_{kind:?}"));
            let state = processor
                .reconcile_cli_runtime_turn_from_runtime(&binding, &binding.workspace_id, &turn)
                .await
                .unwrap();
            assert!(matches!(
                state,
                crate::message::cli_runtime::CLIRuntimeAuthoritativeTurnState::Deferred
            ));
            wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 3).await;
            wait_for_task_cli_activation(&processor, &turn_id).await;
            assert_eq!(
                processor
                    .crud_store
                    .get_turn(&binding.thread_id, &turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            let next = processor
                .crud_store
                .latest_cli_runtime_turn_attempt(&turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(next.attempt_index, 2);
            assert_eq!(next.execution_window_index, attempt.execution_window_index);
            assert!(
                harness
                    .cli_session
                    .turn_starts
                    .lock()
                    .await
                    .last()
                    .unwrap()
                    .input
                    .to_string()
                    .contains(&task_id)
            );
            assert_eq!(
                processor
                    .crud_store
                    .list_tasks_created_by_turn(&binding.workspace_id, &binding.thread_id, &turn_id)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    });
}

#[test]
fn cli_final_candidate_survives_concurrent_detach() {
    run_standard_stack_message_test("CLI detached final candidate race", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("race_child_{kind:?}"));
            let created = invoke_task(&processor, &turn_id, "race-create", "task_create", json!({
                "title":"Concurrent work", "goal":"Inspect the project", "outputInstructions":"Return findings."
            })).await.unwrap();
            assert!(!created.is_error);
            let task_id = created.structured_content.unwrap()["taskId"]
                .as_str()
                .unwrap()
                .to_owned();
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            let key = CLIAgentRuntimeSessionKey::new(
                &binding.workspace_id,
                &binding.runtime_id,
                &binding.continuation_thread_id,
            )
            .unwrap();
            let handle = harness.cli_manager.existing_session(&key).await.unwrap();
            let native_turn_id = binding.native_turn_id.as_deref().unwrap();
            let answer_id = format!("race_answer_{kind:?}");
            processor
                .handle_cli_runtime_timeline_event(
                    handle.instance(),
                    RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        item: None,
                        native_thread_id: Some(binding.native_thread_id.clone()),
                        native_turn_id: native_turn_id.to_owned(),
                        native_item_id: answer_id.clone(),
                        item_kind: "agentMessage".to_owned(),
                        text: Some("Parent outcome preserved".to_owned()),
                        summary: Vec::new(),
                        content: Vec::new(),
                        phase: RuntimeAgentMessagePhase::FinalAnswer,
                        metadata: None,
                        native_item_redacted: None,
                        native: None,
                    }),
                )
                .await;
            let messages = processor
                .crud_store
                .list_completed_agent_messages(&turn_id)
                .await
                .unwrap();
            assert!(messages.iter().any(|item| matches!(item, TurnItem::AgentMessage { id, phase: pioneer_protocol::AgentMessagePhase::Commentary, .. } if id == &answer_id)));
            let detached = invoke_task(
                &processor,
                &turn_id,
                "race-detach",
                "task_detach",
                json!({"taskId":task_id}),
            )
            .await
            .unwrap();
            assert!(!detached.is_error, "{detached:?}");
            harness
                .cli_session
                .record_provider_boundary(&binding.native_thread_id, native_turn_id)
                .await;
            let terminal = if kind == CLIAgentRuntimeKind::Claude {
                crate::cli_runtime::claude_session::tests::mapped_assistant_completion_for_gateway_test(
                    uuid::Uuid::parse_str(&binding.native_thread_id).unwrap(), native_turn_id, uuid::Uuid::new_v4(),
                ).await
            } else {
                RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                    native_thread_id: Some(binding.native_thread_id.clone()),
                    native_turn_id: native_turn_id.to_owned(),
                    status: "completed".to_owned(),
                    native: None,
                })
            };
            processor
                .handle_cli_runtime_timeline_event(handle.instance(), terminal)
                .await;
            assert_eq!(
                processor
                    .crud_store
                    .get_turn(&binding.thread_id, &turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::Completed
            );
            let messages = processor
                .crud_store
                .list_completed_agent_messages(&turn_id)
                .await
                .unwrap();
            assert!(messages.iter().any(|item| matches!(item, TurnItem::AgentMessage { id, text, phase: pioneer_protocol::AgentMessagePhase::FinalAnswer, .. }
                if id == &answer_id && text == "Parent outcome preserved")));
        }
    });
}

#[test]
fn cli_task_tools_cannot_observe_or_control_an_existing_foreign_capsule() {
    run_standard_stack_message_test("CLI Task capsule authorization", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            let foreign_thread = format!("foreign_cli_{kind:?}");
            let foreign_turn = format!("foreign_cli_turn_{kind:?}");
            seed_cli_runtime_skill_preflight_thread(&harness, &foreign_thread).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("foreign_native_{kind:?}"));
            let request = cli_runtime_turn_start_request_with_capabilities(
                &generate_test_request_id("foreign-cli", &format!("{kind:?}")),
                &foreign_thread,
                &foreign_turn,
                &harness.runtime_id,
                kind,
                Vec::new(),
            );
            processor
                .process_request_for_connection(harness.connection_id, &request)
                .await;
            wait_for_task_cli_activation(&processor, &foreign_turn).await;
            let created = invoke_task(&processor, &foreign_turn, "foreign-create", "task_create", json!({
                "title":"Foreign scheduled work", "goal":"Inspect the project later", "instructions":["Inspect and report the result."],
                "outputInstructions":"Return findings.", "trigger":{"kind":"cron", "cronExpr":"0 7 * * *", "timezone":"UTC"}
            })).await.unwrap();
            assert!(!created.is_error, "{created:?}");
            let task_id = created.structured_content.unwrap()["taskId"]
                .as_str()
                .unwrap()
                .to_owned();
            for name in ["task_get", "task_cancel", "task_detach"] {
                let denied =
                    invoke_task(&processor, &turn_id, name, name, json!({"taskId": task_id})).await;
                assert!(
                    denied.map(|result| result.is_error).unwrap_or(true),
                    "{name} must remain capsule-scoped"
                );
            }
            assert_eq!(
                processor
                    .crud_store
                    .get_task(&task_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .status,
                TaskStatus::Scheduled
            );
            let listed = invoke_task(&processor, &turn_id, "capsule-list", "task_list", json!({}))
                .await
                .unwrap();
            assert!(!listed.is_error);
            assert!(
                !listed
                    .structured_content
                    .unwrap()
                    .to_string()
                    .contains(&task_id)
            );
        }
    });
}

#[test]
fn repeated_cli_completion_without_task_actions_is_bounded() {
    run_standard_stack_message_test("CLI stalled orchestration", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("stalled_child_{kind:?}"));
            let created = invoke_task(&processor, &turn_id, "stalled-create", "task_create", json!({
                "title":"Attached work", "goal":"Inspect the project", "outputInstructions":"Return findings."
            })).await.unwrap();
            assert!(!created.is_error, "{created:?}");
            wait_for_task_cli_child(
                &processor,
                created.structured_content.as_ref().unwrap()["runId"]
                    .as_str()
                    .unwrap(),
                None,
            )
            .await;
            for index in 0..4 {
                wait_for_task_cli_activation(&processor, &turn_id).await;
                let binding = processor
                    .crud_store
                    .get_cli_runtime_turn_binding(&turn_id)
                    .await
                    .unwrap()
                    .unwrap();
                let key = CLIAgentRuntimeSessionKey::new(
                    &binding.workspace_id,
                    &binding.runtime_id,
                    &binding.continuation_thread_id,
                )
                .unwrap();
                let handle = harness.cli_manager.existing_session(&key).await.unwrap();
                *harness.cli_session.next_native_turn_id.lock().await =
                    Some(format!("stalled_parent_{kind:?}_{index}"));
                processor
                    .handle_cli_runtime_timeline_event(
                        handle.instance(),
                        RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                            native_thread_id: Some(binding.native_thread_id.clone()),
                            native_turn_id: binding.native_turn_id.unwrap(),
                            status: "completed".to_owned(),
                            native: None,
                        }),
                    )
                    .await;
                if index < 3 {
                    wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, index + 3).await;
                } else {
                    let turn = processor
                        .crud_store
                        .get_turn(&binding.thread_id, &turn_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .1;
                    assert_eq!(turn.status, TurnStatus::Blocked);
                    assert!(turn.error.as_deref().unwrap().contains("attached tasks"));
                    assert_eq!(harness.cli_session.turn_starts.lock().await.len(), 5);
                }
            }
        }
    });
}

#[test]
fn a_lost_wait_response_does_not_count_as_empty_orchestration_or_cancel_child_work() {
    run_standard_stack_message_test("CLI wait transport failure", async {
        let (harness, processor, turn_id) =
            start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        *harness.cli_session.next_native_turn_id.lock().await =
            Some("observation_child".to_owned());
        let created = invoke_task(&processor, &turn_id, "observation-create", "task_create", json!({
            "title":"Long attached work", "goal":"Inspect the project", "outputInstructions":"Return findings."
        })).await.unwrap();
        assert!(!created.is_error);
        wait_for_task_cli_child(
            &processor,
            created.structured_content.as_ref().unwrap()["runId"]
                .as_str()
                .unwrap(),
            None,
        )
        .await;
        let task_id = created.structured_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        for index in 0..4 {
            wait_for_task_cli_activation(&processor, &turn_id).await;
            let binding = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap();
            let key = CLIAgentRuntimeSessionKey::new(
                &binding.workspace_id,
                &binding.runtime_id,
                &binding.continuation_thread_id,
            )
            .unwrap();
            let handle = harness.cli_manager.existing_session(&key).await.unwrap();
            if index == 3 {
                // Gateway finishes this observation; the provider loses its
                // response and records a failed MCP item. Child work continues.
                let observed = invoke_task(
                    &processor,
                    &turn_id,
                    "lost-wait",
                    "task_wait",
                    json!({"taskIds":[task_id], "timeoutMs":1}),
                )
                .await
                .unwrap();
                assert!(!observed.is_error);
                drop(observed);
                processor.handle_cli_runtime_timeline_event(handle.instance(), RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                        item: None,
                    native_thread_id: Some(binding.native_thread_id.clone()), native_turn_id: binding.native_turn_id.clone().unwrap(),
                    native_item_id: "lost-wait".to_owned(), item_kind: "mcpToolCall".to_owned(), text: None,
                    summary: Vec::new(), content: Vec::new(), phase: RuntimeAgentMessagePhase::FinalAnswer,
                    metadata: Some(json!({"canonicalCallableName":"task_wait", "rawToolName":"task_wait", "serverName":"pioneer_tasks",
                        "serverInstallationId":"pioneer-task-tools-v1", "arguments":{"taskIds":[task_id],"timeoutMs":1}, "success":false})),
                    native_item_redacted: None, native: None,
                })).await;
                let items = processor
                    .crud_store
                    .list_turn_items_by_type(&turn_id, "dynamic_tool_call")
                    .await
                    .unwrap();
                assert!(items.iter().any(|item| matches!(item, TurnItem::DynamicToolCall { id, execution_class: pioneer_protocol::TurnItemExecutionClass::DurableWait, status: ToolCallStatus::Failed, .. } if id == "lost-wait")));
            }
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("observing_parent_{index}"));
            processor
                .handle_cli_runtime_timeline_event(
                    handle.instance(),
                    RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                        native_thread_id: Some(binding.native_thread_id.clone()),
                        native_turn_id: binding.native_turn_id.unwrap(),
                        status: "completed".to_owned(),
                        native: None,
                    }),
                )
                .await;
            wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, index + 3).await;
        }
        wait_for_task_cli_activation(&processor, &turn_id).await;
        let binding = processor
            .crud_store
            .get_cli_runtime_turn_binding(&turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            processor
                .crud_store
                .get_turn(&binding.thread_id, &turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        assert_ne!(
            processor
                .crud_store
                .get_task(&task_id)
                .await
                .unwrap()
                .unwrap()
                .task
                .status,
            TaskStatus::Cancelled
        );
    });
}

async fn finish_task_cli_child(
    harness: &CliRuntimeSkillPreflightHarness,
    processor: &Arc<MessageProcessor>,
    child_turn_id: &str,
    text: &str,
) {
    wait_for_task_cli_activation(processor, child_turn_id).await;
    let binding = processor
        .crud_store
        .get_cli_runtime_turn_binding(child_turn_id)
        .await
        .unwrap()
        .unwrap();
    complete_recorded_cli_task_turn(
        processor,
        &harness.cli_session,
        &harness.cli_manager,
        &harness.workspace_id,
        &harness.runtime_id,
        &binding.thread_id,
        &binding.native_thread_id,
        binding.native_turn_id.as_deref().unwrap(),
        text,
    )
    .await;
}

#[test]
fn cli_task_wait_review_revise_accept_and_detach_use_the_existing_task_contract() {
    run_standard_stack_message_test("CLI task review contract", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let mut task_config = review_enabled_task_runtime_config();
            task_config
                .review
                .default_parent_review_for_immediate_attached_agent_tasks = true;
            let (harness, processor, turn_id) =
                start_task_cli_fixture_with_task_config(kind, task_config).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("review_child_{kind:?}"));
            let created = invoke_task(&processor, &turn_id, "review-create", "task_create", json!({
                "title":"Reviewable work", "goal":"Inspect and return evidence", "outputInstructions":"Return a concise report."
            })).await.unwrap();
            assert!(!created.is_error, "{created:?}");
            let created = created.structured_content.unwrap();
            let task_id = created["taskId"].as_str().unwrap().to_owned();
            let run_id = created["runId"].as_str().unwrap().to_owned();
            let persisted = processor
                .crud_store
                .get_task(&task_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                persisted.agent_specs.iter().any(|spec| spec
                    .review_policy
                    .as_ref()
                    .is_some_and(|policy| policy.require_explicit_acceptance)),
                "fixture must admit a task that actually requires review"
            );
            let child_binding = wait_for_task_cli_child(&processor, &run_id, None).await;
            let first_wait = invoke_task(
                &processor,
                &turn_id,
                "wait-window",
                "task_wait",
                json!({"runIds":[run_id], "timeoutMs":1}),
            )
            .await
            .unwrap();
            assert!(!first_wait.is_error);
            assert_eq!(
                first_wait.structured_content.as_ref().unwrap()["timedOut"],
                true,
                "a valid wait window ends while the explicitly unfinished child keeps running"
            );
            let current = processor
                .crud_store
                .get_task(&task_id)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(
                current.task.status,
                pioneer_protocol::TaskStatus::Cancelled,
                "ending a wait window must not cancel the child"
            );
            finish_task_cli_child(
                &harness,
                &processor,
                &child_binding.turn_id,
                "<task_result>{\"summary\":\"First report needs a detail.\"}</task_result>",
            )
            .await;
            let review = invoke_task(
                &processor,
                &turn_id,
                "wait-review",
                "task_wait",
                json!({"runIds":[run_id], "timeoutMs":10000}),
            )
            .await
            .unwrap();
            assert!(!review.is_error, "{review:?}");
            let early_detach = invoke_task(
                &processor,
                &turn_id,
                "detach-during-review",
                "task_detach",
                json!({"taskId":task_id}),
            )
            .await
            .unwrap();
            assert!(early_detach.is_error);
            assert!(
                early_detach.structured_content.as_ref().unwrap()["error"]
                    .as_str()
                    .unwrap()
                    .contains("task_operation_failed")
            );
            let candidate =
                review.structured_content.as_ref().unwrap()["reviewRequired"][0]["candidate"]["id"]
                    .as_str()
                    .unwrap()
                    .to_owned();
            let read = invoke_task(
                &processor,
                &turn_id,
                "read-candidate",
                "task_result",
                json!({"candidateId":candidate}),
            )
            .await
            .unwrap();
            assert!(!read.is_error);
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("revision_child_{kind:?}"));
            let revised = invoke_task(&processor, &turn_id, "revise-candidate", "task_revise", json!({
                "taskId":task_id, "runId":run_id, "candidateId":candidate, "feedback":"Include the missing concrete detail."
            })).await.unwrap();
            assert!(!revised.is_error, "{revised:?}");
            let revised_binding =
                wait_for_task_cli_child(&processor, &run_id, Some(&child_binding.turn_id)).await;
            let revised_turn = revised_binding.turn_id.as_str();
            finish_task_cli_child(&harness, &processor, revised_turn,
                "<task_result>{\"summary\":\"Revised report includes the concrete detail.\"}</task_result>").await;
            let review = invoke_task(
                &processor,
                &turn_id,
                "wait-revision",
                "task_wait",
                json!({"runIds":[run_id], "timeoutMs":10000}),
            )
            .await
            .unwrap();
            assert!(!review.is_error, "{review:?}");
            let revised_candidate = review.structured_content.as_ref().unwrap()["reviewRequired"]
                [0]["candidate"]["id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_ne!(candidate, revised_candidate);
            let accepted = invoke_task(
                &processor,
                &turn_id,
                "accept-candidate",
                "task_accept",
                json!({"taskId":task_id,"runId":run_id,"candidateId":revised_candidate}),
            )
            .await
            .unwrap();
            assert!(!accepted.is_error, "{accepted:?}");
            assert_eq!(
                processor
                    .crud_store
                    .get_task(&task_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .status,
                pioneer_protocol::TaskStatus::Completed
            );
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("detach_child_{kind:?}"));
            let created = invoke_task(
                &processor,
                &turn_id,
                "detach-create",
                "task_create",
                json!({"title":"Continue separately","goal":"Inspect another part of the project"}),
            )
            .await
            .unwrap();
            assert!(!created.is_error, "{created:?}");
            let detached_id = created.structured_content.unwrap()["taskId"]
                .as_str()
                .unwrap()
                .to_owned();
            let detached = invoke_task(
                &processor,
                &turn_id,
                "detach-child",
                "task_detach",
                json!({"taskId":detached_id}),
            )
            .await
            .unwrap();
            assert!(!detached.is_error, "{detached:?}");
            let current = processor
                .crud_store
                .get_task(&detached_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                current.task.lifecycle_policy.unwrap().attachment,
                pioneer_protocol::TaskAttachmentMode::Detached
            );
            let cancelled = invoke_task(
                &processor,
                &turn_id,
                "cancel-detached",
                "task_cancel",
                json!({"taskId":detached_id}),
            )
            .await
            .unwrap();
            assert!(!cancelled.is_error);
        }
    });
}
