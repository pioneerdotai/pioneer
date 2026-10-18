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
    let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
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

async fn invoke_task(
    processor: &MessageProcessor,
    turn_id: &str,
    call_id: &str,
    name: &str,
    arguments: JsonValue,
) -> Result<CanonicalMcpToolResult, crate::turn_mcp::invoker::TurnMcpInvocationError> {
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
                let exported = harness.native_home.join("skills").join(slug);
                let canonical = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../resources/skills/pioneer")
                    .join(slug);
                assert_eq!(
                    std::fs::read(exported.join("SKILL.md")).unwrap(),
                    std::fs::read(canonical.join("SKILL.md")).unwrap()
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
                .elevated_instructions
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
                ("task_wait", json!({"taskIds":[task_id], "timeoutMs":0})),
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
                    json!({"taskIds":[task_id], "timeoutMs":0}),
                )
                .await
                .unwrap();
                assert!(!observed.is_error);
                drop(observed);
                processor.handle_cli_runtime_timeline_event(handle.instance(), RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                    native_thread_id: Some(binding.native_thread_id.clone()), native_turn_id: binding.native_turn_id.clone().unwrap(),
                    native_item_id: "lost-wait".to_owned(), item_kind: "mcpToolCall".to_owned(), text: None,
                    summary: Vec::new(), content: Vec::new(), phase: RuntimeAgentMessagePhase::FinalAnswer,
                    metadata: Some(json!({"canonicalCallableName":"task_wait", "rawToolName":"task_wait", "serverName":"pioneer_tasks",
                        "serverInstallationId":"pioneer-task-tools-v1", "arguments":{"taskIds":[task_id],"timeoutMs":0}, "success":false})),
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
            let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("review_child_{kind:?}"));
            let created = invoke_task(&processor, &turn_id, "review-create", "task_create", json!({
                "title":"Reviewable work", "goal":"Inspect and return evidence", "outputInstructions":"Return a concise report."
            })).await.unwrap();
            assert!(!created.is_error, "{created:?}");
            let created = created.structured_content.unwrap();
            let task_id = created["taskId"].as_str().unwrap().to_owned();
            let run_id = created["runId"].as_str().unwrap().to_owned();
            let first_wait = invoke_task(
                &processor,
                &turn_id,
                "wait-window",
                "task_wait",
                json!({"runIds":[run_id], "timeoutMs":0}),
            )
            .await
            .unwrap();
            assert!(!first_wait.is_error);
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
                created["childTurnId"].as_str().unwrap(),
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
            let revised_turn = revised.structured_content.as_ref().unwrap()["childTurnId"]
                .as_str()
                .unwrap();
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
