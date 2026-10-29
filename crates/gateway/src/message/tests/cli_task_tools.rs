use super::*;
use crate::message::agent_runtime::TurnFailureRecoveryKind;
use crate::message::turn_handlers::cli_runtime_provider_key;
use crate::turn_mcp::invoker::{TurnMcpInvocation, TurnMcpInvocationOrigin, TurnMcpInvoker};
use crate::turn_mcp::result::CanonicalMcpToolResult;
use pioneer_protocol::AgentMessagePhase;
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

async fn start_task_cli_fixture_with_task_config(
    kind: CLIAgentRuntimeKind,
    task_config: pioneer_tasks::TaskRuntimeConfig,
) -> (
    CliRuntimeSkillPreflightHarness,
    Arc<MessageProcessor>,
    String,
) {
    start_task_cli_fixture_with_models(kind, task_config, &[]).await
}

async fn start_task_cli_fixture_with_models(
    kind: CLIAgentRuntimeKind,
    task_config: pioneer_tasks::TaskRuntimeConfig,
    models: &[&str],
) -> (
    CliRuntimeSkillPreflightHarness,
    Arc<MessageProcessor>,
    String,
) {
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
    for model in models {
        processor
            .mark_cli_reasoning_model_ready_for_tests(
                &harness.workspace_id,
                &harness.runtime_id,
                model,
            )
            .await
            .unwrap();
    }
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

#[test]
fn cli_explicit_model_and_reasoning_reach_native_launch_and_survive_mcp_replay() {
    run_standard_stack_message_test("CLI explicit launch selection", async {
        let (harness, processor, parent) = start_task_cli_fixture_with_models(
            CLIAgentRuntimeKind::Codex,
            pioneer_tasks::TaskRuntimeConfig::default(),
            &[],
        )
        .await;
        // Readiness can expose another allowed model after parent admission.
        processor
            .mark_cli_reasoning_model_ready_for_tests(
                &harness.workspace_id,
                &harness.runtime_id,
                "catalog-cli-choice",
            )
            .await
            .unwrap();
        let options = invoke_task(
            &processor,
            &parent,
            "launch-options",
            "threads_start_options",
            json!({}),
        )
        .await
        .unwrap();
        assert!(!options.is_error, "{options:?}");
        // The MCP envelope adds transport metadata beside the domain fields.
        let identities: Vec<pioneer_protocol::AgentToolIdentityOption> =
            serde_json::from_value(options.structured_content.unwrap()["identities"].clone())
                .unwrap();
        let identity = identities
            .iter()
            .find(|identity| {
                identity.source_kind
                    == pioneer_protocol::AgentIdentitySourceKind::CliRuntimeInstance
            })
            .unwrap();
        let provider = format!("cli_runtime:{}", harness.runtime_id);
        let arguments = json!({
            "title":"Explicit CLI model", "goal":"Use the selected CLI model and effort",
            "launch":{"identity":{"kind":"exact","id":identity.id},
                "profile":{"kind":"model","providerId":provider,"modelId":"catalog-cli-choice"},
                "reasoning":{"effort":"high"}}
        });
        harness
            .cli_session
            .set_next_native_turn_id("explicit-cli-child")
            .await;
        let created = invoke_task(
            &processor,
            &parent,
            "explicit-cli",
            "task_create",
            arguments.clone(),
        )
        .await
        .unwrap();
        assert!(!created.is_error, "{created:?}");
        let created = created.structured_content.unwrap();
        let child =
            wait_for_task_cli_child(&processor, created["runId"].as_str().unwrap(), None).await;
        let starts = wait_for_cli_runtime_turn_starts(&harness.cli_session, 2).await;
        let child_authority = processor
            .load_turn_execution_authorization_context(&child.turn_id)
            .await
            .unwrap();
        assert!(
            child_authority
                .grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate)
        );
        let child_facade =
            ThreadsFacade::new(&processor, &child.turn_id, CLIAgentRuntimeKind::Codex).await;
        let options = child_facade
            .call(
                "task-child-options",
                "task-child-options",
                "threads_start_options",
                json!({}),
            )
            .await;
        let private_option = options["threadCreationOptions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|option| option["label"] == "Current collaboration")
            .unwrap()["id"]
            .clone();
        let created_thread = child_facade
            .call(
                "task-child-create",
                "task-child-create",
                "thread_create",
                json!({"optionId":private_option}),
            )
            .await;
        assert_eq!(created_thread["status"], "accepted");
        let start = &starts[1];
        assert_eq!(start.native_thread_id, child.native_thread_id);
        assert_eq!(start.model.as_deref(), Some("catalog-cli-choice"));
        assert_eq!(start.effort.as_deref(), Some("high"));
        assert_eq!(child.runtime_id, harness.runtime_id);
        let snapshot = processor
            .crud_store
            .get_turn_runtime_snapshot(&child.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.provider_name, provider);
        assert_eq!(snapshot.model, "catalog-cli-choice");
        assert_eq!(snapshot.reasoning_effort.as_deref(), Some("high"));
        let actor = processor
            .crud_store
            .get_task_actor_contract(created["taskId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        let saved: serde_json::Value =
            serde_json::from_str(actor.derived_child_launch_grant_json.as_deref().unwrap())
                .unwrap();
        assert!(saved["child_launch_grant"].get("profiles").is_none());
        assert_eq!(saved["profile"]["modelId"], "catalog-cli-choice");
        assert_eq!(
            actor.resolved_identity_id.as_deref(),
            Some(identity.id.as_str())
        );
        assert_eq!(
            actor
                .launch
                .as_ref()
                .unwrap()
                .execution
                .reasoning
                .as_ref()
                .unwrap()
                .effort,
            "high"
        );
        let replay = invoke_task(
            &processor,
            &parent,
            "explicit-cli",
            "task_create",
            arguments,
        )
        .await
        .unwrap();
        assert!(!replay.is_error, "{replay:?}");
        assert_eq!(
            replay.structured_content.unwrap()["taskId"],
            created["taskId"]
        );
        assert_eq!(
            harness.cli_session.turn_starts.lock().await.len(),
            2,
            "replay must not launch a second child with current defaults"
        );
        assert_eq!(
            processor
                .crud_store
                .get_task_actor_contract(created["taskId"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap(),
            actor
        );
    });
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

#[test]
fn threads_first_cli_outbox_dispatch_uses_committed_turn_once() {
    run_standard_stack_message_test("Threads committed CLI first dispatch", async {
        // The parent exercises ordinary fresh CLI admission. The child below
        // must go through the same dispatcher with its already-admitted Turn.
        let (harness, processor, parent) = start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let starts_before = harness.cli_session.turn_starts.lock().await.len();
        let options = invoke_task(
            &processor,
            &parent,
            "first-cli-options",
            "threads_start_options",
            json!({}),
        )
        .await
        .unwrap();
        assert!(!options.is_error, "{options:?}");
        let options = options.structured_content.unwrap();
        let creation = options["threadCreationOptions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|option| option["label"] == "New delegated root")
            .unwrap();
        let created = invoke_task(
            &processor,
            &parent,
            "first-cli-target",
            "thread_create",
            json!({"optionId": creation["id"]}),
        )
        .await
        .unwrap();
        assert!(!created.is_error, "{created:?}");
        let created = created.structured_content.unwrap();
        let target = created["targetOptions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|target| {
                !options["targetOptions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|old| old["id"] == target["id"])
            })
            .unwrap();
        let arguments = json!({
            "targetOptionId": target["id"],
            "input": [{"type":"text", "text":"First command in the delegated CLI root"}]
        });
        let accepted = invoke_task(
            &processor,
            &parent,
            "first-cli-start",
            "threads_turn_start",
            arguments.clone(),
        )
        .await
        .unwrap();
        assert!(!accepted.is_error, "{accepted:?}");
        let db = processor.crud_store.database_connection();
        let accepted = accepted.structured_content.unwrap();
        let outbox = pioneer_entity::agent_action_outbox::Entity::find_by_id(
            accepted["outboxId"].as_str().unwrap(),
        )
        .one(&db)
        .await
        .unwrap()
        .unwrap();
        let action_id = outbox.action_id.clone();
        let payload: JsonValue = serde_json::from_str(&outbox.payload_json).unwrap();
        let params: pioneer_protocol::TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let child = &params.turn_id;
        assert!(
            processor
                .crud_store
                .get_turn_runtime_snapshot(child)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            processor
                .crud_store
                .get_cli_runtime_turn_binding(child)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            processor
                .crud_store
                .latest_cli_runtime_turn_attempt(child)
                .await
                .unwrap()
                .is_none()
        );
        let before = processor
            .crud_store
            .get_turn(&params.thread_id, child)
            .await
            .unwrap()
            .unwrap()
            .1;
        assert!(matches!(&before.author.as_ref().unwrap().actor,
            pioneer_protocol::PersistedActorRef::AgentExecution(id) if id.as_str() == outbox.owner_execution_id));
        let admission = processor
            .crud_store
            .get_turn_admission(child)
            .await
            .unwrap()
            .unwrap();
        let grant = pioneer_crud::load_agent_execution_grant(
            &db,
            payload["spawned_execution_id"].as_str().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let authority = processor
            .load_turn_execution_authorization_context(child)
            .await
            .unwrap()
            .authorization_fingerprint()
            .unwrap();
        let security = processor
            .crud_store
            .get_turn_execution_security_snapshot(child)
            .await
            .unwrap()
            .unwrap();
        // Canonical admission must not be inserted again, even idempotently.
        db.execute_unprepared(&format!(
            "CREATE TRIGGER reject_repeat_thread_admission BEFORE INSERT ON turn_admission WHEN NEW.turn_id='{child}' BEGIN SELECT RAISE(ABORT,'repeated committed admission'); END"
        )).await.unwrap();
        // The recording session otherwise reuses the parent's native ID,
        // which the durable attempt ledger correctly rejects for a new Turn.
        harness
            .cli_session
            .set_next_native_turn_id("first-delegated-cli-child")
            .await;
        let batch =
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
        assert!(batch.errors.is_empty(), "{:#?}", batch.errors);
        wait_for_task_cli_activation(&processor, child).await;
        let starts = harness.cli_session.turn_starts.lock().await.clone();
        assert_eq!(starts.len(), starts_before + 1);
        assert!(
            starts
                .last()
                .unwrap()
                .input
                .as_array()
                .unwrap()
                .iter()
                .any(|item| { item["text"] == "First command in the delegated CLI root" })
        );
        assert_eq!(starts.last().unwrap().model, params.model);
        assert_eq!(
            starts.last().unwrap().effort.as_deref(),
            params.reasoning.as_ref().map(|r| r.effort.as_str())
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn_admission(child)
                .await
                .unwrap()
                .unwrap(),
            admission
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_grant(&db, &grant.execution_id)
                .await
                .unwrap()
                .unwrap(),
            grant
        );
        // Preparation binds MCP and Skill projections after admission. The
        // immutable authority ceiling and provenance must stay the same.
        assert_eq!(
            processor
                .load_turn_execution_authorization_context(child)
                .await
                .unwrap()
                .authorization_fingerprint()
                .unwrap(),
            authority
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn_execution_security_snapshot(child)
                .await
                .unwrap()
                .unwrap(),
            security
        );
        let after = processor
            .crud_store
            .get_turn(&params.thread_id, child)
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(after.author, before.author);
        assert_eq!(after.mode, before.mode);
        assert_eq!(after.origin, before.origin);
        assert_eq!(
            processor.crud_store.get_turn_inputs(child).await.unwrap(),
            params.input
        );
        assert_eq!(
            processor
                .thread_manager
                .thread_get(&params.thread_id)
                .await
                .unwrap()
                .turns
                .iter()
                .filter(|turn| &turn.id == child)
                .count(),
            1
        );
        assert!(
            processor
                .crud_store
                .latest_cli_runtime_turn_attempt(child)
                .await
                .unwrap()
                .is_some()
        );
        let delivered = pioneer_crud::load_agent_action_outbox(&db, action_id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.status, "delivered");
        let replay = invoke_task(
            &processor,
            &parent,
            "first-cli-start",
            "threads_turn_start",
            arguments,
        )
        .await
        .unwrap();
        assert!(!replay.is_error, "{replay:?}");
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(
            harness.cli_session.turn_starts.lock().await.len(),
            starts_before + 1
        );
    });
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
                && let Some(transition) = processor
                    .try_cli_runtime_turn_transition(&binding)
                    .await
                    .unwrap()
            {
                // Activation persists the binding before releasing its gate.
                // Return an active child only after that startup handoff.
                drop(transition);
                return binding;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Task run must activate its exact CLI child")
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
                native_call: None,
                admission_deadline: None,
                canonical_callable_name: name.to_owned(),
                arguments,
                origin: TurnMcpInvocationOrigin::CliFacade,
            },
            tokio_util::sync::CancellationToken::new(),
        )
        .await
}

// Replay provider-shaped native completions through the existing durable item
// lane. The fixture invokes real task handlers, while no provider process runs.
async fn deliver_task_cli_outcome(
    harness: &CliRuntimeSkillPreflightHarness,
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
    call_id: &str,
    name: &str,
    arguments: JsonValue,
    result: &CanonicalMcpToolResult,
    native_success: bool,
    identified: bool,
) {
    let binding = processor
        .crud_store
        .get_cli_runtime_turn_binding(turn_id)
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
    let returned = match binding.runtime_kind.as_str() {
        "opencode" => result.content.clone(),
        "claude" => json!({"content":result.content,"isError":result.is_error}),
        _ => {
            json!({"content":result.content,"structuredContent":result.structured_content,"isError":result.is_error})
        }
    };
    processor
        .handle_cli_runtime_timeline_event(
            handle.instance(),
            RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                native_thread_id: Some(binding.native_thread_id),
                native_turn_id: binding.native_turn_id.unwrap(),
                native_item_id: call_id.into(),
                item_kind: "mcpToolCall".into(),
                text: None,
                summary: vec![],
                content: vec![],
                phase: RuntimeAgentMessagePhase::FinalAnswer,
                metadata: Some(json!({"canonicalCallableName":name,"rawToolName":name,
            "serverName":if identified {"pioneer_tasks"} else {"foreign"},
            "serverInstallationId":if identified {"pioneer-task-tools-v1"} else {"foreign"},
            "providerCallId":call_id,"invocationCorrelationId":format!("fixture-{call_id}"),
            "sessionGeneration":binding.mcp.unwrap().session_generation,
            "arguments":arguments,"result":returned,"success":native_success,
            "status":if native_success {"completed"} else {"failed"},
            "message":"display preview deliberately contains no task facts"})),
                native_item_redacted: None,
                native: None,
            }),
        )
        .await;
}

async fn observed_cli_tasks(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
) -> std::collections::BTreeSet<String> {
    let binding = processor
        .crud_store
        .get_cli_runtime_turn_binding(turn_id)
        .await
        .unwrap()
        .unwrap();
    use pioneer_agent::TaskToolProvider;
    let context = pioneer_agent::TaskTurnContext {
        workspace_id: binding.workspace_id,
        thread_id: binding.thread_id,
        turn_id: turn_id.into(),
    };
    let observed = crate::task_tools::observed_terminal_task_outcomes(processor, &context)
        .await
        .unwrap();
    crate::task_tools::GatewayTaskToolProvider::new(Arc::downgrade(processor))
        .attached_task_finalization_snapshot(context)
        .await
        .unwrap()
        .terminal
        .into_iter()
        .filter(|item| item.was_observed(&observed))
        .map(|item| item.task_id)
        .collect()
}

#[test]
fn cli_accepted_nested_result_keeps_substantive_final_without_another_attempt() {
    run_standard_stack_message_test("CLI accepted final delivery", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let mut config = review_enabled_task_runtime_config();
            config
                .review
                .default_parent_review_for_immediate_attached_agent_tasks = true;
            let (harness, processor, root) =
                start_task_cli_fixture_with_task_config(kind, config).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("middle_{kind:?}"));
            let parent = invoke_task(&processor, &root, "parent-create", "task_create", json!({
                "title":"Integrate research", "goal":"Review research and provide the final explanation",
                "outputInstructions":"Return the complete explanation."
            })).await.unwrap().structured_content.unwrap();
            let middle =
                wait_for_task_cli_child(&processor, parent["runId"].as_str().unwrap(), None).await;
            *harness.cli_session.next_native_turn_id.lock().await =
                Some(format!("researcher_{kind:?}"));
            let child = invoke_task(&processor, &middle.turn_id, "child-create", "task_create", json!({
                "title":"Research", "goal":"Return specific evidence", "outputInstructions":"Return all evidence."
            })).await.unwrap().structured_content.unwrap();
            let researcher =
                wait_for_task_cli_child(&processor, child["runId"].as_str().unwrap(), None).await;
            let research_text = "The build fails because the generated source contract is stale. Regenerate it from the current inputs.";
            finish_task_cli_child(&harness, &processor, &researcher.turn_id, research_text).await;
            let args = json!({"runIds":[child["runId"]],"timeoutMs":10000});
            let review = invoke_task(
                &processor,
                &middle.turn_id,
                "child-wait",
                "task_wait",
                args.clone(),
            )
            .await
            .unwrap();
            let candidate = &review.structured_content.as_ref().unwrap()["reviewRequired"][0];
            assert_eq!(candidate["reviewContent"]["content"], research_text);
            assert_eq!(candidate["reviewContent"]["truncated"], false);
            deliver_task_cli_outcome(
                &harness,
                &processor,
                &middle.turn_id,
                "child-wait",
                "task_wait",
                args,
                &review,
                true,
                true,
            )
            .await;
            assert!(
                observed_cli_tasks(&processor, &middle.turn_id)
                    .await
                    .is_empty(),
                "reviewRequired is not terminal"
            );
            let args = json!({"taskId":child["taskId"],"runId":child["runId"],"candidateId":candidate["candidate"]["id"]});
            let accepted = invoke_task(
                &processor,
                &middle.turn_id,
                "child-accept",
                "task_accept",
                args.clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                accepted.structured_content.as_ref().unwrap()["taskTerminal"],
                true
            );
            assert!(
                observed_cli_tasks(&processor, &middle.turn_id)
                    .await
                    .is_empty(),
                "backend commit alone is not delivery"
            );
            deliver_task_cli_outcome(
                &harness,
                &processor,
                &middle.turn_id,
                "child-accept",
                "task_accept",
                args,
                &accepted,
                true,
                true,
            )
            .await;
            assert!(
                observed_cli_tasks(&processor, &middle.turn_id)
                    .await
                    .contains(child["taskId"].as_str().unwrap())
            );
            let starts = harness.cli_session.turn_starts.lock().await.len();
            let final_text = "Regenerate the source contract, commit the generated changes, and rebuild. The research was reviewed and the evidence supports this fix.";
            finish_task_cli_child(&harness, &processor, &middle.turn_id, final_text).await;
            assert_eq!(
                harness.cli_session.turn_starts.lock().await.len(),
                starts,
                "{kind:?}: accepted research must not require an ACK attempt"
            );
            assert_eq!(
                processor
                    .crud_store
                    .latest_cli_runtime_turn_attempt(&middle.turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .attempt_index,
                1
            );
            let final_item = processor
                .crud_store
                .completed_turn_final_answer(
                    &middle.workspace_id,
                    &middle.thread_id,
                    &middle.turn_id,
                )
                .await
                .unwrap()
                .unwrap();
            assert!(
                matches!(final_item, TurnItem::AgentMessage { phase: AgentMessagePhase::FinalAnswer, ref text, .. } if text == final_text)
            );
            let upper_review = invoke_task(
                &processor,
                &root,
                "parent-wait",
                "task_wait",
                json!({"runIds":[parent["runId"]],"timeoutMs":10000}),
            )
            .await
            .unwrap();
            let upper = &upper_review.structured_content.as_ref().unwrap()["reviewRequired"][0];
            assert_eq!(
                upper["reviewContent"]["content"], final_text,
                "upper parent receives exactly the accepted final"
            );
        }
    });
}

#[test]
fn cli_terminal_outcomes_require_successful_identified_durable_delivery() {
    run_standard_stack_message_test("CLI terminal outcome recovery", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            for tool in ["task_wait", "task_get", "task_cancel"] {
                let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
                *harness.cli_session.next_native_turn_id.lock().await =
                    Some(format!("terminal_child_{kind:?}"));
                let created = invoke_task(
                    &processor,
                    &turn_id,
                    "terminal-create",
                    "task_create",
                    json!({"title":"Terminal child","goal":"Inspect the project"}),
                )
                .await
                .unwrap()
                .structured_content
                .unwrap();
                wait_for_task_cli_child(&processor, created["runId"].as_str().unwrap(), None).await;
                let id = created["taskId"].as_str().unwrap();
                let cancel_args = json!({"taskId":id});
                let cancelled = invoke_task(
                    &processor,
                    &turn_id,
                    "cancel-commit",
                    "task_cancel",
                    cancel_args.clone(),
                )
                .await
                .unwrap();
                assert!(!cancelled.is_error);
                assert!(observed_cli_tasks(&processor, &turn_id).await.is_empty());
                let args = if tool == "task_wait" {
                    json!({"taskIds":[id],"timeoutMs":1})
                } else {
                    cancel_args
                };
                let result =
                    invoke_task(&processor, &turn_id, "terminal-outcome", tool, args.clone())
                        .await
                        .unwrap();
                assert!(!result.is_error);
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "terminal-outcome",
                    tool,
                    args.clone(),
                    &result,
                    false,
                    true,
                )
                .await;
                assert!(
                    observed_cli_tasks(&processor, &turn_id).await.is_empty(),
                    "failed native item cannot inherit committed mutation success"
                );
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "foreign-outcome",
                    tool,
                    args.clone(),
                    &result,
                    true,
                    false,
                )
                .await;
                assert!(
                    observed_cli_tasks(&processor, &turn_id).await.is_empty(),
                    "name alone is not identity"
                );
                // Retrying the same committed mutation yields only references,
                // which must not mask the failed native result.
                let replay =
                    invoke_task(&processor, &turn_id, "terminal-outcome", tool, args.clone())
                        .await
                        .unwrap();
                if tool == "task_cancel" {
                    assert_eq!(
                        replay.structured_content.as_ref().unwrap()["alreadyCommitted"],
                        true
                    );
                    deliver_task_cli_outcome(
                        &harness,
                        &processor,
                        &turn_id,
                        "commit-replay",
                        tool,
                        args.clone(),
                        &replay,
                        true,
                        true,
                    )
                    .await;
                    assert!(observed_cli_tasks(&processor, &turn_id).await.is_empty());
                }
                let revision = processor
                    .crud_store
                    .get_task(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .revision;
                let received =
                    invoke_task(&processor, &turn_id, "received-outcome", tool, args.clone())
                        .await
                        .unwrap();
                // A new received native item establishes knowledge; re-reading
                // the durable source after reconstruction needs no mutation.
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "received-outcome",
                    tool,
                    args,
                    &received,
                    true,
                    true,
                )
                .await;
                assert_eq!(
                    processor
                        .crud_store
                        .get_task(id)
                        .await
                        .unwrap()
                        .unwrap()
                        .task
                        .revision,
                    revision,
                    "retry must not repeat the terminal state mutation"
                );
                let mut reconstructed = (*processor).clone();
                reconstructed.crud_store =
                    Arc::new(CrudStore::new(processor.crud_store.database_connection()));
                let reconstructed = Arc::new(reconstructed);
                assert_eq!(
                    observed_cli_tasks(&reconstructed, &turn_id).await,
                    std::collections::BTreeSet::from([id.to_owned()])
                );
                assert_eq!(
                    processor
                        .crud_store
                        .get_task(id)
                        .await
                        .unwrap()
                        .unwrap()
                        .task
                        .status,
                    TaskStatus::Cancelled
                );
                *harness.cli_session.next_native_turn_id.lock().await =
                    Some(format!("other_child_{kind:?}"));
                let other = invoke_task(
                    &processor,
                    &turn_id,
                    "other-create",
                    "task_create",
                    json!({"title":"Other child","goal":"Inspect another part"}),
                )
                .await
                .unwrap()
                .structured_content
                .unwrap();
                wait_for_task_cli_child(&processor, other["runId"].as_str().unwrap(), None).await;
                assert!(
                    !observed_cli_tasks(&processor, &turn_id)
                        .await
                        .contains(other["taskId"].as_str().unwrap())
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
                processor
                    .handle_cli_runtime_timeline_event(
                        handle.instance(),
                        RuntimeEvent::ItemCompleted(RuntimeItemCompleted {
                            native_thread_id: Some(binding.native_thread_id.clone()),
                            native_turn_id: binding.native_turn_id.clone().unwrap(),
                            native_item_id: "premature-final".into(),
                            item_kind: "agentMessage".into(),
                            text: Some("Premature completion".into()),
                            summary: vec![],
                            content: vec![],
                            phase: RuntimeAgentMessagePhase::FinalAnswer,
                            metadata: None,
                            native_item_redacted: None,
                            native: None,
                        }),
                    )
                    .await;
                assert!(
                    matches!(
                        processor
                            .crud_store
                            .get_turn_item(&turn_id, "premature-final")
                            .await
                            .unwrap(),
                        Some(TurnItem::AgentMessage {
                            phase: AgentMessagePhase::Commentary,
                            ..
                        })
                    ),
                    "another child still blocks the final"
                );
                let args = json!({"taskId":other["taskId"]});
                let cancelled = invoke_task(
                    &processor,
                    &turn_id,
                    "other-cancel",
                    "task_cancel",
                    args.clone(),
                )
                .await
                .unwrap();
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "other-cancel",
                    "task_cancel",
                    args,
                    &cancelled,
                    true,
                    true,
                )
                .await;
                let starts = harness.cli_session.turn_starts.lock().await.len();
                finish_task_cli_child(
                    &harness,
                    &processor,
                    &turn_id,
                    "Child cancellation was observed; the remaining explanation is complete.",
                )
                .await;
                assert_eq!(
                    harness.cli_session.turn_starts.lock().await.len(),
                    starts,
                    "{kind:?}/{tool}: terminal delivery must not add an attempt"
                );
            }
        }
    });
}

#[test]
fn cli_observation_tracks_the_child_outcome_across_same_run_resume() {
    run_standard_stack_message_test("CLI same-run observation freshness", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            for tool in ["task_get", "task_wait"] {
                let (harness, processor, turn_id) = start_task_cli_fixture(kind).await;
                harness
                    .cli_session
                    .set_next_native_turn_id("resume_a")
                    .await;
                let a = invoke_task(
                    &processor,
                    &turn_id,
                    "create-a",
                    "task_create",
                    json!({"title":"Child A","goal":"Inspect A"}),
                )
                .await
                .unwrap()
                .structured_content
                .unwrap();
                let a_child =
                    wait_for_task_cli_child(&processor, a["runId"].as_str().unwrap(), None).await;
                harness
                    .cli_session
                    .set_next_native_turn_id("resume_b")
                    .await;
                let b = invoke_task(
                    &processor,
                    &turn_id,
                    "create-b",
                    "task_create",
                    json!({"title":"Child B","goal":"Inspect B"}),
                )
                .await
                .unwrap()
                .structured_content
                .unwrap();
                wait_for_task_cli_child(&processor, b["runId"].as_str().unwrap(), None).await;
                let a_id = a["taskId"].as_str().unwrap();
                let run_id = a["runId"].as_str().unwrap();
                assert!(
                    processor
                        .mark_turn_blocked(
                            a_child.thread_id.clone(),
                            a_child.turn_id.clone(),
                            "blocked child A".into()
                        )
                        .await
                );
                processor
                    .reconcile_terminal_task_child_turns(64)
                    .await
                    .unwrap();
                assert_eq!(
                    processor
                        .crud_store
                        .get_task_run_turn_by_turn(&a_child.thread_id, &a_child.turn_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    pioneer_protocol::TaskRunTurnStatus::Blocked
                );
                let first_error = processor
                    .crud_store
                    .get_task_run(run_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .error;
                let args = if tool == "task_wait" {
                    json!({"taskIds":[a_id],"timeoutMs":1})
                } else {
                    json!({"taskId":a_id})
                };
                let first = invoke_task(&processor, &turn_id, "first-outcome", tool, args.clone())
                    .await
                    .unwrap();
                assert!(!first.is_error);
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "first-outcome",
                    tool,
                    args.clone(),
                    &first,
                    true,
                    true,
                )
                .await;
                assert!(
                    observed_cli_tasks(&processor, &turn_id)
                        .await
                        .contains(a_id)
                );
                let binding = processor
                    .crud_store
                    .get_cli_runtime_turn_binding(&turn_id)
                    .await
                    .unwrap()
                    .unwrap();
                let state = processor
                    .cli_task_finalization_state(&binding)
                    .await
                    .unwrap();
                assert!(state.blocks_completion(), "B remains pending");
                let a_outcomes = state
                    .snapshot
                    .terminal
                    .iter()
                    .filter(|item| item.task_id == a_id)
                    .map(|item| item.outcome())
                    .collect::<std::collections::BTreeSet<_>>();
                // Both new semantic markers and legacy ID-only markers must not
                // turn into a bypass after the very same run is reopened.
                for details in [json!({"outcomes":a_outcomes}), json!({"taskIds":[a_id]})] {
                    processor
                        .crud_store
                        .materialize_item_completed(
                            ItemCompletedNotification {
                                workspace_id: binding.workspace_id.clone(),
                                thread_id: binding.thread_id.clone(),
                                turn_id: turn_id.clone(),
                                item: TurnItem::SystemEvent {
                                    id: pioneer_protocol::generate_id(21),
                                    level: pioneer_protocol::SystemEventLevel::Info,
                                    message: "Previously delivered A outcome".into(),
                                    code: Some("task.terminal.observed".into()),
                                    details: Some(details),
                                },
                            },
                            now_timestamp_secs(),
                        )
                        .await
                        .unwrap();
                }
                let updated = invoke_task(
                    &processor,
                    &turn_id,
                    "update-b",
                    "task_update",
                    json!({"taskId":b["taskId"],"title":"Child B still active"}),
                )
                .await
                .unwrap();
                assert!(!updated.is_error);
                let mut reconstructed = (*processor).clone();
                reconstructed.crud_store =
                    Arc::new(CrudStore::new(processor.crud_store.database_connection()));
                let processor = Arc::new(reconstructed);
                assert!(
                    observed_cli_tasks(&processor, &turn_id)
                        .await
                        .contains(a_id),
                    "B revision cannot invalidate unchanged A after restart"
                );
                let before = processor
                    .crud_store
                    .get_task(a_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .revision;
                let now = now_timestamp_secs();
                let job = processor
                    .crud_store
                    .enqueue_recovery_job(
                        a_child.turn_id.clone(),
                        "resume-a".into(),
                        pioneer_protocol::TurnItemType::Reasoning,
                        None,
                        pioneer_protocol::RecoveryTrigger::Timeout,
                        pioneer_protocol::RecoveryAction::BlockResumable,
                        Some("blocked A".into()),
                        None,
                        None,
                        None,
                        0,
                        0,
                        json!({}),
                        json!({"base_backoff_secs":0,"max_wall_clock_secs":60}),
                        now,
                    )
                    .await
                    .unwrap();
                processor
                    .crud_store
                    .mark_recovery_job_terminal(
                        &job.id,
                        RecoveryJobStatus::Blocked,
                        Some("blocked A".into()),
                        now,
                    )
                    .await
                    .unwrap();
                let stale_terminal = processor
                    .crud_store
                    .prepare_task_terminal_transition(
                        pioneer_protocol::TaskEventPayload::RunBlocked {
                            task_id: a_id.into(),
                            run_id: run_id.into(),
                            error: first_error.clone(),
                            blocked_at: now,
                        },
                    )
                    .await
                    .unwrap();
                let resumed = processor
                    .crud_store
                    .resume_task_owned_turn(
                        &a_child.thread_id,
                        &a_child.turn_id,
                        Some(&job.id),
                        now,
                        processor.turn_execution_owner_id.as_ref(),
                        now + 100,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let pioneer_crud::TaskOwnedTurnResumeOutcome::Resumed {
                    task,
                    run,
                    task_run_turn: mut resumed_turn,
                    ..
                } = resumed
                else {
                    panic!("same-run task-owned resume must succeed");
                };
                assert_eq!(run.id, run_id);
                assert!(task.revision > before);
                assert!(
                    processor
                        .crud_store
                        .commit_task_terminal_transition(stale_terminal)
                        .await
                        .is_err(),
                    "a terminal write prepared before resume must lose its occurrence fence"
                );
                assert_eq!(
                    processor
                        .crud_store
                        .get_task_run(run_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    pioneer_protocol::TaskRunStatus::Running,
                    "stale terminal rejection must roll back the entire transition"
                );
                assert!(
                    processor
                        .cli_task_finalization_state(&binding)
                        .await
                        .unwrap()
                        .blocks_completion()
                );
                // New terminal outcome deliberately has the same run, status and
                // error as the first. Only durable task revision differs.
                let resumed_execution = pioneer_tasks::TaskExecutionHandle::new(
                    processor.crud_store.clone(),
                    processor.task_runtime.event_bus(),
                    a_id.into(),
                    run_id.into(),
                );
                resumed_turn.status = pioneer_protocol::TaskRunTurnStatus::Blocked;
                resumed_turn.completed_at = Some(now + 1);
                resumed_execution
                    .record_task_run_turn_blocked(
                        resumed_turn.clone(),
                        first_error.clone(),
                        now + 1,
                    )
                    .await
                    .unwrap();
                resumed_execution
                    .block_run(first_error.clone(), now + 1)
                    .await
                    .unwrap();
                let reblocked = processor
                    .crud_store
                    .get_latest_task_run_turn(run_id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    reblocked.status,
                    pioneer_protocol::TaskRunTurnStatus::Blocked
                );
                assert_eq!(reblocked.completed_at, Some(now + 1));
                let revision = processor
                    .crud_store
                    .get_task(a_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .revision;
                resumed_execution
                    .record_task_run_turn_blocked(resumed_turn, first_error.clone(), now + 2)
                    .await
                    .unwrap();
                resumed_execution
                    .block_run(first_error, now + 2)
                    .await
                    .unwrap();
                assert_eq!(
                    processor
                        .crud_store
                        .get_task(a_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .task
                        .revision,
                    revision,
                    "replaying the new outcome must not project it twice"
                );
                assert!(
                    !observed_cli_tasks(&processor, &turn_id)
                        .await
                        .contains(a_id)
                );
                let b_args = json!({"taskId":b["taskId"]});
                let cancelled = invoke_task(
                    &processor,
                    &turn_id,
                    "cancel-b",
                    "task_cancel",
                    b_args.clone(),
                )
                .await
                .unwrap();
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "cancel-b",
                    "task_cancel",
                    b_args,
                    &cancelled,
                    true,
                    true,
                )
                .await;
                let state = processor
                    .cli_task_finalization_state(&binding)
                    .await
                    .unwrap();
                assert!(state.snapshot.pending.is_empty());
                assert!(
                    state.blocks_completion(),
                    "new A outcome alone still gates finalization"
                );
                let mut restarted = (*processor).clone();
                restarted.crud_store =
                    Arc::new(CrudStore::new(processor.crud_store.database_connection()));
                let processor = Arc::new(restarted);
                assert!(
                    !observed_cli_tasks(&processor, &turn_id)
                        .await
                        .contains(a_id),
                    "old items and both marker forms cannot cover reopened A"
                );
                let latest = invoke_task(&processor, &turn_id, "new-outcome", tool, args.clone())
                    .await
                    .unwrap();
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "new-outcome",
                    tool,
                    args.clone(),
                    &latest,
                    false,
                    true,
                )
                .await;
                assert!(
                    processor
                        .cli_task_finalization_state(&binding)
                        .await
                        .unwrap()
                        .blocks_completion(),
                    "failed delivery cannot observe new A"
                );
                deliver_task_cli_outcome(
                    &harness,
                    &processor,
                    &turn_id,
                    "new-outcome-success",
                    tool,
                    args,
                    &latest,
                    true,
                    true,
                )
                .await;
                assert!(
                    !processor
                        .cli_task_finalization_state(&binding)
                        .await
                        .unwrap()
                        .blocks_completion()
                );
                let starts = harness.cli_session.turn_starts.lock().await.len();
                let final_text = "A was resumed in its original run and blocked again; B was cancelled. Both current outcomes have been integrated.";
                finish_task_cli_child(&harness, &processor, &turn_id, final_text).await;
                assert_eq!(harness.cli_session.turn_starts.lock().await.len(), starts);
                assert!(
                    matches!(processor.crud_store.completed_turn_final_answer(&binding.workspace_id, &binding.thread_id, &turn_id).await.unwrap().unwrap(), TurnItem::AgentMessage { ref text, phase:AgentMessagePhase::FinalAnswer, .. } if text == final_text)
                );
            }
        }
    });
}

#[test]
fn cli_recovery_delivers_exact_task_facts_and_preserves_substantive_final() {
    run_standard_stack_message_test("CLI task payload recovery", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            // 0: fresh recovery, 1: committed receipt before marker, 2: marker before provider start.
            for window in 0..3 {
                let mut config = review_enabled_task_runtime_config();
                config
                    .review
                    .default_parent_review_for_immediate_attached_agent_tasks = true;
                let (harness, processor, root) =
                    start_task_cli_fixture_with_task_config(kind, config).await;
                harness
                    .cli_session
                    .set_next_native_turn_id("payload_middle")
                    .await;
                let parent = invoke_task(&processor, &root, "payload-parent", "task_create", json!({"title":"Integrate recovered work","goal":"Return the full explanation"})).await.unwrap().structured_content.unwrap();
                let middle =
                    wait_for_task_cli_child(&processor, parent["runId"].as_str().unwrap(), None)
                        .await;
                let turn_id = middle.turn_id.clone();
                harness
                    .cli_session
                    .set_next_native_turn_id("payload_child")
                    .await;
                let child = invoke_task(
                    &processor,
                    &turn_id,
                    "payload-create",
                    "task_create",
                    json!({"title":"Payload child","goal":"Inspect the project"}),
                )
                .await
                .unwrap()
                .structured_content
                .unwrap();
                wait_for_task_cli_child(&processor, child["runId"].as_str().unwrap(), None).await;
                let future = invoke_task(&processor, &turn_id, "payload-schedule", "task_create", json!({
                    "title":"Future project inspection", "goal":"Inspect the project on schedule",
                    "instructions":["Inspect current project state and report findings without changing files."],
                    "outputInstructions":"Return a concise status report.",
                    "trigger":{"kind":"cron","cronExpr":"0 7 * * *","timezone":"Europe/Moscow"}
                })).await.unwrap().structured_content.unwrap();
                assert_eq!(future["waitable"], false);
                let cancelled = invoke_task(
                    &processor,
                    &turn_id,
                    "payload-cancel",
                    "task_cancel",
                    json!({"taskId":child["taskId"]}),
                )
                .await
                .unwrap();
                assert!(!cancelled.is_error);
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
                let saved_input = if window == 2 {
                    *harness.cli_session.fail_next_resume.lock().await = true;
                    processor
                        .handle_cli_runtime_timeline_event(
                            handle.instance(),
                            RuntimeEvent::TurnCompleted(RuntimeTurnCompleted {
                                native_thread_id: Some(binding.native_thread_id.clone()),
                                native_turn_id: binding.native_turn_id.clone().unwrap(),
                                status: "completed".into(),
                                native: None,
                            }),
                        )
                        .await;
                    let attempt = timeout(Duration::from_secs(10), async {
                        loop {
                            let attempt = processor
                                .crud_store
                                .latest_cli_runtime_turn_attempt(&turn_id)
                                .await
                                .unwrap()
                                .unwrap();
                            if attempt.attempt_index == 2
                                && attempt.status
                                    == pioneer_crud::CliRuntimeTurnAttemptStatus::Failed
                            {
                                break attempt;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert_eq!(
                        harness.cli_session.turn_starts.lock().await.len(),
                        3,
                        "provider never received continuation"
                    );
                    assert!(
                        observed_cli_tasks(&processor, &turn_id)
                            .await
                            .contains(child["taskId"].as_str().unwrap())
                    );
                    let receipt = processor
                        .crud_store
                        .get_cli_runtime_native_event(&format!("{}:task-continuation", attempt.id))
                        .await
                        .unwrap()
                        .unwrap();
                    let payload: JsonValue =
                        serde_json::from_str(&receipt.payload_redacted_json).unwrap();
                    assert!(
                        processor
                            .report_turn_failure(
                                binding.thread_id.clone(),
                                turn_id.clone(),
                                TurnFailureRecoveryKind::RuntimeFailure,
                                "process lost before task payload delivery".into()
                            )
                            .await
                    );
                    Some(payload["input"].clone())
                } else {
                    let saved = if window == 1 {
                        // Persist precisely the production renderer's block + semantic
                        // facts, then model a crash before the separate marker write.
                        let state = processor
                            .cli_task_finalization_state(&binding)
                            .await
                            .unwrap();
                        let input = serde_json::to_value(vec![
                            pioneer_cli_agent_runtime::input::CLIRuntimeTurnInputItem::Text {
                                text: state.continuation_text(),
                            },
                        ])
                        .unwrap();
                        let now = chrono::Utc::now().fixed_offset();
                        let receipt = pioneer_crud::NewCliRuntimeNativeEvent {
                            id: "crashed-task-input".into(),
                            runtime_id: binding.runtime_id.clone(),
                            runtime_kind: binding.runtime_kind.clone(),
                            workspace_id: Some(binding.workspace_id.clone()),
                            thread_id: Some(binding.thread_id.clone()),
                            turn_id: Some(turn_id.clone()),
                            native_thread_id: Some(binding.native_thread_id.clone()),
                            native_turn_id: binding.native_turn_id.clone(),
                            native_method: "gateway/task_continuation".into(),
                            payload_redacted_json: json!({"input":input,"terminalObservations":state.continuation_outcomes()}).to_string(),
                            sequence: now.timestamp_millis(),
                            created_at: now,
                        };
                        processor
                            .crud_store
                            .append_cli_runtime_native_event(receipt)
                            .await
                            .unwrap();
                        assert!(
                            observed_cli_tasks(&processor, &turn_id).await.is_empty(),
                            "receipt commit is not observation"
                        );
                        Some(input)
                    } else {
                        None
                    };
                    processor
                        .handle_cli_runtime_timeline_event(
                            handle.instance(),
                            RuntimeEvent::TurnInterrupted(RuntimeTurnInterrupted {
                                native_thread_id: Some(binding.native_thread_id.clone()),
                                native_turn_id: binding.native_turn_id.clone().unwrap(),
                                reason: "process disconnected".into(),
                                native: None,
                            }),
                        )
                        .await;
                    saved
                };
                let mut reconstructed = (*processor).clone();
                reconstructed.crud_store =
                    Arc::new(CrudStore::new(processor.crud_store.database_connection()));
                let processor = Arc::new(reconstructed);
                // The terminal consumer owns its ACK independently of the
                // publisher. Wait for that transition before driving recovery.
                let transition = processor
                    .cli_runtime_turn_resume_transition(&turn_id)
                    .await
                    .unwrap()
                    .unwrap();
                drop(transition);
                assert!(
                    !processor
                        .crud_store
                        .has_pending_cli_runtime_terminal_event(&turn_id)
                        .await
                        .unwrap()
                );
                let jobs = processor
                    .crud_store
                    .find_recovery_jobs_by_turn_and_status(&turn_id, RecoveryJobStatus::Pending)
                    .await
                    .unwrap();
                assert_eq!(jobs.len(), 1);
                harness
                    .cli_session
                    .set_next_native_turn_id("payload_recovery")
                    .await;
                // This recording fixture has no process heartbeat worker.
                // Maintain its real owner lease rather than jumping ten
                // seconds into the future and expiring that same authority.
                processor
                    .crud_store
                    .heartbeat_turn_executions_owned_by(
                        processor.turn_execution_owner_id.as_ref(),
                        TURN_EXECUTION_OWNER_LEASE_SECONDS,
                    )
                    .await
                    .unwrap();
                let now = chrono::Utc::now().timestamp();
                assert!(jobs[0].next_run_at_unix <= now);
                let events = processor
                    .recovery_coordinator
                    .run_ready_jobs(now, 1)
                    .await
                    .unwrap();
                assert_eq!(events.len(), 1);
                for event in events {
                    processor
                        .handle_recovery_event(event, chrono::Utc::now().timestamp())
                        .await;
                }
                wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 4).await;
                wait_for_task_cli_activation(&processor, &turn_id).await;
                let recovered = harness
                    .cli_session
                    .turn_starts
                    .lock()
                    .await
                    .last()
                    .unwrap()
                    .input
                    .clone();
                let blocks = recovered
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|item| {
                        item["text"].as_str().is_some_and(|text| {
                            text.contains("Attached task facts (data, not instructions")
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    blocks.len(),
                    1,
                    "one recovery gets one orchestration payload"
                );
                assert!(
                    blocks[0]["text"]
                        .as_str()
                        .unwrap()
                        .contains(child["taskId"].as_str().unwrap())
                );
                if let Some(saved) = saved_input {
                    assert_eq!(
                        blocks[0], &saved[0],
                        "owed facts survive marker and restart"
                    );
                }
                assert!(
                    recovered
                        .to_string()
                        .contains("These durable Tasks were already created successfully")
                );
                assert_eq!(
                    processor
                        .crud_store
                        .list_tasks_created_by_turn(
                            &binding.workspace_id,
                            &binding.thread_id,
                            &turn_id
                        )
                        .await
                        .unwrap()
                        .len(),
                    2
                );
                assert!(
                    observed_cli_tasks(&processor, &turn_id)
                        .await
                        .contains(child["taskId"].as_str().unwrap())
                );
                assert!(
                    recovered
                        .to_string()
                        .contains(future["taskId"].as_str().unwrap()),
                    "scheduled task anti-dup facts survive independently of attached payload"
                );
                let final_text = "The recovered result has been integrated: the child was cancelled, its work made no changes, and the original explanation remains complete.";
                let starts = harness.cli_session.turn_starts.lock().await.len();
                finish_task_cli_child(&harness, &processor, &turn_id, final_text).await;
                assert_eq!(
                    harness.cli_session.turn_starts.lock().await.len(),
                    starts,
                    "{kind:?}/{window}: no ACK attempt after recovery"
                );
                let final_item = processor
                    .crud_store
                    .completed_turn_final_answer(
                        &binding.workspace_id,
                        &binding.thread_id,
                        &turn_id,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(final_item, TurnItem::AgentMessage { phase:AgentMessagePhase::FinalAnswer, ref text, .. } if text == final_text)
                );
                let upper = invoke_task(
                    &processor,
                    &root,
                    "payload-upper-wait",
                    "task_wait",
                    json!({"runIds":[parent["runId"]],"timeoutMs":10000}),
                )
                .await
                .unwrap()
                .structured_content
                .unwrap();
                assert_eq!(
                    upper["reviewRequired"][0]["reviewContent"]["content"], final_text,
                    "upper parent gets the recovered substantive final"
                );
            }
        }
    });
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
                assert_eq!(bindings.len(), 5);
                assert!(bindings.iter().any(|binding| binding.skill_id == user_id && binding.source_kind == "user"));
                for slug in ["tasks", "subagents", "html", "threads"] {
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
                let instructions = start.elevated_instructions.text();
                if kind == CLIAgentRuntimeKind::Claude {
                    assert!(instructions.contains(&format!("Selected skills:\n- {user_name}\n- pioneer-system-tasks\n- pioneer-system-subagents\n- pioneer-system-html\n- pioneer-system-threads")),
                        "application order must retain the user's selected skill before automatic system skills");
                }
                for name in [
                    "pioneer-system-tasks",
                    "pioneer-system-subagents",
                    "pioneer-system-html",
                    "pioneer-system-threads",
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
            let child_binding = wait_for_task_cli_child(
                &processor,
                child.structured_content.as_ref().unwrap()["runId"]
                    .as_str()
                    .unwrap(),
                None,
            )
            .await;
            // Creation only persists the run; await its admitted child before
            // checking the recording session's provider dispatch.
            wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 2).await;
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
            for definition in crate::turn_mcp::native_tools::NativeToolFamily::Tasks
                .definitions()
                .into_iter()
                .chain(crate::message::thread_read_tools::thread_read_tool_specs())
                .chain(crate::message::thread_search_tools::thread_search_tool_specs())
                .chain(crate::message::thread_tool_results::thread_result_tool_specs())
            {
                assert!(
                    bindings
                        .iter()
                        .any(|binding| binding.raw_tool_name == definition.spec.name)
                );
            }
            for name in [
                "threads_identity_models_list",
                "thread_message_send",
                "thread_create",
                "threads_turn_start",
            ] {
                assert!(
                    bindings.iter().any(|binding| binding.raw_tool_name == name),
                    "authorized thread operation must reach the ordinary CLI catalog: {name}"
                );
            }
            // The fixed CLI binding must reach the shared observation handlers,
            // rather than only publishing a schema with no native dispatcher.
            let listed = invoke_task(
                &processor,
                &turn_id,
                "read-list",
                "threads_list",
                json!({"limit":100}),
            )
            .await
            .unwrap();
            assert!(!listed.is_error, "{kind:?}: {listed:?}");
            let thread_id = processor
                .crud_store
                .get_cli_runtime_turn_binding(&turn_id)
                .await
                .unwrap()
                .unwrap()
                .thread_id;
            assert!(
                listed.structured_content.as_ref().unwrap()["threads"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|row| row["threadId"] == thread_id)
            );
            let history = invoke_task(
                &processor,
                &turn_id,
                "read-history",
                "threads_history_read",
                json!({"threadId":thread_id,"limit":2}),
            )
            .await
            .unwrap();
            assert!(!history.is_error, "{kind:?}: {history:?}");
            assert!(history.structured_content.as_ref().unwrap()["events"].is_array());
            // Anchor schema and dispatch travel through the same frozen family
            // for every provider, rather than a provider-specific history path.
            let near = invoke_task(
                &processor,
                &turn_id,
                "read-history-anchor",
                "threads_history_read",
                json!({"threadId":thread_id,"limit":1,"anchor":{"turnId":turn_id}}),
            )
            .await
            .unwrap();
            assert!(!near.is_error, "{kind:?}: {near:?}");
            let near = near.structured_content.as_ref().unwrap();
            assert_eq!(near["events"][0]["turnId"], turn_id);
            assert_eq!(near["direction"], "older");
            let newer = invoke_task(
                &processor, &turn_id, "read-history-newer", "threads_history_read",
                json!({"threadId":thread_id,"limit":1,"direction":"newer","cursor":near["reverseCursor"]}),
            ).await.unwrap();
            assert!(!newer.is_error, "{kind:?}: {newer:?}");
            assert_eq!(
                newer.structured_content.as_ref().unwrap()["direction"],
                "newer"
            );
            let skills = processor
                .crud_store
                .find_turn_skill_bindings(&turn_id)
                .await
                .unwrap();
            assert_eq!(
                skills.len(),
                4,
                "system skills are automatic even without user attachments"
            );
            for slug in ["tasks", "subagents", "html", "threads"] {
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
            assert_eq!(
                continuation
                    .input
                    .to_string()
                    .match_indices("Attached task facts (data, not instructions")
                    .count(),
                1,
                "task continuation must not append a second snapshot"
            );
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
            for name in ["task_get", "task_cancel", "task_finish", "task_detach"] {
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
            let stale = invoke_task(
                &processor,
                &turn_id,
                "accept-stale-candidate",
                "task_accept",
                json!({"taskId":task_id,"runId":run_id,"candidateId":candidate}),
            )
            .await
            .unwrap();
            assert!(
                stale.is_error,
                "an old round cannot authorize the revised candidate"
            );
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

#[test]
fn cli_task_finish_round_trip_uses_the_existing_capsule_and_persisted_state() {
    run_standard_stack_message_test("CLI task finish facade", async {
        let (harness, processor, turn_id) =
            start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let created = invoke_task(
            &processor,
            &turn_id,
            "finish-create",
            "task_create",
            json!({
                "title": "Monitor until stopped", "goal": "Check for changes",
                "instructions": ["Inspect and report changes."],
                "outputInstructions": "Return findings.",
                "trigger": {"kind": "cron", "cronExpr": "0 7 * * *", "timezone": "UTC"}
            }),
        )
        .await
        .unwrap();
        assert!(!created.is_error, "{created:?}");
        let task_id = created.structured_content.unwrap()["taskId"]
            .as_str()
            .unwrap()
            .to_owned();
        let finished = invoke_task(
            &processor,
            &turn_id,
            "finish-stop",
            "task_finish",
            json!({"taskId": task_id, "reason": "User requested a permanent stop"}),
        )
        .await
        .unwrap();
        assert!(!finished.is_error, "{finished:?}");
        let output = finished.structured_content.unwrap();
        assert_eq!(output["alreadyFinished"], false);
        assert_eq!(output["task"]["status"], "completed");
        assert_eq!(
            output["task"]["finishRequest"]["reason"],
            "User requested a permanent stop"
        );
        let stored = harness
            .crud_store
            .get_task(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.task.status, TaskStatus::Completed);
        assert!(
            stored.runs.is_empty(),
            "finish must not synthesize an agent run"
        );
        let recovered_receipt = invoke_task(
            &processor,
            &turn_id,
            "finish-stop",
            "task_finish",
            json!({"taskId": task_id, "reason": "User requested a permanent stop"}),
        )
        .await
        .unwrap();
        assert!(!recovered_receipt.is_error, "{recovered_receipt:?}");
        let recovered_receipt = recovered_receipt.structured_content.unwrap();
        assert_eq!(recovered_receipt["alreadyCommitted"], true);
        assert_eq!(recovered_receipt["alreadyFinished"], true);
        assert_eq!(recovered_receipt["task"]["id"], task_id);
        assert_eq!(recovered_receipt["task"]["status"], "completed");
        assert_eq!(
            harness
                .crud_store
                .get_task(&task_id)
                .await
                .unwrap()
                .unwrap()
                .task
                .revision,
            stored.task.revision,
            "durable receipt replay must only read the stopped task"
        );
        let repeated = invoke_task(
            &processor,
            &turn_id,
            "finish-repeat",
            "task_finish",
            json!({"taskId": task_id}),
        )
        .await
        .unwrap();
        assert!(!repeated.is_error, "{repeated:?}");
        assert_eq!(
            repeated.structured_content.unwrap()["alreadyFinished"],
            true
        );
        let read = invoke_task(
            &processor,
            &turn_id,
            "finish-read",
            "task_get",
            json!({"taskId": task_id}),
        )
        .await
        .unwrap();
        assert!(!read.is_error, "{read:?}");
        assert_eq!(
            read.structured_content.unwrap()["task"]["finishRequest"]["reason"],
            "User requested a permanent stop"
        );
    });
}

#[test]
fn cli_frozen_reader_catalog_does_not_gain_search_after_upgrade() {
    run_standard_stack_message_test("CLI frozen search admission", async {
        let (_harness, processor, turn_id) =
            start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let bindings = processor
            .crud_store
            .list_turn_mcp_bindings(&turn_id)
            .await
            .unwrap();
        // A newly selected threads catalog admits the registered search reader.
        assert!(
            bindings
                .iter()
                .any(|binding| binding.raw_tool_name == "threads_search")
        );
        // Reproduce an older persisted selection, which admitted the other
        // readers but predates search. Current definitions must not widen it.
        let previous_bindings = bindings
            .into_iter()
            .filter(|binding| binding.raw_tool_name != "threads_search")
            .collect::<Vec<_>>();
        assert!(
            previous_bindings
                .iter()
                .any(|binding| binding.raw_tool_name == "threads_history_read")
        );
        processor
            .crud_store
            .replace_turn_mcp_bindings(&turn_id, &previous_bindings, 1_792_781_981)
            .await
            .unwrap();
        let error = invoke_task(
            &processor,
            &turn_id,
            "unbound-upgrade-search",
            "threads_search",
            json!({"query":"prior decision","limit":1}),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.code,
            crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::ToolUnbound
        );
        assert!(
            !processor
                .crud_store
                .list_turn_mcp_bindings(&turn_id)
                .await
                .unwrap()
                .iter()
                .any(|binding| binding.raw_tool_name == "threads_search")
        );
    });
}

#[test]
fn admitted_cli_production_recall_hook_searches_another_accessible_conversation() {
    run_standard_stack_message_test("indexed workspace recall hook", async {
        use pioneer_crud::ThreadEpisodicWorkspaceActiveWriteSegmentRequest;
        use pioneer_hooks::*;
        let (_harness, processor, turn_id) =
            start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let binding = processor
            .crud_store
            .get_cli_runtime_turn_binding(&turn_id)
            .await
            .unwrap()
            .unwrap();
        let other = "indexed-recall-other-conversation";
        processor.crud_store.database_connection().execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at,created_by_actor_kind,created_by_actor_id) SELECT ?,workspace_id,'Other indexed conversation','',mode,model,model_provider,status,origin_kind,sidebar_visibility,'workspace',created_at,updated_at,created_by_actor_kind,created_by_actor_id FROM thread WHERE id=?",
            [other.into(),binding.thread_id.clone().into()])).await.unwrap();
        let temp = tempfile::tempdir().unwrap();
        let capsule = processor
            .crud_store
            .resolve_thread_episodic_workspace_active_write_segment(
                ThreadEpisodicWorkspaceActiveWriteSegmentRequest {
                    workspace_id: binding.workspace_id.clone(),
                    storage_uri_root: format!("file://{}", temp.path().display()),
                },
                1_800_000_000,
            )
            .await
            .unwrap();
        crate::message::thread_search_tools::tests::seed(
            &processor,
            &capsule,
            other,
            "hook-other-source",
            "searchneedle remembered in the other conversation",
        )
        .await;
        let context = pioneer_agent::TurnToolContext {
            workspace_id: binding.workspace_id.clone(),
            thread_id: binding.thread_id.clone(),
            turn_id: turn_id.clone(),
        };
        let tool = processor
            .search_threads_page(
                &context,
                pioneer_protocol::ThreadsSearchParams {
                    query: "searchneedle".into(),
                    limit: 10,
                    cursor: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            tool["threads"][0]["threadId"], other,
            "positive control: admitted execution can read the other source"
        );
        let mut memory_config = processor.memory_loop_config();
        memory_config.active_recall.mode = pioneer_memory::hooks::MemoryActiveRecallMode::Hybrid;
        processor.apply_memory_loop_config(memory_config);
        processor.bind_memory_bridge().await;
        let runtime = processor.hook_runtime.read().await.clone().unwrap();
        let mut policy = std::collections::BTreeMap::new();
        for (key, value) in [
            ("recall", "allow"),
            ("prompt", "full"),
            ("read_tools", "allow"),
            ("remember_tool", "allow"),
            ("forget_tool", "allow"),
            ("post_turn_extraction", "allow"),
            ("active_memory", "allow"),
            ("reason_code", "default_allow_read"),
            ("source", "pre_memory_classifier"),
        ] {
            policy.insert(
                HookMetadataKey::new(key).unwrap(),
                HookValue::Text(value.into()),
            );
        }
        policy.insert(
            HookMetadataKey::new("explicit_remember").unwrap(),
            HookValue::Bool(false),
        );
        policy.insert(
            HookMetadataKey::new("explicit_forget").unwrap(),
            HookValue::Bool(false),
        );
        policy.insert(
            HookMetadataKey::new("confidence").unwrap(),
            HookValue::F64(1.0),
        );
        let policies = HookPolicySet::merge_contributions([PolicyContribution {
            domain: HookDomain::new("memory").unwrap(),
            key: HookPolicyKey::new("turn_policy").unwrap(),
            value: HookValue::Object(policy),
            priority: 500,
            diagnostics: Vec::new(),
        }]);
        let input=TurnPostPreflightPromptContextHookInput::from_parts("searchneedle",Some("test-model"),Some("openai"))
            .with_active_memory_recall_preflight_plan(json!({"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},"episodic":{"status":"run","reasonCode":"provider_run","confidence":1.0,"queries":[{"mode":"workspace_thread","query":"searchneedle","targets":[]}]}}));
        let request = HookPhaseRequest::new(
            HookPhase::TurnPostPreflightPromptContext,
            HookContext {
                workspace_id: Some(HookWorkspaceId::new(binding.workspace_id.clone()).unwrap()),
                thread_id: Some(HookThreadId::new(binding.thread_id.clone()).unwrap()),
                turn_id: Some(HookTurnId::new(turn_id.clone()).unwrap()),
                ..HookContext::default()
            },
            HookInput::turn_post_preflight_prompt_context(input),
        )
        .with_policy_set(policies);
        let response = runtime.run_phase_to_completion(request).await.unwrap();
        let rendered = serde_json::to_string(&response.contributions).unwrap();
        assert!(
            rendered.contains("remembered in the other conversation"),
            "installed production hook must consume workspace results: {response:?}"
        );
        assert!(
            rendered.contains("hook-other-source"),
            "real source anchor must reach the hook contribution"
        );
        // Materialize a real restricted Agent policy intersected with the
        // admitted root adapter. The initiating user's workspace ACL stays
        // broad; this binding omits workspace observation by role policy.
        crate::message::thread_search_tools::tests::seed(
            &processor,
            &capsule,
            &binding.thread_id,
            "runner-own-source",
            "searchneedle own runner context",
        )
        .await;
        let parent_binding = processor.agent_action_binding(&turn_id).await.unwrap();
        let parent_adapter = parent_binding.adapter.lock().await;
        let facts = parent_adapter.persistence_facts();
        let (restricted, options, capabilities) =
            crate::authorization::materialize_selected_task_agent_action_binding(
                pioneer_protocol::AgentExecutionId::new("Esearchrunner12345678").unwrap(),
                &facts.home_root_thread_id,
                facts.root_execution_id.clone(),
                "search-runner",
                facts.identity.clone(),
                facts.profile.clone(),
                1,
                1,
                1,
                "agent_runner",
                facts.agent_authorization_policy_generation,
                &parent_adapter,
            )
            .unwrap();
        let restricted_facts = restricted.persistence_facts();
        assert!(
            restricted_facts
                .agent_authorization_allowed_actions
                .iter()
                .any(|a| a == crate::authorization::ResourceAction::ThreadRead.safe_name())
        );
        assert!(
            !restricted_facts
                .agent_authorization_allowed_actions
                .iter()
                .any(|a| a
                    == crate::authorization::ResourceAction::ThreadObserveWorkspace.safe_name())
        );
        drop(parent_adapter);
        processor
            .register_agent_action_binding(
                turn_id.clone(),
                crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                    restricted,
                    options,
                    capabilities,
                ),
            )
            .await;
        let limited = processor
            .search_threads_page(
                &context,
                pioneer_protocol::ThreadsSearchParams {
                    query: "searchneedle".into(),
                    limit: 10,
                    cursor: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(limited["threads"].as_array().unwrap().len(), 1);
        assert_eq!(limited["threads"][0]["threadId"], binding.thread_id);
        assert_eq!(
            limited["threads"][0]["fragments"][0]["source"]["itemId"],
            "runner-own-source"
        );
        assert!(!limited.to_string().contains("hook-other-source"));
    });
}

#[test]
fn admitted_cli_search_preserves_structured_pages_at_projection_byte_boundary() {
    run_standard_stack_message_test("search byte boundary continuation", async {
        let (_harness, processor, turn_id) =
            start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let binding = processor
            .crud_store
            .get_cli_runtime_turn_binding(&turn_id)
            .await
            .unwrap()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let capsule = processor
            .crud_store
            .resolve_thread_episodic_workspace_active_write_segment(
                pioneer_crud::ThreadEpisodicWorkspaceActiveWriteSegmentRequest {
                    workspace_id: binding.workspace_id.clone(),
                    storage_uri_root: format!("file://{}", temp.path().display()),
                },
                1_800_000_000,
            )
            .await
            .unwrap();
        let mut titles = ["A".to_owned(), "B".to_owned(), "C".to_owned()];
        for (n, thread) in ["byte-boundary-a", "byte-boundary-b", "byte-boundary-c"]
            .into_iter()
            .enumerate()
        {
            processor.crud_store.database_connection().execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,
                "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at,created_by_actor_kind,created_by_actor_id) SELECT ?,workspace_id,?,'',mode,model,model_provider,status,origin_kind,sidebar_visibility,'workspace',created_at,updated_at,created_by_actor_kind,created_by_actor_id FROM thread WHERE id=?",
                [thread.into(),titles[n].clone().into(),binding.thread_id.clone().into()])).await.unwrap();
            crate::message::thread_search_tools::tests::seed(
                &processor,
                &capsule,
                thread,
                &format!("byte-source-{n}"),
                "boundaryneedle saved decision",
            )
            .await;
        }
        let context = pioneer_agent::TurnToolContext {
            workspace_id: binding.workspace_id.clone(),
            thread_id: binding.thread_id.clone(),
            turn_id,
        };
        let params = json!({"query":"boundaryneedle", "limit":2, "cursor":null});
        // Calibrate with the actual CLI response: two cards plus a nextCursor.
        // Enlarge the first title so those same two raw cards fit by one byte,
        // but their ToolResultView wrapper would exceed the policy limit.
        let calibration = invoke_task(
            &processor,
            &context.turn_id,
            "search-calibration",
            "threads_search",
            params.clone(),
        )
        .await
        .unwrap();
        assert!(!calibration.is_error);
        let mut raw = calibration.structured_content.unwrap();
        for key in ["tool_outcome", "partial_output", "truncated"] {
            raw.as_object_mut().unwrap().remove(key);
        }
        assert_eq!(raw["threads"].as_array().unwrap().len(), 2);
        assert!(raw["nextCursor"].is_string());
        let limit = 2 * 1024 * 1024;
        let overhead = serde_json::to_vec(&raw).unwrap().len() - titles[0].len();
        titles[0] = "A".repeat(limit - 1 - overhead);
        raw["threads"][0]["title"] = titles[0].clone().into();
        assert_eq!(serde_json::to_vec(&raw).unwrap().len(), limit - 1);
        assert!(
            pioneer_tools::ToolResultView::Json {
                value: raw,
                truncated: false
            }
            .serialized_size_bytes()
                > limit
        );
        processor
            .crud_store
            .database_connection()
            .execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                "UPDATE thread SET name=? WHERE id=?",
                [titles[0].clone().into(), "byte-boundary-a".into()],
            ))
            .await
            .unwrap();

        let first = invoke_task(
            &processor,
            &context.turn_id,
            "search-first",
            "threads_search",
            params.clone(),
        )
        .await
        .unwrap();
        assert!(!first.is_error);
        let first_text = first.content[0]["text"].as_str().unwrap();
        assert!(first_text.len() <= limit);
        let first = first.structured_content.unwrap();
        assert!(serde_json::to_vec(&first).unwrap().len() <= limit);
        assert_eq!(
            serde_json::from_str::<JsonValue>(first_text).unwrap(),
            first
        );
        assert!(first.get("preview").is_none());
        assert_eq!(first["truncated"], false);
        assert_eq!(first["threads"].as_array().unwrap().len(), 1);
        assert_eq!(first["threads"][0]["threadId"], "byte-boundary-a");
        assert_eq!(first["threads"][0]["title"], titles[0]);
        assert_eq!(first["eof"], false);
        let cursor = first["nextCursor"].as_str().unwrap().to_owned();
        let mut next_params = params.clone();
        next_params["cursor"] = cursor.clone().into();
        let second = invoke_task(
            &processor,
            &context.turn_id,
            "search-second",
            "threads_search",
            next_params.clone(),
        )
        .await
        .unwrap();
        assert!(!second.is_error);
        let second = second.structured_content.unwrap();
        assert!(serde_json::to_vec(&second).unwrap().len() <= limit);
        assert!(second.get("preview").is_none());
        assert_eq!(second["threads"].as_array().unwrap().len(), 2);
        assert_eq!(second["threads"][0]["threadId"], "byte-boundary-b");
        assert_eq!(second["threads"][0]["title"], titles[1]);
        assert_eq!(second["threads"][1]["threadId"], "byte-boundary-c");
        assert_eq!(second["threads"][1]["title"], titles[2]);
        assert_eq!(second["eof"], true);
        assert_eq!(second["nextCursor"], JsonValue::Null);
        let mut wrong_query = next_params.clone();
        wrong_query["query"] = "different query".into();
        assert!(
            invoke_task(
                &processor,
                &context.turn_id,
                "search-wrong-query",
                "threads_search",
                wrong_query,
            )
            .await
            .is_err()
        );
        // The fixture's current user is a Superuser. Narrow the execution
        // through the real Agent role intersection instead of pretending a
        // private-thread ACL can revoke that user's absolute access.
        let parent_binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let parent_adapter = parent_binding.adapter.lock().await;
        let facts = parent_adapter.persistence_facts();
        let (restricted, options, capabilities) =
            crate::authorization::materialize_selected_task_agent_action_binding(
                pioneer_protocol::AgentExecutionId::new("Ebyteboundary12345678").unwrap(),
                &facts.home_root_thread_id,
                facts.root_execution_id.clone(),
                "byte-boundary-runner",
                facts.identity.clone(),
                facts.profile.clone(),
                1,
                1,
                1,
                "agent_runner",
                facts.agent_authorization_policy_generation,
                &parent_adapter,
            )
            .unwrap();
        drop(parent_adapter);
        processor
            .register_agent_action_binding(
                context.turn_id.clone(),
                crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                    restricted,
                    options,
                    capabilities,
                ),
            )
            .await;
        // Cursors are bound to the original authority. A narrower identity
        // must restart the search rather than reuse the previous page token.
        let stale_cursor = invoke_task(
            &processor,
            &context.turn_id,
            "search-revoked-cursor",
            "threads_search",
            next_params,
        )
        .await
        .unwrap_err();
        assert_eq!(
            stale_cursor.code,
            crate::turn_mcp::invoker::TurnMcpInvocationErrorCode::InvalidRequest
        );
        let after_revocation = invoke_task(
            &processor,
            &context.turn_id,
            "search-revoked",
            "threads_search",
            params,
        )
        .await
        .unwrap();
        assert!(!after_revocation.is_error);
        let after_revocation = after_revocation.structured_content.unwrap();
        assert!(after_revocation["threads"].as_array().unwrap().is_empty());
        assert_eq!(after_revocation["eof"], true);
        assert!(!after_revocation.to_string().contains("byte-boundary-b"));
    });
}

// Permission-matrix fixture using real admission and the recording CLI runtime.
// The primary positive facade test below uses process_request_for_connection.
async fn start_threads_cli_fixture(
    kind: CLIAgentRuntimeKind,
    denied: &[&str],
) -> (
    CliRuntimeSkillPreflightHarness,
    Arc<MessageProcessor>,
    String,
) {
    let mut profile = default_test_permission_profile();
    profile.effective_policy.denied_tools = denied.iter().map(|name| (*name).to_owned()).collect();
    start_threads_cli_fixture_with_policy(kind, profile).await
}

async fn start_threads_cli_fixture_with_policy(
    kind: CLIAgentRuntimeKind,
    profile: pioneer_protocol::TurnPermissionProfileSnapshot,
) -> (
    CliRuntimeSkillPreflightHarness,
    Arc<MessageProcessor>,
    String,
) {
    let harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
    *harness.cli_session.projected_mcp_store.lock().await = Some(harness.crud_store.clone());
    let thread = format!("cli_threads_{kind:?}").to_lowercase();
    let turn = format!("cli_threads_turn_{kind:?}").to_lowercase();
    seed_cli_runtime_skill_preflight_thread(&harness, &thread).await;
    let processor = Arc::new(harness.processor.clone());
    processor
        .mark_cli_reasoning_model_ready_for_tests(
            &harness.workspace_id,
            &harness.runtime_id,
            "threads-cli-choice",
        )
        .await
        .unwrap();
    processor.bind_agent_tool_bridges().await;
    let request: JsonValue =
        serde_json::from_str(&cli_runtime_turn_start_request_with_capabilities(
            &generate_test_request_id("threads", &format!("{kind:?}")),
            &thread,
            &turn,
            &harness.runtime_id,
            kind,
            vec![],
        ))
        .unwrap();
    let params: pioneer_protocol::TurnStartParams =
        serde_json::from_value(request["params"].clone()).unwrap();
    let connection = processor
        .session_manager
        .connection_context(harness.connection_id)
        .await
        .unwrap();
    use crate::authorization::{
        ExecutionAdmissionEntryPoint, ExecutionAdmissionRequest, ExecutionAdmissionService,
        ResourceAction,
    };
    let source = processor
        .crud_store
        .get_thread_model(&thread)
        .await
        .unwrap()
        .unwrap();
    let mut params = params;
    params.model = Some(source.model.clone());
    params.model_provider = Some(cli_runtime_provider_key(&harness.runtime_id));
    let admission = ExecutionAdmissionRequest::for_turn(
        ExecutionAdmissionEntryPoint::CliRuntime,
        ResourceAction::AgentTurnStart,
        Vec::new(),
        &harness.workspace_id,
        &thread,
        &format!("cli_runtime:{}", harness.runtime_id),
        &source.model,
        &params,
        &[],
        None,
    )
    .unwrap();
    let context = ExecutionAdmissionService::new(processor.crud_store.as_ref().clone())
        .admit_context(
            connection.principal(),
            processor
                .authorization_invalidation_hub
                .current_revision()
                .await
                .unwrap(),
            &admission,
            Some(&pioneer_protocol::task_permission_cap_from_snapshot(
                &profile,
            )),
        )
        .await
        .unwrap();
    let mut security = test_full_access_execution_security_snapshot();
    security.permission_profile = profile.clone();
    let revalidation = processor
        .execution_leases
        .revalidate_context(
            processor.crud_store.as_ref(),
            &context,
            crate::authorization::ResourceAction::AgentTurnStart,
            processor
                .authorization_invalidation_hub
                .current_revision()
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    // Durable authority must use the committed-agent response and activation
    // path; a fresh TurnStart response is rejected before native preparation.
    let author = super::super::message_turn::resolve_turn_author_snapshot(
        processor.crud_store.as_ref(),
        &pioneer_protocol::PersistedActorRef::Principal(
            connection.principal().principal_id.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let outcome = processor
        .thread_manager
        .agent_turn_start_with_permission_profile(params.clone(), profile.clone(), author.clone())
        .await
        .unwrap();
    let admission = context
        .durable_turn_admission_after_revalidation(
            &thread,
            &turn,
            params.execution_backend.as_ref(),
            &revalidation,
        )
        .unwrap();
    processor
        .crud_store
        .materialize_authorized_turn_start_with_reasoning_effort_and_permission_audit(
            &outcome.materialization.thread,
            outcome.materialization.sandbox_mode,
            &outcome.materialization.turn,
            &outcome.materialization.input,
            params
                .reasoning
                .as_ref()
                .map(|reasoning| reasoning.effort.as_str()),
            Default::default(),
            author.actor.clone(),
            processor
                .turn_profile_selected_audit_event(&outcome)
                .unwrap(),
            &context.to_persisted_json().unwrap(),
            None,
            Some(admission),
            Some(
                crate::message::turn_handlers::new_turn_execution(
                    processor.turn_execution_owner_id.as_ref(),
                    params.execution_backend.as_ref(),
                    &outcome.materialization,
                )
                .unwrap(),
            ),
            &security,
            Vec::new(),
            None,
            None,
        )
        .await
        .unwrap();
    // Threads-only catalogs do not include task_create, whose normal launch
    // path persists this snapshot before binding the root Agent execution.
    processor
        .crud_store
        .upsert_turn_runtime_snapshot(
            crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
                &thread,
                &harness.workspace_id,
                &turn,
                pioneer_protocol::ThreadMode::Agent,
                &pioneer_agent::AgentTurnHookRuntimeContext::default(),
                &source.model,
                &cli_runtime_provider_key(&harness.runtime_id),
                None,
                &HashMap::new(),
                &params.input,
                &params.capabilities,
                &[],
                &HashMap::new(),
                &[],
                &[],
            )
            .unwrap(),
        )
        .await
        .expect("Threads fixture runtime snapshot should persist");
    let response = crate::message::turn_handlers::TurnStartSuccessResponse::DurableAgent {
        permission_profile: profile,
        execution_security_snapshot: security,
        continuation_thread_id: thread.clone(),
        context_thread_id: thread.clone(),
        agent_author: author,
        admitted_outcome: Some(Box::new(outcome)),
        completion: Arc::new(std::sync::Mutex::new(Some(sender))),
    };
    let (_, diagnostics) =
        crate::public_error::test_support::capture_events_async(processor.turn_start_cli_runtime(
            harness.connection_id,
            serde_json::from_value(request["id"].clone()).unwrap(),
            pioneer_protocol::PersistedActorRef::Principal(
                connection.principal().principal_id.clone(),
            ),
            params,
            harness.runtime_id.clone(),
            kind,
            crate::message::turn_handlers::TurnExecutionAuthority::Durable {
                context,
                revalidation: Arc::new(revalidation),
            },
            response,
        ))
        .await;
    let prepared = receiver
        .await
        .expect("Threads fixture preparation should return a result")
        .unwrap_or_else(|error| {
            panic!("Threads fixture durable CLI preparation failed: {error:#}; diagnostics: {diagnostics:#?}")
        });
    processor
        .activate_prepared_committed_agent_cli_runtime_turn(prepared)
        .await
        .expect("Threads fixture durable CLI activation should succeed");
    wait_for_recorded_cli_runtime_turn_start(&harness.cli_session).await;
    wait_for_task_cli_activation(&processor, &turn).await;
    (harness, processor, turn)
}

#[tokio::test]
async fn early_public_stop_and_cli_initial_attempt_share_the_exact_writer_fence() {
    for phase in [
        "stop_first",
        "cli_first",
        "replacement_generation",
        "same_owner_generation",
    ] {
        let kind = CLIAgentRuntimeKind::Codex;
        let mut harness = setup_cli_runtime_skill_preflight_harness(kind, false).await;
        *harness.cli_session.projected_mcp_store.lock().await = Some(harness.crud_store.clone());
        let thread = "cli_early_stop_race";
        let turn = "cli_early_stop_race_turn";
        seed_cli_runtime_skill_preflight_thread(&harness, thread).await;
        let processor = Arc::new(harness.processor.clone());
        processor.bind_agent_tool_bridges().await;
        processor.arm_completed_history_preparation_barrier("__cli_before_initial_binding__");
        let request = cli_runtime_turn_start_request_with_capabilities(
            &generate_test_request_id("early-cli", "start"),
            thread,
            turn,
            &harness.runtime_id,
            kind,
            vec![],
        );
        let start = tokio::spawn({
            let processor = processor.clone();
            let connection = harness.connection_id;
            async move {
                processor
                    .process_request_for_connection(connection, &request)
                    .await
            }
        });
        timeout(
            Duration::from_secs(10),
            processor.wait_for_completed_history_preparation_barrier(),
        )
        .await
        .unwrap();
        let store = &harness.crud_store;
        let owner = store.get_turn_execution(turn).await.unwrap().unwrap();
        assert!(
            store
                .latest_cli_runtime_turn_attempt(turn)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .native_cancellation_context(turn)
                .await
                .unwrap()
                .is_none()
        );
        let cancel_id = generate_test_request_id("early-cli", "cancel");
        let cancel_request = json!({"jsonrpc":"2.0", "id":cancel_id, "method":"turn/cancel",
            "params":{"thread_id":thread,"turn_id":turn}})
        .to_string();
        if phase == "stop_first" {
            processor
                .process_request_for_connection(harness.connection_id, &cancel_request)
                .await;
            let reply = recv_response_by_id(&mut harness.rx, &cancel_id).await;
            let reply: TurnCancelResponse = serde_json::from_value(reply.result).unwrap();
            assert_eq!(reply.turn.status, TurnStatus::Interrupted);
            processor.release_completed_history_preparation_barrier();
            timeout(Duration::from_secs(10), start)
                .await
                .unwrap()
                .unwrap();
            assert!(
                store
                    .latest_cli_runtime_turn_attempt(turn)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            // Stop has observed no CLI attempt, but has not entered its writer.
            let reached = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            *processor
                .native_cancellation_materialization_barrier
                .lock()
                .await = Some((reached.clone(), release.clone()));
            let stop = tokio::spawn({
                let processor = processor.clone();
                let connection = harness.connection_id;
                async move {
                    processor
                        .process_request_for_connection(connection, &cancel_request)
                        .await
                }
            });
            timeout(Duration::from_secs(10), reached.notified())
                .await
                .unwrap();
            processor.arm_completed_history_preparation_barrier("__cli_after_initial_binding__");
            processor.release_completed_history_preparation_barrier();
            timeout(
                Duration::from_secs(10),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            assert!(
                store
                    .latest_cli_runtime_turn_attempt(turn)
                    .await
                    .unwrap()
                    .is_some()
            );
            if matches!(phase, "replacement_generation" | "same_owner_generation") {
                let replacement = store
                    .claim_expired_turn_execution(
                        &owner,
                        if phase == "same_owner_generation" {
                            &owner.owner_id
                        } else {
                            "replacement-cli-owner"
                        },
                        owner.lease_until.timestamp() + 1,
                        owner.lease_until.timestamp() + 100,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let before = store.get_turn(thread, turn).await.unwrap().unwrap();
                release.notify_one();
                timeout(Duration::from_secs(10), stop)
                    .await
                    .unwrap()
                    .unwrap();
                recv_error_by_id(&mut harness.rx, &cancel_id).await;
                assert!(!store.native_turn_has_accepted_terminal(turn).await.unwrap());
                assert_eq!(store.get_turn(thread, turn).await.unwrap().unwrap(), before);
                assert_eq!(
                    store.get_turn_execution(turn).await.unwrap().unwrap(),
                    replacement
                );
                // The stale launch is held at the real preparation boundary.
                start.abort();
                let _ = start.await;
                processor.release_completed_history_preparation_barrier();
                assert!(harness.cli_session.turn_starts.lock().await.is_empty());
                continue;
            }
            // Keep activation's gate owned until Stop loses its early writer.
            processor
                .arm_completed_history_preparation_barrier("__early_stop_cli_after_phase_change__");
            release.notify_one();
            timeout(
                Duration::from_secs(10),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            processor.release_completed_history_preparation_barrier();
            processor.release_completed_history_preparation_barrier();
            timeout(Duration::from_secs(10), start)
                .await
                .unwrap()
                .unwrap();
            timeout(Duration::from_secs(10), stop)
                .await
                .unwrap()
                .unwrap();
            let reply = recv_response_by_id(&mut harness.rx, &cancel_id).await;
            let reply: TurnCancelResponse = serde_json::from_value(reply.result).unwrap();
            assert_eq!(reply.turn.status, TurnStatus::Interrupted);
            let binding = store
                .get_cli_runtime_turn_binding(turn)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(binding.status, "interrupted");
            assert_eq!(
                store
                    .latest_cli_runtime_turn_attempt(turn)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                pioneer_crud::CliRuntimeTurnAttemptStatus::Interrupted
            );
            assert!(!harness.cli_session.interrupts.lock().await.is_empty());
        }
        assert!(
            store
                .native_cancellation_context(turn)
                .await
                .unwrap()
                .is_none()
        );
        let saved = store.get_turn(thread, turn).await.unwrap().unwrap().1;
        assert_eq!(saved.status, TurnStatus::Interrupted);
        assert_eq!(
            processor
                .thread_manager
                .turn_get(thread, turn)
                .await
                .unwrap()
                .1,
            saved
        );
        let stopped = store.get_turn_execution(turn).await.unwrap().unwrap();
        assert_eq!(stopped.owner_id, owner.owner_id);
        assert_eq!(stopped.owner_generation, owner.owner_generation);
        assert_eq!(
            stopped.status,
            pioneer_crud::TurnExecutionStatus::Interrupted
        );
        let db = store.database_connection();
        let response = pioneer_crud::load_agent_turn_response(&db, turn)
            .await
            .unwrap()
            .unwrap();
        let resource =
            pioneer_crud::load_agent_execution_resource_state(&db, &response.execution_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(resource.status, "cancelled");
        let permit =
            pioneer_entity::agent_running_permit::Entity::find_by_id(resource.permit_id.unwrap())
                .one(&db)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(permit.status, "released");
        assert!(harness.cli_session.turn_starts.lock().await.is_empty());
    }
}

struct ThreadsFacade {
    facade: Arc<crate::cli_runtime::mcp::facade::CliMcpToolFacade>,
    context: crate::cli_runtime::mcp::facade::CliMcpFacadeRequestContext,
    native_items: Arc<crate::cli_runtime::mcp::native_correlation::NativeMcpCorrelationLedger>,
    claude_items: Arc<crate::cli_runtime::claude_session::ClaudeNativeMcpCorrelationLedger>,
    kind: CLIAgentRuntimeKind,
    native_thread: String,
    native_turn: String,
}

impl ThreadsFacade {
    async fn new(processor: &MessageProcessor, turn: &str, kind: CLIAgentRuntimeKind) -> Self {
        use crate::cli_runtime::mcp::{
            coordinator::CliMcpCoordinator, facade::*, grants::*, native_correlation::*,
        };
        let binding = processor
            .crud_store
            .get_cli_runtime_turn_binding(turn)
            .await
            .unwrap()
            .unwrap();
        let tools = processor
            .crud_store
            .list_turn_mcp_bindings(turn)
            .await
            .unwrap();
        use crate::turn_mcp::native_tools::NativeToolFamily;
        let definitions = [
            NativeToolFamily::Html,
            NativeToolFamily::ComputerUse,
            NativeToolFamily::Tasks,
            NativeToolFamily::Threads,
        ]
        .into_iter()
        .flat_map(|family| {
            let selected = &tools;
            family
                .definitions()
                .into_iter()
                .filter(move |spec| {
                    selected.iter().any(|binding| {
                        binding.server_installation_id == family.server_id()
                            && binding.raw_tool_name == spec.spec.name
                    })
                })
                .map(move |spec| (family, spec))
        })
        .collect();
        let canonical =
            crate::turn_mcp::ResolvedMcpTurnProjection::empty(&binding.workspace_id, turn)
                .with_native_tools(definitions, crate::turn_mcp::McpProjectionLimits::default())
                .unwrap();
        let metadata = binding.mcp.as_ref().unwrap();
        assert_eq!(canonical.manifest_hash, metadata.manifest_hash);
        let limits =
            crate::cli_runtime::mcp::limits::CliMcpFacadeProjectionLimits::transport_bounded(128);
        let projection = match kind {
            CLIAgentRuntimeKind::Codex => {
                crate::cli_runtime::codex_mcp::build_codex_mcp_session_launch_projection(
                    canonical,
                    metadata.provider_contract_fingerprint.clone(),
                )
                .unwrap()
                .facade_projection(limits)
                .unwrap()
            }
            CLIAgentRuntimeKind::Claude => {
                crate::cli_runtime::claude_mcp::build_claude_mcp_session_launch_projection(
                    canonical,
                    metadata.provider_contract_fingerprint.clone(),
                )
                .unwrap()
                .facade_projection(limits)
                .unwrap()
            }
            CLIAgentRuntimeKind::OpenCode => {
                crate::cli_runtime::opencode_mcp::build_opencode_mcp_session_launch_projection(
                    canonical,
                    metadata.provider_contract_fingerprint.clone(),
                )
                .unwrap()
                .facade_projection(limits)
                .unwrap()
            }
        };
        let key = CLIAgentRuntimeSessionKey::new(
            &binding.workspace_id,
            &binding.runtime_id,
            &binding.continuation_thread_id,
        )
        .unwrap();
        let instance =
            crate::cli_runtime::session_instance::CliSessionInstanceId::unmanaged_for_test(
                key,
                binding.mcp.as_ref().unwrap().session_generation as u64,
            )
            .unwrap();
        let scope = CliMcpGrantScope::new(
            instance,
            CliMcpManifestHash::new(binding.mcp.as_ref().unwrap().manifest_hash.clone()).unwrap(),
        );
        let coordinator = Arc::new(CliMcpCoordinator::default());
        let issued = coordinator
            .issue_grant(
                scope.clone(),
                (pioneer_crud::utc_now().timestamp_millis() as u64) + 60_000,
            )
            .await
            .unwrap();
        let reservation = coordinator
            .stage_projection(&issued.grant_ref(), projection.fingerprint().clone())
            .await
            .unwrap();
        let bound = coordinator
            .attach(
                &pioneer_cli_mcp_bridge::AttachRequest {
                    session_id: issued.bridge_session_id,
                    generation: pioneer_cli_mcp_bridge::BridgeGeneration::new(
                        scope.process_instance.generation(),
                    )
                    .unwrap(),
                    nonce: issued.nonce,
                },
                &scope,
                CliMcpConnectionId::for_test(1),
            )
            .await
            .unwrap();
        let native_items = Arc::new(NativeMcpCorrelationLedger::default());
        let claude_items = Arc::new(
            crate::cli_runtime::claude_session::ClaudeNativeMcpCorrelationLedger::default(),
        );
        let inner: Arc<dyn TurnMcpInvoker> = Arc::new(processor.mcp_service.turn_mcp_invoker());
        let invoker: Arc<dyn TurnMcpInvoker> = match kind {
            CLIAgentRuntimeKind::Claude => Arc::new(
                crate::cli_runtime::claude_session::ClaudeCorrelatingTurnMcpInvoker {
                    inner,
                    native_items: claude_items.clone(),
                    coordinator: coordinator.clone(),
                    bound_grant: bound.clone(),
                },
            ),
            _ => Arc::new(CorrelatingTurnMcpInvoker {
                inner,
                native_items: native_items.clone(),
                opencode_binding: (kind == CLIAgentRuntimeKind::OpenCode)
                    .then(|| (coordinator.clone(), bound.clone())),
                codex_binding: (kind == CLIAgentRuntimeKind::Codex).then(|| {
                    CodexMcpCorrelationBinding {
                        coordinator: coordinator.clone(),
                        bound_grant: bound.clone(),
                    }
                }),
            }),
        };
        let facade = CliMcpToolFacade::try_new(
            coordinator.clone(),
            invoker,
            projection,
            Arc::new(CliMcpNoopProgressSink),
            crate::cli_runtime::mcp::limits::CliMcpFacadeLimits::default(),
            match kind {
                CLIAgentRuntimeKind::Codex => CliMcpCallContract::Codex,
                CLIAgentRuntimeKind::Claude => CliMcpCallContract::Claude,
                CLIAgentRuntimeKind::OpenCode => CliMcpCallContract::OpenCode,
            },
        )
        .unwrap();
        let mut context = CliMcpFacadeRequestContext {
            bound_grant: bound,
            projection_generation: reservation.generation,
            activation_generation: None,
        };
        let initialized = facade.handle_message(&context, json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"fixture","version":"1"}}})).await.unwrap();
        assert!(initialized.get("error").is_none(), "{initialized}");
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
        assert!(listed.get("error").is_none(), "{listed}");
        let active = coordinator
            .reserve_turn(
                &context.bound_grant.grant_ref(),
                context.projection_generation,
                &binding.thread_id,
                turn,
            )
            .await
            .unwrap();
        let native_thread = binding.native_thread_id;
        let native_turn = binding.native_turn_id.unwrap();
        coordinator
            .activate_turn(
                &context.bound_grant,
                active.activation_generation,
                native_thread.clone(),
                native_turn.clone(),
            )
            .await
            .unwrap();
        context.activation_generation = Some(active.activation_generation);
        Self {
            facade,
            context,
            native_items,
            claude_items,
            kind,
            native_thread,
            native_turn,
        }
    }

    async fn call(&self, rpc: &str, item: &str, name: &str, arguments: JsonValue) -> JsonValue {
        let response = self.call_response(rpc, item, name, arguments).await;
        assert!(response.get("error").is_none(), "{name}: {response}");
        assert_ne!(response["result"]["isError"], true, "{name}: {response}");
        response["result"]["structuredContent"].clone()
    }

    async fn call_response(
        &self,
        rpc: &str,
        item: &str,
        name: &str,
        arguments: JsonValue,
    ) -> JsonValue {
        let fingerprint =
            crate::cli_runtime::codex_mcp::canonical_value_fingerprint(&arguments).unwrap();
        let item_id = match self.kind {
            CLIAgentRuntimeKind::Claude => format!("toolu_{item}"),
            CLIAgentRuntimeKind::OpenCode => format!("assistant:tool:{item}"),
            CLIAgentRuntimeKind::Codex => item.to_owned(),
        };
        if self.kind == CLIAgentRuntimeKind::Claude {
            self.claude_items
                .register(
                    &crate::cli_runtime::claude_mcp::ClaudeNativeMcpItemBinding {
                        native_thread_id: self.native_thread.clone(),
                        native_turn_id: self.native_turn.clone(),
                        native_item_id: item_id,
                        canonical_callable_name: name.to_owned(),
                        arguments_fingerprint: fingerprint,
                        metadata: pioneer_protocol::ToolMetadata::empty(),
                    },
                )
                .unwrap();
        } else {
            self.native_items
                .register(
                    crate::cli_runtime::mcp::native_correlation::NativeMcpItemBinding {
                        native_thread_id: self.native_thread.clone(),
                        native_turn_id: self.native_turn.clone(),
                        native_item_id: item_id,
                        canonical_callable_name: name.to_owned(),
                        arguments_fingerprint: fingerprint,
                    },
                )
                .unwrap();
        }
        let (wire, meta) = match self.kind {
            CLIAgentRuntimeKind::Codex => (
                arguments,
                json!({"callId":item,"threadId":self.native_thread,"x-codex-turn-metadata":{"thread_id":self.native_thread,"turn_id":self.native_turn}}),
            ),
            CLIAgentRuntimeKind::Claude => (
                arguments,
                json!({"claudecode/toolUseId":format!("toolu_{item}")}),
            ),
            CLIAgentRuntimeKind::OpenCode => (
                json!({"__pioneer_mcp":{"sessionID":self.native_thread,"messageID":"assistant","id":item},"arguments":arguments}),
                json!({"ai.opencode/sessionID":self.native_thread}),
            ),
        };
        let (response, diagnostics) = crate::public_error::test_support::capture_events_async(
            self.facade.handle_message(&self.context, json!({"jsonrpc":"2.0","id":rpc,"method":"tools/call","params":{"name":name,"arguments":wire,"_meta":meta}})),
        ).await;
        response.unwrap_or_else(|| panic!("{name}: no response; diagnostics: {diagnostics:#?}"))
    }
}

#[test]
fn cli_threads_read_errors_keep_safe_reasons_through_the_admitted_facade() {
    run_standard_stack_message_test("Threads CLI safe read errors", async {
        fn assert_read_error(response: &JsonValue, message: &str, reason: &str) {
            assert!(response.get("error").is_none(), "{response}");
            let result = &response["result"];
            assert_eq!(result["isError"], true, "{response}");
            // Exact public payloads exclude source IDs, raw arguments, paths,
            // SQL and ToolError diagnostics from both MCP representations.
            let expected = json!({"error":message});
            assert_eq!(result["structuredContent"], expected);
            assert_eq!(
                result["content"],
                json!([{"type":"text","text":expected.to_string()}])
            );
            assert_eq!(result["_meta"]["pioneer"]["reasonCode"], reason);
            let wire = response.to_string();
            for private in [
                "private_token",
                "/private/source.sqlite",
                "secret-diagnostic",
                "unavailable-private-source",
                "private fragment title",
                "old-workspace-revision",
            ] {
                assert!(!wire.contains(private), "private read diagnostic leaked");
            }
        }

        let (harness, processor, turn) = start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let binding = processor
            .crud_store
            .get_cli_runtime_turn_binding(&turn)
            .await
            .unwrap()
            .unwrap();
        let thread = binding.thread_id;
        let facade = ThreadsFacade::new(&processor, &turn, CLIAgentRuntimeKind::Codex).await;
        // Admission, immutable projection, exact native correlation and Gateway
        // dispatch are the same path as the positive facade regression below.
        let context = pioneer_agent::TurnToolContext {
            workspace_id: harness.workspace_id.clone(),
            thread_id: thread.clone(),
            turn_id: turn.clone(),
        };
        let authority = processor.thread_read_authority(&context).await.unwrap();
        let codec = processor.thread_read_cursor_codec().unwrap();
        // An authentic pre-change cursor must not be interpreted using the new
        // order, even when issued to this same principal/execution and limit.
        let legacy_cursor = codec
            .encode(
                &json!({"revision":"old-workspace-revision","after":thread,"fragment":null}),
                &authority.scope("list", "", false, 1),
            )
            .unwrap();
        let incompatible =
            "invalid or incompatible cursor; restart reading with the same resource and parameters";
        for (item, cursor) in [
            ("legacy-list", legacy_cursor),
            (
                "invalid-list",
                "SELECT private_token FROM /private/source.sqlite; secret-diagnostic".into(),
            ),
        ] {
            let response = facade
                .call_response(
                    item,
                    item,
                    "threads_list",
                    json!({"limit":1,"cursor":cursor}),
                )
                .await;
            assert_read_error(&response, incompatible, "read_cursor_incompatible");
        }

        // These arguments satisfy the frozen JSON schema. Unanchored newer
        // history is rejected by the actual read handler's semantic validation,
        // after admission rather than by JSON-RPC or a mock executor.
        let invalid = facade
            .call_response(
                "invalid-history",
                "invalid-history",
                "threads_history_read",
                json!({"threadId":thread,"limit":1,"direction":"newer"}),
            )
            .await;
        assert_read_error(
            &invalid,
            "invalid read parameters",
            "read_invalid_parameters",
        );
        for (item, name, arguments) in [
            (
                "unavailable-history",
                "threads_history_read",
                json!({"threadId":"unavailable-private-source","limit":1}),
            ),
            (
                "unavailable-children",
                "threads_children_list",
                json!({"threadId":"unavailable-private-source","recursive":false,"limit":1}),
            ),
        ] {
            let response = facade.call_response(item, item, name, arguments).await;
            assert_read_error(
                &response,
                "source unavailable or access denied",
                "read_source_unavailable",
            );
        }

        processor
            .crud_store
            .update_thread_name(&thread, &"private fragment title 🌍\"\n".repeat(150_000))
            .await
            .unwrap();
        let first = facade
            .call(
                "fragment-first",
                "fragment-first",
                "threads_list",
                json!({"limit":1}),
            )
            .await;
        assert_eq!(first["threads"][0]["threadId"], thread);
        assert_eq!(first["threads"][0]["fragment"]["offset"], 0);
        assert_eq!(first["threads"][0]["fragment"]["eof"], false);
        let cursor = first["nextCursor"].as_str().unwrap();
        processor
            .crud_store
            .update_thread_name(&thread, "changed private fragment title")
            .await
            .unwrap();
        let changed = facade
            .call_response(
                "fragment-changed",
                "fragment-changed",
                "threads_list",
                json!({"limit":1,"cursor":cursor}),
            )
            .await;
        assert_read_error(
            &changed,
            "cursor is stale; restart reading from the first page",
            "read_source_changed",
        );

        let malformed = facade
            .facade
            .handle_bytes(&facade.context, b"{")
            .await
            .unwrap();
        let malformed: JsonValue = serde_json::from_slice(&malformed).unwrap();
        assert_eq!(malformed["error"]["code"], -32700);
        assert!(malformed.get("result").is_none());
    });
}

#[test]
fn cli_threads_actions_use_facade_shared_handlers_routes_and_replay_for_all_providers() {
    run_standard_stack_message_test("Threads CLI integration", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            let (harness, processor, turn) = start_task_cli_fixture_with_models(
                kind,
                pioneer_tasks::TaskRuntimeConfig::default(),
                &["threads-cli-choice"],
            )
            .await;
            let bindings = processor
                .crud_store
                .list_turn_mcp_bindings(&turn)
                .await
                .unwrap();
            assert_eq!(
                bindings
                    .iter()
                    .filter(|tool| tool.raw_tool_name == "threads_start_options")
                    .count(),
                1
            );
            let authority = processor
                .load_turn_execution_authorization_context(&turn)
                .await
                .unwrap();
            let permission =
                pioneer_protocol::task_permission_cap_snapshot(authority.permission_profile_cap());
            assert!(permission.effective_policy.allowed_tools.is_empty());
            assert!(!permission.effective_policy.allowed_tools_restricted);
            assert!(
                authority
                    .grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
            );
            for tool in bindings.iter().filter(|tool| {
                pioneer_tools::THREADS_DOMAIN_TOOL_NAMES.contains(&tool.raw_tool_name.as_str())
            }) {
                let expected = if kind == CLIAgentRuntimeKind::OpenCode {
                    format!("pioneer_{}", tool.raw_tool_name)
                } else {
                    format!("mcp__pioneer__{}", tool.raw_tool_name)
                };
                assert_eq!(tool.provider_callable_name, expected);
                assert!(
                    crate::turn_mcp::native_tools::current_identity(&harness.workspace_id, tool)
                        .is_ok()
                );
            }
            let facade = ThreadsFacade::new(&processor, &turn, kind).await;
            let options = facade
                .call("options", "options", "threads_start_options", json!({}))
                .await;
            let creation = options["threadCreationOptions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|option| option["label"] == "Current collaboration")
                .unwrap()["id"]
                .clone();
            let create_args = json!({"optionId":creation});
            let created = facade
                .call("create", "create", "thread_create", create_args.clone())
                .await;
            assert_eq!(created["status"], "accepted");
            let replay = ThreadsFacade::new(&processor, &turn, kind)
                .await
                .call("create-reconnected", "create", "thread_create", create_args)
                .await;
            assert_eq!(replay["receiptId"], created["receiptId"]);
            let target = created["targetOptions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|option| {
                    !options["targetOptions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|old| old["id"] == option["id"])
                })
                .unwrap()["id"]
                .clone();
            let send_args = json!({"targetOptionId":target,"input":[{"type":"text","text":"Facade message without model execution"}]});
            let sent = facade
                .call("send", "send", "thread_message_send", send_args.clone())
                .await;
            let replay = ThreadsFacade::new(&processor, &turn, kind)
                .await
                .call("send-reconnected", "send", "thread_message_send", send_args)
                .await;
            assert_eq!(sent["receiptId"], replay["receiptId"]);
            let listed = facade
                .call("read", "read", "threads_list", json!({"limit":100}))
                .await;
            let children = listed["threads"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|thread| {
                    thread["parentThreadId"] == format!("cli_tasks_{kind:?}").to_lowercase()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                children.len(),
                1,
                "replayed creation must not add another thread"
            );
            let child = children[0]["threadId"].clone();
            let history = facade
                .call(
                    "history",
                    "history",
                    "threads_history_read",
                    json!({"threadId":child,"limit":100}),
                )
                .await;
            assert_eq!(
                history
                    .to_string()
                    .matches("Facade message without model execution")
                    .count(),
                1
            );
            let identity = options["identities"]
                .as_array()
                .unwrap()
                .iter()
                .find(|identity| identity["sourceKind"] == "cli_runtime_instance")
                .unwrap()["id"]
                .clone();
            let models = facade
                .call(
                    "models",
                    "models",
                    "threads_identity_models_list",
                    json!({"identityId":identity}),
                )
                .await;
            let executor = models["executors"]
                .as_array()
                .unwrap()
                .iter()
                .find(|executor| {
                    executor["providerId"] == format!("cli_runtime:{}", harness.runtime_id)
                })
                .unwrap();
            let model = executor["models"]
                .as_array()
                .unwrap()
                .iter()
                .find(|model| model["id"] == "threads-cli-choice")
                .unwrap();
            let effort = model["reasoning"]["effort_options"].as_array().unwrap()[0].clone();
            let args = json!({"targetOptionId":target,"input":[{"type":"text","text":"Report findings in this conversation"}],"launch":{"identity":{"kind":"exact","id":identity},"profile":{"kind":"model","providerId":executor["providerId"],"modelId":model["id"]},"reasoning":{"effort":effort}}});
            let started = facade
                .call("start", "start", "threads_turn_start", args.clone())
                .await;
            let replay = ThreadsFacade::new(&processor, &turn, kind)
                .await
                .call("start-reconnected", "start", "threads_turn_start", args)
                .await;
            assert_eq!(started["receiptId"], replay["receiptId"]);
            let native_start_item = match kind {
                CLIAgentRuntimeKind::Codex => "start".to_owned(),
                CLIAgentRuntimeKind::Claude => "toolu_start".to_owned(),
                CLIAgentRuntimeKind::OpenCode => "assistant:tool:start".to_owned(),
            };
            let logical_call = crate::turn_mcp::native_tools::cli_task_call_id(
                &turn,
                "threads_turn_start",
                &native_start_item,
                &json!({}),
            );
            let action = crate::authorization::action_id_for_call(&logical_call);
            let outbox = pioneer_crud::load_agent_action_outbox(
                &processor.crud_store.database_connection(),
                action.as_str(),
            )
            .await
            .unwrap()
            .unwrap();
            let payload: JsonValue = serde_json::from_str(&outbox.payload_json).unwrap();
            let params: pioneer_protocol::TurnStartParams =
                serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
            assert_eq!(params.model.as_deref(), model["id"].as_str());
            assert_eq!(
                params.model_provider.as_deref(),
                executor["providerId"].as_str()
            );
            assert_eq!(
                params.reasoning.as_ref().unwrap().effort,
                effort.as_str().unwrap()
            );
            assert_eq!(payload["dispatch"]["identity"]["id"], identity);
            harness
                .cli_session
                .set_next_native_turn_id("threads-facade-explicit-child")
                .await;
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
            let calls = wait_for_cli_runtime_turn_starts(&harness.cli_session, 2).await;
            assert_eq!(calls[1].model.as_deref(), model["id"].as_str());
            assert_eq!(calls[1].effort.as_deref(), effort.as_str());
            // One replayed start has one persisted child execution and payload.
            assert_eq!(replay["outcome"], "agent_action_already_committed");
        }
        let (_harness, processor, turn) =
            start_threads_cli_fixture(CLIAgentRuntimeKind::Codex, &["threads_search"]).await;
        assert!(
            !processor
                .crud_store
                .list_turn_mcp_bindings(&turn)
                .await
                .unwrap()
                .iter()
                .any(|binding| binding.raw_tool_name == "threads_search")
        );
        let denied = ThreadsFacade::new(&processor, &turn, CLIAgentRuntimeKind::Codex)
            .await
            .call_response(
                "denied-search",
                "denied-search",
                "threads_search",
                json!({"query":"test","limit":1}),
            )
            .await;
        assert_eq!(denied["error"]["data"]["kind"], "tool_unbound");
        let (_harness, processor, turn) =
            start_threads_cli_fixture(CLIAgentRuntimeKind::Codex, &["threads_start_options"]).await;
        let bindings = processor
            .crud_store
            .list_turn_mcp_bindings(&turn)
            .await
            .unwrap();
        assert!(
            !bindings
                .iter()
                .any(|tool| tool.raw_tool_name == "threads_start_options")
        );
        assert!(
            bindings
                .iter()
                .any(|tool| tool.raw_tool_name == "threads_list")
        );
        let facade = ThreadsFacade::new(&processor, &turn, CLIAgentRuntimeKind::Codex).await;
        let denied = facade.facade.handle_message(&facade.context, json!({"jsonrpc":"2.0","id":"denied-options","method":"tools/call","params":{"name":"threads_start_options","arguments":{}}})).await.unwrap();
        assert_eq!(denied["error"]["data"]["kind"], "tool_unbound");
    });
}

#[test]
fn cli_threads_permission_matrix_preserves_restricted_names_denies_and_missing_grants() {
    run_standard_stack_message_test("CLI Threads permission ceiling", async {
        for (restricted, allowed, denied, expect_create, expect_models) in [
            (false, vec![], vec![], true, true),
            (true, vec![], vec![], false, false),
            (false, vec!["thread_create"], vec![], true, false),
            (
                false,
                vec!["thread_create"],
                vec!["thread_create"],
                false,
                false,
            ),
        ] {
            let mut profile = default_test_permission_profile();
            profile.effective_policy.allowed_tools_restricted = restricted;
            profile.effective_policy.allowed_tools =
                allowed.into_iter().map(str::to_owned).collect();
            profile.effective_policy.denied_tools = denied.into_iter().map(str::to_owned).collect();
            let (_harness, processor, turn) =
                start_threads_cli_fixture_with_policy(CLIAgentRuntimeKind::Codex, profile).await;
            let bindings = processor
                .crud_store
                .list_turn_mcp_bindings(&turn)
                .await
                .unwrap();
            assert_eq!(
                bindings
                    .iter()
                    .any(|binding| binding.raw_tool_name == "thread_create"),
                expect_create
            );
            assert_eq!(
                bindings
                    .iter()
                    .any(|binding| binding.raw_tool_name == "threads_identity_models_list"),
                expect_models
            );
            assert!(
                bindings
                    .iter()
                    .filter(|binding| binding.raw_tool_name == "threads_start_options")
                    .count()
                    <= 1
            );
        }
        let (harness, processor, turn) = start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
        let facade = ThreadsFacade::new(&processor, &turn, CLIAgentRuntimeKind::Codex).await;
        let options = facade
            .call(
                "old-options",
                "old-options",
                "threads_start_options",
                json!({}),
            )
            .await;
        let option = options["threadCreationOptions"][0]["id"].clone();
        let original_binding = processor.agent_action_binding(&turn).await.unwrap();
        let mut limited_binding = original_binding.clone();
        limited_binding
            .capabilities
            .remove(&pioneer_protocol::AgentToolCapability::ThreadCreate);
        processor
            .register_agent_action_binding(turn.clone(), limited_binding)
            .await;
        let limited = facade
            .call_response(
                "limited-create",
                "limited-create",
                "thread_create",
                json!({"optionId":option}),
            )
            .await;
        assert!(
            limited.get("error").is_some() || limited["result"]["isError"] == true,
            "{limited}"
        );
        assert_ne!(
            limited["error"]["data"]["kind"], "invalid_request",
            "correlation must reach the restricted binding"
        );
        processor
            .register_agent_action_binding(turn.clone(), original_binding)
            .await;
        // Model an older immutable admission with no creation actions, retaining
        // the already-frozen transport catalog. Broader current user rights
        // cannot substitute for either missing action grant.
        let authority = processor
            .load_turn_execution_authorization_context(&turn)
            .await
            .unwrap();
        let mut encoded: JsonValue =
            serde_json::from_str(&authority.to_persisted_json().unwrap()).unwrap();
        let private = crate::authorization::ResourceAction::ThreadCreatePrivate.safe_name();
        let workspace = crate::authorization::ResourceAction::ThreadCreateWorkspace.safe_name();
        encoded["grant_manifest"]["actions"]
            .as_array_mut()
            .unwrap()
            .retain(|value| value.as_str() != Some(private) && value.as_str() != Some(workspace));
        processor
            .crud_store
            .set_turn_execution_authorization_context(&turn, &encoded.to_string())
            .await
            .unwrap();
        let reply = facade
            .call_response(
                "old-create",
                "old-create",
                "thread_create",
                json!({"optionId":option}),
            )
            .await;
        assert_eq!(
            reply["error"]["data"]["kind"], "permission_denied",
            "{reply}"
        );
        let restored = processor
            .load_turn_execution_authorization_context(&turn)
            .await
            .unwrap();
        assert!(!restored.grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate));
        assert!(
            !restored.grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
        );
        // A new Task must not restore creation rights absent from the source
        // execution, despite Full Access and the initiator's current role.
        harness
            .cli_session
            .set_next_native_turn_id("old-manifest-task-child")
            .await;
        let task = facade
            .call(
                "old-task-create",
                "old-task-create",
                "task_create",
                json!({"title":"Retain the source ceiling","goal":"Inspect without creating conversations"}),
            )
            .await;
        let admission = processor
            .crud_store
            .get_task_execution_admission(task["taskId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        let admission = crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
            &admission.authorization_context_json,
        )
        .unwrap();
        let child =
            wait_for_task_cli_child(&processor, task["runId"].as_str().unwrap(), None).await;
        let child_authority = processor
            .load_turn_execution_authorization_context(&child.turn_id)
            .await
            .unwrap();
        for context in [&admission, &child_authority] {
            assert!(
                !context.grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate)
            );
            assert!(
                !context.grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
            );
        }
        assert!(
            processor
                .crud_store
                .list_turn_mcp_bindings(&child.turn_id)
                .await
                .unwrap()
                .iter()
                .all(|binding| binding.raw_tool_name != "thread_create")
        );
        let child_facade =
            ThreadsFacade::new(&processor, &child.turn_id, CLIAgentRuntimeKind::Codex).await;
        let child_options = child_facade
            .call(
                "old-child-options",
                "old-child-options",
                "threads_start_options",
                json!({}),
            )
            .await;
        assert!(
            child_options["threadCreationOptions"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let denied = child_facade
            .call_response(
                "old-child-create",
                "old-child-create",
                "thread_create",
                json!({"optionId":option}),
            )
            .await;
        assert!(
            denied.get("error").is_some() || denied["result"]["isError"] == true,
            "{denied}"
        );
    });
}

#[test]
fn task_resume_preserves_persisted_admission_but_constrains_a_new_legacy_seed() {
    run_standard_stack_message_test("immutable Task readmission ceiling", async {
        for persisted in [true, false] {
            let (harness, processor, turn) =
                start_task_cli_fixture(CLIAgentRuntimeKind::Codex).await;
            harness
                .cli_session
                .set_next_native_turn_id("immutable-resume-child")
                .await;
            let facade = ThreadsFacade::new(&processor, &turn, CLIAgentRuntimeKind::Codex).await;
            let created = facade
                .call(
                    "resume-create",
                    "resume-create",
                    "task_create",
                    json!({"title":"Preserve admitted Task authority","goal":"Readmitted work"}),
                )
                .await;
            let task_id = created["taskId"].as_str().unwrap();
            let child =
                wait_for_task_cli_child(&processor, created["runId"].as_str().unwrap(), None).await;
            let saved = processor
                .crud_store
                .get_task_execution_admission(task_id)
                .await
                .unwrap()
                .unwrap();
            let original =
                crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
                    &saved.authorization_context_json,
                )
                .unwrap();
            assert!(
                original.grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
            );
            // The running binding is persisted before startup releases its
            // session gate. Wait for that handoff and hold the gate while blocking.
            let transition = timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(transition) = processor
                        .try_cli_runtime_turn_transition(&child)
                        .await
                        .unwrap()
                    {
                        break transition;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("Task child must release its startup transition");
            assert!(
                processor
                    .mark_turn_blocked_with_transition(
                        child.thread_id.clone(),
                        child.turn_id.clone(),
                        "resumable Task".into(),
                        Some(transition),
                    )
                    .await
            );
            processor
                .reconcile_terminal_task_child_turns(64)
                .await
                .unwrap();
            assert_eq!(
                processor
                    .crud_store
                    .get_task(task_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .status,
                pioneer_protocol::TaskStatus::Blocked
            );
            // A different, still-running execution in the same capsule may
            // manage A without lending its own create rights to a new admission.
            // Model the restricted source on the principal-authored root fixture
            // before task_create derives the sibling admission and execution graph.
            let source = processor
                .load_turn_execution_authorization_context(&turn)
                .await
                .unwrap();
            let mut encoded: JsonValue =
                serde_json::from_str(&source.to_persisted_json().unwrap()).unwrap();
            encoded["grant_manifest"]["actions"]
                .as_array_mut()
                .unwrap()
                .retain(|action| {
                    action.as_str()
                        != Some(
                            crate::authorization::ResourceAction::ThreadCreatePrivate.safe_name(),
                        )
                        && action.as_str()
                            != Some(
                                crate::authorization::ResourceAction::ThreadCreateWorkspace
                                    .safe_name(),
                            )
                });
            processor
                .crud_store
                .set_turn_execution_authorization_context(&turn, &encoded.to_string())
                .await
                .unwrap();
            harness
                .cli_session
                .set_next_native_turn_id("immutable-resume-caller")
                .await;
            let sibling = facade.call("resume-caller-create", "resume-caller-create", "task_create",
                json!({"title":"Authorized Task controller","goal":"Manage another permitted Task"}),
            ).await;
            let caller =
                wait_for_task_cli_child(&processor, sibling["runId"].as_str().unwrap(), None).await;
            assert_ne!(caller.turn_id, turn);
            // The normal loader checks the child's graph fingerprint and lineage.
            // Never rewrite an already-pinned Task child's authority envelope.
            let caller_authority = processor
                .load_turn_execution_authorization_context(&caller.turn_id)
                .await
                .unwrap();
            assert!(
                !caller_authority
                    .grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate)
            );
            assert!(
                !caller_authority
                    .grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
            );
            assert_eq!(caller_authority.root_thread_id(), saved.root_thread_id);
            if !persisted {
                // Model the supported legacy case with no immutable admission;
                // source ceiling applies when the real readmission path creates it.
                pioneer_entity::task_execution_admission::Entity::delete_by_id(task_id)
                    .exec(&processor.crud_store.database_connection())
                    .await
                    .unwrap();
            }
            let caller_facade =
                ThreadsFacade::new(&processor, &caller.turn_id, CLIAgentRuntimeKind::Codex).await;
            let resumed = caller_facade
                .call(
                    "resume-frozen",
                    "resume-frozen",
                    "task_resume",
                    json!({"taskId":task_id}),
                )
                .await;
            assert_eq!(resumed["task"]["id"], task_id, "{resumed}");
            assert_ne!(
                processor
                    .crud_store
                    .get_task(task_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .task
                    .status,
                pioneer_protocol::TaskStatus::Blocked
            );
            let current = processor
                .crud_store
                .get_task_execution_admission(task_id)
                .await
                .unwrap()
                .unwrap();
            if persisted {
                assert_eq!(
                    current, saved,
                    "existing immutable admission is reused verbatim"
                );
            } else {
                let legacy =
                    crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
                        &current.authorization_context_json,
                    )
                    .unwrap();
                assert!(
                    !legacy
                        .grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate)
                );
                assert!(
                    !legacy
                        .grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
                );
            }
            let caller = processor
                .load_turn_execution_authorization_context(&caller.turn_id)
                .await
                .unwrap();
            assert!(
                !caller.grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate)
            );
            assert!(
                !caller.grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace)
            );
        }
    });
}

#[test]
fn ordinary_cli_delegated_root_dispatch_uses_cli_attempt_and_replays_without_native_actor() {
    run_standard_stack_message_test("CLI Threads first root dispatch", async {
        for kind in [
            CLIAgentRuntimeKind::Codex,
            CLIAgentRuntimeKind::Claude,
            CLIAgentRuntimeKind::OpenCode,
        ] {
            for proof_fault in ["none", "lease", "context", "catalog"] {
                let (harness, processor, turn) = start_task_cli_fixture(kind).await;
                let facade = ThreadsFacade::new(&processor, &turn, kind).await;
                let options = facade
                    .call(
                        "root-options",
                        "root-options",
                        "threads_start_options",
                        json!({}),
                    )
                    .await;
                let creation = options["threadCreationOptions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|option| option["label"] == "New delegated root")
                    .unwrap()["id"]
                    .clone();
                let created = facade
                    .call(
                        "root-create",
                        "root-create",
                        "thread_create",
                        json!({"optionId":creation}),
                    )
                    .await;
                let target = created["targetOptions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|option| {
                        !options["targetOptions"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|old| old["id"] == option["id"])
                    })
                    .unwrap()["id"]
                    .clone();
                let started = facade.call("root-start", "root-start", "threads_turn_start", json!({
                "targetOptionId":target, "input":[{"type":"text","text":"Ordinary CLI root execution"}],
            })).await;
                let db = processor.crud_store.database_connection();
                let outbox = pioneer_entity::agent_action_outbox::Entity::find_by_id(
                    started["outboxId"].as_str().unwrap(),
                )
                .one(&db)
                .await
                .unwrap()
                .unwrap();
                let payload: JsonValue = serde_json::from_str(&outbox.payload_json).unwrap();
                let params: pioneer_protocol::TurnStartParams =
                    serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
                let execution = pioneer_crud::load_agent_execution(
                    &db,
                    payload["spawned_execution_id"].as_str().unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(execution.parent_execution_id.is_none());
                assert_eq!(execution.work_graph_root_execution_id, execution.id);
                assert_eq!(execution.home_root_thread_id, params.thread_id);
                assert!(
                    processor
                        .crud_store
                        .get_task_run_turn_by_turn(&params.thread_id, &params.turn_id)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(matches!(
                    processor
                        .agent_manager
                        .observe_turn(&params.thread_id, &params.turn_id)
                        .await,
                    Err(pioneer_agent::AgentControlError::ThreadNotFound)
                ));
                harness
                    .cli_session
                    .set_next_native_turn_id("threads-independent-cli")
                    .await;
                if proof_fault != "none" {
                    processor.arm_completed_history_preparation_barrier(
                        if proof_fault == "lease" {
                            "__cli_agent_before_lease_proof__"
                        } else if proof_fault == "catalog" {
                            "__cli_agent_catalog_before_task_proof__"
                        } else {
                            "__cli_agent_before_context_proof__"
                        },
                    );
                    let dispatch = tokio::spawn({
                        let processor = processor.clone();
                        async move {
                            crate::message::agent_action_tools::process_due_agent_action_outbox(
                                &processor, 64,
                            )
                            .await
                            .unwrap()
                        }
                    });
                    timeout(
                        Duration::from_secs(10),
                        processor.wait_for_completed_history_preparation_barrier(),
                    )
                    .await
                    .unwrap();
                    db.execute_unprepared("ALTER TABLE turn RENAME TO unavailable_turn")
                        .await
                        .unwrap();
                    if proof_fault == "catalog" {
                        processor.arm_completed_history_preparation_barrier(
                            "__cli_agent_catalog_proof_failed__",
                        );
                    }
                    processor.release_completed_history_preparation_barrier();
                    if proof_fault == "catalog" {
                        timeout(
                            Duration::from_secs(10),
                            processor.wait_for_completed_history_preparation_barrier(),
                        )
                        .await
                        .unwrap();
                        // Restore before the finalizer caller decides whether to
                        // write Blocked: terminal persistence is now available.
                        db.execute_unprepared("ALTER TABLE unavailable_turn RENAME TO turn")
                            .await
                            .unwrap();
                        processor.release_completed_history_preparation_barrier();
                    }
                    dispatch.await.unwrap();
                    if proof_fault != "catalog" {
                        db.execute_unprepared("ALTER TABLE unavailable_turn RENAME TO turn")
                            .await
                            .unwrap();
                    }
                    assert_eq!(
                        processor
                            .crud_store
                            .get_turn(&params.thread_id, &params.turn_id)
                            .await
                            .unwrap()
                            .unwrap()
                            .1
                            .status,
                        pioneer_protocol::TurnStatus::InProgress
                    );
                    assert!(
                        !processor
                            .crud_store
                            .native_turn_has_accepted_terminal(&params.turn_id)
                            .await
                            .unwrap()
                    );
                    assert!(
                        processor
                            .crud_store
                            .latest_cli_runtime_turn_attempt(&params.turn_id)
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert_eq!(harness.cli_session.turn_starts.lock().await.len(), 1);
                    use sea_orm::{ActiveModelTrait, Set};
                    let failed =
                        pioneer_entity::agent_action_outbox::Entity::find_by_id(&outbox.id)
                            .one(&db)
                            .await
                            .unwrap()
                            .unwrap();
                    assert_eq!(
                        failed.last_error.as_deref(),
                        Some("outbox_storage_unavailable")
                    );
                    let mut retry: pioneer_entity::agent_action_outbox::ActiveModel = failed.into();
                    retry.next_attempt_at =
                        Set(Some(pioneer_crud::utc_now() - chrono::Duration::seconds(1)));
                    retry.update(&db).await.unwrap();
                }
                crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                    .await
                    .unwrap();
                wait_for_recorded_cli_runtime_turn_count(&harness.cli_session, 2).await;
                wait_for_task_cli_activation(&processor, &params.turn_id).await;
                assert!(
                    processor
                        .crud_store
                        .latest_cli_runtime_turn_attempt(&params.turn_id)
                        .await
                        .unwrap()
                        .is_some()
                );
                assert!(matches!(
                    processor
                        .agent_manager
                        .observe_turn(&params.thread_id, &params.turn_id)
                        .await,
                    Err(pioneer_agent::AgentControlError::ThreadNotFound)
                ));
                // Lose the dispatch ACK, then replay the exact durable operation.
                use sea_orm::{ActiveModelTrait, Set};
                let mut pending: pioneer_entity::agent_action_outbox::ActiveModel =
                    pioneer_entity::agent_action_outbox::Entity::find_by_id(&outbox.id)
                        .one(&db)
                        .await
                        .unwrap()
                        .unwrap()
                        .into();
                pending.status = Set("pending".into());
                pending.delivered_at = Set(None);
                pending.next_attempt_at = Set(None);
                pending.update(&db).await.unwrap();
                crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                    .await
                    .unwrap();
                assert_eq!(harness.cli_session.turn_starts.lock().await.len(), 2);
            }
        }
    });
}
