//! Execution-bound thread actions: fixture catalogs never start real providers.
use super::*;
use pioneer_agent::TurnToolContext;
use pioneer_protocol::{AgentToolLaunchSelection, AgentToolProfileChoice, TurnStartParams};
use pioneer_tools::{ToolEventBus, ToolInvocation, ToolPayload};
use std::path::PathBuf;

struct ChoiceModelsProvider(PromptParityCaptureProvider);
#[async_trait::async_trait]
impl Provider for ChoiceModelsProvider {
    fn name(&self) -> &str {
        "openai"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        self.0.capabilities()
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        self.0.chat(request).await
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        self.0.stream_chat(request).await
    }
    async fn list_models(&self) -> anyhow::Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        let mut models = self.0.list_models().await?;
        let mut no_effort = models[0].clone();
        no_effort.id = "catalog-no-effort".into();
        no_effort.name = Some("Model without effort controls".into());
        no_effort.capabilities.reasoning =
            Some(pioneer_protocol::ProviderModelReasoningCapabilities {
                native: Default::default(),
                supported: Some(false),
                effort_options: vec![],
                default_effort: None,
                mandatory: None,
                supports_token_budget: None,
                source: Some(pioneer_protocol::ReasoningCapabilitySource::StaticRegistry),
            });
        no_effort.capabilities.thinking = Some(false);
        models.push(no_effort);
        let mut other = models[0].clone();
        // Discovery cannot give an unknown OpenAI family an effort contract.
        other.id = "gpt-5.4-catalog-other".into();
        other.name = Some("Another selectable model".into());
        models.push(other);
        Ok(models)
    }
}

async fn fixture() -> (
    Arc<MessageProcessor>,
    TurnToolContext,
    pioneer_agent::TurnToolMaterialization,
) {
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        Arc::new(ChoiceModelsProvider(
            PromptParityCaptureProvider::with_visible_tools(vec![]),
        )),
    ));
    let secrets = test_gateway_secrets();
    secrets
        .set_workspace_provider_api_key(&workspace, "openai", "fixture-secret")
        .unwrap();
    let processor = Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("composer-model", "openai")),
        registry,
        Arc::new(SessionManager::new()),
        workspaces,
        store,
        secrets,
        test_summary_config(),
        test_tool_loop_config(),
    ));
    let context = TurnToolContext {
        workspace_id: workspace.clone(),
        thread_id: "choices-root".into(),
        turn_id: "choices-parent".into(),
    };
    ensure_test_superuser_execution_turn(
        &processor,
        &workspace,
        &context.thread_id,
        &context.turn_id,
    )
    .await;
    let mut authority = processor
        .load_turn_execution_authorization_context(&context.turn_id)
        .await
        .unwrap();
    authority.bind_test_provider_authority(
        "openai",
        "gpt-5.4",
        processor
            .provider_registry
            .authority_fingerprint_for_workspace(&workspace, "openai")
            .unwrap()
            .as_str(),
    );
    processor
        .crud_store
        .set_turn_execution_authorization_context(
            &context.turn_id,
            &authority.to_persisted_json().unwrap(),
        )
        .await
        .unwrap();
    processor
        .crud_store
        .set_turn_execution_security_snapshot(
            &context.turn_id,
            &pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
                "/tmp/project",
                pioneer_crud::utc_now().timestamp_millis(),
            ),
        )
        .await
        .unwrap();
    let now = pioneer_crud::utc_now();
    processor
        .crud_store
        .upsert_turn_runtime_snapshot(pioneer_crud::NewTurnRuntimeSnapshot {
            turn_id: context.turn_id.clone(),
            thread_id: context.thread_id.clone(),
            workspace_id: workspace,
            mode_json: serde_json::to_string(&ThreadMode::Agent).unwrap(),
            model: "gpt-5.4".into(),
            provider_name: "openai".into(),
            reasoning_effort: Some("high".into()),
            agent_skill_versions_json: None,
            hook_runtime_context_json: serde_json::to_string(
                &pioneer_agent::AgentTurnHookRuntimeContext::default(),
            )
            .unwrap(),
            workspace_skill_policies_json: "[]".into(),
            input_json: "[]".into(),
            capabilities_json: "[]".into(),
            resolved_artifacts_json: "[]".into(),
            runtime_environment_json: "{}".into(),
            history_json: "[]".into(),
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let tools = processor
        .materialize_agent_action_tools(context.clone())
        .await
        .unwrap();
    (processor, context, tools)
}

pub(super) async fn call(
    tools: &pioneer_agent::TurnToolMaterialization,
    name: &str,
    id: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    let handler = tools
        .bundles
        .iter()
        .flat_map(|bundle| &bundle.handlers)
        .find(|(tool, _)| tool == name)
        .unwrap()
        .1
        .clone();
    let output = handler
        .handle(
            ToolInvocation {
                call_id: id.into(),
                tool_name: name.into(),
                source: pioneer_tools::ToolCallSource::Model,
                payload: ToolPayload::Function { arguments },
                workdir: PathBuf::new(),
                environment: Default::default(),
                attempt_id: 0,
                idempotency_key: None,
                recovery: Default::default(),
                permission_metadata: Default::default(),
                execution_security_snapshot: None,
                apply_patch_preflight: None,
                cancellation: tokio_util::sync::CancellationToken::new(),
            },
            ToolEventBus::new(8).start_trace("choices-parent", id, name),
        )
        .await
        .unwrap_or_else(|error| panic!("{name} ({id}) failed: {error}"));
    assert!(output.success(), "{name} ({id}): {}", output.raw_text());
    output.raw_json()
}

#[tokio::test]
async fn inherited_configuration_survives_visible_thread_creation_message_and_start_replay() {
    let (processor, context, tools) = fixture().await;
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let facts = binding.adapter.lock().await.persistence_facts();
    assert_eq!(facts.profile.model_id, "gpt-5.4");
    let inherited = crate::message::agent_action_tools::resolve_tool_launch(
        &processor,
        &context,
        AgentToolLaunchSelection::default(),
        "inherit",
    )
    .await
    .unwrap();
    assert_eq!(inherited.reasoning.as_ref().unwrap().effort, "high");
    assert!(
        matches!(&inherited.profile, AgentToolProfileChoice::Exact { id } if id == &facts.profile.id)
    );
    let mut effort_only = AgentToolLaunchSelection::default();
    effort_only.reasoning = Some(pioneer_protocol::TurnReasoningSelection {
        effort: "low".into(),
    });
    let effort_only = crate::message::agent_action_tools::resolve_tool_launch(
        &processor,
        &context,
        effort_only,
        "effort",
    )
    .await
    .unwrap();
    assert_eq!(effort_only.profile, inherited.profile);
    assert_eq!(effort_only.reasoning.unwrap().effort, "low");
    let option = binding
        .options
        .thread_creation_options
        .iter()
        .find(|option| option.label == "New delegated root")
        .unwrap();
    let created = call(
        &tools,
        "thread_create",
        "create-visible",
        json!({"optionId": option.id}),
    )
    .await;
    let targets: Vec<pioneer_protocol::AgentToolTargetOption> =
        serde_json::from_value(created["targetOptions"].clone()).unwrap();
    let target = targets
        .iter()
        .find(|target| {
            !binding
                .options
                .target_options
                .iter()
                .any(|old| old.id == target.id)
        })
        .unwrap();
    let action = crate::authorization::action_id_for_call("create-visible");
    let db = processor.crud_store.database_connection();
    let outbox = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
        .await
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&outbox.payload_json).unwrap();
    let thread_id = payload["created_thread_id"].as_str().unwrap();
    let thread = processor
        .crud_store
        .get_thread_model(thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(thread.sidebar_visibility, ThreadSidebarVisibility::Visible);
    assert_eq!(
        (
            &*thread.model_provider,
            &*thread.model,
            thread.reasoning_effort.as_deref()
        ),
        ("openai", "gpt-5.4", Some("high"))
    );
    assert!(thread.turns.is_empty());
    let empty_list = processor
        .crud_store
        .list_threads_for_workspace(&context.workspace_id, 100)
        .await
        .unwrap();
    let empty_list = empty_list
        .iter()
        .find(|thread| thread.id == thread_id)
        .unwrap();
    assert_eq!(empty_list.model_provider, "openai");
    assert_eq!(empty_list.model, "gpt-5.4");
    assert_eq!(empty_list.reasoning_effort.as_deref(), Some("high"));
    call(&tools, "thread_message_send", "post-only", json!({"targetOptionId": target.id, "input": [{"type":"text","text":"A message, without a model launch"}]})).await;
    let message_thread = processor
        .crud_store
        .get_thread_model(thread_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message_thread.turns[0].mode, ThreadMode::Message);
    let loaded = processor
        .thread_manager
        .thread_get(thread_id)
        .await
        .unwrap();
    assert!(
        processor
            .thread_manager
            .unload_orphaned_thread_if_idle(thread_id)
            .await
    );
    processor
        .ensure_thread_loaded(thread_id, &context.workspace_id)
        .await
        .unwrap();
    let restored = processor
        .thread_manager
        .thread_get(thread_id)
        .await
        .unwrap();
    let listed = processor
        .crud_store
        .list_threads_for_workspace(&context.workspace_id, 100)
        .await
        .unwrap();
    let listed = listed.iter().find(|thread| thread.id == thread_id).unwrap();
    for snapshot in [&loaded, &message_thread, &restored, listed] {
        assert_eq!(snapshot.model_provider, "openai");
        assert_eq!(snapshot.model, "gpt-5.4");
        assert_eq!(snapshot.reasoning_effort.as_deref(), Some("high"));
    }
    for snapshot in [&loaded, &message_thread, listed] {
        assert_eq!(snapshot.turns.len(), 1, "sending must not launch a model");
        assert_eq!(snapshot.turns[0].mode, ThreadMode::Message);
    }
    assert_eq!(
        message_thread.turns[0].author.as_ref().unwrap().actor,
        pioneer_protocol::PersistedActorRef::AgentExecution(facts.execution_id.clone())
    );
    let arguments = json!({"targetOptionId": target.id, "input": [{"type":"text","text":"Child task"}], "launch":{"permissionProfile":{"mode":"full_access"}}});
    call(
        &tools,
        "threads_turn_start",
        "start-visible",
        arguments.clone(),
    )
    .await;
    let outbox = pioneer_crud::load_agent_action_outbox(
        &db,
        crate::authorization::action_id_for_call("start-visible").as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    let persisted: serde_json::Value = serde_json::from_str(&outbox.payload_json).unwrap();
    let start: TurnStartParams =
        serde_json::from_value(persisted["dispatch"]["params"].clone()).unwrap();
    assert_eq!(start.model.as_deref(), Some("gpt-5.4"));
    assert_eq!(start.model_provider.as_deref(), Some("openai"));
    assert_eq!(start.reasoning.as_ref().unwrap().effort, "high");
    let before = outbox.payload_json;
    let replay = call(&tools, "threads_turn_start", "start-visible", arguments).await;
    assert_eq!(replay["outcome"], "agent_action_already_committed");
    let after = pioneer_crud::load_agent_action_outbox(
        &db,
        crate::authorization::action_id_for_call("start-visible").as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(after.payload_json, before);
    call(
        &tools,
        "threads_turn_start",
        "explicit-api",
        json!({
            "targetOptionId": target.id, "input": [{"type":"text", "text":"Use another model"}],
            "launch": {"profile":{"kind":"model", "providerId":"openai", "modelId":"gpt-5.4-catalog-other"},
                "reasoning":{"effort":"high"}, "permissionProfile":{"mode":"full_access"}}
        }),
    )
    .await;
    let explicit = pioneer_crud::load_agent_action_outbox(
        &db,
        crate::authorization::action_id_for_call("explicit-api").as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    let explicit: serde_json::Value = serde_json::from_str(&explicit.payload_json).unwrap();
    let explicit: TurnStartParams =
        serde_json::from_value(explicit["dispatch"]["params"].clone()).unwrap();
    assert_eq!(explicit.model.as_deref(), Some("gpt-5.4-catalog-other"));
    assert_eq!(explicit.reasoning.unwrap().effort, "high");
}

#[tokio::test]
async fn identity_models_catalog_exposes_model_reasoning_without_secrets_and_rejects_unavailable_models()
 {
    let (processor, context, tools) = fixture().await;
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let identity = binding.adapter.lock().await.persistence_facts().identity.id;
    let result = call(
        &tools,
        "threads_identity_odels_list",
        "models",
        json!({"identityId":identity}),
    )
    .await;
    let result: pioneer_protocol::AgentIdentityModelsToolResult =
        serde_json::from_value(result).unwrap();
    let executor = result
        .executors
        .iter()
        .find(|executor| executor.provider_id == "openai")
        .unwrap();
    let model = executor
        .models
        .iter()
        .find(|model| model.id == "gpt-5.4-catalog-other")
        .unwrap();
    assert!(!model.display_name.is_empty());
    assert!(
        model
            .reasoning
            .as_ref()
            .unwrap()
            .effort_options
            .contains(&"high".into())
    );
    assert_eq!(executor.default_model_id.as_deref(), Some("gpt-5.4"));
    let no_effort = executor
        .models
        .iter()
        .find(|model| model.id == "catalog-no-effort")
        .unwrap();
    assert_eq!(no_effort.reasoning.as_ref().unwrap().supported, Some(false));
    assert!(
        no_effort
            .reasoning
            .as_ref()
            .unwrap()
            .effort_options
            .is_empty()
    );

    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("fixture-secret")
    );
    let (_, public_launch) = crate::message::agent_action_tools::resolve_workspace_task_launch(
        &processor,
        &context.workspace_id,
        "openai",
        "gpt-5.4",
        None,
        None,
        "public-task-metadata",
    )
    .await
    .unwrap();
    let (public_identity, public_profile) = public_launch.unwrap();
    let authority = processor
        .load_turn_execution_authorization_context(&context.turn_id)
        .await
        .unwrap();
    let ceiling = crate::message::agent_action_tools::current_workspace_child_launch_ceiling(
        &processor,
        &context.workspace_id,
        &public_identity,
        &public_profile,
        &authority,
        true,
        vec![],
        vec![],
        authority.permission_profile_cap().clone(),
    )
    .await
    .unwrap();
    assert!(
        ceiling.profiles.contains(&public_profile),
        "Task actor profile must match its immutable grant exactly"
    );
    let creation = binding
        .options
        .thread_creation_options
        .iter()
        .find(|option| option.label == "New delegated root")
        .unwrap();
    let created = call(
        &tools,
        "thread_create",
        "create-no-effort",
        json!({"optionId":creation.id}),
    )
    .await;
    let targets: Vec<pioneer_protocol::AgentToolTargetOption> =
        serde_json::from_value(created["targetOptions"].clone()).unwrap();
    let target = targets
        .iter()
        .find(|target| {
            !binding
                .options
                .target_options
                .iter()
                .any(|existing| existing.id == target.id)
        })
        .unwrap();
    let arguments = json!({
        "targetOptionId":target.id, "input":[{"type":"text","text":"Use native defaults"}],
        "launch":{"profile":{"kind":"model", "providerId":"openai", "modelId":"catalog-no-effort"},
            "useDefaultReasoning":true, "permissionProfile":{"mode":"full_access"}}
    });
    call(&tools, "threads_turn_start", "no-effort", arguments.clone()).await;
    let db = processor.crud_store.database_connection();
    let action_id = crate::authorization::action_id_for_call("no-effort");
    let persisted = pioneer_crud::load_agent_action_outbox(&db, action_id.as_str())
        .await
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&persisted.payload_json).unwrap();
    let start: TurnStartParams =
        serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
    assert_eq!(start.model.as_deref(), Some("catalog-no-effort"));
    assert_eq!(start.model_provider.as_deref(), Some("openai"));
    assert!(start.reasoning.is_none());
    let replay = call(&tools, "threads_turn_start", "no-effort", arguments).await;
    assert_eq!(replay["outcome"], "agent_action_already_committed");
    assert_eq!(
        pioneer_crud::load_agent_action_outbox(&db, action_id.as_str())
            .await
            .unwrap()
            .unwrap()
            .payload_json,
        persisted.payload_json
    );
    // A real model launch with no explicit effort must retain NULL; it must
    // not recover the earlier high effort from the parent's configuration.
    let thread_id = payload["dispatch"]["thread_id"].as_str().unwrap();
    call(
        &tools,
        "thread_message_send",
        "post-no-effort",
        json!({
            "targetOptionId":target.id, "input":[{"type":"text","text":"Keep native defaults"}]
        }),
    )
    .await;
    let reloaded = processor
        .crud_store
        .get_thread_model(thread_id)
        .await
        .unwrap()
        .unwrap();
    assert!(reloaded.reasoning_effort.is_none());
    let listed = processor
        .crud_store
        .list_threads_for_workspace(&context.workspace_id, 100)
        .await
        .unwrap();
    assert!(
        listed
            .iter()
            .find(|thread| thread.id == thread_id)
            .unwrap()
            .reasoning_effort
            .is_none()
    );
    // task_create uses the same resolver and persists its canonical default
    // selection in the actor contract rather than re-inheriting at execution.
    use pioneer_agent::TaskToolProvider;
    let task_tools = crate::task_tools::GatewayTaskToolProvider::new(Arc::downgrade(&processor))
        .materialize_task_tools(pioneer_agent::TaskTurnContext {
            workspace_id: context.workspace_id.clone(),
            thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
        })
        .await
        .unwrap();
    let task_tools = pioneer_agent::TurnToolMaterialization {
        bundles: task_tools.bundles,
        ..Default::default()
    };
    let task = call(&task_tools, "task_create", "task-no-effort", json!({
        "title":"Model without effort", "goal":"Use the selected model's defaults",
        "launch":{"profile":{"kind":"model","providerId":"openai","modelId":"catalog-no-effort"},
            "useDefaultReasoning":true}
    })).await;
    let actor = processor
        .crud_store
        .get_task_actor_contract(task["taskId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    let launch = actor.launch.unwrap();
    assert!(launch.execution.reasoning.is_none());
    let task = processor
        .crud_store
        .get_task(task["taskId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        task.agent_specs
            .iter()
            .any(|spec| spec.model.as_deref() == Some("catalog-no-effort")
                && spec.model_provider.as_deref() == Some("openai"))
    );
    let incompatible: AgentToolLaunchSelection = serde_json::from_value(json!({
        "profile":{"kind":"model", "providerId":"openai", "modelId":"catalog-no-effort"},
        "reasoning":{"effort":"high"}
    }))
    .unwrap();
    assert!(
        crate::message::agent_action_tools::resolve_tool_launch(
            &processor,
            &context,
            incompatible,
            "incompatible"
        )
        .await
        .is_err()
    );
    let conflicting: AgentToolLaunchSelection = serde_json::from_value(json!({
        "useDefaultReasoning":true, "reasoning":{"effort":"high"}
    }))
    .unwrap();
    assert!(
        crate::message::agent_action_tools::resolve_tool_launch(
            &processor,
            &context,
            conflicting,
            "conflicting-reasoning"
        )
        .await
        .is_err()
    );
    let unavailable: pioneer_protocol::AgentToolLaunchSelection = serde_json::from_value(
        json!({"profile":{"kind":"model", "providerId":"openai", "modelId":"missing-model"}}),
    )
    .unwrap();
    assert!(
        crate::message::agent_action_tools::resolve_tool_launch(
            &processor,
            &context,
            unavailable,
            "missing"
        )
        .await
        .is_err()
    );
}
