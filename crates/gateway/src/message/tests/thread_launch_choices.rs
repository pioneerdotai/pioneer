//! Execution-bound thread actions: fixture catalogs never start real providers.
use super::*;
use pioneer_agent::TurnToolContext;
use pioneer_protocol::{AgentToolLaunchSelection, AgentToolProfileChoice, TurnStartParams};
use pioneer_tools::{ToolEventBus, ToolInvocation, ToolPayload};
use std::path::PathBuf;

#[derive(Clone, Default)]
enum ChoiceCatalogResponse {
    #[default]
    Default,
    Models(Vec<pioneer_protocol::ProviderModelInfo>),
    Unavailable,
}

#[derive(Default)]
struct ChoiceRuntimeProbe {
    main_calls: AtomicUsize,
    main_started: Notify,
    release_main: Notify,
    hold_main: std::sync::atomic::AtomicBool,
}

struct ChoiceModelsProvider {
    name: &'static str,
    probe: Arc<ChoiceRuntimeProbe>,
    response: Arc<std::sync::RwLock<ChoiceCatalogResponse>>,
    inner: PromptParityCaptureProvider,
    extra_models: Arc<std::sync::atomic::AtomicUsize>,
    catalog_calls: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait::async_trait]
impl Provider for ChoiceModelsProvider {
    fn name(&self) -> &str {
        self.name
    }
    fn capabilities(&self) -> ProviderCapabilities {
        self.inner.capabilities()
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if matches!(
            prompt_parity_request_kind(&request),
            Some(PromptParityRequestKind::Main)
        ) {
            self.probe.main_calls.fetch_add(1, Ordering::SeqCst);
            self.probe.main_started.notify_one();
            if self.probe.hold_main.load(Ordering::SeqCst) {
                self.probe.release_main.notified().await;
            }
        }
        self.inner.chat(request).await
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        Ok(futures_util::stream::iter(vec![
            Ok(StreamChunk::delta(response.text)),
            Ok(StreamChunk::final_chunk_with(
                pioneer_provider::ProviderTermination::Complete,
            )),
        ])
        .boxed())
    }
    async fn list_models(&self) -> anyhow::Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        self.catalog_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match self.response.read().unwrap().clone() {
            ChoiceCatalogResponse::Default => {}
            ChoiceCatalogResponse::Models(models) => return Ok(models),
            ChoiceCatalogResponse::Unavailable => anyhow::bail!("fixture catalog unavailable"),
        }
        let mut models = self.inner.list_models().await?;
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
        for index in 0..self.extra_models.load(std::sync::atomic::Ordering::SeqCst) {
            let mut model = models[0].clone();
            model.id = format!("gpt-5.4-later-{index}");
            model.name = Some(format!("Later model {index}"));
            models.push(model);
        }
        Ok(models)
    }
}

async fn fixture() -> (
    Arc<MessageProcessor>,
    TurnToolContext,
    pioneer_agent::TurnToolMaterialization,
) {
    fixture_with_catalog(Default::default(), Default::default()).await
}

async fn fixture_with_catalog(
    extra_models: Arc<std::sync::atomic::AtomicUsize>,
    catalog_calls: Arc<std::sync::atomic::AtomicUsize>,
) -> (
    Arc<MessageProcessor>,
    TurnToolContext,
    pioneer_agent::TurnToolMaterialization,
) {
    fixture_with_parent(extra_models, catalog_calls, Default::default(), false).await
}

async fn fixture_with_parent(
    extra_models: Arc<std::sync::atomic::AtomicUsize>,
    catalog_calls: Arc<std::sync::atomic::AtomicUsize>,
    response: Arc<std::sync::RwLock<ChoiceCatalogResponse>>,
    cli_parent: bool,
) -> (
    Arc<MessageProcessor>,
    TurnToolContext,
    pioneer_agent::TurnToolMaterialization,
) {
    fixture_with_probe(
        extra_models,
        catalog_calls,
        response,
        cli_parent,
        Default::default(),
    )
    .await
}

async fn fixture_with_probe(
    extra_models: Arc<std::sync::atomic::AtomicUsize>,
    catalog_calls: Arc<std::sync::atomic::AtomicUsize>,
    response: Arc<std::sync::RwLock<ChoiceCatalogResponse>>,
    cli_parent: bool,
    probe: Arc<ChoiceRuntimeProbe>,
) -> (
    Arc<MessageProcessor>,
    TurnToolContext,
    pioneer_agent::TurnToolMaterialization,
) {
    fixture_with_principal_probe(
        extra_models,
        catalog_calls,
        response,
        cli_parent,
        probe,
        false,
    )
    .await
}

async fn fixture_with_principal_probe(
    extra_models: Arc<std::sync::atomic::AtomicUsize>,
    catalog_calls: Arc<std::sync::atomic::AtomicUsize>,
    response: Arc<std::sync::RwLock<ChoiceCatalogResponse>>,
    cli_parent: bool,
    probe: Arc<ChoiceRuntimeProbe>,
    member: bool,
) -> (
    Arc<MessageProcessor>,
    TurnToolContext,
    pioneer_agent::TurnToolMaterialization,
) {
    let (workspaces, store, workspace) = setup_workspace_manager().await;
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        Arc::new(ChoiceModelsProvider {
            name: "openai",
            probe,
            response,
            inner: PromptParityCaptureProvider::with_visible_tools(vec![]),
            extra_models,
            catalog_calls,
        }),
    ));
    let secrets = test_gateway_secrets();
    secrets
        .set_workspace_provider_api_key(&workspace, "openai", "fixture-secret")
        .unwrap();
    let processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("composer-model", "openai")),
        registry,
        Arc::new(SessionManager::new()),
        workspaces,
        store,
        secrets,
        test_summary_config(),
        test_tool_loop_config(),
    );
    let processor = Arc::new(if cli_parent {
        with_enabled_test_cli_runtime_catalog(processor)
    } else {
        processor
    });
    let mut context = TurnToolContext {
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
    let mut principal = (*authenticated_test_superuser()).clone();
    let mut permission = default_test_permission_profile();
    if member {
        const MEMBER: &str = "P0000000000000000000M";
        principal.principal_id = pioneer_protocol::PrincipalId::new(MEMBER).unwrap();
        principal.kind = pioneer_protocol::PrincipalKind::User;
        principal.role_key = Some(pioneer_protocol::RoleKey::member());
        permission = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
            pioneer_protocol::TurnPermissionMode::Supervised,
            pioneer_protocol::TurnPermissionProfileSource::Composer,
        );
        processor.crud_store.database_connection().execute_unprepared(&format!(
            "INSERT INTO gateway_principal(id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,created_at,updated_at) \
             VALUES('{MEMBER}','{}','user','member','active','Threads Member','threads-member','threads-member',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
             INSERT INTO workspace_membership(principal_id,workspace_id,granted_by_actor_kind,created_at,updated_at) \
             VALUES('{MEMBER}','{workspace}','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP); \
             INSERT INTO thread_membership(thread_id,principal_id,added_by_actor_kind,created_at,updated_at) \
             VALUES('{}','{MEMBER}','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            crate::session::test_support::TEST_GATEWAY_ID, context.thread_id)).await.unwrap();
        let mut old_turn = processor
            .crud_store
            .get_turn(&context.thread_id, &context.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1;
        old_turn.status = TurnStatus::Completed;
        processor
            .crud_store
            .materialize_native_agent_turn_event(
                pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(
                    pioneer_protocol::TurnCompletedNotification {
                        workspace_id: workspace.clone(),
                        thread_id: context.thread_id.clone(),
                        turn: old_turn.clone(),
                    },
                ),
                now_timestamp_secs(),
                None,
            )
            .await
            .unwrap();
        context.turn_id = "choices-member-parent".into();
        let mut turn = old_turn;
        turn.id = context.turn_id.clone();
        turn.status = TurnStatus::InProgress;
        turn.permission_profile = permission.clone();
        turn.author = crate::message::message_turn::resolve_turn_author_snapshot(
            processor.crud_store.as_ref(),
            &pioneer_protocol::PersistedActorRef::Principal(principal.principal_id.clone()),
        )
        .await
        .unwrap();
        let thread = processor
            .crud_store
            .get_thread_model(&context.thread_id)
            .await
            .unwrap()
            .unwrap();
        processor
            .crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                pioneer_protocol::PersistedActorRef::Principal(principal.principal_id.clone()),
            )
            .await
            .unwrap();
    }
    if cli_parent {
        sync_test_cli_runtime_identities(&processor).await.unwrap();
    }
    let provider = if cli_parent {
        "cli_runtime:codex"
    } else {
        "openai"
    };
    let model = if cli_parent { "test-model" } else { "gpt-5.4" };
    let backend = if cli_parent {
        AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: "codex".into(),
            runtime_kind: CLIAgentRuntimeKind::Codex,
        }
    } else {
        AgentExecutionBackend::ApiProvider {
            provider: "openai".into(),
        }
    };
    let params: TurnStartParams = serde_json::from_value(json!({
        "thread_id":context.thread_id, "turn_id":context.turn_id, "input":[],
        "model":model, "model_provider":provider, "execution_backend":backend,
    }))
    .unwrap();
    let request = crate::authorization::ExecutionAdmissionRequest::for_turn(
        crate::authorization::ExecutionAdmissionEntryPoint::InteractiveTurn,
        crate::authorization::ResourceAction::AgentTurnStart,
        Vec::new(),
        &workspace,
        &context.thread_id,
        provider,
        model,
        &params,
        &[],
        if cli_parent {
            None
        } else {
            Some(
                processor
                    .provider_registry
                    .authority_fingerprint_for_workspace(&workspace, "openai")
                    .unwrap()
                    .as_str()
                    .to_owned(),
            )
        },
    )
    .unwrap();
    let authority =
        crate::authorization::ExecutionAdmissionService::new(processor.crud_store.as_ref().clone())
            .admit_context(
                &principal,
                processor.current_authorization_revision().await.unwrap(),
                &request,
                Some(&pioneer_protocol::task_permission_cap_from_snapshot(
                    &permission,
                )),
            )
            .await
            .unwrap();
    processor
        .crud_store
        .set_turn_execution_authorization_context(
            &context.turn_id,
            &authority.to_persisted_json().unwrap(),
        )
        .await
        .unwrap();
    let mut security = pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
        "/tmp/project",
        pioneer_crud::utc_now().timestamp_millis(),
    );
    security.permission_profile = permission;
    processor
        .crud_store
        .set_turn_execution_security_snapshot(&context.turn_id, &security)
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
            model: if cli_parent { "test-model" } else { "gpt-5.4" }.into(),
            provider_name: if cli_parent {
                "cli_runtime:codex"
            } else {
                "openai"
            }
            .into(),
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
    if cli_parent {
        let authority = processor
            .load_turn_execution_authorization_context(&context.turn_id)
            .await
            .unwrap();
        let mut thread = processor
            .crud_store
            .get_thread_model(&context.thread_id)
            .await
            .unwrap()
            .unwrap();
        thread.model = "test-model".into();
        thread.model_provider = "cli_runtime:codex".into();
        let backend = AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: "codex".into(),
            runtime_kind: CLIAgentRuntimeKind::Codex,
        };
        let prepared = crate::message::agent_action_tools::prepare_root_agent_execution_admission(
            &processor,
            &context,
            &thread,
            &authority,
            None,
            None,
            Some(&backend),
            None,
            Some("high"),
        )
        .await
        .unwrap();
        processor
            .crud_store
            .commit_agent_execution_graph(prepared.graph.clone())
            .await
            .unwrap();
        crate::message::agent_action_tools::register_prepared_root_agent_action_binding(
            &processor, &context, prepared,
        )
        .await
        .unwrap();
    }
    let tools = processor
        .materialize_agent_action_tools(context.clone())
        .await
        .unwrap();
    (processor, context, tools)
}

async fn invoke_with_mutation(
    tools: &pioneer_agent::TurnToolMaterialization,
    name: &str,
    id: &str,
    arguments: serde_json::Value,
    mutation_key: Option<&str>,
) -> Result<serde_json::Value, pioneer_tools::ToolError> {
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
                idempotency_key: mutation_key.map(str::to_owned),
                recovery: Default::default(),
                permission_metadata: Default::default(),
                execution_security_snapshot: None,
                apply_patch_preflight: None,
                cancellation: tokio_util::sync::CancellationToken::new(),
            },
            ToolEventBus::new(8).start_trace("choices-parent", id, name),
        )
        .await?;
    assert!(output.success(), "{name} ({id}): {}", output.raw_text());
    Ok(output.raw_json())
}

pub(super) async fn call(
    tools: &pioneer_agent::TurnToolMaterialization,
    name: &str,
    id: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    invoke_with_mutation(tools, name, id, arguments, None)
        .await
        .unwrap_or_else(|error| panic!("{name} ({id}) failed: {error}"))
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
    let (inherited, _) = crate::message::agent_action_tools::resolve_tool_launch(
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
    let (effort_only, _) = crate::message::agent_action_tools::resolve_tool_launch(
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
    assert!(
        processor
            .mark_turn_interrupted(
                start.thread_id.clone(),
                start.turn_id.clone(),
                "Finish the first accepted launch before the explicit launch".into()
            )
            .await
    );
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

async fn api_choice_models(ids: &[&str]) -> Vec<pioneer_protocol::ProviderModelInfo> {
    let template = PromptParityCaptureProvider::with_visible_tools(vec![])
        .list_models()
        .await
        .unwrap()
        .remove(0);
    ids.iter()
        .map(|id| {
            let mut model = template.clone();
            model.id = (*id).to_owned();
            model
        })
        .collect()
}

async fn delegated_launch_target(
    tools: &pioneer_agent::TurnToolMaterialization,
    options: &pioneer_protocol::AgentToolOptionsProjection,
) -> pioneer_protocol::AgentToolTargetOption {
    let creation = options
        .thread_creation_options
        .iter()
        .find(|option| option.label == "New delegated root")
        .unwrap();
    let created = call(
        tools,
        "thread_create",
        "review-target",
        json!({"optionId":creation.id}),
    )
    .await;
    let targets: Vec<pioneer_protocol::AgentToolTargetOption> =
        serde_json::from_value(created["targetOptions"].clone()).unwrap();
    targets
        .into_iter()
        .find(|target| !options.target_options.iter().any(|old| old.id == target.id))
        .unwrap()
}

#[tokio::test]
async fn cli_parent_selects_only_unambiguous_api_choices_for_threads_and_tasks() {
    use pioneer_agent::TaskToolProvider;
    for (model_selected, model_ids, second_provider, allowed) in [
        (true, vec!["gpt-5.4", "gpt-5.4-other"], false, true),
        (false, vec!["gpt-5.4"], false, true),
        (true, vec!["gpt-5.4"], true, false),
        (false, vec!["gpt-5.4", "gpt-5.4-other"], false, false),
    ] {
        let response = Arc::new(std::sync::RwLock::new(ChoiceCatalogResponse::Models(
            api_choice_models(&model_ids).await,
        )));
        let (processor, context, tools) = fixture_with_parent(
            Default::default(),
            Default::default(),
            response.clone(),
            true,
        )
        .await;
        if second_provider {
            processor
                .gateway_secrets
                .set_workspace_provider_api_key(
                    &context.workspace_id,
                    "anthropic",
                    "fixture-secret",
                )
                .unwrap();
            processor
                .provider_registry
                .insert(
                    "anthropic",
                    Arc::new(ChoiceModelsProvider {
                        name: "anthropic",
                        probe: Default::default(),
                        response: response.clone(),
                        inner: PromptParityCaptureProvider::with_visible_tools(vec![]),
                        extra_models: Default::default(),
                        catalog_calls: Default::default(),
                    }),
                )
                .unwrap();
        }
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        assert!(matches!(
            binding
                .adapter
                .lock()
                .await
                .persistence_facts()
                .profile
                .backend,
            pioneer_protocol::AgentExecutionProfileBackend::CliRuntime { .. }
        ));
        let target = delegated_launch_target(&tools, &binding.options).await;
        let mut launch = json!({"identity":{"kind":"default_pioneer"},"useDefaultReasoning":true});
        if model_selected {
            launch["profile"] = json!({"kind":"model","modelId":"gpt-5.4"});
        }
        let started = invoke_with_mutation(&tools, "threads_turn_start", "cli-to-api", json!({
            "targetOptionId":target.id, "input":[{"type":"text","text":"Select an API executor"}], "launch":launch,
        }), None).await;
        let task_tools =
            crate::task_tools::GatewayTaskToolProvider::new(Arc::downgrade(&processor))
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
        let task = invoke_with_mutation(&task_tools, "task_create", "cli-to-api-task", json!({
            "title":"API identity", "goal":"Keep the selected API configuration", "launch":launch,
        }), None).await;
        let database = processor.crud_store.database_connection();
        if allowed {
            started.unwrap();
            let outbox = pioneer_crud::load_agent_action_outbox(
                &database,
                crate::authorization::action_id_for_call("cli-to-api").as_str(),
            )
            .await
            .unwrap()
            .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&outbox.payload_json).unwrap();
            let params: TurnStartParams =
                serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
            assert_eq!(params.model_provider.as_deref(), Some("openai"));
            assert_eq!(params.model.as_deref(), Some("gpt-5.4"));
            assert!(matches!(
                params.execution_backend,
                Some(AgentExecutionBackend::ApiProvider { .. })
            ));
            let task = task.unwrap();
            let actor = processor
                .crud_store
                .get_task_actor_contract(task["taskId"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap();
            actor.validate().unwrap();
            let pioneer_protocol::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch {
                identity,
                profile,
                ..
            } = serde_json::from_str(actor.derived_child_launch_grant_json.as_deref().unwrap())
                .unwrap();
            assert_eq!(
                identity.source_kind,
                pioneer_protocol::AgentIdentitySourceKind::NativeAgent
            );
            assert_eq!(profile.provider_id, "openai");
            assert_eq!(profile.model_id, "gpt-5.4");
            assert_eq!(
                profile.backend,
                pioneer_protocol::AgentExecutionProfileBackend::ApiProvider
            );
        } else {
            assert!(matches!(
                started,
                Err(pioneer_tools::ToolError::InvalidArguments(_))
            ));
            assert!(matches!(
                task,
                Err(pioneer_tools::ToolError::InvalidArguments(_))
            ));
            for id in ["cli-to-api", "cli-to-api-task"] {
                assert!(
                    pioneer_crud::load_agent_action_outbox(
                        &database,
                        crate::authorization::action_id_for_call(id).as_str()
                    )
                    .await
                    .unwrap()
                    .is_none()
                );
            }
        }
    }
}

#[tokio::test]
async fn inherited_reasoning_replay_uses_committed_choice_when_catalog_changes() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for unavailable in [false, true] {
        let response = Arc::new(std::sync::RwLock::new(ChoiceCatalogResponse::Default));
        let calls = Arc::new(AtomicUsize::new(0));
        let (processor, context, tools) =
            fixture_with_parent(Default::default(), calls.clone(), response.clone(), false).await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let parent = binding.adapter.lock().await.persistence_facts();
        assert_eq!(
            parent.profile.allowed_reasoning,
            vec![pioneer_protocol::TurnReasoningSelection {
                effort: "high".into()
            }]
        );
        let target = delegated_launch_target(&tools, &binding.options).await;
        let request = json!({"targetOptionId":target.id,"input":[{"type":"text","text":"Inherited model, changed effort"}],
            "launch":{"reasoning":{"effort":"low"},"permissionProfile":{"mode":"full_access"}}});
        call(
            &tools,
            "threads_turn_start",
            "inherit-reasoning",
            request.clone(),
        )
        .await;
        let database = processor.crud_store.database_connection();
        let action_id = crate::authorization::action_id_for_call("inherit-reasoning");
        let before = pioneer_crud::load_agent_action_outbox(&database, action_id.as_str())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&before.payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        assert_eq!(params.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(params.reasoning.as_ref().unwrap().effort, "low");
        let child_id = payload["spawned_execution_id"].as_str().unwrap();
        let grant = pioneer_crud::load_agent_execution_grant(&database, child_id)
            .await
            .unwrap()
            .unwrap();
        // Remove the selected model. OpenAI's documented effort options are
        // supplemented by the reasoning registry, so discovery cannot remove low.
        let changed_models = api_choice_models(&["gpt-5.4-catalog-other"]).await;
        *response.write().unwrap() = if unavailable {
            ChoiceCatalogResponse::Unavailable
        } else {
            ChoiceCatalogResponse::Models(changed_models)
        };
        let before_replay = calls.load(Ordering::SeqCst);
        let replay = call(
            &tools,
            "threads_turn_start",
            "inherit-reasoning",
            request.clone(),
        )
        .await;
        assert_eq!(replay["outcome"], "agent_action_already_committed");
        for change_reasoning in [false, true] {
            let mut changed_request = request.clone();
            if change_reasoning {
                changed_request["launch"]["reasoning"]["effort"] = json!("high");
            } else {
                changed_request["input"][0]["text"] = json!("Different content");
            }
            assert!(
                invoke_with_mutation(
                    &tools,
                    "threads_turn_start",
                    "inherit-reasoning",
                    changed_request,
                    None
                )
                .await
                .is_err()
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            before_replay,
            "replay must not rediscover model or reasoning"
        );
        assert_eq!(
            pioneer_crud::load_agent_action_outbox(&database, action_id.as_str())
                .await
                .unwrap()
                .unwrap(),
            before
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_grant(&database, child_id)
                .await
                .unwrap()
                .unwrap(),
            grant
        );
        assert!(
            invoke_with_mutation(
                &tools,
                "threads_turn_start",
                "new-inherit-reasoning",
                request,
                None
            )
            .await
            .is_err()
        );
        assert!(
            calls.load(Ordering::SeqCst) > before_replay,
            "a new launch still checks current metadata"
        );
    }
}

#[tokio::test]
async fn inherited_other_cli_replay_does_not_reselect_a_model() {
    let (processor, context, tools) = fixture_with_parent(
        Default::default(),
        Default::default(),
        Default::default(),
        true,
    )
    .await;
    processor
        .mark_cli_runtimes_ready_for_tests(&context.workspace_id)
        .await
        .unwrap();
    processor
        .mark_cli_reasoning_model_ready_for_tests(
            &context.workspace_id,
            "claude",
            "selected-cli-model",
        )
        .await
        .unwrap();
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let target = delegated_launch_target(&tools, &binding.options).await;
    let identity = binding
        .options
        .identities
        .iter()
        .find(|id| id.nickname == "claude")
        .unwrap();
    let request = json!({"targetOptionId":target.id,"input":[{"type":"text","text":"Another CLI identity"}],
        "launch":{"identity":{"kind":"exact","id":identity.id},"useDefaultReasoning":true,"permissionProfile":{"mode":"full_access"}}});
    call(
        &tools,
        "threads_turn_start",
        "inherit-other-cli",
        request.clone(),
    )
    .await;
    let database = processor.crud_store.database_connection();
    let action_id = crate::authorization::action_id_for_call("inherit-other-cli");
    let before = pioneer_crud::load_agent_action_outbox(&database, action_id.as_str())
        .await
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&before.payload_json).unwrap();
    let params: TurnStartParams =
        serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
    assert_eq!(params.model_provider.as_deref(), Some("cli_runtime:claude"));
    assert_eq!(params.model.as_deref(), Some("selected-cli-model"));
    processor
        .mark_cli_reasoning_model_ready_for_tests(
            &context.workspace_id,
            "claude",
            "another-cli-model",
        )
        .await
        .unwrap();
    let replay = call(
        &tools,
        "threads_turn_start",
        "inherit-other-cli",
        request.clone(),
    )
    .await;
    assert_eq!(replay["outcome"], "agent_action_already_committed");
    assert!(
        invoke_with_mutation(
            &tools,
            "threads_turn_start",
            "ambiguous-other-cli",
            request.clone(),
            None
        )
        .await
        .is_err()
    );
    // Readiness now has no models. Existing replay still needs only the exact grant.
    processor
        .mark_cli_runtimes_ready_for_tests(&context.workspace_id)
        .await
        .unwrap();
    let replay = call(&tools, "threads_turn_start", "inherit-other-cli", request).await;
    assert_eq!(replay["outcome"], "agent_action_already_committed");
    assert_eq!(
        pioneer_crud::load_agent_action_outbox(&database, action_id.as_str())
            .await
            .unwrap()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn large_later_catalog_stays_out_of_task_and_execution_grants_and_replay() {
    use pioneer_agent::TaskToolProvider;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let extra_models = Arc::new(AtomicUsize::new(4096));
    let calls = Arc::new(AtomicUsize::new(0));
    let response = Arc::new(std::sync::RwLock::new(ChoiceCatalogResponse::Default));
    let (processor, context, tools) =
        fixture_with_parent(extra_models.clone(), calls.clone(), response.clone(), false).await;
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let facts = binding.adapter.lock().await.persistence_facts();
    assert_eq!(facts.profile.model_id, "gpt-5.4");
    let database = processor.crud_store.database_connection();
    let root_grant =
        pioneer_crud::load_agent_execution_grant(&database, facts.execution_id.as_str())
            .await
            .unwrap()
            .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "root admission must not discover model catalogs"
    );
    assert!(root_grant.grant_json.len() < 131_072);
    extra_models.store(4097, Ordering::SeqCst);
    call(&tools, "threads_start_options", "large-options", json!({})).await;
    crate::message::agent_action_tools::resolve_tool_launch(
        &processor,
        &context,
        AgentToolLaunchSelection::default(),
        "large-inherit",
    )
    .await
    .unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "options and exact inheritance do not discover models"
    );

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
    let launch = json!({"profile":{"kind":"model","providerId":"openai","modelId":"gpt-5.4-later-4096"},"useDefaultReasoning":true});
    let task = call(
        &task_tools,
        "task_create",
        "large-task",
        json!({
            "title":"Later model", "goal":"Keep this exact selection", "launch":launch,
        }),
    )
    .await;
    let actor = processor
        .crud_store
        .get_task_actor_contract(task["taskId"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    actor.validate().unwrap();
    let saved_task_grant = actor.derived_child_launch_grant_json.as_deref().unwrap();
    assert!(saved_task_grant.len() < 131_072);
    let saved_task: serde_json::Value = serde_json::from_str(saved_task_grant).unwrap();
    assert_eq!(saved_task["profile"]["modelId"], "gpt-5.4-later-4096");
    assert!(saved_task["child_launch_grant"].get("profiles").is_none());
    assert!(
        saved_task["child_launch_grant"]
            .get("maxReasoning")
            .is_none()
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
        "large-thread",
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
                .any(|old| old.id == target.id)
        })
        .unwrap();
    let request = json!({"targetOptionId":target.id,"input":[{"type":"text","text":"Pinned child"}],"launch":launch});
    call(&tools, "threads_turn_start", "large-child", request.clone()).await;
    let action_id = crate::authorization::action_id_for_call("large-child");
    let before = pioneer_crud::load_agent_action_outbox(&database, action_id.as_str())
        .await
        .unwrap()
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&before.payload_json).unwrap();
    let params: TurnStartParams =
        serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
    assert_eq!(params.model.as_deref(), Some("gpt-5.4-later-4096"));
    let child_grant = pioneer_crud::load_agent_execution_grant(
        &database,
        payload["spawned_execution_id"].as_str().unwrap(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(child_grant.grant_json.len() < 131_072);
    let child: serde_json::Value = serde_json::from_str(&child_grant.grant_json).unwrap();
    assert!(child["child_launch_grant"].get("profiles").is_none());
    assert_eq!(child["profile"]["modelId"], "gpt-5.4-later-4096");

    extra_models.store(0, Ordering::SeqCst);
    let calls_before_replay = calls.load(Ordering::SeqCst);
    for unavailable in [false, true] {
        *response.write().unwrap() = if unavailable {
            ChoiceCatalogResponse::Unavailable
        } else {
            ChoiceCatalogResponse::Default
        };
        let replay = call(&tools, "threads_turn_start", "large-child", request.clone()).await;
        assert_eq!(replay["outcome"], "agent_action_already_committed");
        // All other arguments stay identical, but inheritance now means the
        // parent's gpt-5.4 instead of the explicitly selected later model.
        let mut inherited_request = request.clone();
        inherited_request["launch"]["profile"] = json!({"kind":"inherit_parent"});
        assert!(matches!(
            invoke_with_mutation(
                &tools,
                "threads_turn_start",
                "large-child",
                inherited_request,
                None
            )
            .await,
            Err(pioneer_tools::ToolError::InvalidArguments(_))
        ));
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        calls_before_replay,
        "replay uses its saved exact profile"
    );
    assert_eq!(
        pioneer_crud::load_agent_action_outbox(&database, action_id.as_str())
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_grant(
            &database,
            payload["spawned_execution_id"].as_str().unwrap()
        )
        .await
        .unwrap()
        .unwrap(),
        child_grant
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_grant(&database, facts.execution_id.as_str())
            .await
            .unwrap()
            .unwrap(),
        root_grant
    );
    assert_eq!(
        processor
            .crud_store
            .get_task_actor_contract(task["taskId"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap(),
        actor
    );
    *response.write().unwrap() = ChoiceCatalogResponse::Default;
    let unavailable: AgentToolLaunchSelection = serde_json::from_value(launch).unwrap();
    assert!(
        crate::message::agent_action_tools::resolve_tool_launch(
            &processor,
            &context,
            unavailable,
            "new-after-removal"
        )
        .await
        .is_err()
    );
    let principal = crate::auth::AuthenticatedSessionPrincipal {
        gateway_id: pioneer_protocol::GatewayId::new("G".repeat(21)).unwrap(),
        principal_id: pioneer_protocol::PrincipalId::new("P".repeat(21)).unwrap(),
        kind: pioneer_protocol::PrincipalKind::User,
        role_key: Some(pioneer_protocol::RoleKey::new("synthetic_executor").unwrap()),
        device_id: pioneer_protocol::DeviceId::new("D".repeat(21)).unwrap(),
        session_id: pioneer_protocol::AuthSessionId::new("S".repeat(21)).unwrap(),
        access_jti: "J".repeat(21),
        access_expires_at_unix: u64::MAX,
    };
    let restricted = crate::authorization::ExecutionAuthorizationContext::for_test(
        &principal,
        &context.workspace_id,
        &context.thread_id,
        &pioneer_protocol::inherited_turn_permission_profile_snapshot(
            pioneer_protocol::TurnPermissionMode::Supervised,
        ),
        None,
    );
    let before_denial = calls.load(Ordering::SeqCst);
    let denied = crate::message::agent_action_tools::validate_new_launch_model(
        &processor,
        &context,
        &facts.profile,
        None,
        &restricted,
        false,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(denied, pioneer_tools::ToolError::InvalidArguments(code) if code == "agent_execution_selection_not_allowed")
    );
    assert_eq!(calls.load(Ordering::SeqCst), before_denial);
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
        "threads_identity_models_list",
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
    pioneer_protocol::validate_selected_agent_launch_profile(
        &public_identity,
        &public_profile,
        &ceiling.identities,
    )
    .unwrap();
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

#[tokio::test]
async fn webhook_task_tools_create_get_and_replay_require_current_task_read() {
    use crate::authorization::ResourceAction;
    use pioneer_agent::TaskToolProvider;
    for attached in [false, true] {
        let (processor, context, _) = fixture().await;
        processor.set_gateway_listen_addr("[::1]:41234".parse().unwrap());
        let materialized =
            crate::task_tools::GatewayTaskToolProvider::for_cli(Arc::downgrade(&processor))
                .materialize_task_tools(pioneer_agent::TaskTurnContext {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    turn_id: context.turn_id.clone(),
                })
                .await
                .unwrap();
        let tools = pioneer_agent::TurnToolMaterialization {
            bundles: materialized.bundles,
            ..Default::default()
        };
        let mut arguments = json!({
            "title":"Webhook tool contract", "goal":"Interpret each request independently",
            "instructions":["Interpret the external webhook input using the saved objective."],
            "outputInstructions":"Return a concise result for this request.",
            "trigger":{"kind":"external","source":"webhook"}
        });
        if attached {
            arguments["lifecyclePolicy"] = json!({"attachment":"attached", "onParentCancel":"cancel",
                "onParentFailure":"cancel", "completion":"keep_active_for_recurring"});
        }
        let mutation = Some("webhook-create-stable-mutation");
        let created = invoke_with_mutation(
            &tools,
            "task_create",
            "webhook-create",
            arguments.clone(),
            mutation,
        )
        .await
        .unwrap();
        assert!(serde_json::Value::is_null(&created["runId"]));
        assert_eq!(created["waitable"], false);
        assert_eq!(
            created["attachment"],
            if attached { "attached" } else { "detached" }
        );
        let task_id = created["taskId"].as_str().unwrap();
        let initial = url::Url::parse(created["webhookUrl"].as_str().unwrap()).unwrap();
        assert_eq!(initial.scheme(), "http");
        assert_eq!(initial.host_str(), Some("[::1]"));
        assert_eq!(initial.port(), Some(41234));
        assert_eq!(created["webhookScope"], "loopback");
        let segments = initial.path_segments().unwrap().collect::<Vec<_>>();
        assert_eq!(&segments[..2], &["webhooks", "tasks"]);
        let trigger_id = segments[2];
        let secret = segments[3];
        assert_eq!(trigger_id.len(), 21);
        assert_eq!(secret.len(), 64);
        let fetched = call(&tools, "task_get", "webhook-get", json!({"taskId":task_id})).await;
        assert_eq!(fetched["webhookUrl"], created["webhookUrl"]);
        assert_eq!(fetched["triggers"][0]["id"], trigger_id);
        assert!(fetched["runs"].as_array().unwrap().is_empty());
        // Re-reading a task does not provision a new capability. Replaying the
        // real create handler recomputes its host using the same mutation key.
        let again = call(
            &tools,
            "task_get",
            "webhook-get-again",
            json!({"taskId":task_id}),
        )
        .await;
        assert_eq!(again["webhookUrl"], created["webhookUrl"]);
        processor.set_gateway_listen_addr("[::1]:41235".parse().unwrap());
        let replay = invoke_with_mutation(
            &tools,
            "task_create",
            "webhook-create",
            arguments.clone(),
            mutation,
        )
        .await
        .unwrap();
        assert_eq!(replay["taskId"], created["taskId"]);
        assert_eq!(replay["waitable"], false);
        let replay_url = url::Url::parse(replay["webhookUrl"].as_str().unwrap()).unwrap();
        assert_eq!(replay_url.port(), Some(41235));
        assert_eq!(replay_url.path(), initial.path());
        let persisted = processor
            .crud_store
            .get_task(task_id)
            .await
            .unwrap()
            .unwrap();
        assert!(persisted.runs.is_empty());
        assert_eq!(persisted.triggers[0].id, trigger_id);
        assert_eq!(
            persisted.task.lifecycle_policy.as_ref().unwrap().attachment,
            if attached {
                pioneer_protocol::TaskAttachmentMode::Attached
            } else {
                pioneer_protocol::TaskAttachmentMode::Detached
            }
        );

        // Drop every handler holding the CLI mutation cache. Re-materializing
        // now must recover the committed create receipt from the database.
        drop(tools);
        let materialized =
            crate::task_tools::GatewayTaskToolProvider::for_cli(Arc::downgrade(&processor))
                .materialize_task_tools(pioneer_agent::TaskTurnContext {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    turn_id: context.turn_id.clone(),
                })
                .await
                .unwrap();
        let tools = pioneer_agent::TurnToolMaterialization {
            bundles: materialized.bundles,
            ..Default::default()
        };

        // Narrow the existing execution authorization fixture: TaskCreate stays
        // admitted, but TaskRead is absent even though the initiating user has
        // broader rights. The actual handlers must honor that current ceiling.
        let original = processor
            .load_turn_execution_authorization_context(&context.turn_id)
            .await
            .unwrap()
            .to_persisted_json()
            .unwrap();
        let mut narrowed: serde_json::Value = serde_json::from_str(&original).unwrap();
        let actions = narrowed["grant_manifest"]["actions"]
            .as_array_mut()
            .unwrap();
        assert!(
            actions
                .iter()
                .any(|action| action.as_str() == Some(ResourceAction::TaskRead.safe_name()))
        );
        actions.retain(|action| action.as_str() != Some(ResourceAction::TaskRead.safe_name()));
        assert!(
            actions
                .iter()
                .any(|action| action.as_str() == Some(ResourceAction::TaskCreate.safe_name()))
        );
        processor
            .crud_store
            .set_turn_execution_authorization_context(&context.turn_id, &narrowed.to_string())
            .await
            .unwrap();
        let denial = invoke_with_mutation(
            &tools,
            "task_get",
            "webhook-get-denied",
            json!({"taskId":task_id}),
            None,
        )
        .await
        .unwrap_err();
        assert!(denial.to_string().contains("task_access_denied"));
        let withheld =
            invoke_with_mutation(&tools, "task_create", "webhook-create", arguments, mutation)
                .await
                .unwrap();
        assert_eq!(withheld["alreadyCommitted"], true);
        assert_eq!(withheld["taskId"], created["taskId"]);
        assert!(withheld.get("webhookUrl").is_none());
        assert!(withheld.get("webhookScope").is_none());
        assert!(!withheld.to_string().contains(secret));
        let tasks = processor
            .crud_store
            .list_tasks(pioneer_protocol::TaskListParams {
                workspace_id: context.workspace_id.clone(),
                limit: Some(20),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            tasks
                .iter()
                .filter(|task| task.title == "Webhook tool contract")
                .count(),
            1
        );
        processor
            .crud_store
            .set_turn_execution_authorization_context(&context.turn_id, &original)
            .await
            .unwrap();
        let restored = call(
            &tools,
            "task_get",
            "webhook-get-restored",
            json!({"taskId":task_id}),
        )
        .await;
        assert_eq!(restored["webhookUrl"], replay["webhookUrl"]);
    }
}

// This crosses real action admission, canonical persistence, outbox dispatch,
// native preparation and the recording provider. It never launches a process.
#[tokio::test]
async fn delegated_root_first_dispatch_replay_and_stop_have_an_independent_owner() {
    use sea_orm::{ActiveModelTrait, Set};
    for source_terminal in ["completed", "cancelled"] {
        let probe = Arc::new(ChoiceRuntimeProbe::default());
        probe.hold_main.store(true, Ordering::SeqCst);
        let (processor, context, tools) = fixture_with_probe(
            Default::default(),
            Default::default(),
            Default::default(),
            false,
            probe.clone(),
        )
        .await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let source = binding.adapter.lock().await.persistence_facts();
        let target = delegated_launch_target(&tools, &binding.options).await;
        call(
            &tools,
            "threads_turn_start",
            "independent-first",
            json!({
                "targetOptionId":target.id, "input":[{"type":"text","text":"Run independently"}],
            }),
        )
        .await;
        let db = processor.crud_store.database_connection();
        let action = crate::authorization::action_id_for_call("independent-first");
        let outbox = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&outbox.payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let execution_id = payload["spawned_execution_id"].as_str().unwrap();
        let execution = pioneer_crud::load_agent_execution(&db, execution_id)
            .await
            .unwrap()
            .unwrap();
        assert!(execution.parent_execution_id.is_none());
        assert_eq!(execution.work_graph_root_execution_id, execution.id);
        assert_ne!(
            execution.work_graph_root_execution_id,
            source.root_execution_id.as_str()
        );
        assert_eq!(execution.home_root_thread_id, params.thread_id);
        assert!(
            processor
                .crud_store
                .get_task_thread_lineage(&params.thread_id)
                .await
                .unwrap()
                .is_none()
        );
        let (_, accepted) = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            accepted.author.as_ref().unwrap().actor,
            pioneer_protocol::PersistedActorRef::AgentExecution(source.execution_id.clone())
        );
        assert!(matches!(
            processor
                .agent_manager
                .observe_turn(&params.thread_id, &params.turn_id)
                .await,
            Err(pioneer_agent::AgentControlError::ThreadNotFound)
        ));
        assert!(
            processor
                .crud_store
                .get_turn_runtime_snapshot(&params.turn_id)
                .await
                .unwrap()
                .is_none()
        );

        // Terminating the source graph before delivery cannot cancel the new root.
        let (_, parent) = processor
            .crud_store
            .get_turn(&context.thread_id, &context.turn_id)
            .await
            .unwrap()
            .unwrap();
        if source_terminal == "completed" {
            processor
                .crud_store
                .finalize_agent_execution(
                    source.execution_id.as_str(),
                    "completed",
                    pioneer_crud::utc_now(),
                )
                .await
                .unwrap();
        } else {
            assert!(
                processor
                    .cancel_root_agent_work_graph_for_turn(
                        &context.thread_id,
                        &parent,
                        "source stopped"
                    )
                    .await
                    .unwrap()
            );
        }
        let result =
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
        assert!(result.errors.is_empty(), "{:?}", result.errors);
        timeout(Duration::from_secs(5), probe.main_started.notified())
            .await
            .unwrap();
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 1);
        let runtime = processor
            .crud_store
            .get_turn_runtime_snapshot(&params.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                runtime.provider_name.as_str(),
                runtime.model.as_str(),
                runtime.reasoning_effort.as_deref()
            ),
            ("openai", "gpt-5.4", Some("high"))
        );

        // Restore the pending ACK exactly as after a lost delivery acknowledgement.
        let mut pending: pioneer_entity::agent_action_outbox::ActiveModel =
            pioneer_crud::load_agent_action_outbox(&db, action.as_str())
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
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 1);
        assert!(
            processor
                .cancel_root_agent_work_graph_for_turn(
                    &params.thread_id,
                    &accepted,
                    "destination stopped"
                )
                .await
                .unwrap()
        );
        assert!(
            processor
                .mark_turn_interrupted(
                    params.thread_id.clone(),
                    params.turn_id.clone(),
                    "destination stopped".into()
                )
                .await
        );
        assert!(
            processor
                .mark_turn_interrupted(
                    params.thread_id.clone(),
                    params.turn_id.clone(),
                    "repeat Stop".into()
                )
                .await
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(&db, execution_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "cancelled"
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(&db, source.execution_id.as_str())
                .await
                .unwrap()
                .unwrap()
                .status,
            source_terminal
        );
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 1);
        probe.release_main.notify_waiters();
    }
}

// The fixture's source execution has no model actor. Complete its canonical
// foreground Turn while retaining the source ledger for saved action delivery.
async fn finish_choice_source_foreground(
    processor: &Arc<MessageProcessor>,
    context: &TurnToolContext,
) {
    let (_, mut turn) = processor
        .crud_store
        .get_turn(&context.thread_id, &context.turn_id)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    processor
        .crud_store
        .materialize_native_agent_turn_event(
            pioneer_crud::CanonicalTurnEventPayload::TurnCompleted(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: context.workspace_id.clone(),
                    thread_id: context.thread_id.clone(),
                    turn: turn.clone(),
                },
            ),
            now_timestamp_secs(),
            None,
        )
        .await
        .unwrap();
    processor
        .thread_manager
        .commit_terminal_turn(&context.thread_id, &turn)
        .await
        .unwrap();
}

#[tokio::test]
async fn delegated_root_foreground_admission_rejects_competing_start_without_persisting_it() {
    for target_label in [
        "New delegated root",
        "Current thread",
        "Current collaboration",
    ] {
        let (processor, context, tools) = fixture().await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let target = if target_label == "New delegated root" {
            delegated_launch_target(&tools, &binding.options).await
        } else {
            // Both own-root routes must refuse while the source owns foreground.
            let target = binding
                .options
                .target_options
                .iter()
                .find(|target| target.label == target_label)
                .unwrap()
                .clone();
            let busy = invoke_with_mutation(
                &tools,
                "threads_turn_start",
                "own-root-busy",
                json!({"targetOptionId":target.id,"input":[{"type":"text","text":"Busy root"}]}),
                None,
            )
            .await;
            assert!(
                matches!(busy, Err(pioneer_tools::ToolError::ExecutionFailed(code)) if code == "agent_action_conflict")
            );
            assert!(
                pioneer_crud::load_agent_action_outbox(
                    &processor.crud_store.database_connection(),
                    crate::authorization::action_id_for_call("own-root-busy").as_str()
                )
                .await
                .unwrap()
                .is_none()
            );
            finish_choice_source_foreground(&processor, &context).await;
            target
        };
        let arguments = json!({"targetOptionId":target.id,"input":[{"type":"text","text":"One foreground owner"}]});
        let (first, second) = tokio::join!(
            invoke_with_mutation(
                &tools,
                "threads_turn_start",
                "foreground-a",
                arguments.clone(),
                None
            ),
            invoke_with_mutation(
                &tools,
                "threads_turn_start",
                "foreground-b",
                arguments,
                None
            ),
        );
        assert_ne!(
            first.is_ok(),
            second.is_ok(),
            "one foreground start must be refused"
        );
        let rejection = first
            .as_ref()
            .err()
            .or_else(|| second.as_ref().err())
            .unwrap();
        assert!(
            matches!(rejection, pioneer_tools::ToolError::ExecutionFailed(code) if code == "agent_action_conflict")
        );
        let db = processor.crud_store.database_connection();
        let mut accepted = Vec::new();
        for id in ["foreground-a", "foreground-b"] {
            if let Some(row) = pioneer_crud::load_agent_action_outbox(
                &db,
                crate::authorization::action_id_for_call(id).as_str(),
            )
            .await
            .unwrap()
            {
                accepted.push(row);
            }
        }
        assert_eq!(
            accepted.len(),
            1,
            "refusal cannot leave a second durable launch"
        );
        let payload: serde_json::Value = serde_json::from_str(&accepted[0].payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let thread = processor
            .crud_store
            .get_thread_model(&params.thread_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            thread
                .turns
                .iter()
                .filter(|turn| turn.status == TurnStatus::InProgress)
                .count(),
            1
        );
        let source = binding.adapter.lock().await.persistence_facts();
        let (_, accepted_turn) = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap();
        if target_label == "New delegated root" {
            assert!(
                processor
                    .cancel_root_agent_work_graph_for_turn(
                        &params.thread_id,
                        &accepted_turn,
                        "Stop new root"
                    )
                    .await
                    .unwrap()
            );
        }
        assert!(
            processor
                .mark_turn_interrupted(
                    params.thread_id.clone(),
                    params.turn_id.clone(),
                    "Stop before activation".into()
                )
                .await
        );
        let stopped = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(stopped.status, TurnStatus::Interrupted);
        assert!(
            processor
                .crud_store
                .native_cancellation_context(&params.turn_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(
                &db,
                payload["spawned_execution_id"].as_str().unwrap()
            )
            .await
            .unwrap()
            .unwrap()
            .status,
            "cancelled"
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(&db, source.execution_id.as_str())
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert!(
            processor
                .crud_store
                .get_turn_runtime_snapshot(&params.turn_id)
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
    }
}

#[tokio::test]
async fn queued_internal_thread_stop_releases_only_its_child_resources_before_dispatch() {
    use sea_orm::{ActiveModelTrait, Set};
    let probe = Arc::new(ChoiceRuntimeProbe::default());
    let (processor, context, tools) = fixture_with_probe(
        Default::default(),
        Default::default(),
        Default::default(),
        false,
        probe.clone(),
    )
    .await;
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let source = binding.adapter.lock().await.persistence_facts();
    let db = processor.crud_store.database_connection();
    let mut scope: pioneer_entity::agent_work_resource_scope::ActiveModel =
        pioneer_entity::agent_work_resource_scope::Entity::find_by_id(
            source.root_execution_id.as_str(),
        )
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .into();
    scope.max_concurrency = Set(1);
    scope.update(&db).await.unwrap();
    let creation = binding
        .options
        .thread_creation_options
        .iter()
        .find(|option| option.label == "Current collaboration")
        .unwrap();
    let created = call(
        &tools,
        "thread_create",
        "queued-thread",
        json!({"optionId":creation.id}),
    )
    .await;
    let target = created["targetOptions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|option| {
            !binding
                .options
                .target_options
                .iter()
                .any(|old| old.id == option["id"].as_str().unwrap())
        })
        .unwrap()["id"]
        .clone();
    call(
        &tools,
        "threads_turn_start",
        "queued-child",
        json!({"targetOptionId":target,"input":[{"type":"text","text":"Wait for capacity"}]}),
    )
    .await;
    let row = pioneer_crud::load_agent_action_outbox(
        &db,
        crate::authorization::action_id_for_call("queued-child").as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&row.payload_json).unwrap();
    let params: TurnStartParams =
        serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
    let child_id = payload["spawned_execution_id"].as_str().unwrap();
    let child = pioneer_crud::load_agent_execution(&db, child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.parent_execution_id.as_deref(),
        Some(source.execution_id.as_str())
    );
    assert_eq!(
        child.work_graph_root_execution_id,
        source.root_execution_id.as_str()
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_resource_state(&db, child_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "queued"
    );
    assert!(
        processor
            .mark_turn_interrupted(
                params.thread_id.clone(),
                params.turn_id.clone(),
                "Stop while queued".into()
            )
            .await
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_resource_state(&db, child_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    assert_eq!(
        pioneer_crud::load_agent_execution(&db, source.execution_id.as_str())
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
        .await
        .unwrap();
    assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn definitive_start_refusal_has_safe_cause_terminal_turn_and_released_resources() {
    use sea_orm::{ActiveModelTrait, Set};
    for denial in ["retired_identity", "suspended_initiator"] {
        let probe = Arc::new(ChoiceRuntimeProbe::default());
        let (processor, context, tools) = fixture_with_probe(
            Default::default(),
            Default::default(),
            Default::default(),
            false,
            probe.clone(),
        )
        .await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let target = delegated_launch_target(&tools, &binding.options).await;
        call(
        &tools,
        "threads_turn_start",
        "refused-start",
        json!({"targetOptionId":target.id,"input":[{"type":"text","text":"An accepted launch"}]}),
    )
    .await;
        let db = processor.crud_store.database_connection();
        let action = crate::authorization::action_id_for_call("refused-start");
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let execution_id = payload["spawned_execution_id"].as_str().unwrap();
        if denial == "retired_identity" {
            let mut identity: pioneer_entity::agent_identity::ActiveModel =
                pioneer_crud::load_agent_identity(
                    &db,
                    payload["dispatch"]["identity"]["id"].as_str().unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .into();
            identity.status = Set("retired".into());
            identity.update(&db).await.unwrap();
        } else {
            let authority = processor
                .load_turn_execution_authorization_context(&params.turn_id)
                .await
                .unwrap();
            db.execute_unprepared(&format!(
                "UPDATE gateway_principal SET status='suspended' WHERE id='{}'",
                authority.initiating_principal_id()
            ))
            .await
            .unwrap();
        }
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (row.status.as_str(), row.attempts, row.next_attempt_at),
            ("failed", 1, None)
        );
        assert_eq!(
            row.last_error.as_deref(),
            Some(if denial == "retired_identity" {
                "outbox_start_integrity_rejected"
            } else {
                "outbox_start_authority_denied"
            })
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Failed
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(&db, execution_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "failed"
        );
        let resource = pioneer_crud::load_agent_execution_resource_state(&db, execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resource.status, "failed");
        if let Some(permit) = resource.permit_id {
            assert_eq!(
                pioneer_entity::agent_running_permit::Entity::find_by_id(permit)
                    .one(&db)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "released"
            );
        }
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap()
                .attempts,
            1
        );
    }
}

#[tokio::test]
async fn persisted_foreground_conflict_refuses_pending_root_and_preserves_other_turn() {
    for target_label in [
        "New delegated root",
        "Current thread",
        "Current collaboration",
    ] {
        let probe = Arc::new(ChoiceRuntimeProbe::default());
        let (processor, context, tools) = fixture_with_probe(
            Default::default(),
            Default::default(),
            Default::default(),
            false,
            probe.clone(),
        )
        .await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let target = if target_label == "New delegated root" {
            delegated_launch_target(&tools, &binding.options).await
        } else {
            finish_choice_source_foreground(&processor, &context).await;
            binding
                .options
                .target_options
                .iter()
                .find(|target| target.label == target_label)
                .unwrap()
                .clone()
        };
        call(
        &tools,
        "threads_turn_start",
        "legacy-conflict",
        json!({"targetOptionId":target.id,"input":[{"type":"text","text":"Saved pending start"}]}),
    )
    .await;
        let db = processor.crud_store.database_connection();
        let action = crate::authorization::action_id_for_call("legacy-conflict");
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let (_, accepted) = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap();
        // Reproduce the formerly legal persisted conflict using the existing
        // concurrent-child admission. No runtime has been activated for either Turn.
        let mut conflicting = params.clone();
        conflicting.turn_id = "legacy-other-turn".into();
        let other = processor
            .thread_manager
            .concurrent_agent_turn_start_with_permission_profile(
                conflicting,
                accepted.permission_profile.clone(),
                accepted.author.clone().unwrap(),
            )
            .await
            .unwrap();
        processor
            .crud_store
            .materialize_turn_start(
                &other.materialization.thread,
                other.materialization.sandbox_mode,
                &other.materialization.turn,
                &other.materialization.input,
                accepted.author.as_ref().unwrap().actor.clone(),
            )
            .await
            .unwrap();
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(
            processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::Failed
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn(&params.thread_id, "legacy-other-turn")
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        let outbox = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outbox.last_error.as_deref(),
            Some("outbox_foreground_conflict")
        );
        assert!(outbox.next_attempt_at.is_none());
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
        assert!(
            processor
                .mark_turn_interrupted(
                    params.thread_id,
                    "legacy-other-turn".into(),
                    "fixture cleanup".into()
                )
                .await
        );
    }
}

#[tokio::test]
async fn member_creation_uses_both_workspace_variants_and_revocation_fences_the_shared_handler() {
    let (processor, context, tools) = fixture_with_principal_probe(
        Default::default(),
        Default::default(),
        Default::default(),
        false,
        Default::default(),
        true,
    )
    .await;
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let authority = processor
        .load_turn_execution_authorization_context(&context.turn_id)
        .await
        .unwrap();
    for (index, label) in ["Current collaboration", "New delegated root"]
        .into_iter()
        .enumerate()
    {
        let option = binding
            .options
            .thread_creation_options
            .iter()
            .find(|option| option.label == label)
            .unwrap();
        let result = call(
            &tools,
            "thread_create",
            &format!("member-create-{index}"),
            json!({"optionId":option.id}),
        )
        .await;
        assert_eq!(result["status"], "accepted");
    }
    assert!(
        invoke_with_mutation(
            &tools,
            "thread_create",
            "member-invalid-option",
            json!({"optionId":"O00000000000000000000"}),
            None
        )
        .await
        .is_err()
    );
    let option = binding.options.thread_creation_options[0].id.clone();
    processor.crud_store.database_connection().execute_unprepared(&format!(
        "DELETE FROM workspace_membership WHERE principal_id='P0000000000000000000M' AND workspace_id='{}'", context.workspace_id,
    )).await.unwrap();
    assert!(
        invoke_with_mutation(
            &tools,
            "thread_create",
            "member-revoked-create",
            json!({"optionId":option}),
            None
        )
        .await
        .is_err()
    );
    let db = processor.crud_store.database_connection();
    assert!(
        pioneer_crud::load_agent_action_outbox(
            &db,
            crate::authorization::action_id_for_call("member-revoked-create").as_str()
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(authority.grants_action(crate::authorization::ResourceAction::ThreadCreatePrivate));
    assert!(authority.grants_action(crate::authorization::ResourceAction::ThreadCreateWorkspace));
}

#[tokio::test]
async fn exhausted_child_start_cannot_reconcile_a_sibling_from_another_action() {
    use sea_orm::{ActiveModelTrait, Set};
    for attempts in [0, pioneer_crud::AGENT_ACTION_OUTBOX_MAX_ATTEMPTS] {
        let (processor, context, tools) = fixture().await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let creation = binding
            .options
            .thread_creation_options
            .iter()
            .find(|option| option.label == "Current collaboration")
            .unwrap();
        let created = call(
            &tools,
            "thread_create",
            "reconcile-thread",
            json!({"optionId":creation.id}),
        )
        .await;
        let target = created["targetOptions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|option| {
                !binding
                    .options
                    .target_options
                    .iter()
                    .any(|old| old.id == option["id"].as_str().unwrap())
            })
            .unwrap()["id"]
            .clone();
        let db = processor.crud_store.database_connection();
        let mut rows = Vec::new();
        for call_id in ["reconcile-first", "reconcile-sibling"] {
            call(
                &tools,
                "threads_turn_start",
                call_id,
                json!({
                    "targetOptionId":target, "input":[{"type":"text","text":"Child command"}]
                }),
            )
            .await;
            rows.push(
                pioneer_crud::load_agent_action_outbox(
                    &db,
                    crate::authorization::action_id_for_call(call_id).as_str(),
                )
                .await
                .unwrap()
                .unwrap(),
            );
        }
        let original: serde_json::Value = serde_json::from_str(&rows[0].payload_json).unwrap();
        let sibling: serde_json::Value = serde_json::from_str(&rows[1].payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(sibling["dispatch"]["params"].clone()).unwrap();
        let execution_id = sibling["spawned_execution_id"].as_str().unwrap();
        let execution = pioneer_crud::load_agent_execution(&db, execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            execution.parent_execution_id.as_deref(),
            Some(rows[0].owner_execution_id.as_str())
        );
        if attempts == 0 {
            // Ordinary replay must prove its own action before touching an already
            // projected sibling terminal. Use the production pre-activation Stop.
            assert!(
                processor
                    .mark_turn_interrupted(
                        params.thread_id.clone(),
                        params.turn_id.clone(),
                        "sibling stopped".into(),
                    )
                    .await
            );
        }
        let sibling_turn = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            sibling_turn.1.status,
            if attempts == 0 {
                TurnStatus::Interrupted
            } else {
                TurnStatus::InProgress
            }
        );
        let sibling_memory = processor
            .thread_manager
            .turn_get(&params.thread_id, &params.turn_id)
            .await;
        let sibling_owner = processor
            .crud_store
            .get_turn_execution(&params.turn_id)
            .await
            .unwrap();
        let execution = pioneer_crud::load_agent_execution(&db, execution_id)
            .await
            .unwrap()
            .unwrap();
        let own_params: TurnStartParams =
            serde_json::from_value(original["dispatch"]["params"].clone()).unwrap();
        let own_execution_id = original["spawned_execution_id"].as_str().unwrap();
        let own_turn = processor
            .crud_store
            .get_turn(&own_params.thread_id, &own_params.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(own_turn.1.status, TurnStatus::InProgress);
        let own_resource = pioneer_crud::load_agent_execution_resource_state(&db, own_execution_id)
            .await
            .unwrap()
            .unwrap();
        let own_execution = pioneer_crud::load_agent_execution(&db, own_execution_id)
            .await
            .unwrap()
            .unwrap();
        let resource = pioneer_crud::load_agent_execution_resource_state(&db, execution_id)
            .await
            .unwrap()
            .unwrap();
        let grant = pioneer_crud::load_agent_execution_grant(&db, execution_id)
            .await
            .unwrap()
            .unwrap();
        let mut held: pioneer_entity::agent_action_outbox::ActiveModel = rows[1].clone().into();
        held.next_attempt_at = Set(Some(pioneer_crud::utc_now() + chrono::Duration::hours(1)));
        held.update(&db).await.unwrap();
        let mut redirected = original.clone();
        redirected["spawned_execution_id"] = sibling["spawned_execution_id"].clone();
        redirected["dispatch"] = sibling["dispatch"].clone();
        let mut redirected_row: pioneer_entity::agent_action_outbox::ActiveModel =
            rows[0].clone().into();
        redirected_row.payload_json = Set(redirected.to_string());
        redirected_row.status = Set(if attempts == 0 { "pending" } else { "failed" }.into());
        redirected_row.attempts = Set(attempts);
        redirected_row.next_attempt_at = Set(None);
        redirected_row.update(&db).await.unwrap();
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(
            processor
                .crud_store
                .native_turn_has_accepted_terminal(&params.turn_id)
                .await
                .unwrap(),
            attempts == 0,
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap(),
            sibling_turn
        );
        assert_eq!(
            processor
                .thread_manager
                .turn_get(&params.thread_id, &params.turn_id)
                .await,
            sibling_memory
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn_execution(&params.turn_id)
                .await
                .unwrap(),
            sibling_owner
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(&db, execution_id)
                .await
                .unwrap()
                .unwrap(),
            execution
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn(&own_params.thread_id, &own_params.turn_id)
                .await
                .unwrap()
                .unwrap(),
            own_turn
        );
        assert!(
            !processor
                .crud_store
                .native_turn_has_accepted_terminal(&own_params.turn_id)
                .await
                .unwrap()
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_resource_state(&db, own_execution_id)
                .await
                .unwrap()
                .unwrap(),
            own_resource
        );
        assert_eq!(
            pioneer_crud::load_agent_execution(&db, own_execution_id)
                .await
                .unwrap()
                .unwrap(),
            own_execution
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_resource_state(&db, execution_id)
                .await
                .unwrap()
                .unwrap(),
            resource
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_grant(&db, execution_id)
                .await
                .unwrap()
                .unwrap(),
            grant
        );
        let unresolved = pioneer_crud::load_agent_action_outbox(&db, &rows[0].action_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            unresolved.attempts,
            if attempts == 0 { 1 } else { attempts }
        );
        assert_eq!(unresolved.status, "failed");
        assert!(unresolved.delivered_at.is_none());
        assert!(unresolved.next_attempt_at.is_some());
        if attempts == 0 {
            continue;
        }
        // The original exact grant remains reconcilable; the sibling remains live.
        let mut restored: pioneer_entity::agent_action_outbox::ActiveModel = unresolved.into();
        restored.payload_json = Set(original.to_string());
        restored.next_attempt_at =
            Set(Some(pioneer_crud::utc_now() - chrono::Duration::seconds(1)));
        restored.update(&db).await.unwrap();
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(
            pioneer_crud::load_agent_action_outbox(&db, &rows[0].action_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "delivered"
        );
        assert_eq!(
            processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_resource_state(&db, execution_id)
                .await
                .unwrap()
                .unwrap(),
            resource
        );
    }
}

#[tokio::test]
async fn start_outbox_waits_for_accepted_terminal_projection_before_acknowledging_runtime() {
    use sea_orm::{ActiveModelTrait, Set};
    for attempts in [1, pioneer_crud::AGENT_ACTION_OUTBOX_MAX_ATTEMPTS] {
        let probe = Arc::new(ChoiceRuntimeProbe::default());
        probe.hold_main.store(true, Ordering::SeqCst);
        let (processor, context, tools) = fixture_with_probe(
            Default::default(),
            Default::default(),
            Default::default(),
            false,
            probe.clone(),
        )
        .await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let target = delegated_launch_target(&tools, &binding.options).await;
        call(
            &tools,
            "threads_turn_start",
            "pending-terminal-start",
            json!({
                "targetOptionId":target.id, "input":[{"type":"text","text":"Start once"}]
            }),
        )
        .await;
        let db = processor.crud_store.database_connection();
        let action = crate::authorization::action_id_for_call("pending-terminal-start");
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let execution_id = payload["spawned_execution_id"].as_str().unwrap();
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        timeout(Duration::from_secs(5), probe.main_started.notified())
            .await
            .unwrap();
        let owner = processor
            .crud_store
            .get_turn_execution(&params.turn_id)
            .await
            .unwrap()
            .unwrap();
        assert!(owner.started_at.is_some());
        let (_, mut terminal) = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap();
        terminal.status = TurnStatus::Failed;
        terminal.error = Some("fixture terminal outcome".into());
        // Reproduce process loss between the real canonical append and projector.
        // Dispatch still runs normally and must inspect the pending acceptance.
        processor
            .crud_store
            .accept_native_turn_event_without_projection_for_test(
                pioneer_crud::CanonicalTurnEventPayload::TurnFailed(
                    pioneer_protocol::TurnFailedNotification {
                        workspace_id: context.workspace_id.clone(),
                        thread_id: params.thread_id.clone(),
                        turn: terminal.clone(),
                    },
                ),
                now_timestamp_secs(),
                &owner.owner_id,
            )
            .await
            .unwrap();
        let mut lost: pioneer_entity::agent_action_outbox::ActiveModel =
            pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap()
                .into();
        lost.status = Set("pending".into());
        lost.attempts = Set(attempts);
        lost.delivered_at = Set(None);
        lost.next_attempt_at = Set(None);
        lost.update(&db).await.unwrap();
        let pending =
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
        assert_eq!(pending.delivered, 0);
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.status, "failed");
        assert!(row.delivered_at.is_none());
        assert!(row.next_attempt_at.is_some());
        assert_eq!(
            processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1
                .status,
            TurnStatus::InProgress
        );
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 1);
        let replay = processor
            .crud_store
            .with_maintenance_access()
            .replay_due_turn_event_projections(now_timestamp_secs(), 64)
            .await
            .unwrap();
        assert_eq!(replay.projected, 1);
        assert_eq!(replay.failed, 0);
        let mut due: pioneer_entity::agent_action_outbox::ActiveModel = row.into();
        due.next_attempt_at = Set(Some(pioneer_crud::utc_now() - chrono::Duration::seconds(1)));
        due.update(&db).await.unwrap();
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert_eq!(
            pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap()
                .status,
            "delivered"
        );
        assert_eq!(
            processor
                .thread_manager
                .turn_get(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .1,
            terminal
        );
        assert_eq!(
            pioneer_crud::load_agent_execution_resource_state(&db, execution_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "failed"
        );
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 1);
        let _ = processor
            .agent_manager
            .cancel_turn(&params.thread_id, &params.turn_id, "fixture cleanup")
            .await;
        probe.release_main.notify_waiters();
    }
}

// These faults occur at the real storage/activation boundaries. The fixture
// database is disposable; no result is supplied in place of production dispatch.
#[tokio::test]
async fn prepared_agent_start_retries_before_claim_but_reconciles_after_claim_or_exhaustion() {
    use sea_orm::{ActiveModelTrait, PaginatorTrait, Set};
    for fault in [
        "lease",
        "claim_storage",
        "authority_read",
        "exhausted_storage",
        "exhausted_terminal",
        "claimed_unknown",
        "actor_admission_storage",
        "terminal_memory",
        "terminal_restart",
        "terminal_resume",
    ] {
        let probe = Arc::new(ChoiceRuntimeProbe::default());
        probe.hold_main.store(true, Ordering::SeqCst);
        let (processor, context, tools) = fixture_with_probe(
            Default::default(),
            Default::default(),
            Default::default(),
            false,
            probe.clone(),
        )
        .await;
        let binding = processor
            .agent_action_binding(&context.turn_id)
            .await
            .unwrap();
        let target = delegated_launch_target(&tools, &binding.options).await;
        call(&tools, "threads_turn_start", "storage-boundary-start", json!({
            "targetOptionId":target.id, "input":[{"type":"text","text":"One permitted model call"}]
        })).await;
        let db = processor.crud_store.database_connection();
        let action = crate::authorization::action_id_for_call("storage-boundary-start");
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.payload_json).unwrap();
        let params: TurnStartParams =
            serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
        let execution_id = payload["spawned_execution_id"].as_str().unwrap();
        let exhausted = fault.starts_with("exhausted") || fault.starts_with("terminal_");
        if exhausted {
            let mut last: pioneer_entity::agent_action_outbox::ActiveModel = row.clone().into();
            last.attempts = Set(7);
            last.update(&db).await.unwrap();
        }
        if fault == "exhausted_terminal" {
            // Force a semantic refusal, then fail its real canonical append.
            let identity_id = payload["dispatch"]["identity"]["id"].as_str().unwrap();
            db.execute_unprepared(&format!(
                "UPDATE agent_identity SET status='retired' WHERE id='{identity_id}'"
            ))
            .await
            .unwrap();
            db.execute_unprepared(&format!("CREATE TRIGGER fail_terminal_append BEFORE INSERT ON turn_event WHEN NEW.turn_id='{}' BEGIN SELECT RAISE(ABORT,'fixture terminal storage unavailable'); END", params.turn_id)).await.unwrap();
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
            assert!(
                !processor
                    .crud_store
                    .native_turn_has_accepted_terminal(&params.turn_id)
                    .await
                    .unwrap()
            );
            db.execute_unprepared("DROP TRIGGER fail_terminal_append")
                .await
                .unwrap();
        } else {
            let barrier = match fault {
                "authority_read" => "__agent_start_before_authority_read__",
                "claimed_unknown"
                | "actor_admission_storage"
                | "terminal_memory"
                | "terminal_restart"
                | "terminal_resume" => "__agent_start_after_activation_claim__",
                _ => "__agent_start_before_activation_claim__",
            };
            processor.arm_completed_history_preparation_barrier(barrier);
            let mut dispatch = Some(tokio::spawn({
                let processor = processor.clone();
                async move {
                    crate::message::agent_action_tools::process_due_agent_action_outbox(
                        &processor, 64,
                    )
                    .await
                    .unwrap()
                }
            }));
            timeout(
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            if fault != "authority_read" {
                assert!(
                    processor
                        .crud_store
                        .get_turn_runtime_snapshot(&params.turn_id)
                        .await
                        .unwrap()
                        .is_some()
                );
            }
            assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
            match fault {
                "lease" => {
                    db.execute_unprepared(&format!("UPDATE turn_execution SET lease_until='2000-01-01 00:00:00+00:00' WHERE turn_id='{}'", params.turn_id)).await.unwrap();
                }
                "claim_storage" | "exhausted_storage" => {
                    db.execute_unprepared(&format!("CREATE TRIGGER fail_activation_claim BEFORE UPDATE OF status ON turn_execution WHEN OLD.turn_id='{}' AND NEW.status='running' BEGIN SELECT RAISE(ABORT,'fixture storage unavailable'); END", params.turn_id)).await.unwrap();
                }
                "authority_read" => {
                    // Earlier admission/provenance reads succeeded. Fail the actual
                    // persisted-context read, preserving its typed driver cause.
                    db.execute_unprepared("ALTER TABLE turn RENAME TO unavailable_turn")
                        .await
                        .unwrap();
                }
                "actor_admission_storage" => {
                    processor
                        .agent_manager
                        .remove_thread(&params.thread_id)
                        .await;
                    db.execute_unprepared("CREATE TRIGGER fail_recovery_admission BEFORE INSERT ON recovery_job BEGIN SELECT RAISE(ABORT,'fixture recovery storage unavailable'); END").await.unwrap();
                    db.execute_unprepared(&format!("CREATE TRIGGER fail_dispatch_terminal BEFORE INSERT ON turn_event WHEN NEW.turn_id='{}' BEGIN SELECT RAISE(ABORT,'fixture terminal storage unavailable'); END", params.turn_id)).await.unwrap();
                }
                "claimed_unknown" | "terminal_memory" | "terminal_restart" | "terminal_resume" => {
                    // Loss between durable claim and runtime admission is genuinely
                    // ambiguous. Never turn this replay into a second activation.
                    let dispatch = dispatch.take().unwrap();
                    dispatch.abort();
                    let _ = dispatch.await;
                }
                _ => unreachable!(),
            }
            if fault != "claimed_unknown" && !fault.starts_with("terminal_") {
                processor.release_completed_history_preparation_barrier();
                dispatch.take().unwrap().await.unwrap();
            }
            match fault {
                "lease" => {
                    let mut owner: pioneer_entity::turn_execution::ActiveModel =
                        pioneer_entity::turn_execution::Entity::find_by_id(&params.turn_id)
                            .one(&db)
                            .await
                            .unwrap()
                            .unwrap()
                            .into();
                    owner.lease_until = Set(pioneer_crud::utc_now() + chrono::Duration::hours(1));
                    owner.update(&db).await.unwrap();
                }
                "claim_storage" | "exhausted_storage" => {
                    db.execute_unprepared("DROP TRIGGER fail_activation_claim")
                        .await
                        .unwrap();
                }
                "actor_admission_storage" => {
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
                            .find_unresolved_recovery_job_for_turn(&params.turn_id)
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert_ne!(
                        pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                            .await
                            .unwrap()
                            .unwrap()
                            .status,
                        "delivered"
                    );
                    db.execute_unprepared(
                        "DROP TRIGGER fail_recovery_admission; DROP TRIGGER fail_dispatch_terminal",
                    )
                    .await
                    .unwrap();
                }
                "authority_read" => {
                    db.execute_unprepared("ALTER TABLE unavailable_turn RENAME TO turn")
                        .await
                        .unwrap();
                }
                _ => {}
            }
        }
        assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
        let saved = processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(
            saved.status,
            TurnStatus::InProgress,
            "{fault}: unavailable proof/storage is not terminal"
        );
        let row = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
            .await
            .unwrap()
            .unwrap();
        if fault == "authority_read" {
            assert_eq!(
                row.last_error.as_deref(),
                Some("outbox_storage_unavailable")
            );
        }
        if fault == "exhausted_storage" {
            pioneer_crud::compact_terminal_agent_action_ledger(
                &db,
                pioneer_crud::utc_now() + chrono::Duration::days(32),
                100,
            )
            .await
            .unwrap();
            let unresolved = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(unresolved.payload_json, row.payload_json);
        }
        let mut due: pioneer_entity::agent_action_outbox::ActiveModel = row.into();
        due.next_attempt_at = Set(Some(pioneer_crud::utc_now() - chrono::Duration::seconds(1)));
        due.update(&db).await.unwrap();
        if fault.starts_with("terminal_") {
            processor.arm_completed_history_preparation_barrier(
                "__agent_start_after_terminal_acceptance__",
            );
            let delivery = tokio::spawn({
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
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            assert!(
                processor
                    .crud_store
                    .native_turn_has_accepted_terminal(&params.turn_id)
                    .await
                    .unwrap()
            );
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(&params.thread_id, &params.turn_id)
                    .await
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            db.execute_unprepared("ALTER TABLE turn RENAME TO unavailable_turn")
                .await
                .unwrap();
            processor.release_completed_history_preparation_barrier();
            delivery.await.unwrap();
            db.execute_unprepared("ALTER TABLE unavailable_turn RENAME TO turn")
                .await
                .unwrap();
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(&params.thread_id, &params.turn_id)
                    .await
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            let mut retry: pioneer_entity::agent_action_outbox::ActiveModel =
                pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                    .await
                    .unwrap()
                    .unwrap()
                    .into();
            retry.next_attempt_at =
                Set(Some(pioneer_crud::utc_now() - chrono::Duration::seconds(1)));
            retry.update(&db).await.unwrap();
        }
        if fault == "terminal_resume" {
            processor
                .arm_completed_history_preparation_barrier("__agent_terminal_before_memory_sync__");
            let replay = tokio::spawn({
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
                Duration::from_secs(5),
                processor.wait_for_completed_history_preparation_barrier(),
            )
            .await
            .unwrap();
            let before = processor
                .crud_store
                .get_turn_execution(&params.turn_id)
                .await
                .unwrap()
                .unwrap();
            let now = now_timestamp_secs();
            let job = processor
                .crud_store
                .enqueue_recovery_job(
                    params.turn_id.clone(),
                    "reasoning_resume".into(),
                    pioneer_protocol::TurnItemType::Reasoning,
                    None,
                    pioneer_protocol::RecoveryTrigger::Timeout,
                    pioneer_protocol::RecoveryAction::BlockResumable,
                    None,
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
                    pioneer_protocol::RecoveryJobStatus::Blocked,
                    None,
                    now,
                )
                .await
                .unwrap();
            assert!(
                processor
                    .resume_blocked_turn_control(
                        &context.workspace_id,
                        &params.thread_id,
                        &params.turn_id,
                        Some(&job.id),
                        now
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
            let canonical = processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1;
            processor
                .ensure_runtime_turn_loaded_for_lifecycle(
                    &context.workspace_id,
                    &params.thread_id,
                    &canonical,
                )
                .await
                .unwrap();
            let resumed = processor
                .crud_store
                .get_turn_execution(&params.turn_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(resumed.owner_generation, before.owner_generation + 1);
            assert_eq!(resumed.owner_id, before.owner_id);
            assert_eq!(canonical.status, TurnStatus::InProgress);
            // A late actor completion carries the old captured durable owner;
            // even the real cancellation consumer must reject that generation.
            let stale = processor
                .commit_durable_agent_event(pioneer_protocol::AgentDurableEvent::TurnInterrupted {
                    thread_id: params.thread_id.clone(),
                    turn_id: params.turn_id.clone(),
                    reason: "old initial Stop".into(),
                    recovery: None,
                    cancellation_owner: Some((before.owner_id.clone(), before.owner_generation)),
                })
                .await
                .unwrap_err();
            assert!(!stale.is_retryable());
            assert_eq!(stale.code(), "execution_generation_changed");
            processor.release_completed_history_preparation_barrier();
            replay.await.unwrap();
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(&params.thread_id, &params.turn_id)
                    .await
                    .unwrap()
                    .1,
                canonical
            );
            let mut next = params.clone();
            next.turn_id = "competing-with-resume".into();
            let rejected = processor
                .thread_manager
                .agent_turn_start_with_permission_profile(
                    next,
                    canonical.permission_profile.clone(),
                    canonical.author.clone().unwrap(),
                )
                .await
                .unwrap_err();
            assert!(
                rejected
                    .downcast_ref::<crate::thread::ForegroundTurnConflict>()
                    .is_some()
            );
            assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
            // A different process cannot treat the resumed generation as an old terminal receipt.
            let restarted = restart_choice_processor(&processor);
            let mut retry: pioneer_entity::agent_action_outbox::ActiveModel =
                pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                    .await
                    .unwrap()
                    .unwrap()
                    .into();
            retry.status = Set("pending".into());
            retry.next_attempt_at = Set(None);
            retry.delivered_at = Set(None);
            retry.update(&db).await.unwrap();
            crate::message::agent_action_tools::process_due_agent_action_outbox(&restarted, 64)
                .await
                .unwrap();
            let retained = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap();
            assert_ne!(retained.status, "delivered");
            assert!(retained.next_attempt_at.is_some());
            assert_eq!(
                processor
                    .crud_store
                    .get_turn_execution(&params.turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .owner_generation,
                resumed.owner_generation
            );
            assert_eq!(
                processor
                    .crud_store
                    .get_turn(&params.thread_id, &params.turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1,
                canonical
            );
            continue;
        }
        if fault == "terminal_restart" {
            let executions_before = pioneer_entity::agent_execution::Entity::find()
                .count(&db)
                .await
                .unwrap();
            let resources_before = pioneer_entity::agent_execution_resource_state::Entity::find()
                .count(&db)
                .await
                .unwrap();
            let permits_before = pioneer_entity::agent_running_permit::Entity::find()
                .count(&db)
                .await
                .unwrap();
            let restarted = restart_choice_processor(&processor);
            assert_ne!(
                restarted.turn_execution_owner_id,
                processor.turn_execution_owner_id
            );
            restarted
                .ensure_thread_loaded(&params.thread_id, &context.workspace_id)
                .await
                .unwrap();
            assert!(
                restarted
                    .thread_manager
                    .thread_get(&params.thread_id)
                    .await
                    .unwrap()
                    .turns
                    .is_empty()
            );
            let first =
                crate::message::agent_action_tools::process_due_agent_action_outbox(&restarted, 64)
                    .await
                    .unwrap();
            assert_eq!(first.delivered, 1); // confirms the terminal dispatch boundary
            let closed = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(closed.attempts, 8);
            assert!(closed.next_attempt_at.is_none());
            assert_eq!(closed.status, "delivered");
            assert_eq!(
                pioneer_entity::agent_execution::Entity::find()
                    .count(&db)
                    .await
                    .unwrap(),
                executions_before
            );
            assert_eq!(
                pioneer_entity::agent_execution_resource_state::Entity::find()
                    .count(&db)
                    .await
                    .unwrap(),
                resources_before
            );
            assert_eq!(
                pioneer_entity::agent_running_permit::Entity::find()
                    .count(&db)
                    .await
                    .unwrap(),
                permits_before
            );

            assert!(
                !pioneer_crud::claim_agent_action_outbox(&db, 2)
                    .await
                    .unwrap()
                    .rows
                    .iter()
                    .any(|row| row.id == closed.id)
            );
            assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
            assert!(
                restarted
                    .thread_manager
                    .thread_get(&params.thread_id)
                    .await
                    .unwrap()
                    .turns
                    .is_empty()
            );
        }
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        if exhausted
            || matches!(
                fault,
                "claimed_unknown"
                    | "actor_admission_storage"
                    | "terminal_memory"
                    | "terminal_restart"
                    | "terminal_resume"
            )
        {
            let turn = processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1;
            assert_eq!(turn.status, TurnStatus::Blocked, "{fault}");
            assert!(
                processor
                    .crud_store
                    .native_turn_has_accepted_terminal(&params.turn_id)
                    .await
                    .unwrap()
            );
            let owner = processor
                .crud_store
                .get_turn_execution(&params.turn_id)
                .await
                .unwrap()
                .unwrap();
            assert!(!owner.status.is_active());
            let resource = pioneer_crud::load_agent_execution_resource_state(&db, execution_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(resource.status, "failed");
            assert_eq!(
                pioneer_crud::load_agent_execution(&db, execution_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "blocked"
            );
            if let Some(permit) = resource.permit_id {
                assert_eq!(
                    pioneer_entity::agent_running_permit::Entity::find_by_id(permit)
                        .one(&db)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    "released"
                );
            }
            assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                    .await
                    .unwrap()
                    .unwrap()
                    .attempts,
                if exhausted {
                    8
                } else if fault.starts_with("terminal_") {
                    3
                } else {
                    2
                }
            );
        } else {
            timeout(Duration::from_secs(5), probe.main_started.notified())
                .await
                .unwrap();
            assert_eq!(probe.main_calls.load(Ordering::SeqCst), 1, "{fault}");
            assert_eq!(
                processor
                    .crud_store
                    .get_turn(&params.thread_id, &params.turn_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .status,
                TurnStatus::InProgress
            );
            // Lose the delivery ACK while preserving the actual native owner.
            let mut lost: pioneer_entity::agent_action_outbox::ActiveModel =
                pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                    .await
                    .unwrap()
                    .unwrap()
                    .into();
            lost.status = Set("pending".into());
            lost.delivered_at = Set(None);
            lost.next_attempt_at = Set(None);
            lost.update(&db).await.unwrap();
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
            assert_eq!(
                probe.main_calls.load(Ordering::SeqCst),
                1,
                "{fault}: replay"
            );
            assert!(
                processor
                    .mark_turn_interrupted(
                        params.thread_id.clone(),
                        params.turn_id.clone(),
                        "fixture Stop".into()
                    )
                    .await
            );
            let _ = processor
                .agent_manager
                .cancel_turn(&params.thread_id, &params.turn_id, "fixture Stop")
                .await;
            probe.release_main.notify_waiters();
        }
        if fault == "terminal_memory" {
            let canonical = processor
                .crud_store
                .get_turn(&params.thread_id, &params.turn_id)
                .await
                .unwrap()
                .unwrap()
                .1;
            assert_eq!(
                processor
                    .thread_manager
                    .turn_get(&params.thread_id, &params.turn_id)
                    .await
                    .unwrap()
                    .1,
                canonical
            );
            let mut next = params.clone();
            next.turn_id = "after-projected-blocked".into();
            let admitted = processor
                .thread_manager
                .agent_turn_start_with_permission_profile(
                    next,
                    canonical.permission_profile.clone(),
                    canonical.author.unwrap(),
                )
                .await
                .unwrap();
            processor
                .thread_manager
                .rollback_turn_start(admitted.rollback_context)
                .await;
        }
        if fault == "exhausted_terminal" {
            pioneer_crud::compact_terminal_agent_action_ledger(
                &db,
                pioneer_crud::utc_now() + chrono::Duration::days(32),
                100,
            )
            .await
            .unwrap();
            let compacted = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap();
            let compacted_payload: serde_json::Value =
                serde_json::from_str(&compacted.payload_json).unwrap();
            assert_eq!(compacted.status, "delivered");
            assert!(compacted_payload.get("_pioneer_compacted").is_some());
            assert_eq!(compacted_payload["dispatch"]["turn_id"], params.turn_id);
            assert!(compacted_payload["dispatch"].get("params").is_none());
            // Already-compacted older receipts may lack dispatch entirely. The
            // immutable source grant restores only the canonical terminal link.
            let mut legacy = compacted_payload.clone();
            legacy.as_object_mut().unwrap().remove("dispatch");
            let mut lost_ack: pioneer_entity::agent_action_outbox::ActiveModel =
                compacted.clone().into();
            lost_ack.payload_json = Set(legacy.to_string());
            // Legacy versions retained this confirmed exhausted result as
            // failed/8/NULL. It must be ACKed once, then leave the due index.
            lost_ack.status = Set("failed".into());
            lost_ack.delivered_at = Set(None);
            lost_ack.next_attempt_at = Set(None);
            lost_ack.update(&db).await.unwrap();
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap();
            let reconciled = pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reconciled.attempts, 8);
            assert_eq!(reconciled.status, "delivered");
            assert!(reconciled.next_attempt_at.is_none());
            for _ in 0..2 {
                assert!(
                    !pioneer_crud::claim_agent_action_outbox(&db, 64)
                        .await
                        .unwrap()
                        .rows
                        .iter()
                        .any(|row| row.id == compacted.id)
                );
            }
            assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
        }
        crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
            .await
            .unwrap();
        assert!(
            pioneer_crud::load_agent_action_outbox(&db, action.as_str())
                .await
                .unwrap()
                .unwrap()
                .attempts
                <= 8
        );
    }
}

fn restart_choice_processor(processor: &Arc<MessageProcessor>) -> Arc<MessageProcessor> {
    Arc::new(MessageProcessor::new(
        Arc::new(ThreadManager::new("composer-model", "openai")),
        processor.provider_registry.clone(),
        Arc::new(SessionManager::new()),
        processor.workspace_manager.clone(),
        processor.crud_store.clone(),
        processor.gateway_secrets.clone(),
        test_summary_config(),
        test_tool_loop_config(),
    ))
}

#[tokio::test]
async fn recovery_terminalization_cannot_apply_blocked_over_a_resumed_generation() {
    let probe = Arc::new(ChoiceRuntimeProbe::default());
    let (processor, context, tools) = fixture_with_probe(
        Default::default(),
        Default::default(),
        Default::default(),
        false,
        probe.clone(),
    )
    .await;
    processor.bind_agent_tool_bridges().await;
    let binding = processor
        .agent_action_binding(&context.turn_id)
        .await
        .unwrap();
    let target = delegated_launch_target(&tools, &binding.options).await;
    call(
        &tools,
        "threads_turn_start",
        "recovery-sync-start",
        json!({
            "targetOptionId":target.id, "input":[{"type":"text","text":"Recover one admitted turn"}]
        }),
    )
    .await;
    let db = processor.crud_store.database_connection();
    let row = pioneer_crud::load_agent_action_outbox(
        &db,
        crate::authorization::action_id_for_call("recovery-sync-start").as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    let payload: JsonValue = serde_json::from_str(&row.payload_json).unwrap();
    let params: TurnStartParams =
        serde_json::from_value(payload["dispatch"]["params"].clone()).unwrap();
    // Keep the real prepared snapshot and activation fence, before actor/model
    // admission. The recovery worker owns the ensuing terminal transition.
    processor.arm_completed_history_preparation_barrier("__agent_start_after_activation_claim__");
    let dispatch = tokio::spawn({
        let processor = processor.clone();
        async move {
            crate::message::agent_action_tools::process_due_agent_action_outbox(&processor, 64)
                .await
                .unwrap()
        }
    });
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    dispatch.abort();
    let _ = dispatch.await;
    processor.release_completed_history_preparation_barrier();
    let before = processor
        .crud_store
        .get_turn_execution(&params.turn_id)
        .await
        .unwrap()
        .unwrap();
    let now = now_timestamp_secs();
    let job = processor
        .crud_store
        .enqueue_recovery_job(
            params.turn_id.clone(),
            "recovery-sync-reasoning".into(),
            pioneer_protocol::TurnItemType::Reasoning,
            None,
            pioneer_protocol::RecoveryTrigger::Timeout,
            pioneer_protocol::RecoveryAction::BlockResumable,
            None,
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
    assert!(
        processor
            .crud_store
            .mark_recovery_job_terminal(
                &job.id,
                pioneer_protocol::RecoveryJobStatus::Blocked,
                None,
                now,
            )
            .await
            .unwrap()
    );
    processor.arm_completed_history_preparation_barrier("__agent_terminal_before_memory_sync__");
    let recovery = tokio::spawn({
        let processor = processor.clone();
        async move {
            processor
                .process_due_recovery_terminalizations(now, 64)
                .await
                .unwrap()
        }
    });
    timeout(
        Duration::from_secs(5),
        processor.wait_for_completed_history_preparation_barrier(),
    )
    .await
    .unwrap();
    assert_eq!(
        processor
            .crud_store
            .get_turn(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .unwrap()
            .1
            .status,
        TurnStatus::Blocked
    );
    assert!(
        processor
            .resume_blocked_turn_control(
                &context.workspace_id,
                &params.thread_id,
                &params.turn_id,
                Some(&job.id),
                now,
            )
            .await
            .unwrap()
            .is_some()
    );
    let resumed = processor
        .crud_store
        .get_turn_execution(&params.turn_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.owner_id, before.owner_id);
    assert_eq!(resumed.owner_generation, before.owner_generation + 1);
    processor.release_completed_history_preparation_barrier();
    assert_eq!(recovery.await.unwrap(), 1);
    let canonical = processor
        .crud_store
        .get_turn(&params.thread_id, &params.turn_id)
        .await
        .unwrap()
        .unwrap()
        .1;
    assert_eq!(canonical.status, TurnStatus::InProgress);
    assert_eq!(
        processor
            .thread_manager
            .turn_get(&params.thread_id, &params.turn_id)
            .await
            .unwrap()
            .1,
        canonical
    );
    let mut competing = params.clone();
    competing.turn_id = "competing-with-recovery-resume".into();
    let conflict = processor
        .thread_manager
        .agent_turn_start_with_permission_profile(
            competing,
            canonical.permission_profile.clone(),
            canonical.author.clone().unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        conflict
            .downcast_ref::<crate::thread::ForegroundTurnConflict>()
            .is_some()
    );
    assert_eq!(probe.main_calls.load(Ordering::SeqCst), 0);
}
