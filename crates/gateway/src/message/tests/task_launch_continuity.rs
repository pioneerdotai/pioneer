//! Storage/admission regressions and assertions shared by the controlled
//! materialization fixtures in the parent module. No real provider is used.
use super::*;
use crate::message::task_agent_executor::{
    TaskParentRuntimeContext, exact_agent_execution_author,
    materialize_task_agent_action_binding_for_execution, persist_task_agent_execution_graph,
    resolve_task_child_execution_authorization_context,
};

/// Check the stored result of the complete Task-child materialization path,
/// including every security intersection, rather than only graph admission.
pub(super) async fn assert_materialized_child_security(
    processor: &Arc<MessageProcessor>,
    run: &TaskRun,
    lineage: &TestChildRuntimeAnchor,
    expected_parent: &str,
    backend: crate::turn_security::TurnSecurityResolverExecutionBackend,
) {
    assert_eq!(lineage.created_by_turn_id.as_deref(), Some(expected_parent));
    let security = processor
        .crud_store
        .get_turn_execution_security_snapshot(&lineage.child_turn_id)
        .await
        .unwrap()
        .unwrap()
        .snapshot;
    assert_eq!(
        security.parent_cap.as_ref().unwrap().parent_turn_id,
        expected_parent
    );
    let parent_security = processor
        .crud_store
        .get_turn_execution_security_snapshot(expected_parent)
        .await
        .unwrap()
        .unwrap()
        .snapshot;
    let response = processor
        .crud_store
        .get_task(&run.task_id)
        .await
        .unwrap()
        .unwrap();
    let cap = pioneer_protocol::task_permission_cap_snapshot(
        response.agent_specs[0].permission_cap.as_ref().unwrap(),
    );
    assert_eq!(
        pioneer_protocol::intersect_turn_permission_profiles(
            &security.permission_profile,
            &cap,
            pioneer_protocol::TurnPermissionProfileSource::TaskPermissionCap,
        ),
        security.permission_profile,
    );
    if let Some(policy) = response.agent_specs[0].tool_policy.as_ref() {
        if !policy.network_access {
            assert_eq!(
                security.permission_profile.effective_policy.network,
                pioneer_protocol::PermissionBehavior::Deny,
            );
        }
        if policy.write_mode == pioneer_protocol::TaskAgentWriteMode::ReadOnly {
            assert_eq!(
                security.permission_profile.effective_policy.file_write,
                pioneer_protocol::PermissionBehavior::Deny,
            );
        }
    }
    let native = matches!(
        &backend,
        crate::turn_security::TurnSecurityResolverExecutionBackend::NativeApiProvider { .. }
    );
    let mut expected = crate::turn_security::resolve_task_child_execution_security_for_backend(
        &response.task.workspace_id,
        expected_parent,
        &parent_security,
        response.agent_specs[0].security_cap.as_ref().unwrap(),
        security.permission_profile.clone(),
        backend,
        &lineage.child_thread_id,
        &lineage.child_turn_id,
        security.created_at_unix_ms,
    )
    .unwrap();
    if native {
        processor
            .add_native_turn_runtime_sandbox_roots(
                &mut expected,
                &response.task.workspace_id,
                &lineage.child_thread_id,
                &lineage.child_turn_id,
            )
            .unwrap();
    }
    assert_eq!(
        security, expected,
        "persisted security must derive from the lineage parent"
    );
    if expected_parent == run.id {
        let execution = processor
            .crud_store
            .load_execution_for_run(&run.id)
            .await
            .unwrap()
            .unwrap();
        let (_, occurrence) = processor
            .crud_store
            .get_turn(lineage.created_by_thread_id.as_deref().unwrap(), &run.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            occurrence.author,
            Some(
                exact_agent_execution_author(processor, &execution.id)
                    .await
                    .unwrap()
            )
        );
    }
}

async fn processor_with_registry(
    registry: Arc<pioneer_provider::ProviderRegistry>,
) -> (Arc<MessageProcessor>, String) {
    let (workspace_manager, store, workspace) = setup_workspace_manager().await;
    let processor = with_enabled_test_cli_runtime_catalog(MessageProcessor::new(
        Arc::new(ThreadManager::new("test-model", "openai")),
        registry,
        Arc::new(SessionManager::new()),
        workspace_manager,
        store,
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    ));
    let processor = Arc::new(processor);
    ensure_test_superuser_execution_turn(
        &processor,
        &workspace,
        "continuity-root",
        "continuity-parent",
    )
    .await;
    (processor, workspace)
}

async fn task_fixture(
    processor: &Arc<MessageProcessor>,
    workspace: &str,
    cli: bool,
) -> (
    pioneer_protocol::TaskGetResponse,
    TaskRun,
    pioneer_protocol::TaskRunExecution,
) {
    let mut params = test_task_create_params(
        workspace,
        "continuity-root",
        "continuity-parent",
        "continuity",
        3,
    );
    if cli {
        params = detached_cli_task_create_params(
            workspace,
            "continuity-root",
            "continuity-parent",
            "codex",
            CLIAgentRuntimeKind::Codex,
            "pinned-cli-model",
            "continuity",
        );
        params.launch = Some(
            exact_cli_task_launch_for_test(
                processor,
                workspace,
                "cli_runtime:codex",
                "pinned-cli-model",
                "codex",
            )
            .await
            .unwrap(),
        );
    }
    let created = create_task_for_test(processor, params).await.unwrap();
    let run = created.run.unwrap();
    let execution = processor
        .crud_store
        .reserve_execution_for_run(&run.id, TaskExecutorKind::Agent, run.created_at)
        .await
        .unwrap();
    let response = processor
        .crud_store
        .get_task(&run.task_id)
        .await
        .unwrap()
        .unwrap();
    (response, run, execution)
}

fn parent() -> TaskParentRuntimeContext {
    TaskParentRuntimeContext {
        parent_thread_id: "continuity-root".to_owned(),
        parent_turn_id: Some("continuity-parent".to_owned()),
        presentation_thread_id: "continuity-root".to_owned(),
        home_root_thread_id: "continuity-root".to_owned(),
        root_thread_id: "continuity-root".to_owned(),
    }
}

async fn prepared(
    processor: &Arc<MessageProcessor>,
    response: &pioneer_protocol::TaskGetResponse,
    run: &TaskRun,
    execution: &pioneer_protocol::TaskRunExecution,
) -> anyhow::Result<crate::authorization::AgentExecutionPersistenceFacts> {
    let (adapter, _, _) = materialize_task_agent_action_binding_for_execution(
        processor,
        &response.task,
        &response.agent_specs[0],
        &execution.id,
        &run.id,
        "continuity-root",
        processor.current_authorization_revision().await?,
        false,
    )
    .await?;
    Ok(adapter.persistence_facts())
}

async fn commit(
    processor: &Arc<MessageProcessor>,
    response: &pioneer_protocol::TaskGetResponse,
    run: &TaskRun,
    facts: &crate::authorization::AgentExecutionPersistenceFacts,
) -> anyhow::Result<pioneer_crud::AgentExecutionGraphCommitResult> {
    let backend = match &facts.profile.backend {
        pioneer_protocol::AgentExecutionProfileBackend::CliRuntime {
            runtime_instance_id,
        } => AgentExecutionBackend::CLIAgentRuntime {
            runtime_id: runtime_instance_id.clone(),
            runtime_kind: CLIAgentRuntimeKind::Codex,
        },
        _ => AgentExecutionBackend::ApiProvider {
            provider: facts.profile.provider_id.clone(),
        },
    };
    let (authorization, _, _) = resolve_task_child_execution_authorization_context(
        processor,
        &response.task,
        &parent(),
        &facts.profile.provider_id,
        &facts.profile.model_id,
        Some(&backend),
        &[],
        &pioneer_protocol::task_permission_cap_snapshot(
            response.agent_specs[0].permission_cap.as_ref().unwrap(),
        ),
        "continuity-child-turn",
        &run.id,
        facts.execution_id.as_str(),
    )
    .await?;
    persist_task_agent_execution_graph(
        processor,
        response,
        run,
        &response.agent_specs[0],
        &parent(),
        facts,
        &authorization.context.authorization_fingerprint()?,
    )
    .await
}

async fn change_cli(processor: &MessageProcessor, enabled: bool, binary: &str) {
    let config = crate::isolated_test_app_config().unwrap();
    let name =
        crate::settings::normalize_settings_file_name(&config.gateway.settings_file_name).unwrap();
    let path = processor.artifact_runtime_home.join(&name);
    let mut settings = crate::settings::load_or_create_gateway_settings(
        &path,
        config.gateway.settings_version,
        &name,
    )
    .unwrap();
    let mut runtimes = crate::cli_runtime::config::load_effective_cli_runtime_identity_settings(
        processor.artifact_runtime_home.as_path(),
    )
    .unwrap();
    let codex = runtimes
        .instances
        .iter_mut()
        .find(|runtime| runtime.id == "codex")
        .unwrap();
    codex.enabled = enabled;
    codex.binary_path = binary.to_owned();
    if binary == "/fake/later-codex" {
        codex.display_name = "Renamed Codex".to_owned();
        codex.nickname = "renamed-codex".to_owned();
    }
    settings
        .set_cli_runtime_settings_for_tests(runtimes)
        .unwrap();
    crate::settings::save_gateway_settings(&path, &settings).unwrap();
    sync_test_cli_runtime_identities(processor).await.unwrap();
}

#[tokio::test]
async fn existing_cli_task_recovery_refuses_changed_engine_with_a_current_catalog() {
    let (processor, workspace) = processor_with_registry(test_provider()).await;
    let (response, run, execution) = task_fixture(&processor, &workspace, true).await;
    let facts = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    commit(&processor, &response, &run, &facts).await.unwrap();
    let db = processor.crud_store.database_connection();
    let saved = pioneer_crud::load_agent_execution(&db, &execution.id)
        .await
        .unwrap()
        .unwrap();
    let presentation = pioneer_crud::load_agent_presentation_snapshot(
        &db,
        saved.presentation_snapshot_id.as_deref().unwrap(),
    )
    .await
    .unwrap();
    let grant = pioneer_crud::load_agent_execution_grant(&db, &execution.id)
        .await
        .unwrap();

    let config = crate::isolated_test_app_config().unwrap();
    let name =
        crate::settings::normalize_settings_file_name(&config.gateway.settings_file_name).unwrap();
    let path = processor.artifact_runtime_home.join(&name);
    let mut settings = crate::settings::load_or_create_gateway_settings(
        &path,
        config.gateway.settings_version,
        &name,
    )
    .unwrap();
    let mut runtimes = crate::cli_runtime::config::load_effective_cli_runtime_identity_settings(
        processor.artifact_runtime_home.as_path(),
    )
    .unwrap();
    runtimes
        .instances
        .iter_mut()
        .find(|runtime| runtime.id == "codex")
        .unwrap()
        .kind = CLIAgentRuntimeKind::Claude;
    settings
        .set_cli_runtime_settings_for_tests(runtimes)
        .unwrap();
    crate::settings::save_gateway_settings(&path, &settings).unwrap();
    sync_test_cli_runtime_identities(&processor).await.unwrap();
    let current = pioneer_crud::load_agent_identity(&db, facts.identity.id.as_str())
        .await
        .unwrap()
        .unwrap();
    assert!(current.source_revision > saved.identity_source_revision);
    let current_snapshot = pioneer_crud::load_current_agent_presentation_snapshot(
        &db,
        &current.id,
        current.source_revision,
        &current.source_fingerprint,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(current_snapshot.role_label.as_deref(), Some("claude"));

    let error = super::super::agent_action_tools::current_agent_identity_source_fence(
        &processor,
        &execution.id,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("admitted runtime kind"));
    assert!(
        prepared(&processor, &response, &run, &execution)
            .await
            .is_err()
    );
    assert!(commit(&processor, &response, &run, &facts).await.is_err());
    assert_eq!(
        pioneer_crud::load_agent_execution(&db, &execution.id)
            .await
            .unwrap()
            .unwrap(),
        saved
    );
    assert_eq!(
        pioneer_crud::load_agent_presentation_snapshot(
            &db,
            saved.presentation_snapshot_id.as_deref().unwrap()
        )
        .await
        .unwrap(),
        presentation
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_grant(&db, &execution.id)
            .await
            .unwrap(),
        grant
    );
}

#[tokio::test]
async fn old_cli_task_admits_after_off_on_technical_change_and_return_without_rewriting_contract() {
    let (processor, workspace) = processor_with_registry(test_provider()).await;
    let (response, run, execution) = task_fixture(&processor, &workspace, true).await;
    let contract = processor
        .crud_store
        .get_task_actor_contract(&response.task.id)
        .await
        .unwrap()
        .unwrap();
    let original = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    change_cli(&processor, false, "codex").await;
    let denied = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap_err();
    assert_eq!(
        denied
            .downcast_ref::<pioneer_tasks::TaskStartFailure>()
            .unwrap()
            .descriptor()
            .reason,
        Some(pioneer_tasks::TaskStartReason::LaunchSourceUnavailable)
    );
    change_cli(&processor, true, "/fake/updated-codex").await;
    let changed = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    assert_eq!(changed.identity.id, original.identity.id);
    assert!(changed.identity.source_revision > original.identity.source_revision);
    assert_eq!(
        changed.profile, original.profile,
        "model/profile and all ceilings remain pinned"
    );
    change_cli(&processor, true, "codex").await;
    let returned = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    assert!(returned.identity.source_revision > changed.identity.source_revision);
    assert_eq!(
        returned.identity.source_fingerprint,
        original.identity.source_fingerprint
    );
    assert_eq!(returned.profile.model_id, "pinned-cli-model");
    let admitted = commit(&processor, &response, &run, &returned)
        .await
        .unwrap();
    assert_eq!(admitted.execution_id, execution.id);
    assert_eq!(
        processor
            .crud_store
            .get_task_actor_contract(&response.task.id)
            .await
            .unwrap()
            .unwrap(),
        contract
    );
    let author = exact_agent_execution_author(&processor, &execution.id)
        .await
        .unwrap();
    let row = pioneer_crud::load_agent_execution(
        &processor.crud_store.database_connection(),
        &execution.id,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        row.identity_source_revision,
        returned.identity.source_revision as i64
    );
    assert_eq!(
        author.agent.as_ref().unwrap().identity_source_revision,
        returned.identity.source_revision
    );
    assert_eq!(
        author.agent.as_ref().unwrap().agent_identity_id,
        returned.identity.id
    );
    let grant = pioneer_crud::load_agent_execution_grant(
        &processor.crud_store.database_connection(),
        &execution.id,
    )
    .await
    .unwrap()
    .unwrap();
    change_cli(&processor, true, "/fake/later-codex").await;
    let restored = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    assert_eq!(
        restored.identity, returned.identity,
        "existing execution keeps its historical revision"
    );
    assert_eq!(restored.profile, returned.profile);
    let replay = commit(&processor, &response, &run, &restored)
        .await
        .unwrap();
    assert_eq!(replay.execution_id, admitted.execution_id);
    assert_eq!(
        exact_agent_execution_author(&processor, &execution.id)
            .await
            .unwrap(),
        author
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_grant(
            &processor.crud_store.database_connection(),
            &execution.id
        )
        .await
        .unwrap()
        .unwrap(),
        grant
    );
    let (adapter, options, capabilities) = materialize_task_agent_action_binding_for_execution(
        &processor,
        &response.task,
        &response.agent_specs[0],
        &execution.id,
        &run.id,
        "continuity-root",
        processor.current_authorization_revision().await.unwrap(),
        false,
    )
    .await
    .unwrap();
    let mut binding = crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
        adapter,
        options,
        capabilities,
    );
    binding
        .refresh_start_options_catalog(&processor, "continuity-parent")
        .await
        .unwrap();
    let (child_identity, child_profile) = binding
        .adapter
        .lock()
        .await
        .resolve_task_launch(contract.launch.as_ref().unwrap())
        .unwrap();
    assert!(child_identity.source_revision > restored.identity.source_revision);
    assert_eq!(child_identity.id, returned.identity.id);
    assert_eq!(
        child_profile.model_id, "pinned-cli-model",
        "composer/defaults cannot change child model"
    );
    let pioneer_protocol::TaskDerivedChildLaunchGrant::ResolvedTaskLaunch {
        child_launch_grant,
        ..
    } = serde_json::from_str(contract.derived_child_launch_grant_json.as_deref().unwrap()).unwrap();
    let candidate = binding.adapter.lock().await;
    let (reviewer_identity, reviewer_profile) = candidate
        .resolve_named_task_reviewer_launch("codex", &child_launch_grant)
        .unwrap();
    assert_eq!(reviewer_identity.id, returned.identity.id);
    assert_eq!(reviewer_identity.nickname, "renamed-codex");
    let (reviewer, _, _) = crate::authorization::materialize_selected_task_agent_action_binding(
        pioneer_protocol::AgentExecutionId::new("Econtinuityreview1234").unwrap(),
        "continuity-root",
        returned.root_execution_id.clone(),
        "reviewer",
        reviewer_identity,
        reviewer_profile,
        1,
        1,
        1,
        "agent_reviewer",
        processor.current_authorization_revision().await.unwrap(),
        &candidate,
    )
    .unwrap();
    let reviewer_facts = reviewer.persistence_facts();
    assert!(
        reviewer_facts
            .agent_authorization_allowed_actions
            .iter()
            .all(|action| {
                returned
                    .agent_authorization_allowed_actions
                    .contains(action)
            })
    );
}

#[tokio::test]
async fn configuration_change_between_preparation_and_graph_commit_rolls_back() {
    let (processor, workspace) = processor_with_registry(test_provider()).await;
    let (response, run, execution) = task_fixture(&processor, &workspace, true).await;
    let facts = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    change_cli(&processor, true, "/fake/racing-codex").await;
    assert!(commit(&processor, &response, &run, &facts).await.is_err());
    assert!(
        pioneer_crud::load_agent_execution(
            &processor.crud_store.database_connection(),
            &execution.id
        )
        .await
        .unwrap()
        .is_none()
    );
    assert!(
        pioneer_crud::load_agent_execution_grant(
            &processor.crud_store.database_connection(),
            &execution.id
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn native_source_disable_between_preparation_and_commit_is_not_a_connection_refresh() {
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
    let (processor, workspace) = processor_with_registry(test_provider()).await;
    let (response, run, execution) = task_fixture(&processor, &workspace, false).await;
    let facts = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    let source = pioneer_crud::load_agent_identity(
        &processor.crud_store.database_connection(),
        facts.identity.id.as_str(),
    )
    .await
    .unwrap()
    .unwrap();
    pioneer_entity::native_agent_config::Entity::update_many()
        .col_expr(
            pioneer_entity::native_agent_config::Column::Enabled,
            Expr::value(false),
        )
        .filter(pioneer_entity::native_agent_config::Column::Id.eq(&source.source_id))
        .exec(&processor.crud_store.database_connection())
        .await
        .unwrap();
    assert!(commit(&processor, &response, &run, &facts).await.is_err());
    assert!(
        prepared(&processor, &response, &run, &execution)
            .await
            .is_err()
    );
    assert!(
        pioneer_crud::load_agent_execution(
            &processor.crud_store.database_connection(),
            &execution.id,
        )
        .await
        .unwrap()
        .is_none()
    );
}

#[tokio::test]
async fn legacy_cli_task_provider_grant_is_not_resolved_as_an_api_connection() {
    let registry = test_provider();
    let (processor, workspace) = processor_with_registry(registry.clone()).await;
    let mut params = detached_cli_task_create_params(
        &workspace,
        "continuity-root",
        "continuity-parent",
        "codex",
        CLIAgentRuntimeKind::Codex,
        "pinned-cli-model",
        "legacy CLI admission",
    );
    params.metadata = None;
    let response = create_task_for_test(&processor, params).await.unwrap();
    let admission = processor
        .crud_store
        .get_task_execution_admission(&response.task.id)
        .await
        .unwrap()
        .unwrap();
    let context = crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
        processor.crud_store.as_ref(),
        &admission,
    )
    .await
    .unwrap();
    assert_eq!(
        context.continuation_provider(),
        Some(("cli_runtime:codex", "pinned-cli-model"))
    );
    assert!(
        registry
            .available_authority_fingerprint_for_workspace(&workspace, "cli_runtime:codex")
            .is_err()
    );
    let before = context.to_persisted_json().unwrap();
    let revalidated = processor
        .execution_leases
        .revalidate_context(
            processor.crud_store.as_ref(),
            &context,
            crate::authorization::ResourceAction::TaskCreate,
            processor.current_authorization_revision().await.unwrap(),
        )
        .await
        .unwrap();
    let resolved = context
        .resolve_new_task_provider_authority(&registry, &revalidated)
        .unwrap();
    assert_eq!(resolved.to_persisted_json().unwrap(), before);
    resolved
        .verify_current_provider_authority(&registry)
        .unwrap();
}

#[tokio::test]
async fn api_task_rotates_connection_only_after_current_authorization_and_keeps_saved_envelope() {
    let key = Arc::new(std::sync::RwLock::new("key-one".to_owned()));
    let registry = Arc::new(pioneer_provider::ProviderRegistry::new({
        let key = key.clone();
        move |_| key.read().unwrap().clone()
    }));
    registry
        .insert("openai", Arc::new(EchoProvider::new()))
        .unwrap();
    let (processor, workspace) = processor_with_registry(registry.clone()).await;
    let (response, run, execution) = task_fixture(&processor, &workspace, false).await;
    let record = processor
        .crud_store
        .get_task_execution_admission(&response.task.id)
        .await
        .unwrap()
        .unwrap();
    let context = crate::authorization::ExecutionAuthorizationContext::load_for_task_admission(
        processor.crud_store.as_ref(),
        &record,
    )
    .await
    .unwrap();
    let before = context.to_persisted_json().unwrap();
    *key.write().unwrap() = "key-two".to_owned();
    assert!(
        context
            .verify_current_provider_authority(&registry)
            .is_err()
    );
    let revalidated = processor
        .execution_leases
        .revalidate_context(
            processor.crud_store.as_ref(),
            &context,
            crate::authorization::ResourceAction::TaskCreate,
            processor.current_authorization_revision().await.unwrap(),
        )
        .await
        .unwrap();
    let fresh = context
        .resolve_new_task_provider_authority(&registry, &revalidated)
        .unwrap();
    fresh.verify_current_provider_authority(&registry).unwrap();
    assert_eq!(
        fresh.continuation_provider(),
        context.continuation_provider()
    );
    assert_eq!(fresh.granted_action_names(), context.granted_action_names());
    assert_eq!(fresh.granted_skill_ids(), context.granted_skill_ids());
    assert_eq!(
        fresh.granted_mcp_server_capability_ids(),
        context.granted_mcp_server_capability_ids()
    );
    assert_eq!(
        fresh.permission_profile_cap(),
        context.permission_profile_cap()
    );
    assert_eq!(context.to_persisted_json().unwrap(), before);
    assert_eq!(
        processor
            .crud_store
            .get_task_execution_admission(&response.task.id)
            .await
            .unwrap()
            .unwrap(),
        record
    );
    let facts = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    let (child_authority, _, occurrence_authority) =
        resolve_task_child_execution_authorization_context(
            &processor,
            &response.task,
            &parent(),
            &facts.profile.provider_id,
            &facts.profile.model_id,
            None,
            &[],
            &pioneer_protocol::task_permission_cap_snapshot(context.permission_profile_cap()),
            "continuity-api-child",
            &run.id,
            &execution.id,
        )
        .await
        .unwrap();
    assert_eq!(
        child_authority
            .context
            .provider_authority_for_selection("openai", "test-model"),
        occurrence_authority.provider_authority_for_selection("openai", "test-model"),
    );
    persist_task_agent_execution_graph(
        &processor,
        &response,
        &run,
        &response.agent_specs[0],
        &parent(),
        &facts,
        &child_authority.context.authorization_fingerprint().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(facts.profile.provider_id, "openai");
    assert_eq!(facts.profile.model_id, "test-model");
    let persisted = pioneer_crud::load_agent_execution(
        &processor.crud_store.database_connection(),
        &execution.id,
    )
    .await
    .unwrap()
    .unwrap();
    let persisted_grant = pioneer_crud::load_agent_execution_grant(
        &processor.crud_store.database_connection(),
        &execution.id,
    )
    .await
    .unwrap()
    .unwrap();
    *key.write().unwrap() = "key-three".to_owned();
    assert!(
        child_authority
            .context
            .verify_current_provider_authority(&registry)
            .is_err()
    );
    // Even the crash gap before a working Turn exists cannot silently bind
    // an already admitted graph to another credential authority.
    let restored = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    assert!(
        commit(&processor, &response, &run, &restored)
            .await
            .is_err()
    );
    assert_eq!(
        pioneer_crud::load_agent_execution(
            &processor.crud_store.database_connection(),
            &execution.id,
        )
        .await
        .unwrap()
        .unwrap(),
        persisted
    );
    assert_eq!(
        pioneer_crud::load_agent_execution_grant(
            &processor.crud_store.database_connection(),
            &execution.id,
        )
        .await
        .unwrap()
        .unwrap(),
        persisted_grant
    );
    let alien = context
        .derive_agent_continuation(
            "another-capsule",
            "openai",
            "test-model",
            None,
            &[],
            &pioneer_protocol::task_permission_cap_snapshot(context.permission_profile_cap()),
            Some(&"a".repeat(64)),
        )
        .unwrap();
    assert!(
        alien
            .resolve_new_task_provider_authority(&registry, &revalidated)
            .is_err()
    );
    // Downgrade and suspend the isolated initiator atomically: a superuser
    // cannot itself have suspended status. A rotated key cannot restore access.
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
    pioneer_entity::gateway_principal::Entity::update_many()
        .filter(
            pioneer_entity::gateway_principal::Column::Id
                .eq(context.initiating_principal_id().as_str()),
        )
        .col_expr(
            pioneer_entity::gateway_principal::Column::Kind,
            Expr::value("user"),
        )
        .col_expr(
            pioneer_entity::gateway_principal::Column::RoleKey,
            Expr::value("member"),
        )
        .col_expr(
            pioneer_entity::gateway_principal::Column::Status,
            Expr::value("suspended"),
        )
        .exec(&processor.crud_store.database_connection())
        .await
        .unwrap();
    assert!(
        processor
            .execution_leases
            .revalidate_context(
                processor.crud_store.as_ref(),
                &context,
                crate::authorization::ResourceAction::TaskCreate,
                processor.current_authorization_revision().await.unwrap(),
            )
            .await
            .is_err()
    );
}

// Shared with the executor's regressions so they can exercise real private
// reviewer recovery and public delivery entry points on the same full fixture.
pub(in crate::message) struct HistoricalTaskFixture {
    pub processor: Arc<MessageProcessor>,
    pub response: pioneer_protocol::TaskGetResponse,
    pub run: TaskRun,
    pub execution: pioneer_protocol::TaskRunExecution,
    pub facts: crate::authorization::AgentExecutionPersistenceFacts,
    provider: Arc<CaptureSummaryProvider>,
}

pub(in crate::message) async fn historical_task_fixture(
    mode: TaskDeliveryMode,
) -> HistoricalTaskFixture {
    let provider = Arc::new(
        CaptureSummaryProvider::new("unexpected model execution")
            .named("openai")
            .with_native_attachments(),
    );
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        provider.clone(),
    ));
    let (processor, workspace) = processor_with_registry(registry).await;
    let mut params = test_task_create_params(
        &workspace,
        "continuity-root",
        "continuity-parent",
        "historical delivery",
        3,
    );
    params.delivery_policy = Some(TaskDeliveryPolicy {
        mode,
        thread_target: (mode == TaskDeliveryMode::Thread)
            .then_some(pioneer_protocol::TaskDeliveryThreadTarget::ExactThread),
        thread_id: (mode == TaskDeliveryMode::Thread).then(|| "continuity-root".to_owned()),
        webhook_url: None,
        include_result: true,
        format: TaskDeliveryFormat::Summary,
    });
    let created = create_task_for_test(&processor, params).await.unwrap();
    let run = created.run.unwrap();
    let execution = processor
        .crud_store
        .reserve_execution_for_run(&run.id, TaskExecutorKind::Agent, run.created_at)
        .await
        .unwrap();
    let response = processor
        .crud_store
        .get_task(&run.task_id)
        .await
        .unwrap()
        .unwrap();
    let facts = prepared(&processor, &response, &run, &execution)
        .await
        .unwrap();
    let admitted = commit(&processor, &response, &run, &facts).await.unwrap();
    assert!(!admitted.queued);
    HistoricalTaskFixture {
        processor,
        response,
        run,
        execution,
        facts,
        provider,
    }
}

impl HistoricalTaskFixture {
    pub fn assert_no_provider_requests(&self) {
        assert!(
            self.provider.snapshot_requests().is_empty(),
            "delivery/rejected recovery must not call a provider"
        );
    }

    pub async fn complete(&self) -> pioneer_protocol::TaskDelivery {
        let now = now_timestamp_secs();
        self.processor
            .crud_store
            .claim_execution_at(&self.execution.id, "historical-fixture", now, now + 600)
            .await
            .unwrap()
            .expect("fixture owns its Task attempt");
        let handle = pioneer_tasks::TaskExecutionHandle::new(
            self.processor.crud_store.clone(),
            self.processor.task_runtime.event_bus(),
            self.response.task.id.clone(),
            self.run.id.clone(),
        );
        handle.mark_started(now).await.unwrap();
        handle
            .complete_run(
                Some(pioneer_protocol::TaskResult {
                    summary: Some("saved historical answer".into()),
                    data: None,
                    artifacts: vec![],
                    completed_by_run_id: Some(self.run.id.clone()),
                }),
                now,
            )
            .await
            .unwrap();
        let deliveries = self
            .processor
            .crud_store
            .list_task_deliveries(pioneer_protocol::TaskDeliveriesParams {
                workspace_id: self.response.task.workspace_id.clone(),
                task_id: Some(self.response.task.id.clone()),
                run_id: Some(self.run.id.clone()),
                statuses: vec![],
                limit: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(deliveries.deliveries.len(), 1);
        assert_eq!(
            deliveries.deliveries[0]
                .result_snapshot
                .as_ref()
                .unwrap()
                .summary
                .as_deref(),
            Some("saved historical answer")
        );
        deliveries.deliveries.into_iter().next().unwrap()
    }

    pub async fn add_delivery_route(&self) -> (String, String, String, i64) {
        use sea_orm::{ActiveModelTrait, IntoActiveModel};
        let store = &self.processor.crud_store;
        let db = store.database_connection();
        let actor = store
            .get_task_actor_contract(&self.response.task.id)
            .await
            .unwrap()
            .unwrap();
        let admission = store
            .get_task_execution_admission(&self.response.task.id)
            .await
            .unwrap()
            .unwrap();
        let context = crate::authorization::ExecutionAuthorizationContext::from_persisted_json(
            &admission.authorization_context_json,
        )
        .unwrap();
        let policy_fingerprint = context.policy_fingerprint().to_owned();
        let authority = serde_json::to_string(&actor.creator).unwrap();
        assert!(matches!(
            actor.creator,
            pioneer_protocol::PersistedActorRef::Principal(_)
        ));
        let generation = self
            .processor
            .current_authorization_revision()
            .await
            .unwrap() as i64;
        let now = pioneer_crud::utc_now();
        let expires = now + chrono::Duration::hours(1);
        let id = pioneer_crud::canonical_agent_id('R', "historical-delivery-route");
        let route = store
            .create_agent_delegation_route(pioneer_crud::AgentDelegationRouteInput {
                id: id.clone(),
                source_execution_id: self.execution.id.clone(),
                destination_thread_id: "continuity-root".into(),
                source_capsule_id: Some(self.facts.home_root_thread_id.clone()),
                destination_capsule_id: Some("continuity-root".into()),
                source_workspace_id: Some(self.response.task.workspace_id.clone()),
                destination_workspace_id: Some(self.response.task.workspace_id.clone()),
                source_gateway_id: Some("local".into()),
                destination_gateway_id: Some("local".into()),
                source_identity_id: Some("A00000000000000000001".into()),
                destination_agent_identity_id: None,
                destination_profile_id: None,
                home_capsule_id: Some(self.facts.home_root_thread_id.clone()),
                route_kind: "execution_bound".into(),
                authority_actor_json: authority.clone(),
                authority_fingerprint: policy_fingerprint.clone(),
                allowed_actions_json: serde_json::to_string(&[
                    pioneer_protocol::AgentRouteAction::DeliverResult,
                ])
                .unwrap(),
                disclosure_json: serde_json::to_string(
                    &pioneer_protocol::AgentRouteDisclosurePolicy {
                        result_return: pioneer_protocol::AgentResultReturnPolicy::SummaryOnly,
                        ..Default::default()
                    },
                )
                .unwrap(),
                route_generation: 1,
                source_policy_generation: generation,
                destination_policy_generation: generation,
                hop_count: 0,
                max_hops: 1,
                return_route_id: None,
                grant_fingerprint: policy_fingerprint.clone(),
                status: "active".into(),
                updated_at: now,
                expires_at: Some(expires),
                now,
            })
            .await
            .unwrap();
        let mut actor = actor;
        actor.delivery.route_id = Some(id.clone());
        actor.delivery.route_expires_at_millis = Some(expires.timestamp_millis());
        actor.delivery.route_receipt_json = Some(json!({
            "routeId":id,"routeGeneration":route.route_generation,"sourcePolicyGeneration":route.source_policy_generation,
            "destinationPolicyGeneration":route.destination_policy_generation,"action":"deliver_result",
        }).to_string());
        actor.validate().unwrap();
        let mut row =
            pioneer_entity::task_actor_contract::Entity::find_by_id(&self.response.task.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .into_active_model();
        row.delivery_json = Set(serde_json::to_string(&actor.delivery).unwrap());
        row.update(&db).await.unwrap();
        (id, authority, policy_fingerprint, generation)
    }

    /// Install a fixed released V2 snapshot, not a checksum generated by the
    /// verifier under test. All graph/permit/occurrence facts were admitted by
    /// the normal fixture. Only the historical source/snapshots are substituted.
    pub async fn install_historical_grants(&self, execution_ids: &[&str]) {
        use sea_orm::{ActiveModelTrait, IntoActiveModel};
        let child = json!({
            "version":2,
            "identities":[{"id":"A00000000000000000001","source_kind":"native_agent",
                "display_name":"Pioneer","nickname":"pioneer","source_revision":3,"source_fingerprint":"generation-a"}],
            "allowInheritParentIdentity":false,"allowServerDerivedEphemeral":false,
            "profiles":[{"id":"P00000000000000000003","compatibleAgentIdentityIds":["A00000000000000000001"],
                "backend":"api_provider","providerId":"provider-opaque","modelId":"model-opaque",
                "providerDisplayName":"Provider","modelDisplayName":"Model","allowedReasoning":[{"effort":"medium"}],
                "allowedPermissionProfiles":["full_access"],"catalogGeneration":4,"policyGeneration":5,"fingerprint":"profile-fingerprint"}],
            "allowInheritParentProfile":false,"skillIds":[],"mcpServerIds":[],
            "maxPermissionProfile":{"mode":"supervised","effective_policy":{
                "default_behavior":"ask","file_read":"allow","file_write":"ask","shell_command":"ask","network":"ask",
                "mcp_read":"allow","mcp_write_or_unknown":"ask","dynamic_skill_tool":"ask","computer_use":"ask",
                "task_subagent":"ask","memory_write":"ask","agent_action":"ask"}},
            "maxReasoning":{"allowed":[{"effort":"medium"}]},
            "fingerprint":"1620c8eca0ca3eace03dc7d614e3af348b2f77224cde1064bcf7f492c3177047"
        });
        // The checksum above is pinned by the historical writer in d907644f.
        let identity: pioneer_protocol::AgentIdentityProjection =
            serde_json::from_value(child["identities"][0].clone()).unwrap();
        let profile: pioneer_protocol::AgentExecutionProfileProjection =
            serde_json::from_value(child["profiles"][0].clone()).unwrap();
        let db = self.processor.crud_store.database_connection();
        let now = pioneer_crud::utc_now();
        pioneer_crud::ensure_native_agent_config(
            &db,
            &pioneer_crud::NativeAgentConfigInput {
                id: "historical-grant-source".into(),
                workspace_id: self.response.task.workspace_id.clone(),
                system_key: None,
                display_name: "Historical source".into(),
                nickname: "historical-source".into(),
                enabled: true,
                avatar_revision: None,
                config_revision: 3,
                now,
            },
        )
        .await
        .unwrap();
        pioneer_crud::ensure_agent_identity(
            &db,
            &pioneer_crud::AgentIdentityInput {
                id: identity.id.to_string(),
                workspace_id: self.response.task.workspace_id.clone(),
                source_kind: pioneer_crud::SOURCE_NATIVE_AGENT.into(),
                source_id: "historical-grant-source".into(),
                source_revision: 3,
                source_fingerprint: "generation-a".into(),
                now,
            },
        )
        .await
        .unwrap();
        let snapshot_id = pioneer_crud::canonical_agent_id('S', "historical-grant-presentation");
        pioneer_crud::insert_presentation_snapshot(
            &db,
            &pioneer_crud::PresentationSnapshotInput {
                id: snapshot_id.clone(),
                agent_identity_id: identity.id.to_string(),
                source_revision: 3,
                source_fingerprint: "generation-a".into(),
                display_name: identity.display_name.clone(),
                nickname: identity.nickname.clone(),
                avatar_revision: None,
                role_label: None,
                now,
            },
        )
        .await
        .unwrap();
        let launch = pioneer_protocol::AgentLaunchSelection {
            agent: pioneer_protocol::AgentIdentitySelection::Exact {
                agent_identity_id: identity.id.clone(),
            },
            execution: pioneer_protocol::AgentExecutionSelection {
                profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
                    profile_id: profile.id.clone(),
                },
                reasoning: None,
                permission_profile: None,
                skill_ids: vec![],
                mcp_server_ids: vec![],
                selected_capabilities: vec![],
            },
        };
        let seed = |role: &str, home: &str| {
            crate::authorization::derive_task_agent_authorization_grant_seed(
                identity.id.clone(),
                home,
                role,
                self.facts.agent_authorization_policy_generation,
                pioneer_protocol::ChildAgentLaunchGrantSet::new(vec![identity.clone()]).unwrap(),
            )
            .unwrap()
        };
        let authorization = seed(
            &self.facts.agent_authorization_role_key,
            &self.facts.home_root_thread_id,
        );
        let mut actor =
            pioneer_entity::task_actor_contract::Entity::find_by_id(&self.response.task.id)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .into_active_model();
        actor.launch_selection_json = Set(Some(serde_json::to_string(&launch).unwrap()));
        actor.requested_identity_json = Set(Some(serde_json::to_string(&launch.agent).unwrap()));
        actor.resolved_identity_id = Set(Some(identity.id.to_string()));
        actor.resolved_profile_id = Set(Some(profile.id.to_string()));
        actor.source_config_fingerprint = Set(Some("generation-a".into()));
        actor.derived_child_launch_grant_json = Set(Some(json!({
            "kind":"resolved_task_launch","identity":identity,"profile":profile,
            "role_key":authorization.role_key,"agent_policy_generation":authorization.policy_generation,
            "allowed_actions":authorization.allowed_actions,"agent_authorization_fingerprint":authorization.fingerprint,
            "child_launch_grant":child,
        }).to_string()));
        actor.update(&db).await.unwrap();
        for execution_id in execution_ids {
            let row = pioneer_crud::load_agent_execution(&db, execution_id)
                .await
                .unwrap()
                .unwrap();
            let grant = pioneer_crud::load_agent_execution_grant(&db, execution_id)
                .await
                .unwrap()
                .unwrap();
            let mut value: serde_json::Value = serde_json::from_str(&grant.grant_json).unwrap();
            let authorization = seed(
                value["role_key"].as_str().unwrap(),
                &row.home_root_thread_id,
            );
            value["identity"] = serde_json::to_value(&identity).unwrap();
            value["profile"] = serde_json::to_value(&profile).unwrap();
            value["child_launch_grant"] = child.clone();
            value["agent_policy_generation"] = json!(authorization.policy_generation);
            value["allowed_actions"] = json!(authorization.allowed_actions);
            value["agent_authorization_fingerprint"] = json!(authorization.fingerprint);
            let grant_json = value.to_string();
            let fingerprint = pioneer_crud::agent_execution_grant_fingerprint(&grant_json).unwrap();
            let mut grant = grant.into_active_model();
            grant.child_identity_id = Set(identity.id.to_string());
            grant.grant_json = Set(grant_json);
            grant.grant_fingerprint = Set(fingerprint);
            grant.update(&db).await.unwrap();
            let mut row = row.into_active_model();
            row.agent_identity_id = Set(identity.id.to_string());
            row.identity_source_revision = Set(3);
            row.identity_source_fingerprint = Set("generation-a".into());
            row.resolved_profile_id = Set(Some(profile.id.to_string()));
            row.resolved_profile_fingerprint = Set(Some(profile.fingerprint.clone()));
            row.presentation_snapshot_id = Set(Some(snapshot_id.clone()));
            row.requested_identity_selection_json =
                Set(serde_json::to_string(&launch.agent).unwrap());
            row.requested_profile_selection_json =
                Set(serde_json::to_string(&launch.execution).unwrap());
            row.update(&db).await.unwrap();
            // Prove the fixture has reached supported, active current source
            // fences rather than relying on an unrelated delivery error.
            crate::message::agent_action_tools::current_agent_identity_source_fence(
                &self.processor,
                execution_id,
            )
            .await
            .unwrap();
            pioneer_crud::load_agent_execution_grant(&db, execution_id)
                .await
                .unwrap()
                .unwrap();
        }
        self.processor
            .crud_store
            .get_task_actor_contract(&self.response.task.id)
            .await
            .unwrap()
            .unwrap();
    }
}
