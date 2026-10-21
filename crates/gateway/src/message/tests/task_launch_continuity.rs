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
    // Revoke all collaborators in this isolated fixture. Rotating credentials
    // cannot turn a saved allowance into proof of current capsule access.
    use sea_orm::{EntityTrait, sea_query::Expr};
    pioneer_entity::gateway_principal::Entity::update_many()
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
