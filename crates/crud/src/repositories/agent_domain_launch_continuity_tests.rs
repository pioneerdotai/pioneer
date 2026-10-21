use super::*;
use migration::{Migrator, MigratorTrait};
use sea_orm::{ActiveModelTrait, Database, PaginatorTrait};

async fn store() -> crate::CrudStore {
    let connection = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&connection, None).await.unwrap();
    let store = crate::CrudStore::new(connection);
    pioneer_entity::workspace::ActiveModel {
        id: Set("continuity-workspace".to_owned()),
        name: Set("Continuity".to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        ..Default::default()
    }
    .insert(&store.database_connection())
    .await
    .unwrap();
    store
}

async fn sync(store: &crate::CrudStore, enabled: bool, settings: &str) {
    crate::sync_cli_runtime_identity_catalog(
        &store.database_connection(),
        &[crate::CliRuntimeIdentitySeed {
            id: "continuity-cli".to_owned(),
            kind: "codex".to_owned(),
            display_name: "CLI".to_owned(),
            nickname: "continuity-cli".to_owned(),
            enabled,
            source_revision_material: settings.to_owned(),
        }],
        utc_now(),
    )
    .await
    .unwrap();
}

async fn graph(
    store: &crate::CrudStore,
    execution_id: &str,
    parent_id: Option<&str>,
) -> AgentExecutionGraphCommitInput {
    graph_for_source(store, execution_id, parent_id, "continuity-cli").await
}

async fn graph_for_source(
    store: &crate::CrudStore,
    execution_id: &str,
    parent_id: Option<&str>,
    source_id: &str,
) -> AgentExecutionGraphCommitInput {
    let now = utc_now();
    let row = load_agent_identity_by_source(
        &store.database_connection(),
        "continuity-workspace",
        SOURCE_CLI_RUNTIME_INSTANCE,
        source_id,
    )
    .await
    .unwrap()
    .unwrap();
    let snapshot = load_current_agent_presentation_snapshot(
        &store.database_connection(),
        &row.id,
        row.source_revision,
        &row.source_fingerprint,
    )
    .await
    .unwrap()
    .unwrap();
    let identity = pioneer_protocol::AgentIdentityProjection::new(
        AgentIdentityId::new(row.id.clone()).unwrap(),
        AgentIdentitySourceKind::CliRuntimeInstance,
        snapshot.display_name.clone(),
        snapshot.nickname.clone(),
        None,
        snapshot.role_label.clone(),
        row.source_revision as u64,
        row.source_fingerprint.clone(),
    )
    .unwrap();
    let profile = pioneer_protocol::AgentExecutionProfileProjection {
        id: pioneer_protocol::AgentExecutionProfileId::new("Pcontinuity1234567890").unwrap(),
        compatible_agent_identity_ids: vec![identity.id.clone()],
        backend: pioneer_protocol::AgentExecutionProfileBackend::CliRuntime {
            runtime_instance_id: row.source_id.clone(),
        },
        provider_id: format!("cli_runtime:{}", row.source_id),
        model_id: "pinned-model".to_owned(),
        provider_display_name: "CLI".to_owned(),
        model_display_name: "Pinned".to_owned(),
        allowed_reasoning: vec![pioneer_protocol::TurnReasoningSelection {
            effort: "low".to_owned(),
        }],
        allowed_permission_profiles: vec![pioneer_protocol::TurnPermissionMode::Supervised],
        catalog_generation: 1,
        policy_generation: 1,
        fingerprint: "profile".to_owned(),
    };
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
        vec![identity.clone()],
        vec![profile.clone()],
    )
    .unwrap();
    let grant_json = serde_json::json!({"identity": identity, "profile": profile, "child_launch_grant": ceiling}).to_string();
    let root_id = parent_id.unwrap_or(execution_id).to_owned();
    let execution = AgentExecutionInput {
        id: execution_id.to_owned(),
        workspace_id: row.workspace_id.clone(),
        agent_identity_id: row.id.clone(),
        identity_source_revision: row.source_revision,
        identity_source_fingerprint: row.source_fingerprint.clone(),
        parent_execution_id: parent_id.map(str::to_owned),
        parent_task_id: None,
        parent_thread_id: None,
        home_root_thread_id: "continuity-root".to_owned(),
        work_graph_root_execution_id: root_id.clone(),
        requested_identity_selection_json: "{}".to_owned(),
        requested_profile_selection_json: "{}".to_owned(),
        resolved_profile_id: Some(profile.id.as_str().to_owned()),
        resolved_profile_fingerprint: Some(profile.fingerprint.clone()),
        presentation_snapshot_id: Some(snapshot.id.clone()),
        authorization_context_fingerprint: "authorization".to_owned(),
        execution_generation: if parent_id.is_some() { 2 } else { 1 },
        status: "created".to_owned(),
        now,
    };
    let resource = AgentResourceStateInput {
        id: canonical_agent_id('R', execution_id),
        execution_id: execution_id.to_owned(),
        attempt_generation: 1,
        branch_key: "continuity".to_owned(),
        fair_order: 1,
        now,
    };
    AgentExecutionGraphCommitInput {
        identity: AgentIdentityInput {
            id: row.id.clone(),
            workspace_id: row.workspace_id,
            source_kind: row.source_kind,
            source_id: row.source_id,
            source_revision: row.source_revision,
            source_fingerprint: row.source_fingerprint.clone(),
            now,
        },
        presentation: PresentationSnapshotInput {
            id: snapshot.id,
            agent_identity_id: row.id.clone(),
            source_revision: row.source_revision,
            source_fingerprint: row.source_fingerprint,
            display_name: snapshot.display_name,
            nickname: snapshot.nickname,
            avatar_revision: snapshot.avatar_revision,
            role_label: snapshot.role_label,
            now,
        },
        root_execution_id: root_id,
        root_execution: parent_id.is_none().then(|| execution.clone()),
        child_execution: execution,
        root_resource_state: parent_id.is_none().then(|| resource.clone()),
        child_resource_state: resource,
        grant: AgentExecutionGrantInput {
            id: canonical_agent_id('G', execution_id),
            execution_id: execution_id.to_owned(),
            parent_execution_id: parent_id.map(str::to_owned),
            child_identity_id: row.id,
            grant_fingerprint: agent_execution_grant_fingerprint(&grant_json).unwrap(),
            grant_json,
            now,
        },
        response: None,
        root_routes: Vec::new(),
        max_concurrency: 8,
        max_queue_depth: 8,
        max_depth: 8,
        max_fan_out: 8,
        max_total_nodes: 32,
        idle_timeout_secs: 30,
        hard_timeout_secs: 60,
        child_permit_id: canonical_agent_id('P', execution_id),
        child_queue_id: canonical_agent_id('Q', execution_id),
        task_actor_contract: None,
        task_occurrence_contract: None,
        contract_now: now.timestamp(),
    }
}

#[tokio::test]
async fn updated_allowed_child_reaches_atomic_graph_admission_and_replay_preserves_grant() {
    let store = store().await;
    sync(&store, true, "original").await;
    let root = graph(&store, "Econtinuityroot123456", None).await;
    store
        .commit_agent_execution_graph(root.clone())
        .await
        .unwrap();
    let old_grant =
        load_agent_execution_grant(&store.database_connection(), &root.child_execution.id)
            .await
            .unwrap()
            .unwrap();
    sync(&store, false, "disabled").await;
    sync(&store, true, "original").await;
    let child = graph(
        &store,
        "Econtinuitychild12345",
        Some(&root.child_execution.id),
    )
    .await;
    assert!(child.identity.source_revision > root.identity.source_revision);
    assert_eq!(
        child.identity.source_fingerprint,
        root.identity.source_fingerprint
    );
    let first = store
        .commit_agent_execution_graph(child.clone())
        .await
        .unwrap();
    let grant = load_agent_execution_grant(&store.database_connection(), &child.child_execution.id)
        .await
        .unwrap()
        .unwrap();
    let replay = store
        .commit_agent_execution_graph(child.clone())
        .await
        .unwrap();
    assert_eq!(first.execution_id, replay.execution_id);
    assert_eq!(
        load_agent_execution_grant(&store.database_connection(), &child.child_execution.id)
            .await
            .unwrap()
            .unwrap(),
        grant
    );
    assert_eq!(
        load_agent_execution_grant(&store.database_connection(), &root.child_execution.id)
            .await
            .unwrap()
            .unwrap(),
        old_grant
    );
    assert_eq!(
        agent_execution::Entity::find()
            .count(&store.database_connection())
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        agent_running_permit::Entity::find()
            .count(&store.database_connection())
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn graph_rejects_widened_child_models_skills_mcp_permissions_and_foreign_sources_atomically()
{
    let store = store().await;
    sync(&store, true, "original").await;
    let root = graph(&store, "Econtinuityroot123456", None).await;
    store
        .commit_agent_execution_graph(root.clone())
        .await
        .unwrap();
    for change in 0..7 {
        let mut child = graph(
            &store,
            "Econtinuitychild12345",
            Some(&root.child_execution.id),
        )
        .await;
        let (identity, mut profile, ceiling) =
            parse_agent_execution_launch_grant(&child.grant.grant_json).unwrap();
        let mut skills = Vec::new();
        let mut mcp = Vec::new();
        let mut cap = ceiling.max_permission_profile.clone();
        match change {
            0 => profile.model_id = "added-model".to_owned(),
            1 => profile
                .allowed_reasoning
                .push(pioneer_protocol::TurnReasoningSelection {
                    effort: "high".to_owned(),
                }),
            2 => skills
                .push(pioneer_protocol::SkillId::new("Scontinuity1234567890".to_owned()).unwrap()),
            3 => mcp.push("mcp-server:workspace:added".to_owned()),
            4 => {
                cap = pioneer_protocol::task_permission_cap_for_mode(
                    pioneer_protocol::TurnPermissionMode::FullAccess,
                )
            }
            5 => child.identity.workspace_id = "another-workspace".to_owned(),
            _ => child.identity.source_id = "another-source".to_owned(),
        }
        let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
            vec![identity.clone()],
            vec![profile.clone()],
        )
        .unwrap()
        .with_policy(false, false, false, skills, mcp, cap)
        .unwrap();
        child.child_execution.resolved_profile_fingerprint = Some(profile.fingerprint.clone());
        child.grant.grant_json = serde_json::json!({"identity": identity, "profile": profile, "child_launch_grant": ceiling}).to_string();
        child.grant.grant_fingerprint =
            agent_execution_grant_fingerprint(&child.grant.grant_json).unwrap();
        assert!(
            store
                .commit_agent_execution_graph(child.clone())
                .await
                .is_err(),
            "change {change} must be refused"
        );
        assert!(
            load_agent_execution(&store.database_connection(), &child.child_execution.id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            load_agent_execution_grant(&store.database_connection(), &child.child_execution.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn narrowed_child_reasoning_and_tool_cap_reach_graph_admission() {
    let store = store().await;
    sync(&store, true, "original").await;
    let root = graph(&store, "Econtinuityroot123456", None).await;
    store
        .commit_agent_execution_graph(root.clone())
        .await
        .unwrap();
    let mut child = graph(
        &store,
        "Econtinuitychild12345",
        Some(&root.child_execution.id),
    )
    .await;
    let (identity, mut profile, ceiling) =
        parse_agent_execution_launch_grant(&child.grant.grant_json).unwrap();
    profile.allowed_reasoning.clear();
    let mut cap = ceiling.max_permission_profile;
    cap.effective_policy.denied_tools = vec!["shell_command".to_owned()];
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
        vec![identity.clone()],
        vec![profile.clone()],
    )
    .unwrap()
    .with_policy(false, false, false, Vec::new(), Vec::new(), cap)
    .unwrap();
    child.grant.grant_json = serde_json::json!({"identity": identity, "profile": profile, "child_launch_grant": ceiling}).to_string();
    child.grant.grant_fingerprint =
        agent_execution_grant_fingerprint(&child.grant.grant_json).unwrap();
    store
        .commit_agent_execution_graph(child.clone())
        .await
        .unwrap();
    let stored =
        load_agent_execution_grant(&store.database_connection(), &child.child_execution.id)
            .await
            .unwrap()
            .unwrap();
    let (_, profile, ceiling) = parse_agent_execution_launch_grant(&stored.grant_json).unwrap();
    assert!(profile.allowed_reasoning.is_empty());
    assert_eq!(
        ceiling.max_permission_profile.effective_policy.denied_tools,
        vec!["shell_command"]
    );
}

#[tokio::test]
async fn graph_detects_source_change_after_preparation_and_retirement_before_commit() {
    let store = store().await;
    sync(&store, true, "original").await;
    let prepared = graph(&store, "Econtinuityroot123456", None).await;
    sync(&store, true, "updated-binary-and-args").await;
    assert!(
        store
            .commit_agent_execution_graph(prepared.clone())
            .await
            .is_err()
    );
    assert!(
        load_agent_execution(&store.database_connection(), &prepared.child_execution.id)
            .await
            .unwrap()
            .is_none()
    );
    let current = graph(&store, "Econtinuityroot123456", None).await;
    crate::sync_cli_runtime_identity_catalog(&store.database_connection(), &[], utc_now())
        .await
        .unwrap();
    assert!(store.commit_agent_execution_graph(current).await.is_err());
}

#[tokio::test]
async fn existing_cli_graph_replays_technical_revision_but_refuses_changed_engine() {
    let store = store().await;
    sync(&store, true, "original").await;
    let original = graph(&store, "Econtinuityroot123456", None).await;
    store
        .commit_agent_execution_graph(original.clone())
        .await
        .unwrap();
    let before = saved_graph_state(&store, &original).await;

    sync(&store, false, "disabled").await;
    sync(&store, true, "new-binary-home-args-timeouts").await;
    let mut replay = original.clone();
    replay.identity = graph(&store, &original.child_execution.id, None)
        .await
        .identity;
    assert!(replay.identity.source_revision > original.identity.source_revision);
    assert_ne!(
        replay.identity.source_fingerprint,
        original.identity.source_fingerprint
    );
    store
        .commit_agent_execution_graph(replay.clone())
        .await
        .unwrap();
    assert_eq!(saved_graph_state(&store, &original).await, before);

    crate::sync_cli_runtime_identity_catalog(
        &store.database_connection(),
        &[crate::CliRuntimeIdentitySeed {
            id: "continuity-cli".to_owned(),
            kind: "claude".to_owned(),
            display_name: "Renamed CLI".to_owned(),
            nickname: "renamed-cli".to_owned(),
            enabled: true,
            source_revision_material: "changed-engine".to_owned(),
        }],
        utc_now(),
    )
    .await
    .unwrap();
    // Use the real current source fence, while retaining every historical
    // execution/presentation/profile/grant fact exactly as recovery does.
    replay.identity = graph(&store, &original.child_execution.id, None)
        .await
        .identity;
    let error = store
        .commit_agent_execution_graph(replay)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("admitted runtime kind"));
    assert_eq!(saved_graph_state(&store, &original).await, before);
}

async fn saved_graph_state(
    store: &crate::CrudStore,
    graph: &AgentExecutionGraphCommitInput,
) -> serde_json::Value {
    let db = store.database_connection();
    serde_json::json!({
        "executions": agent_execution::Entity::find().all(&db).await.unwrap(),
        "presentation": load_agent_presentation_snapshot(&db, &graph.presentation.id).await.unwrap(),
        "grants": agent_execution_grant::Entity::find().all(&db).await.unwrap(),
        "resources": agent_execution_resource_state::Entity::find().all(&db).await.unwrap(),
        "scopes": agent_work_resource_scope::Entity::find().all(&db).await.unwrap(),
        "permits": agent_running_permit::Entity::find().all(&db).await.unwrap(),
        "queue": agent_work_queue::Entity::find().all(&db).await.unwrap(),
    })
}

#[tokio::test]
async fn changed_cli_engine_cannot_reuse_an_old_label_to_pass_parent_ceiling() {
    let store = store().await;
    sync(&store, true, "original").await;
    let root = graph(&store, "Econtinuityroot123456", None).await;
    store
        .commit_agent_execution_graph(root.clone())
        .await
        .unwrap();
    crate::sync_cli_runtime_identity_catalog(
        &store.database_connection(),
        &[crate::CliRuntimeIdentitySeed {
            id: "continuity-cli".to_owned(),
            kind: "claude".to_owned(),
            display_name: "CLI".to_owned(),
            nickname: "continuity-cli".to_owned(),
            enabled: true,
            source_revision_material: "changed-engine".to_owned(),
        }],
        utc_now(),
    )
    .await
    .unwrap();
    let mut child = graph(
        &store,
        "Econtinuitychild12345",
        Some(&root.child_execution.id),
    )
    .await;
    let (mut identity, profile, _) =
        parse_agent_execution_launch_grant(&child.grant.grant_json).unwrap();
    assert_eq!(identity.role_label.as_deref(), Some("claude"));
    // The source revision/fingerprint are real, but the claimed engine is old.
    identity.role_label = Some("codex".to_owned());
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
        vec![identity.clone()],
        vec![profile.clone()],
    )
    .unwrap();
    child.grant.grant_json = serde_json::json!({"identity": identity, "profile": profile, "child_launch_grant": ceiling}).to_string();
    child.grant.grant_fingerprint =
        agent_execution_grant_fingerprint(&child.grant.grant_json).unwrap();
    assert!(
        store
            .commit_agent_execution_graph(child.clone())
            .await
            .is_err()
    );
    assert!(
        load_agent_execution(&store.database_connection(), &child.child_execution.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn ephemeral_cli_execution_is_fenced_by_its_registered_runtime_source() {
    let store = store().await;
    sync(&store, true, "original").await;
    let mut root = graph(&store, "Econtinuityroot123456", None).await;
    let (identity, profile, ceiling) =
        parse_agent_execution_launch_grant(&root.grant.grant_json).unwrap();
    let ceiling = ceiling
        .clone()
        .with_policy(
            false,
            true,
            false,
            Vec::new(),
            Vec::new(),
            ceiling.max_permission_profile,
        )
        .unwrap();
    root.grant.grant_json = serde_json::json!({"identity": identity, "profile": profile, "child_launch_grant": ceiling}).to_string();
    root.grant.grant_fingerprint =
        agent_execution_grant_fingerprint(&root.grant.grant_json).unwrap();
    store
        .commit_agent_execution_graph(root.clone())
        .await
        .unwrap();
    let mut child = graph(
        &store,
        "Econtinuitychild12345",
        Some(&root.child_execution.id),
    )
    .await;
    let (backing, mut profile, _) =
        parse_agent_execution_launch_grant(&child.grant.grant_json).unwrap();
    let mut actor = backing.clone();
    actor.id = AgentIdentityId::new("Acontinuityephem12345").unwrap();
    actor.source_kind = AgentIdentitySourceKind::Ephemeral;
    actor.source_revision = 1;
    actor.source_fingerprint = "ephemeral-source".to_owned();
    actor.display_name = "Ephemeral".to_owned();
    actor.nickname = "ephemeral".to_owned();
    actor.role_label = None;
    profile.compatible_agent_identity_ids.push(actor.id.clone());
    profile.compatible_agent_identity_ids.sort();
    child.identity.id = actor.id.as_str().to_owned();
    child.identity.source_kind = SOURCE_EPHEMERAL.to_owned();
    child.identity.source_id = "ephemeral:test".to_owned();
    child.identity.source_revision = 1;
    child.identity.source_fingerprint = actor.source_fingerprint.clone();
    child.presentation.id = canonical_agent_id('S', "ephemeral:test");
    child.presentation.agent_identity_id = child.identity.id.clone();
    child.presentation.source_revision = 1;
    child.presentation.source_fingerprint = actor.source_fingerprint.clone();
    child.presentation.display_name = actor.display_name.clone();
    child.presentation.nickname = actor.nickname.clone();
    child.presentation.role_label = None;
    child.child_execution.agent_identity_id = child.identity.id.clone();
    child.child_execution.identity_source_revision = 1;
    child.child_execution.identity_source_fingerprint = actor.source_fingerprint.clone();
    child.child_execution.presentation_snapshot_id = Some(child.presentation.id.clone());
    child.grant.child_identity_id = child.identity.id.clone();
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
        vec![actor.clone(), backing],
        vec![profile.clone()],
    )
    .unwrap();
    child.grant.grant_json =
        serde_json::json!({"identity": actor, "profile": profile, "child_launch_grant": ceiling})
            .to_string();
    child.grant.grant_fingerprint =
        agent_execution_grant_fingerprint(&child.grant.grant_json).unwrap();
    sync(&store, true, "updated-binary").await;
    assert!(
        store
            .commit_agent_execution_graph(child.clone())
            .await
            .is_err()
    );
    assert!(
        load_agent_execution(&store.database_connection(), &child.child_execution.id)
            .await
            .unwrap()
            .is_none()
    );
    let (current, _, _) = parse_agent_execution_launch_grant(
        &graph(
            &store,
            "Econtinuitychild12345",
            Some(&root.child_execution.id),
        )
        .await
        .grant
        .grant_json,
    )
    .unwrap();
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(
        vec![actor.clone(), current],
        vec![profile.clone()],
    )
    .unwrap();
    child.grant.grant_json =
        serde_json::json!({"identity": actor, "profile": profile, "child_launch_grant": ceiling})
            .to_string();
    child.grant.grant_fingerprint =
        agent_execution_grant_fingerprint(&child.grant.grant_json).unwrap();
    store.commit_agent_execution_graph(child).await.unwrap();
}

#[tokio::test]
async fn new_workspace_executor_is_not_added_to_an_old_parent_grant() {
    let store = store().await;
    sync(&store, true, "original").await;
    let root = graph(&store, "Econtinuityroot123456", None).await;
    store
        .commit_agent_execution_graph(root.clone())
        .await
        .unwrap();
    let seeds = ["continuity-cli", "outsider"].map(|id| crate::CliRuntimeIdentitySeed {
        id: id.to_owned(),
        kind: "codex".to_owned(),
        display_name: "CLI".to_owned(),
        nickname: id.to_owned(),
        enabled: true,
        source_revision_material: "original".to_owned(),
    });
    crate::sync_cli_runtime_identity_catalog(&store.database_connection(), &seeds, utc_now())
        .await
        .unwrap();
    let child = graph_for_source(
        &store,
        "Econtinuitychild12345",
        Some(&root.child_execution.id),
        "outsider",
    )
    .await;
    assert!(
        store
            .commit_agent_execution_graph(child.clone())
            .await
            .is_err()
    );
    assert!(
        load_agent_execution(&store.database_connection(), &child.child_execution.id)
            .await
            .unwrap()
            .is_none()
    );
}
