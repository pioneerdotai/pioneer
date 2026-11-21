use super::*;
use pioneer_crud_execution::agent_actions::{
    AgentActionInput, load_agent_action_outbox, load_agent_action_receipt,
};
use pioneer_crud_execution::agent_execution::{AgentExecutionInput, AgentResourceStateInput};
use pioneer_crud_execution::agent_grant::{
    AgentExecutionGrantInput, agent_execution_grant_fingerprint,
};
use pioneer_crud_execution::agent_identity::{
    AgentIdentityInput, PresentationSnapshotInput, SOURCE_CLI_RUNTIME_INSTANCE,
    load_agent_identity_by_source, load_current_agent_presentation_snapshot,
};
use pioneer_crud_execution::canonical_id::canonical_agent_id;
use pioneer_crud_execution::time::utc_now;
use pioneer_entity::agent_action;
use pioneer_entity::task_occurrence_contract;
use pioneer_postgres_test::TestDatabase;
use pioneer_protocol::{AgentIdentityId, AgentIdentitySourceKind};
use sea_orm::{ActiveModelTrait, PaginatorTrait};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, ExprTrait, FromQueryResult, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait, TryGetable,
};
use sha2::{Digest, Sha256};

async fn store(fixture: &TestDatabase) -> crate::CrudStore {
    crate::tests::postgres_store_with_workspace(fixture, "continuity-workspace").await
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
    let ceiling = pioneer_protocol::ChildAgentLaunchGrantSet::new(vec![identity.clone()]).unwrap();
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

// TaskRuntime::resume_task on an already active, nonblocked Task calls this
// durable boundary with no Task events. No caller dispatch/publish follows it.
#[tokio::test]
async fn empty_task_resume_action_wakes_control_after_commit_and_exact_replay_parks() {
    pioneer_postgres_test::with_database(async |fixture| {
        use futures_util::FutureExt;

        let store = store(fixture).await;
        sync(&store, true, "resume-action").await;
        let mut graph = graph(&store, "Eresumeaction12345678", None).await;
        let db = store.database_connection();
        let generation = pioneer_crud_access::policy_generation::current_policy_generation_on(&db)
            .await
            .unwrap()
            .get() as i64;
        let fingerprint = "a".repeat(64);
        let mut grant: serde_json::Value = serde_json::from_str(&graph.grant.grant_json).unwrap();
        grant["agent_authorization_fingerprint"] = serde_json::json!(fingerprint);
        grant["role_key"] = serde_json::json!("owner");
        grant["agent_policy_generation"] = serde_json::json!(generation);
        grant["allowed_actions"] = serde_json::json!(["task_schedule_manage"]);
        graph.grant.grant_json = grant.to_string();
        graph.grant.grant_fingerprint =
            agent_execution_grant_fingerprint(&graph.grant.grant_json).unwrap();
        store.commit_agent_execution_graph(&graph).await.unwrap();

        let mut task = crate::tests::sample_task(graph.contract_now);
        task.workspace_id = "continuity-workspace".into();
        store
            .append_task_events(
                vec![crate::TaskEventPayload::TaskCreated { task: task.clone() }],
                graph.contract_now,
            )
            .await
            .unwrap();
        let before = store.get_task(&task.id).await.unwrap().unwrap();
        assert_eq!(before.task.status, pioneer_protocol::TaskStatus::Scheduled);
        assert!(before.triggers.is_empty());
        let action = AgentCommitInput {
            mutation_kind: "control_task".into(),
            idempotency_key: "resume-active-task".into(),
            request_fingerprint: "resume-request".into(),
            actor_identity_id: graph.identity.id.clone(),
            action: AgentActionInput {
                id: canonical_agent_id('A', "resume-active-task"),
                execution_id: graph.child_execution.id.clone(),
                action_kind: "control_task".into(),
                idempotency_key: "resume-active-task".into(),
                request_fingerprint: "resume-request".into(),
                now: graph.child_execution.now,
            },
            receipt_id: canonical_agent_id('R', "resume-active-task"),
            outbox_id: canonical_agent_id('O', "resume-active-task"),
            action_response_json: Some("{}".into()),
            receipt_response_json: Some("{}".into()),
            route_receipt_json: None,
            outbox_payload_json: "{}".into(),
            policy_fingerprint: fingerprint.clone(),
            execution_grant_fingerprint: fingerprint,
            execution_grant_policy_generation: generation,
            source_scope_id: graph.child_execution.home_root_thread_id.clone(),
            destination_scope_id: None,
            subject_role_key: "owner".into(),
            authorized_resource_action: "task_schedule_manage".into(),
            source_policy_generation: generation,
            destination_policy_generation: None,
            route_generation: None,
            disclosure_class: "source_capsule".into(),
            expected_execution_generation: graph.child_execution.execution_generation,
            expected_current_identity_source_revision: graph.identity.source_revision,
            expected_current_identity_source_fingerprint: graph.identity.source_fingerprint.clone(),
            expected_attempt_generation: 1,
            expected_policy_generation: generation,
            requires_cross_capsule_route: false,
            resource: None,
        };
        let mut control = db.work_signal("gateway.control").subscribe();
        control.begin_pass(&db).await.unwrap();
        assert!(
            load_agent_action_outbox(&db, &action.action.id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(control.wait(None).now_or_never().is_none());

        let appended = store
            .append_task_events_with_agent_action(Vec::new(), graph.contract_now, action.clone())
            .await
            .unwrap();
        assert!(appended.is_empty());
        assert!(
            control.wait(None).now_or_never().is_some(),
            "new action-only commit must wake a parked control consumer"
        );
        let committed = agent_action::Entity::find_by_id(action.action.id.clone())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(committed.status, "committed");
        let receipt = load_agent_action_receipt(&db, &action.action.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.id, action.receipt_id);
        let outbox = load_agent_action_outbox(&db, &action.action.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(outbox.id, action.outbox_id);
        assert_eq!(outbox.status, "pending");
        assert_eq!(outbox.attempts, 0);
        assert_eq!(
            store.get_task(&task.id).await.unwrap().unwrap().task,
            before.task
        );

        control.begin_pass(&db).await.unwrap();
        assert!(
            store
                .append_task_events_with_agent_action(
                    Vec::new(),
                    graph.contract_now,
                    action.clone()
                )
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            control.wait(None).now_or_never().is_none(),
            "exact replay must leave control parked"
        );
        assert_eq!(
            load_agent_action_receipt(&db, &action.action.id)
                .await
                .unwrap()
                .unwrap(),
            receipt
        );
        assert_eq!(
            load_agent_action_outbox(&db, &action.action.id)
                .await
                .unwrap()
                .unwrap(),
            outbox
        );
        assert_eq!(agent_action::Entity::find().count(&db).await.unwrap(), 1);
        assert_eq!(
            agent_domain_commit::Entity::find()
                .count(&db)
                .await
                .unwrap(),
            1
        );
        Ok(())
    })
    .await
    .unwrap();
}
