//! Repository reads must authenticate legacy snapshots without writer admission.
use pioneer_crud_execution::{
    agent_grant::{
        agent_execution_grant_fingerprint, load_agent_execution_grant,
        parse_agent_execution_launch_grant,
    },
    time::utc_now,
};
use pioneer_crud_tasks::view_store::TaskViewStore;
use pioneer_entity::{agent_execution_grant, task_actor_contract};
use pioneer_sqlite::{SqliteDatabase, SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver};
use sea_orm::{ConnectionTrait, DbBackend, EntityTrait, Set, Statement};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Writes(AtomicUsize);
impl SqliteWriteObserver for Writes {
    fn observe(&self, event: SqliteWriteEvent) {
        if matches!(event, SqliteWriteEvent::Enqueued { .. }) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

async fn insert_task_actor_contract<C: ConnectionTrait>(
    database: &C,
    task_id: &str,
    grant_json: &str,
) {
    let now = utc_now();
    let identity_selection = pioneer_protocol::AgentIdentitySelection::Exact {
        agent_identity_id: pioneer_protocol::AgentIdentityId::new(
            "A00000000000000000001".to_owned(),
        )
        .expect("identity id should be valid"),
    };
    let launch = pioneer_protocol::AgentLaunchSelection {
        agent: identity_selection.clone(),
        execution: pioneer_protocol::AgentExecutionSelection {
            profile: pioneer_protocol::AgentExecutionProfileSelection::Exact {
                profile_id: pioneer_protocol::AgentExecutionProfileId::new(
                    "P00000000000000000003".to_owned(),
                )
                .expect("profile id should be valid"),
            },
            reasoning: None,
            permission_profile: None,
            skill_ids: Vec::new(),
            mcp_server_ids: Vec::new(),
            selected_capabilities: Vec::new(),
        },
    };
    let delivery = pioneer_protocol::TaskDeliveryActorContract {
        enabled: false,
        destination_thread_id: None,
        destination_user_id: None,
        destination_webhook_url_fingerprint: None,
        route_id: None,
        return_route_id: None,
        author_snapshot: None,
        route_receipt_json: None,
        disclosure_generation: 1,
        route_expires_at_millis: None,
    };
    task_actor_contract::Entity::insert(task_actor_contract::ActiveModel {
        task_id: Set(task_id.to_owned()),
        workspace_id: Set("W00000000000000000001".to_owned()),
        creator_json: Set(
            serde_json::to_string(&pioneer_protocol::PersistedActorRef::System)
                .expect("creator should serialize"),
        ),
        creator_snapshot_json: Set(None),
        reviewer_json: Set(serde_json::to_string(
            &pioneer_protocol::TaskReviewerIntent::RuntimeAuto,
        )
        .expect("reviewer should serialize")),
        delivery_json: Set(serde_json::to_string(&delivery).expect("delivery should serialize")),
        launch_selection_json: Set(Some(
            serde_json::to_string(&launch).expect("launch should serialize"),
        )),
        requested_identity_json: Set(Some(
            serde_json::to_string(&identity_selection)
                .expect("identity selection should serialize"),
        )),
        resolved_identity_id: Set(Some("A00000000000000000001".to_owned())),
        resolved_profile_id: Set(Some("P00000000000000000003".to_owned())),
        source_config_fingerprint: Set(Some("generation-a".to_owned())),
        derived_child_launch_grant_json: Set(Some(grant_json.to_owned())),
        execution_destination_thread_id: Set(None),
        execution_route_id: Set(None),
        execution_route_receipt_json: Set(None),
        execution_route_expires_at_millis: Set(None),
        creator_work_graph_root_execution_id: Set(None),
        work_graph_root_execution_id: Set(None),
        root_resource_scope_id: Set(None),
        accounting_attribution_json: Set(None),
        controller_principal_id: Set(None),
        revision: Set(1),
        created_at: Set(now.clone()),
        updated_at: Set(now),
    })
    .exec(database)
    .await
    .expect("Task actor contract should insert");
}

#[pioneer_postgres_test::postgres_test]
async fn history_reads_both_durable_containers_without_rewrites_or_writer_admission() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connect_with_limit(1).await.unwrap();
    let reader = fixture.connect_read_only().await.unwrap();
    let writes = Arc::new(Writes::default());
    let executor = SqliteWriteExecutor::with_observer(writer, writes.clone());
    let db = SqliteDatabase::from_executor(reader, executor);
    let store = TaskViewStore::new(&db);
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('W00000000000000000001','History');
         INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
         VALUES('A00000000000000000001','W00000000000000000001','ephemeral','grant','generation-a');",
    ).await.unwrap();
    let v1: serde_json::Value = serde_json::from_str(include_str!(
        "../../crud-execution/tests/fixtures/child-launch-grant-v1.json"
    ))
    .unwrap();
    let mut v2 = v1.clone();
    v2["version"] = serde_json::json!(2);
    v2["maxPermissionProfile"]["effective_policy"]["memory_write"] = serde_json::json!("ask");
    v2["maxPermissionProfile"]["effective_policy"]["agent_action"] = serde_json::json!("ask");
    // Independently pinned by the released writer in d907644f.
    v2["fingerprint"] =
        serde_json::json!("1620c8eca0ca3eace03dc7d614e3af348b2f77224cde1064bcf7f492c3177047");
    let mut saved = Vec::new();
    for (index, child) in [v1, v2].into_iter().enumerate() {
        let task_id = format!("T{index:020}");
        let execution_id = format!("E{index:020}");
        let task_json = serde_json::json!({
            "kind":"resolved_task_launch", "identity":child["identities"][0],
            "profile":child["profiles"][0], "role_key":"member", "agent_policy_generation":1,
            "allowed_actions":["create_task"], "agent_authorization_fingerprint":"a".repeat(64),
            "child_launch_grant":child
        })
        .to_string();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO task(id,workspace_id,owner_kind,executor_kind,status,title,goal)
             VALUES($1,'W00000000000000000001','system','agent','completed','History','History')",
            [task_id.clone().into()],
        ))
        .await
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO agent_execution(
                id,workspace_id,agent_identity_id,identity_source_revision,
                identity_source_fingerprint,home_root_thread_id,work_graph_root_execution_id,
                requested_identity_selection_json,requested_profile_selection_json,authorization_context_fingerprint)
             VALUES($1,'W00000000000000000001','A00000000000000000001',1,'generation-a',
                'history-thread',$1,'{}','{}','fixture')",
            [execution_id.clone().into()],
        )).await.unwrap();
        insert_task_actor_contract(&db, &task_id, &task_json).await;
        let execution_json = serde_json::json!({
            "identity":child["identities"][0], "profile":child["profiles"][0],
            "reference":{"fingerprint":"keep-outer-metadata"}, "child_launch_grant":child
        })
        .to_string();
        let fingerprint = agent_execution_grant_fingerprint(&execution_json).unwrap();
        agent_execution_grant::Entity::insert(agent_execution_grant::ActiveModel {
            id: Set(format!("G{index:020}")),
            execution_id: Set(execution_id.clone()),
            parent_execution_id: Set(None),
            child_identity_id: Set("A00000000000000000001".into()),
            grant_json: Set(execution_json.clone()),
            grant_fingerprint: Set(fingerprint.clone()),
            created_at: Set(utc_now()),
        })
        .exec(&db)
        .await
        .unwrap();
        saved.push((
            task_id,
            execution_id,
            task_json,
            execution_json,
            fingerprint,
        ));
    }
    writes.0.store(0, Ordering::SeqCst);
    for (task_id, execution_id, task_json, execution_json, fingerprint) in saved {
        let actor = store
            .get_task_actor_contract(&task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actor.derived_child_launch_grant_json.as_deref(),
            Some(task_json.as_str())
        );
        let grant = load_agent_execution_grant(&db, &execution_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(grant.grant_json, execution_json);
        assert_eq!(grant.grant_fingerprint, fingerprint);
        let (_, _, child) = parse_agent_execution_launch_grant(&grant.grant_json).unwrap();
        assert!(
            child.validate_for_execution().is_err(),
            "readable history is not admission authority"
        );
        let raw = task_actor_contract::Entity::find_by_id(task_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raw.derived_child_launch_grant_json.as_deref(),
            Some(task_json.as_str())
        );
    }
    assert_eq!(
        writes.0.load(Ordering::SeqCst),
        0,
        "history readers must never enqueue a migration/update"
    );
}
