use super::*;
use pioneer_crud_execution::time::utc_now;
use pioneer_entity::thread;
use pioneer_entity::{
    agent_execution, agent_execution_resource_state, agent_identity, agent_presentation_snapshot,
    agent_running_permit, agent_turn_response_execution, agent_work_queue,
    agent_work_resource_scope, turn,
};
use sea_orm::TransactionTrait;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, EntityTrait, ExprTrait, FromQueryResult,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, TryGetable,
};

#[pioneer_postgres_test::postgres_test]
async fn root_graph_cancellation_fences_only_the_exact_graph_and_releases_resources() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection().clone();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('workspace-cancel','Cancel');
         INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
         VALUES('I00000000000000000001','workspace-cancel','ephemeral','cancel','source');
         INSERT INTO agent_presentation_snapshot
            (id,agent_identity_id,source_revision,source_fingerprint,display_name,nickname)
         VALUES('S00000000000000000001','I00000000000000000001',1,'source','Cancel','cancel');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
         VALUES('thread-cancel','workspace-cancel','','agent','m','p','active'),
               ('thread-revisions','workspace-cancel','','agent','m','p','active');",
    )
    .await
    .unwrap();
    let now = utc_now();
    let root_id = "Ecancelroot1234567890";
    let child_id = "Ecancelchild123456789";
    let queued_id = "Ecancelqueue123456789";
    let unrelated_id = "Eunrelated12345678901";
    for (id, root, parent, status) in [
        (root_id, root_id, None, "running"),
        (child_id, root_id, Some(root_id), "running"),
        (queued_id, root_id, Some(root_id), "queued"),
        (unrelated_id, unrelated_id, None, "running"),
    ] {
        agent_execution::ActiveModel {
            id: Set(id.to_owned()),
            workspace_id: Set("workspace-cancel".to_owned()),
            agent_identity_id: Set("I00000000000000000001".to_owned()),
            identity_source_revision: Set(1),
            identity_source_fingerprint: Set("source".to_owned()),
            parent_execution_id: Set(parent.map(str::to_owned)),
            parent_task_id: Set(None),
            parent_thread_id: Set(Some("thread-cancel".to_owned())),
            home_root_thread_id: Set("thread-cancel".to_owned()),
            work_graph_root_execution_id: Set(root.to_owned()),
            requested_identity_selection_json: Set("{}".to_owned()),
            requested_profile_selection_json: Set("{}".to_owned()),
            resolved_profile_id: Set(None),
            resolved_profile_fingerprint: Set(None),
            presentation_snapshot_id: Set(None),
            authorization_context_fingerprint: Set("authorization".to_owned()),
            execution_generation: Set(1),
            status: Set(status.to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            finished_at: Set(None),
        }
        .insert(&db)
        .await
        .unwrap();
    }
    agent_work_resource_scope::ActiveModel {
        root_execution_id: Set(root_id.to_owned()),
        scope_generation: Set(1),
        max_concurrency: Set(2),
        max_queue_depth: Set(8),
        max_depth: Set(8),
        max_fan_out: Set(8),
        max_total_nodes: Set(64),
        aggregate_usage_json: Set("{}".to_owned()),
        queue_generation: Set(1),
        last_scheduled_sequence: Set(1),
        last_scheduled_at: Set(None),
        status: Set("active".to_owned()),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(&db)
    .await
    .unwrap();
    for (id, execution_id, permit_id, status) in [
        ("state-root", root_id, Some("permit-root"), "running"),
        ("state-child", child_id, Some("permit-child"), "running"),
        ("state-queued", queued_id, None, "queued"),
    ] {
        agent_execution_resource_state::ActiveModel {
            id: Set(id.to_owned()),
            execution_id: Set(execution_id.to_owned()),
            attempt_generation: Set(1),
            progress_sequence: Set(0),
            progress_frontier_json: Set("{}".to_owned()),
            last_progress_at: Set(None),
            last_heartbeat_at: Set(None),
            idle_deadline: Set(None),
            hard_deadline: Set(None),
            local_usage_json: Set("{}".to_owned()),
            permit_id: Set(permit_id.map(str::to_owned)),
            branch_key: Set(id.to_owned()),
            fair_order: Set(1),
            status: Set(status.to_owned()),
            fencing_generation: Set(1),
            fenced_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();
    }
    for (id, execution_id) in [("permit-root", root_id), ("permit-child", child_id)] {
        agent_running_permit::ActiveModel {
            id: Set(id.to_owned()),
            root_execution_id: Set(root_id.to_owned()),
            execution_id: Set(execution_id.to_owned()),
            attempt_generation: Set(1),
            lease_generation: Set(1),
            status: Set("held".to_owned()),
            acquired_at: Set(now),
            released_at: Set(None),
            fenced_at: Set(None),
        }
        .insert(&db)
        .await
        .unwrap();
    }
    agent_work_queue::ActiveModel {
        id: Set("queue-child".to_owned()),
        root_execution_id: Set(root_id.to_owned()),
        execution_id: Set(queued_id.to_owned()),
        attempt_generation: Set(1),
        branch_key: Set("queued".to_owned()),
        enqueue_sequence: Set(1),
        state: Set("queued".to_owned()),
        eligible_at: Set(None),
        claim_token: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    }
    .insert(&db)
    .await
    .unwrap();

    // A Task occurrence may retain several completed revisions plus a
    // currently admitted revision: response cardinality exceeds node count.
    for (id, status) in [
        ("initial", "completed"),
        ("revision-1", "completed"),
        ("revision-2", "completed"),
        ("revision-active", "in_progress"),
    ] {
        turn::ActiveModel {
            id: Set(id.to_owned()),
            thread_id: Set("thread-revisions".to_owned()),
            status: Set(status.to_owned()),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
            turn_kind: Set("agent".to_owned()),
            origin: Set("task".to_owned()),
            mentions_json: Set("[]".to_owned()),
            message_revision: Set(1),
            work_owner: Set("turn".to_owned()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        agent_turn_response_execution::ActiveModel {
            turn_id: Set(id.to_owned()),
            execution_id: Set(child_id.to_owned()),
            presentation_snapshot_id: Set("S00000000000000000001".to_owned()),
            created_at: Set(now),
        }
        .insert(&db)
        .await
        .unwrap();
    }
    let transaction = db.begin().await.unwrap();
    let targets = cancel_agent_work_graph(&transaction, root_id, "root cancelled", now)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(targets.len(), 3);
    let active_child = targets
        .iter()
        .find(|target| target.execution_id == child_id)
        .unwrap();
    assert_eq!(active_child.turn_id.as_deref(), Some("revision-active"));
    // Standalone still uses its original parent-thread targeting semantics.
    assert_eq!(active_child.thread_id.as_deref(), Some("thread-cancel"));
    // A committed execution fence must still rediscover its live direct
    // response until the caller has interrupted that Turn.
    let transaction = db.begin().await.unwrap();
    let retry = cancel_agent_work_graph(&transaction, root_id, "repeat", now)
        .await
        .unwrap();
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].execution_id, child_id);
    assert_eq!(retry[0].turn_id.as_deref(), Some("revision-active"));
    transaction.rollback().await.unwrap();
    // Completion must retain the exact graph after that same durable fence.
    let transaction = db.begin().await.unwrap();
    let owners = cancel_agent_work_graph_and_collect_owners(&transaction, root_id, "wait", now)
        .await
        .unwrap();
    assert_eq!(owners.len(), 6);
    let revisions = owners
        .iter()
        .filter(|target| target.execution_id == child_id)
        .collect::<Vec<_>>();
    assert_eq!(revisions.len(), 4);
    assert!(
        revisions
            .iter()
            .all(|target| target.thread_id.as_deref() == Some("thread-revisions"))
    );
    assert_eq!(
        revisions
            .iter()
            .filter(|target| target.turn_pending)
            .count(),
        1
    );
    assert!(
        owners
            .iter()
            .all(|target| target.execution_id != unrelated_id)
    );
    assert!(
        owners
            .iter()
            .find(|target| target.execution_id == queued_id)
            .unwrap()
            .turn_id
            .is_none()
    );
    transaction.rollback().await.unwrap();
    for id in [root_id, child_id, queued_id] {
        assert_eq!(
            agent_execution::Entity::find_by_id(id)
                .one(&db)
                .await
                .unwrap()
                .unwrap()
                .status,
            "cancelled"
        );
    }
    assert_eq!(
        agent_execution::Entity::find_by_id(unrelated_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert!(
        agent_running_permit::Entity::find()
            .all(&db)
            .await
            .unwrap()
            .iter()
            .all(|permit| permit.status == "released")
    );
    assert_eq!(
        agent_work_queue::Entity::find_by_id("queue-child")
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "cancelled"
    );
    assert_eq!(
        agent_work_resource_scope::Entity::find_by_id(root_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "closed"
    );

    let transaction = db.begin().await.unwrap();
    assert!(
        cancel_agent_work_graph(&transaction, child_id, "forged child cancellation", now)
            .await
            .is_err()
    );
    transaction.rollback().await.unwrap();
}
