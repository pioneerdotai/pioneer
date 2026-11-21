use super::*;
use sea_orm::{ActiveModelTrait, Set};

#[pioneer_postgres_test::postgres_test]
async fn root_turn_projects_exact_graph_queue_without_graph_wide_blocking() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection();
    db.execute_unprepared(
        "INSERT INTO workspace(id,name) VALUES('workspace-graph','Graph');
         INSERT INTO agent_identity(id,workspace_id,source_kind,source_id,source_fingerprint)
         VALUES('Aidentity123456789012','workspace-graph','ephemeral','graph','source');
         INSERT INTO agent_presentation_snapshot
            (id,agent_identity_id,source_revision,source_fingerprint,display_name,nickname)
         VALUES('S00000000000000000001','Aidentity123456789012',1,'source','Graph','graph');
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
         VALUES('thread-graph','workspace-graph','','agent','m','p','active');
         INSERT INTO turn(id,thread_id,status) VALUES('turn-graph','thread-graph','in_progress');",
    )
    .await
    .unwrap();
    let now = chrono::Utc::now().fixed_offset();
    let root_id = "Egraphroot12345678901";
    let queued_id = "Egraphqueue1234567890";
    let blocked_id = "Egraphblock1234567890";
    for (id, parent_execution_id, status) in [
        (root_id, None, "running"),
        (queued_id, Some(root_id), "queued"),
        (blocked_id, Some(root_id), "blocked"),
    ] {
        agent_execution::ActiveModel {
            id: Set(id.to_owned()),
            workspace_id: Set("workspace-graph".to_owned()),
            agent_identity_id: Set("Aidentity123456789012".to_owned()),
            identity_source_revision: Set(1),
            identity_source_fingerprint: Set("source".to_owned()),
            parent_execution_id: Set(parent_execution_id.map(str::to_owned)),
            parent_task_id: Set(None),
            parent_thread_id: Set(Some("thread-graph".to_owned())),
            home_root_thread_id: Set("thread-graph".to_owned()),
            work_graph_root_execution_id: Set(root_id.to_owned()),
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
            finished_at: Set((status == "blocked").then_some(now)),
        }
        .insert(db)
        .await
        .unwrap();
    }
    agent_work_resource_scope::ActiveModel {
        root_execution_id: Set(root_id.to_owned()),
        scope_generation: Set(1),
        max_concurrency: Set(1),
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
    .insert(db)
    .await
    .unwrap();
    for (id, execution_id, attempt_generation, progress_sequence, status) in [
        ("state-root-old", root_id, 1, 2, "failed"),
        ("state-root", root_id, 2, 7, "running"),
        ("state-queued", queued_id, 1, 0, "queued"),
        ("state-blocked", blocked_id, 1, 3, "failed"),
    ] {
        agent_execution_resource_state::ActiveModel {
            id: Set(id.to_owned()),
            execution_id: Set(execution_id.to_owned()),
            attempt_generation: Set(attempt_generation),
            progress_sequence: Set(progress_sequence),
            progress_frontier_json: Set("{}".to_owned()),
            last_progress_at: Set(None),
            last_heartbeat_at: Set(None),
            idle_deadline: Set(None),
            hard_deadline: Set(None),
            local_usage_json: Set("{}".to_owned()),
            permit_id: Set((status == "running").then(|| "permit-root".to_owned())),
            branch_key: Set(execution_id.to_owned()),
            fair_order: Set(1),
            status: Set(status.to_owned()),
            fencing_generation: Set(1),
            fenced_at: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(db)
        .await
        .unwrap();
    }
    agent_turn_response_execution::ActiveModel {
        turn_id: Set("turn-graph".to_owned()),
        execution_id: Set(root_id.to_owned()),
        presentation_snapshot_id: Set("S00000000000000000001".to_owned()),
        created_at: Set(now),
    }
    .insert(db)
    .await
    .unwrap();

    let graph = load_agent_work_graph_projection_for_turn(db, "turn-graph")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(graph.root_execution_id.as_str(), root_id);
    assert_eq!(graph.updated_at_unix_micros, now.timestamp_micros());
    assert_eq!(graph.running_count, 1);
    assert_eq!(graph.queued_count, 1);
    assert_eq!(graph.terminal_count, 1);
    assert!(graph.saturated);
    assert_eq!(graph.nodes.len(), 3);
    assert!(graph.nodes.iter().any(|node| {
        node.execution_id.as_str() == root_id
            && node.state == AgentWorkNodeState::Running
            && node.progress_revision == 7
    }));
    assert!(graph.nodes.iter().any(|node| {
        node.execution_id.as_str() == blocked_id && node.state == AgentWorkNodeState::Blocked
    }));
}
