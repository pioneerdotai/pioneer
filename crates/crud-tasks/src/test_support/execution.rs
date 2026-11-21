use crate::occurrence_repository::TaskOccurrenceRepository;
use crate::values::unix_to_datetime;
use pioneer_crud_execution::{
    agent_execution::{
        AgentExecutionInput, AgentResourceStateInput, insert_agent_execution,
        insert_agent_resource_state,
    },
    agent_graph_resources::ensure_root_resource_scope,
    agent_identity::load_current_agent_presentation_snapshot,
    agent_identity_catalog::ensure_pioneer_for_workspace,
};
use pioneer_protocol::{TaskOccurrenceContract, TaskOccurrenceStatus};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

pub async fn ensure_test_agent_identity(db: &SqliteDatabase, workspace_id: &str, timestamp: i64) {
    let now = unix_to_datetime(timestamp);
    ensure_pioneer_for_workspace(db, workspace_id, now.clone())
        .await
        .expect("agent domain workspace identity should seed");
}

pub async fn attach_test_agent_execution_contract(
    db: &SqliteDatabase,
    workspace_id: &str,
    task_id: &str,
    run_id: &str,
    execution_id: &str,
    home_root_thread_id: &str,
    timestamp: i64,
) {
    let now = unix_to_datetime(timestamp);
    let identity = pioneer_entity::agent_identity::Entity::find()
        .filter(pioneer_entity::agent_identity::Column::WorkspaceId.eq(workspace_id))
        .one(db)
        .await
        .expect("test Agent identity lookup should succeed")
        .expect("test Agent identity should exist");
    let snapshot = load_current_agent_presentation_snapshot(
        db,
        identity.id.as_str(),
        identity.source_revision,
        identity.source_fingerprint.as_str(),
    )
    .await
    .expect("test Agent presentation lookup should succeed")
    .expect("test Agent presentation should exist");

    let tx = db.begin().await.unwrap();
    let (execution, inserted) = insert_agent_execution(
        &tx,
        &AgentExecutionInput {
            id: execution_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            agent_identity_id: identity.id,
            identity_source_revision: identity.source_revision,
            identity_source_fingerprint: identity.source_fingerprint,
            parent_execution_id: None,
            parent_task_id: Some(task_id.to_owned()),
            parent_thread_id: Some(home_root_thread_id.to_owned()),
            home_root_thread_id: home_root_thread_id.to_owned(),
            work_graph_root_execution_id: execution_id.to_owned(),
            requested_identity_selection_json: "{}".to_owned(),
            requested_profile_selection_json: "{}".to_owned(),
            resolved_profile_id: None,
            resolved_profile_fingerprint: None,
            presentation_snapshot_id: Some(snapshot.id),
            authorization_context_fingerprint: "test-authorization".to_owned(),
            execution_generation: 1,
            status: "running".to_owned(),
            now,
        },
    )
    .await
    .expect("test AgentExecution should persist");
    if inserted {
        crate::task_occurrence_producers::agent_change(&tx, None, Some(&execution))
            .await
            .expect("Task occurrence source should update");
    }
    ensure_root_resource_scope(&tx, execution_id, 1, 8, 8, 8, 64, now)
        .await
        .expect("test Agent root scope should persist");
    insert_agent_resource_state(
        &tx,
        &AgentResourceStateInput {
            id: format!("resource-{execution_id}-1"),
            execution_id: execution_id.to_owned(),
            attempt_generation: 1,
            branch_key: format!("task:{task_id}:{run_id}"),
            fair_order: 1,
            now,
        },
    )
    .await
    .expect("test Agent resource attempt should persist");
    tx.commit().await.unwrap();
    TaskOccurrenceRepository::new(db.clone())
        .upsert_task_occurrence_contract(
            &TaskOccurrenceContract {
                occurrence_id: run_id.to_owned(),
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                trigger_id: None,
                occurrence_key: format!("immediate:{run_id}"),
                execution_generation: 1,
                agent_execution_id: Some(execution_id.to_owned()),
                work_graph_root_execution_id: Some(execution_id.to_owned()),
                root_resource_scope_id: Some(execution_id.to_owned()),
                status: TaskOccurrenceStatus::Running,
                queue_position: None,
                retry_attempt: 0,
                action_idempotency_key: format!("task:{task_id}:{run_id}"),
                route_id: None,
                result_return_route_id: None,
                delivery_plan: None,
                terminal_reason: None,
            },
            timestamp,
        )
        .await
        .expect("test Task occurrence contract should persist");
}

use crate::event_fanout::{TaskEventFanoutOutcome, TaskEventFanoutPage};
use crate::event_records::AppendedTaskEvent;
use crate::event_store::TaskEventStore;
use crate::test_support::task_projection::NoFrozenOutput;

pub(crate) async fn drain_custom_publisher_fanout(
    store: &TaskEventStore<NoFrozenOutput>,
) -> Vec<AppendedTaskEvent> {
    let clock = || chrono::Utc::now().timestamp();
    let mut delivered = Vec::new();
    for row in store.due_task_event_fanout(clock(), 64).await.unwrap() {
        let claim = store
            .claim_task_event_fanout(&row, &clock)
            .await
            .unwrap()
            .unwrap();
        let after = store
            .get_task_event_fanout_cursor(&row.task_id)
            .await
            .unwrap()
            .unwrap_or(0);
        let mut bytes = crate::event_fanout::TASK_EVENT_FANOUT_BYTE_BUDGET;
        let TaskEventFanoutPage::Prefix { events, .. } = store
            .task_event_fanout_page(&row.task_id, after, 128, &mut bytes, true)
            .await
            .unwrap()
        else {
            panic!("small custom publisher fixture must fit its fanout budget")
        };
        for event in events {
            let event = event.unwrap();
            store
                .ack_task_event_fanout(&claim, event.sequence, &clock)
                .await
                .unwrap();
            delivered.push(event);
        }
        store
            .release_task_event_fanout(&claim, TaskEventFanoutOutcome::Delivered, &clock)
            .await
            .unwrap();
    }
    assert!(!store.has_pending_task_event_fanout().await.unwrap());
    delivered
}
