use crate::child_repository::TaskChildWorkRepository;
use crate::execution_store::TaskExecutionStore;
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
    recovery_job::RecoveryJobRecord,
    recovery_store::RecoveryStore,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{
    RecoveryAction, RecoveryJobStatus, RecoveryTrigger, Task, TaskEventPayload, TaskExecutorKind,
    TaskOccurrenceContract, TaskOccurrenceStatus, TaskRun, TaskRunExecutionStatus, TaskRunStatus,
    TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus, TaskStatus, TurnItemType,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Set, Statement,
    TransactionTrait,
};
use std::sync::Arc;
use tokio::sync::Notify;

pub async fn ensure_test_agent_identity(db: &PostgresDatabase, workspace_id: &str, timestamp: i64) {
    let now = unix_to_datetime(timestamp);
    ensure_pioneer_for_workspace(db, workspace_id, now.clone())
        .await
        .expect("agent domain workspace identity should seed");
}

pub async fn attach_test_agent_execution_contract(
    db: &PostgresDatabase,
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
use crate::test_support::task_projection::{NoFrozenOutput, sample_task, sample_task_run};

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

pub(crate) async fn seed_task_resume_thread(
    db: &PostgresDatabase,
    workspace: &str,
    thread_id: &str,
) {
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_by_actor_kind)
         VALUES ($1,$2,'','agent','model','provider','active','task_run','internal','system')",
        [thread_id.into(), workspace.into()],
    ))
    .await
    .expect("seed the Task resume fixture thread");
}

pub async fn seed_task_owned_resume_conflict(
    db: &PostgresDatabase,
    workspace_id: &str,
    suffix: &str,
) -> (Task, TaskRun, RecoveryJobRecord, String, String) {
    let timestamp = 1_700_011_000;
    let thread_id = format!("thread_task_owned_{suffix}");
    let turn_id = format!("turn_task_owned_{suffix}");
    seed_task_resume_thread(db, workspace_id, &thread_id).await;
    let store = TaskEventStore::new(db.clone(), NoFrozenOutput);
    ensure_test_agent_identity(db, workspace_id, timestamp).await;

    let mut task = sample_task(timestamp);
    task.id = format!("task_task_owned_{suffix}");
    task.workspace_id = workspace_id.to_owned();
    task.status = TaskStatus::Blocked;
    let mut run = sample_task_run(timestamp);
    run.id = format!("run_task_owned_{suffix}");
    run.task_id = task.id.clone();
    run.trigger_id = None;
    run.run_group_id = run.id.clone();
    run.status = TaskRunStatus::Blocked;

    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
            ],
            timestamp,
        )
        .await
        .expect("blocked conflict fixture should project");
    let execution = TaskExecutionStore::new(db)
        .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, timestamp + 1)
        .await
        .expect("conflict fixture execution should reserve");
    attach_test_agent_execution_contract(
        db,
        task.workspace_id.as_str(),
        task.id.as_str(),
        run.id.as_str(),
        execution.id.as_str(),
        thread_id.as_str(),
        timestamp + 1,
    )
    .await;
    TaskExecutionStore::new(db)
        .mark_execution_terminal(
            execution.id.as_str(),
            TaskRunExecutionStatus::Blocked,
            timestamp + 2,
            None,
            None,
        )
        .await
        .expect("conflict fixture execution should block");
    TaskChildWorkRepository::new(db)
        .upsert_task_run_turn(TaskRunTurn {
            id: format!("task_run_turn_task_owned_{suffix}"),
            task_id: task.id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id),
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::Blocked,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 2,
            started_at: Some(timestamp + 2),
            completed_at: Some(timestamp + 3),
        })
        .await
        .expect("conflict fixture run turn should persist");
    pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
        id: Set(turn_id.clone()),
        thread_id: Set(thread_id.clone()),
        status: Set("blocked".to_owned()),
        turn_kind: Set("conversation".to_owned()),
        origin: Set("system".to_owned()),
        prompt_manifest_json: Set("{}".to_owned()),
        created_at: Set(unix_to_datetime(timestamp + 2)),
        updated_at: Set(unix_to_datetime(timestamp + 3)),
        ..Default::default()
    })
    .exec(db)
    .await
    .expect("conflict fixture child turn should persist");
    let wake = Arc::new(Notify::new());
    let recovery = RecoveryStore::new(db, &wake);
    let job = recovery
        .enqueue_recovery_job(
            turn_id.clone(),
            format!("task_owned_{suffix}_item"),
            TurnItemType::Reasoning,
            None,
            RecoveryTrigger::Timeout,
            RecoveryAction::BlockResumable,
            Some("task child blocked".to_owned()),
            None,
            None,
            None,
            0,
            0,
            serde_json::json!({}),
            serde_json::json!({"base_backoff_secs": 0, "max_wall_clock_secs": 60}),
            timestamp + 4,
        )
        .await
        .expect("conflict fixture recovery job should enqueue");
    recovery
        .mark_recovery_job_terminal(
            job.id.as_str(),
            RecoveryJobStatus::Blocked,
            Some("task child blocked".to_owned()),
            timestamp + 5,
        )
        .await
        .expect("conflict fixture recovery job should block");

    (task, run, job, thread_id, turn_id)
}
