use super::*;
use crate::event_store::TaskEventStore;
use crate::occurrence_repository::TaskOccurrenceRepository;
use crate::test_support::{
    execution::{attach_test_agent_execution_contract, ensure_test_agent_identity},
    task_projection::{NoFrozenOutput, sample_task, sample_task_run},
};
use pioneer_crud_execution::{
    agent_execution::{AgentResourceStateInput, insert_agent_resource_state},
    execution_heartbeat::Registries,
    turn_ownership::TurnOwnership,
};
use pioneer_protocol::TaskEventPayload;
use sea_orm::{ColumnTrait, QueryFilter, sea_query::Expr};

#[tokio::test]
async fn task_execution_observation_uses_current_attempt_activity_and_heartbeat() {
    pioneer_postgres_test::with_database(async |fixture| {
        let workspace_id = "ws_task_observation";
        let writer = fixture.connection();
        writer
            .execute_unprepared(
                "UPDATE workspace SET is_current = false WHERE is_current;
                 INSERT INTO workspace (id, name, is_active, is_current)
                 VALUES ('ws_task_observation', 'Observation fixture', true, true)",
            )
            .await?;
        let db = SqliteDatabase::new(fixture.connect_read_only().await?, writer.clone());
        let store = TaskExecutionStore::new(&db);
        let occurrences = TaskOccurrenceRepository::new(db.clone());
        let registries = Registries::default();
        let execution_store = TurnOwnership::new(&db, &registries);

        let timestamp = 1_700_020_000;
        ensure_test_agent_identity(&db, workspace_id, timestamp).await;
        let mut task = sample_task(timestamp);
        task.id = "task_observation".to_owned();
        task.workspace_id = workspace_id.to_owned();
        let mut run = sample_task_run(timestamp);
        run.id = "run_observation".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();
        TaskEventStore::new(db.clone(), NoFrozenOutput)
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
            .expect("task and run should project");
        let execution = store
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, timestamp)
            .await
            .expect("execution should reserve");
        attach_test_agent_execution_contract(
            &db,
            workspace_id,
            task.id.as_str(),
            run.id.as_str(),
            execution.id.as_str(),
            "thread_observation",
            timestamp,
        )
        .await;

        let observe = || async {
            store
                .list_task_run_execution_observations(&[run.id.clone()])
                .await
        };
        let initial = observe().await.expect("initial observation should load");
        assert_eq!(initial[0].status, TaskRunExecutionStatus::Reserved);
        assert_eq!(initial[0].heartbeat_at, None);
        assert_eq!(initial[0].last_activity_at, None);

        store
            .mark_execution_running(execution.id.as_str(), timestamp + 1, None)
            .await
            .expect("execution should start");
        pioneer_entity::agent_execution_resource_state::Entity::update_many()
            .col_expr(
                pioneer_entity::agent_execution_resource_state::Column::Status,
                Expr::value("running"),
            )
            .filter(
                pioneer_entity::agent_execution_resource_state::Column::ExecutionId
                    .eq(execution.id.clone()),
            )
            .exec(&db)
            .await
            .expect("test resource attempt should start");
        store
            .heartbeat_execution_for_agent_attempt(execution.id.as_str(), 1, timestamp + 2, None)
            .await
            .expect("heartbeat should persist");
        let heartbeat_only = observe().await.expect("heartbeat observation should load");
        assert_eq!(heartbeat_only[0].heartbeat_at, Some(timestamp + 2));
        assert_eq!(heartbeat_only[0].last_activity_at, None);

        assert!(
            execution_store
                .record_agent_execution_progress(
                    execution.id.as_str(),
                    1,
                    "{}",
                    timestamp + 3,
                    None
                )
                .await
                .expect("progress should persist")
        );
        let active = observe().await.expect("activity observation should load");
        assert_eq!(active[0].last_activity_at, Some(timestamp + 3));

        let mut occurrence = occurrences
            .get_task_occurrence_contract_by_run(run.id.as_str())
            .await
            .expect("occurrence should load")
            .expect("occurrence should exist");
        occurrence.retry_attempt = 1;
        occurrences
            .upsert_task_occurrence_contract(&occurrence, timestamp + 4)
            .await
            .expect("current attempt should advance");
        let retry = observe().await.expect("retry observation should load");
        assert_eq!(retry[0].status, TaskRunExecutionStatus::Running);
        assert_eq!(retry[0].heartbeat_at, None);
        assert_eq!(retry[0].last_activity_at, None);

        insert_agent_resource_state(
            &db,
            &AgentResourceStateInput {
                id: format!("resource-{}-2", execution.id),
                execution_id: execution.id.clone(),
                attempt_generation: 2,
                branch_key: format!("task:{}:{}", task.id, run.id),
                fair_order: 1,
                now: unix_to_datetime(timestamp + 4),
            },
        )
        .await
        .expect("new resource attempt should persist");
        pioneer_entity::agent_execution_resource_state::Entity::update_many()
            .col_expr(
                pioneer_entity::agent_execution_resource_state::Column::Status,
                Expr::value("running"),
            )
            .filter(
                pioneer_entity::agent_execution_resource_state::Column::ExecutionId
                    .eq(execution.id.clone()),
            )
            .filter(pioneer_entity::agent_execution_resource_state::Column::AttemptGeneration.eq(2))
            .exec(&db)
            .await
            .expect("new resource attempt should start");
        store
            .heartbeat_execution_for_agent_attempt(execution.id.as_str(), 2, timestamp + 5, None)
            .await
            .expect("new attempt heartbeat should persist");
        assert!(
            execution_store
                .record_agent_execution_progress(
                    execution.id.as_str(),
                    2,
                    "{}",
                    timestamp + 6,
                    None
                )
                .await
                .expect("new attempt progress should persist")
        );
        let resumed = observe()
            .await
            .expect("new attempt observation should load");
        assert_eq!(resumed[0].heartbeat_at, Some(timestamp + 5));
        assert_eq!(resumed[0].last_activity_at, Some(timestamp + 6));
        Ok(())
    })
    .await
    .unwrap();
}
