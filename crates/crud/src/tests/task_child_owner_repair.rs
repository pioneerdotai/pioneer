//! Concurrent parent and execution ownership changes.
use super::*;

#[pioneer_postgres_test::postgres_test]
async fn counterpart_repair_cannot_suppress_later_run_obligation() {
    use pioneer_entity::task_run_occurrence_reconcile_pending as parents;
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let db = fixture.connection().clone();
    db.execute_unprepared(
        "INSERT INTO workspace (id,name,is_active,is_current) VALUES ('ws_task','Task',TRUE,FALSE)",
    )
    .await
    .unwrap();
    let id = pioneer_protocol::generate_id(21);
    let task_id = pioneer_protocol::generate_id(21);
    let thread_id = pioneer_protocol::generate_id(21);
    let now = chrono::Utc::now().fixed_offset();
    let init = db.begin().await.unwrap();
    let mut task = sample_task(now.timestamp());
    task.id = task_id.clone();
    crate::repositories::task::upsert_prepared_task(
        &init,
        crate::repositories::task::prepare_task_projection(&task).unwrap(),
    )
    .await
    .unwrap();
    let mut run = sample_task_run(now.timestamp());
    run.id = id.clone();
    run.run_group_id = id.clone();
    run.task_id = task_id;
    run.status = TaskRunStatus::Running;
    run.completed_at = None;
    run.trigger_id = None;
    crate::repositories::task_run::upsert_run(&init, &run)
        .await
        .unwrap();
    let thread = sample_thread("ws_task", &thread_id, now.timestamp());
    crate::repositories::thread::upsert_thread_with_creator(
        &init,
        &thread,
        &pioneer_protocol::PersistedActorRef::System,
        now,
        now,
    )
    .await
    .unwrap();
    let mut parent = sample_turn(&id);
    parent.turn_kind = pioneer_protocol::TurnKind::TaskRun;
    parent.status = pioneer_protocol::TurnStatus::Blocked;
    crate::repositories::turn::upsert_turn(&init, &id, &thread_id, &parent, None, None, now, now)
        .await
        .unwrap();
    init.commit().await.unwrap();
    let source = db
        .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
        .await
        .unwrap();
    assert_eq!(
        pioneer_entity::turn::Entity::find_by_id(&id)
            .one(&source)
            .await
            .unwrap()
            .unwrap()
            .status,
        "blocked"
    );
    assert_eq!(
        pioneer_entity::task_run::Entity::find_by_id(&id)
            .one(&source)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    // A legitimate repair while Run is running makes the old blocked Turn
    // in_progress. Its producer knows the pair is consistent/nonterminal.
    let other = db.begin().await.unwrap();
    parent.status = pioneer_protocol::TurnStatus::InProgress;
    crate::repositories::turn::upsert_turn(&other, &id, &thread_id, &parent, None, None, now, now)
        .await
        .unwrap();
    other.commit().await.unwrap();
    assert!(
        parents::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
    let error =
        crate::repositories::task_run::update_run_status(&source, &id, TaskRunStatus::Blocked, now)
            .await
            .unwrap_err();
    assert!(pioneer_sqlite::is_retryable_database_conflict(&error));
    source.rollback().await.unwrap();
    assert_eq!(
        pioneer_entity::task_run::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    let retry = db.begin().await.unwrap();
    crate::repositories::task_run::update_run_status(&retry, &id, TaskRunStatus::Blocked, now)
        .await
        .unwrap();
    retry.commit().await.unwrap();
    assert_eq!(
        pioneer_entity::turn::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .status,
        "in_progress"
    );
    assert!(
        parents::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .is_some(),
        "current blocked/in_progress mismatch must retain durable work"
    );
    // Distinct RR boundary: a mapping born after the agent transaction's
    // snapshot can have its first ticket drained before agent finalization.
    use crate::repositories::{
        agent_domain, agent_identity_catalog, task_run_execution as executions,
    };
    use pioneer_entity::{
        agent_identity, task_occurrence_reconcile_pending as contracts,
        task_run_execution as mapping,
    };
    let execution_id = pioneer_protocol::generate_id(21);
    let init = db.begin().await.unwrap();
    agent_identity_catalog::ensure_pioneer_for_workspace(&init, "ws_task", now)
        .await
        .unwrap();
    let identity = agent_identity::Entity::find()
        .filter(agent_identity::Column::WorkspaceId.eq("ws_task"))
        .one(&init)
        .await
        .unwrap()
        .unwrap();
    agent_domain::insert_agent_execution(
        &init,
        &agent_domain::AgentExecutionInput {
            id: execution_id.clone(),
            workspace_id: "ws_task".into(),
            agent_identity_id: identity.id,
            identity_source_revision: identity.source_revision,
            identity_source_fingerprint: identity.source_fingerprint,
            parent_execution_id: None,
            parent_task_id: Some(run.task_id.clone()),
            parent_thread_id: Some(thread_id.clone()),
            home_root_thread_id: thread_id.clone(),
            work_graph_root_execution_id: execution_id.clone(),
            requested_identity_selection_json: "{}".into(),
            requested_profile_selection_json: "{}".into(),
            resolved_profile_id: None,
            resolved_profile_fingerprint: None,
            presentation_snapshot_id: None,
            authorization_context_fingerprint: "regression".into(),
            execution_generation: 1,
            status: "running".into(),
            now,
        },
    )
    .await
    .unwrap();
    init.commit().await.unwrap();
    let source = db
        .begin_with_config(Some(sea_orm::IsolationLevel::RepeatableRead), None)
        .await
        .unwrap();
    assert!(
        mapping::Entity::find_by_id(&execution_id)
            .one(&source)
            .await
            .unwrap()
            .is_none()
    );
    let other = db.begin().await.unwrap();
    executions::insert_execution_if_absent(
        &other,
        executions::NewTaskRunExecution {
            id: execution_id.clone(),
            task_id: run.task_id.clone(),
            task_run_id: id.clone(),
            executor_kind: pioneer_protocol::TaskExecutorKind::Agent,
            status: pioneer_protocol::TaskRunExecutionStatus::Running,
            worker_id: None,
            lease_until: None,
            heartbeat_at: None,
            started_at: None,
            completed_at: None,
            result: None,
            error: None,
            created_at: now.timestamp(),
            updated_at: now.timestamp(),
        },
    )
    .await
    .unwrap();
    other.commit().await.unwrap();
    contracts::Entity::delete_by_id(&id)
        .exec(&db)
        .await
        .unwrap();
    let outcome =
        agent_domain::finalize_agent_execution(&source, &execution_id, "succeeded", now).await;
    match outcome {
        Ok(true) => source.commit().await.unwrap(),
        Err(error) => {
            // A stale sequence CAS is allowed only by unwinding the entire
            // source transaction; never retry its old RR snapshot in place.
            assert!(
                error.downcast_ref::<crate::util::PreflightRace>().is_some()
                    || pioneer_sqlite::is_retryable_database_conflict(&error)
            );
            source.rollback().await.unwrap();
            let retry = db.begin().await.unwrap();
            assert!(
                agent_domain::finalize_agent_execution(&retry, &execution_id, "succeeded", now)
                    .await
                    .unwrap()
            );
            retry.commit().await.unwrap();
        }
        Ok(false) => panic!("agent must finalize"),
    }
    assert!(
        contracts::Entity::find_by_id(&id)
            .one(&db)
            .await
            .unwrap()
            .is_some(),
        "late mapping must receive the final agent obligation after the earlier ticket was drained"
    );
}
