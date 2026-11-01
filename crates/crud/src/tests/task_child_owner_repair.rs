//! R10 primary lifecycle regression. Gate CLOSED; NOT_RUN.
use super::*;
use crate::events::TurnEventPayload;
use pioneer_entity::{
    task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
    turn_event as event, turn_event_projection_state as receipt,
    turn_event_projection_stream_state as stream,
};

#[tokio::test]
async fn owner_only_backfill_revives_parked_child_with_already_covered_terminal_receipt() {
    let (store, task) = fixture().await;
    terminal_child(
        &store,
        &task,
        91,
        "owner_repair_child",
        TaskRunStatus::Succeeded,
        AT,
    )
    .await;
    let run_id = format!("r{:020}", 91);
    // Persist a real supported agent-diff completion, then corrupt only its
    // legacy owners. Terminal acceptance/watermarks come from real projection.
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws_task".into(),
                thread_id: "thr_task".into(),
                turn_id: run_id.clone(),
                item: TurnItem::SystemEvent {
                    id: "owner_repair_diff".into(),
                    level: pioneer_protocol::SystemEventLevel::Info,
                    message: "Diff updated".into(),
                    code: Some("agent_diff_updated".into()),
                    details: None,
                },
            },
            AT + 1,
        )
        .await
        .unwrap();
    let db = store.database_connection();
    let diff = event::Entity::find()
        .filter(event::Column::TurnId.eq(&run_id))
        .filter(event::Column::EventType.eq(pioneer_protocol::constants::events::ITEM_COMPLETED))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let mut payload: TurnEventPayload = serde_json::from_str(&diff.payload).unwrap();
    let TurnEventPayload::ItemCompleted(ref mut item) = payload else {
        panic!("item completion required")
    };
    item.thread_id = "legacy_session_thread".into();
    let payload_json = serde_json::to_string(&payload).unwrap();
    let key = payload.idempotency_key().unwrap();
    let tx = db.begin().await.unwrap();
    event::Entity::update_many()
        .col_expr(
            event::Column::ThreadId,
            Expr::value("legacy_session_thread"),
        )
        .col_expr(event::Column::Payload, Expr::value(payload_json))
        .col_expr(event::Column::IdempotencyKey, Expr::value(key))
        .filter(event::Column::Id.eq(&diff.id))
        .exec(&tx)
        .await
        .unwrap();
    receipt::Entity::update_many()
        .col_expr(
            receipt::Column::ThreadId,
            Expr::value("legacy_session_thread"),
        )
        .filter(receipt::Column::EventId.eq(&diff.id))
        .exec(&tx)
        .await
        .unwrap();
    stream::Entity::update_many()
        .col_expr(
            stream::Column::ThreadId,
            Expr::value("legacy_session_thread"),
        )
        .filter(stream::Column::TurnId.eq(&run_id))
        .exec(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before = stream::Entity::find_by_id(&run_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(before.accepted_terminal_sequence.is_some());
    assert!(before.projected_through_sequence >= before.accepted_terminal_sequence.unwrap());
    let claim = claimed_child(&store, "th_owner_repair_child", "owner_repair_child").await;
    park_after_cutover(&store, &claim).await;
    // A refused owner UPDATE must roll back earlier event/delivery repairs,
    // leave the dependency parked, and never publish a synthetic after-image.
    db.execute_unprepared("CREATE TRIGGER test_owner_repair_refusal BEFORE UPDATE ON turn_event_projection_stream_state WHEN OLD.turn_id='r00000000000000000091' BEGIN SELECT RAISE(IGNORE); END").await.unwrap();
    assert!(
        store
            .backfill_turn_event_projection_stream_states_batch(None, 64)
            .await
            .is_err()
    );
    assert_eq!(
        stream::Entity::find_by_id(&run_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(
        event::Entity::find_by_id(&diff.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .thread_id,
        "legacy_session_thread"
    );
    assert!(
        scope::Entity::find_by_id(("run".to_owned(), run_id.clone()))
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
    assert_parked(&store, &claim.turn_id).await;
    db.execute_unprepared("DROP TRIGGER test_owner_repair_refusal")
        .await
        .unwrap();
    // A successful repair and its publication must roll back together.
    let tx = db.begin().await.unwrap();
    crate::repositories::turn_event_projection_state::repair_legacy_agent_diff_thread_owners_for_turn(
        &tx, &run_id, "thr_task").await.unwrap();
    assert!(
        scope::Entity::find_by_id(("run".to_owned(), run_id.clone()))
            .one(&tx)
            .await
            .unwrap()
            .is_some()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        stream::Entity::find_by_id(&run_id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert!(
        scope::Entity::find_by_id(("run".to_owned(), run_id.clone()))
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
    assert_parked(&store, &claim.turn_id).await;
    let batch = store
        .backfill_turn_event_projection_stream_states_batch(None, 64)
        .await
        .unwrap();
    assert_eq!(batch.events_repaired, 1);
    assert_eq!(
        batch.watermarks_advanced, 0,
        "owner repair must publish independently of watermark advancement"
    );
    let after = stream::Entity::find_by_id(&run_id)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.thread_id, "thr_task");
    assert_eq!(
        after.projected_through_sequence,
        before.projected_through_sequence
    );
    assert_eq!(
        after.accepted_terminal_sequence,
        before.accepted_terminal_sequence
    );
    drain_dependency_scopes(&store).await;
    consume_woken_capsule(&store, &claim.turn_id).await;
    assert!(
        store
            .list_turn_llm_context(&claim.turn_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        point::Entity::find_by_id(&claim.turn_id)
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
    // Idempotent public rerun has no owner change and publishes no new Run work.
    assert_eq!(
        store
            .backfill_turn_event_projection_stream_states_batch(None, 64)
            .await
            .unwrap()
            .events_repaired,
        0
    );
    assert!(
        scope::Entity::find_by_id(("run".to_owned(), run_id))
            .one(&db)
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires disposable provisioned MySQL; validation gate CLOSED"]
async fn mysql_rr_parent_counterpart_repair_cannot_suppress_later_run_obligation() {
    use pioneer_entity::task_run_occurrence_reconcile_pending as parents;
    let url = std::env::var("PIONEER_DISPOSABLE_MYSQL_TEST_URL")
        .expect("disposable migrated schema with ws_task required");
    assert!(url.starts_with("mysql://"));
    let db = sea_orm::Database::connect(url).await.unwrap();
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
    crate::repositories::task_run::update_run_status(&source, &id, TaskRunStatus::Blocked, now)
        .await
        .unwrap();
    source.commit().await.unwrap();
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
                error
                    .downcast_ref::<crate::repositories::portable_payload::PreflightRace>()
                    .is_some()
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
