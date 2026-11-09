//! Discovery contract regressions. Execution remains gated on external review.
use super::*;
use crate::repositories::{task, task_run, thread_lineage};
use pioneer_protocol::TaskThreadLineage;

const AT: i64 = 1_700_000_000;

async fn fixture() -> (CrudStore, Task) {
    let store = test_store_with_workspace("ws_task").await;
    // Match Gateway's FK-disabled runtime, including deliberately inconsistent
    // persisted links below. Do not rely on declarative FK ownership checks.
    pioneer_sqlite::apply_sqlite_pragmas(&store.connection)
        .await
        .unwrap();
    let task = sample_task(AT);
    task::upsert_prepared_task(
        &store.connection,
        task::prepare_task_projection(&task).unwrap(),
    )
    .await
    .unwrap();
    (store.with_maintenance_access(), task)
}

async fn run(store: &CrudStore, task: &Task, n: u32, status: TaskRunStatus) -> TaskRun {
    let mut run = sample_task_run(AT);
    run.id = format!("r{n:020}");
    run.run_group_id = run.id.clone();
    run.task_id = task.id.clone();
    run.trigger_id = None;
    run.run_number = i64::from(n) + 1;
    run.status = status;
    run.completed_at = status.is_terminal().then_some(AT);
    task_run::upsert_run(&store.connection, &run).await.unwrap();
    run
}

async fn child(
    store: &CrudStore,
    run: &TaskRun,
    id: &str,
    status: TurnStatus,
    link_status: TaskRunTurnStatus,
    updated_at: i64,
) -> TaskRunTurn {
    let thread = sample_thread("ws_task", &format!("th_{id}"), AT);
    store
        .upsert_thread_model(&thread, PersistedActorRef::System)
        .await
        .unwrap();
    let turn = Turn {
        status,
        error: (status == TurnStatus::Failed).then(|| "child failure".to_owned()),
        ..sample_turn(id)
    };
    turn::upsert_turn(
        &store.connection,
        id,
        &thread.id,
        &turn,
        None,
        None,
        unix_to_datetime(AT - 10),
        unix_to_datetime(updated_at),
    )
    .await
    .unwrap();
    let link = TaskRunTurn {
        id: format!("link_{id}"),
        task_id: run.task_id.clone(),
        run_id: run.id.clone(),
        execution_id: None,
        thread_id: thread.id,
        turn_id: id.to_owned(),
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: link_status,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: AT - 10,
        started_at: Some(AT - 10),
        completed_at: Some(updated_at),
    };
    store.upsert_task_run_turn(link.clone()).await.unwrap();
    link
}

async fn discovered(store: &CrudStore, limit: u64) -> Vec<String> {
    store
        .list_unreconciled_terminal_task_child_turns(limit)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.turn_id)
        .collect()
}

#[tokio::test]
async fn many_runs_of_one_task_preserve_active_discovery_cases() {
    use TaskRunStatus as R;
    use TaskRunTurnStatus as L;
    use TurnStatus as T;

    let (store, task) = fixture().await;
    // Explicit expected outcomes, not a copy of the SQL predicate. Each row
    // belongs to a distinct run of the same task, including historical runs.
    let cases = [
        (R::Queued, L::InProgress, T::Completed, true),
        (R::Starting, L::InProgress, T::Failed, true),
        (R::Running, L::InProgress, T::Blocked, true),
        (R::Waiting, L::InProgress, T::Interrupted, true),
        (R::WaitingReview, L::InProgress, T::Completed, true),
        (R::Running, L::CandidateCreated, T::Completed, true),
        (R::WaitingReview, L::CandidateCreated, T::Completed, false),
        (R::Running, L::Cancelled, T::Interrupted, true),
        (R::Running, L::Cancelled, T::Completed, false),
        (R::Running, L::Blocked, T::Blocked, false),
        (R::Running, L::ReviewRecorded, T::Completed, false),
        (R::Running, L::InProgress, T::InProgress, false),
        (R::Succeeded, L::CandidateCreated, T::Completed, false),
        (R::Failed, L::Failed, T::Failed, false),
        (R::Cancelled, L::Cancelled, T::Interrupted, false),
        (R::TimedOut, L::Failed, T::Failed, false),
        (R::Blocked, L::InProgress, T::Completed, false),
    ];
    let mut expected = Vec::new();
    for (n, (run_status, link_status, turn_status, included)) in cases.into_iter().enumerate() {
        let id = format!("child_{n:02}");
        let run = run(&store, &task, n as u32, run_status).await;
        child(&store, &run, &id, turn_status, link_status, AT).await;
        if included {
            expected.push(id);
        }
    }
    assert_eq!(discovered(&store, 64).await, expected);
    let rows = store
        .list_unreconciled_terminal_task_child_turns(64)
        .await
        .unwrap();
    assert_eq!(rows[1].status, T::Failed);
    assert_eq!(rows[1].error.as_deref(), Some("child failure"));
    assert_eq!(rows[1].thread_id, "th_child_01");

    let mut closed_task = task;
    closed_task.status = TaskStatus::Completed;
    task::upsert_prepared_task(
        &store.connection,
        task::prepare_task_projection(&closed_task).unwrap(),
    )
    .await
    .unwrap();
    assert!(discovered(&store, 64).await.is_empty());
}

#[tokio::test]
async fn discovery_rejects_cross_task_run_and_turn_thread_links() {
    let (store, task) = fixture().await;
    let active = run(&store, &task, 1, TaskRunStatus::Running).await;
    let mut link = child(
        &store,
        &active,
        "owned_child",
        TurnStatus::Completed,
        TaskRunTurnStatus::InProgress,
        AT,
    )
    .await;
    assert_eq!(discovered(&store, 64).await, ["owned_child"]);

    let mut other_task = task.clone();
    other_task.id = "other_task".to_owned();
    task::upsert_prepared_task(
        &store.connection,
        task::prepare_task_projection(&other_task).unwrap(),
    )
    .await
    .unwrap();
    let foreign_run = run(&store, &other_task, 2, TaskRunStatus::Running).await;
    link.run_id = foreign_run.id;
    store.upsert_task_run_turn(link.clone()).await.unwrap();
    assert!(discovered(&store, 64).await.is_empty());

    // An old run of the same task must not borrow the active run's status.
    let historical = run(&store, &task, 3, TaskRunStatus::Succeeded).await;
    link.run_id = historical.id;
    store.upsert_task_run_turn(link.clone()).await.unwrap();
    assert!(discovered(&store, 64).await.is_empty());

    store
        .upsert_thread_model(
            &sample_thread("ws_task", "thr_other", AT),
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    link.run_id = active.id;
    link.thread_id = "thr_other".to_owned();
    store.upsert_task_run_turn(link.clone()).await.unwrap();
    assert!(discovered(&store, 64).await.is_empty());
    link.thread_id = "th_owned_child".to_owned();
    store.upsert_task_run_turn(link).await.unwrap();
    assert_eq!(discovered(&store, 64).await, ["owned_child"]);

    pioneer_entity::turn::Entity::update_many()
        .col_expr(
            pioneer_entity::turn::Column::TurnKind,
            Expr::value("task_run"),
        )
        .filter(pioneer_entity::turn::Column::Id.eq("owned_child"))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(discovered(&store, 64).await.is_empty());
}

async fn terminal_child(
    store: &CrudStore,
    task: &Task,
    n: u32,
    id: &str,
    status: TaskRunStatus,
    updated_at: i64,
) {
    let run = run(store, task, n, status).await;
    let link = child(
        store,
        &run,
        id,
        TurnStatus::Completed,
        TaskRunTurnStatus::CandidateCreated,
        updated_at,
    )
    .await;
    let parent_thread = sample_thread("ws_task", "thr_task", AT);
    let mut parent = Turn {
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::ScheduledTask,
        ..sample_turn(&run.id)
    };
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &parent,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    parent.status = TurnStatus::Completed;
    // Produce the healthy terminal receipt through canonical projection;
    // never fabricate accepted/projected sequence numbers in a fixture.
    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: "ws_task".to_owned(),
                thread_id: parent_thread.id.clone(),
                turn: parent,
            },
            AT,
        )
        .await
        .unwrap();
    thread_lineage::upsert_lineage(
        &store.connection,
        &TaskThreadLineage {
            child_thread_id: link.thread_id,
            parent_thread_id: parent_thread.id.clone(),
            root_thread_id: parent_thread.id.clone(),
            depth: 1,
            origin_kind: Some("task_run".to_owned()),
            created_by_thread_id: Some(parent_thread.id),
            created_by_turn_id: Some(run.id),
            created_at: AT,
        },
    )
    .await
    .unwrap();
    store
        .insert_turn_llm_context(NewTurnLlmContextEntry {
            turn_id: id.to_owned(),
            item_id: None,
            attempt_id: None,
            sequence: 0,
            source: "discovery_fixture".to_owned(),
            tool_name: None,
            payload: "{}".to_owned(),
            output_policy_snapshot: "{}".to_owned(),
            created_at: unix_to_datetime(AT),
            expires_at: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn discovery_globally_orders_and_limits_active_and_ready_terminal_children() {
    let (store, task) = fixture().await;
    // Insert out of order. The oldest timestamp wins before id; equal
    // timestamps interleave both branches under one shared limit.
    for (n, id, status, at) in [
        (1, "f_terminal", TaskRunStatus::TimedOut, AT),
        (2, "e_terminal", TaskRunStatus::Cancelled, AT),
        (3, "c_terminal", TaskRunStatus::Failed, AT),
        (4, "b_terminal", TaskRunStatus::Succeeded, AT - 1),
    ] {
        terminal_child(&store, &task, n, id, status, at).await;
    }
    for (n, id) in [(5, "d_active"), (6, "a_active")] {
        let run = run(&store, &task, n, TaskRunStatus::Running).await;
        child(
            &store,
            &run,
            id,
            TurnStatus::Completed,
            TaskRunTurnStatus::InProgress,
            AT,
        )
        .await;
    }
    let expected = [
        "b_terminal",
        "a_active",
        "c_terminal",
        "d_active",
        "e_terminal",
        "f_terminal",
    ];
    assert!(discovered(&store, 0).await.is_empty());
    assert_eq!(discovered(&store, 1).await, expected[..1]);
    assert_eq!(discovered(&store, 3).await, expected[..3]);
    assert_eq!(discovered(&store, u64::MAX).await, expected);

    // READY alone cannot keep selecting a capsule after its last cleanup
    // obligation disappears, including retained model history only.
    store
        .delete_turn_llm_context_for_turn("b_terminal")
        .await
        .unwrap();
    pioneer_entity::turn_llm_context::Entity::update_many()
        .col_expr(
            pioneer_entity::turn_llm_context::Column::Source,
            Expr::value("assistant_round"),
        )
        .filter(pioneer_entity::turn_llm_context::Column::TurnId.eq("c_terminal"))
        .exec(&store.connection)
        .await
        .unwrap();
    assert_eq!(
        discovered(&store, 64).await,
        ["a_active", "d_active", "e_terminal", "f_terminal"]
    );
}

async fn claimed_child(
    store: &CrudStore,
    thread: &str,
    turn: &str,
) -> pioneer_entity::task_child_reconcile_pending::Model {
    let tx = store.connection.begin().await.unwrap();
    crate::repositories::task_child_work::enqueue_point(&tx, thread, turn, AT)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let row = pioneer_entity::task_child_reconcile_pending::Entity::find_by_id(turn)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    store
        .claim_child_point_with_clock(&row, &|| AT)
        .await
        .unwrap()
        .unwrap()
}
async fn publish_capsule(store: &CrudStore, id: &str) {
    store
        .insert_turn_llm_context(NewTurnLlmContextEntry {
            turn_id: id.into(),
            item_id: None,
            attempt_id: None,
            sequence: 0,
            source: "late_capsule_fixture".into(),
            tool_name: None,
            payload: "{}".into(),
            output_policy_snapshot: "{}".into(),
            created_at: unix_to_datetime(AT),
            expires_at: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn queued_cleanup_revalidates_late_lineage_receipt_and_capsule_without_self_enqueue() {
    use crate::repositories::turn_event_projection_stream_state as receipt;
    use pioneer_entity::{
        task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
    };
    let (store, task) = fixture().await;
    terminal_child(
        &store,
        &task,
        77,
        "gated_child",
        TaskRunStatus::Succeeded,
        AT,
    )
    .await;
    let child_thread = "th_gated_child";
    let run_id = format!("r{:020}", 77);
    let claim = claimed_child(&store, child_thread, "gated_child").await;
    // An old/repair writer may miss a queue hook. Domain proof must still
    // reject a changed child kind while the old claim remains present.
    use pioneer_entity::turn as turn_rows;
    turn_rows::Entity::update_many()
        .filter(turn_rows::Column::Id.eq("gated_child"))
        .col_expr(turn_rows::Column::TurnKind, Expr::value("task_run"))
        .exec(&store.connection)
        .await
        .unwrap();
    assert!(!store.cleanup_claimed_child_capsule(&claim).await.unwrap());
    turn_rows::Entity::update_many()
        .filter(turn_rows::Column::Id.eq("gated_child"))
        .col_expr(turn_rows::Column::TurnKind, Expr::value("conversation"))
        .exec(&store.connection)
        .await
        .unwrap();
    let mut lineage = TaskThreadLineage {
        child_thread_id: child_thread.into(),
        parent_thread_id: "thr_task".into(),
        root_thread_id: "thr_task".into(),
        depth: 1,
        origin_kind: None,
        created_by_thread_id: Some("thr_task".into()),
        created_by_turn_id: Some(run_id.clone()),
        created_at: AT,
    };
    let tx = store.connection.begin().await.unwrap();
    thread_lineage::upsert_lineage(&tx, &lineage).await.unwrap();
    tx.commit().await.unwrap();
    assert!(
        !store.cleanup_claimed_child_capsule(&claim).await.unwrap(),
        "queue ownership is not positive lineage proof"
    );
    assert!(
        !store
            .list_turn_llm_context("gated_child")
            .await
            .unwrap()
            .is_empty()
    );
    lineage.origin_kind = Some("task_run".into());
    let tx = store.connection.begin().await.unwrap();
    thread_lineage::upsert_lineage(&tx, &lineage).await.unwrap();
    receipt::quarantine(
        &tx,
        "thr_task",
        &run_id,
        "repair-event",
        "repair required".into(),
        unix_to_datetime(AT + 1),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(
        !store.cleanup_claimed_child_capsule(&claim).await.unwrap(),
        "late quarantine invalidates healthy receipt authority even while claim remains valid"
    );
    let tx = store.connection.begin().await.unwrap();
    receipt::restore(&tx, &run_id, "repair-event", unix_to_datetime(AT + 2))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // These durable model-history rows outlive the transient child capsule.
    // Exercise the real insert/cleanup/discovery paths, not a copied source predicate.
    for (sequence, source) in [
        "turn_input_projection_v1",
        "runtime_context_v1",
        "system_snapshot_v1",
    ]
    .into_iter()
    .enumerate()
    {
        store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: "gated_child".into(),
                item_id: Some(source.into()),
                attempt_id: None,
                sequence: sequence as i64 + 1,
                source: source.into(),
                tool_name: None,
                payload: "{}".into(),
                output_policy_snapshot: "{}".into(),
                created_at: unix_to_datetime(AT),
                expires_at: None,
            })
            .await
            .unwrap();
    }
    let scopes_before = scope::Entity::find()
        .order_by_asc(scope::Column::ScopeKind)
        .order_by_asc(scope::Column::ScopeKey)
        .all(&store.connection)
        .await
        .unwrap();
    assert!(store.cleanup_claimed_child_capsule(&claim).await.unwrap());
    let mut retained_sources = store
        .list_turn_llm_context("gated_child")
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.source)
        .collect::<Vec<_>>();
    retained_sources.sort();
    assert_eq!(
        retained_sources,
        [
            "runtime_context_v1",
            "system_snapshot_v1",
            "turn_input_projection_v1",
        ],
        "capsule cleanup must preserve model history while removing transient context"
    );
    assert!(discovered(&store, 64).await.is_empty());
    assert!(
        point::Entity::find_by_id("gated_child")
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        scopes_before,
        scope::Entity::find()
            .order_by_asc(scope::Column::ScopeKind)
            .order_by_asc(scope::Column::ScopeKey)
            .all(&store.connection)
            .await
            .unwrap(),
        "successful capsule cleanup must not enqueue its own scope"
    );
    let empty_capsule_claim = claimed_child(&store, child_thread, "gated_child").await;
    assert!(
        store
            .child_point_record(&empty_capsule_claim)
            .await
            .unwrap()
            .is_none(),
        "broad scope rediscovery without a capsule must be a no-op"
    );
    store
        .finish_child_point(&empty_capsule_claim, None, true)
        .await
        .unwrap();
    store
        .insert_turn_llm_context(NewTurnLlmContextEntry {
            turn_id: "gated_child".into(),
            item_id: Some("late-runtime-observation".into()),
            attempt_id: None,
            sequence: 4,
            source: "runtime_context_v1".into(),
            tool_name: None,
            payload: "{}".into(),
            output_policy_snapshot: "{}".into(),
            created_at: unix_to_datetime(AT + 3),
            expires_at: None,
        })
        .await
        .unwrap();
    assert!(
        point::Entity::find_by_id("gated_child")
            .one(&store.connection)
            .await
            .unwrap()
            .is_none(),
        "a model-history append must not create a transient capsule obligation"
    );
    publish_capsule(&store, "gated_child").await;
    assert!(
        point::Entity::find_by_id("gated_child")
            .one(&store.connection)
            .await
            .unwrap()
            .is_some(),
        "late eligible capsule birth must publish a fresh obligation"
    );
    let next = claimed_child(&store, child_thread, "gated_child").await;
    assert_ne!(next.revision, claim.revision);
    assert!(store.cleanup_claimed_child_capsule(&next).await.unwrap());
}

// Complete cutover before parking, then discard setup-only fanout so no
// historical seed or unrelated fixture obligation can accidentally revive it.
async fn park_after_cutover(
    store: &CrudStore,
    claim: &pioneer_entity::task_child_reconcile_pending::Model,
) {
    use pioneer_entity::{
        task_child_reconcile_pending as point, task_child_reconcile_scope as scope,
    };
    while store.expand_child_scope(AT, true).await.unwrap() {}
    assert!(store.child_work_queues_ready().await.unwrap());
    scope::Entity::delete_many()
        .filter(scope::Column::ScopeKind.ne("historical-seed"))
        .exec(&store.connection)
        .await
        .unwrap();
    // Seed preserves the active claim; park through the same fenced operation
    // used by the real consumer when domain proof is missing.
    assert_eq!(
        store
            .terminal_task_child_cleanup_ready(&claim.thread_id, &claim.turn_id)
            .await
            .unwrap(),
        Some(false)
    );
    assert!(store.finish_child_point(claim, None, false).await.unwrap());
    let parked = point::Entity::find_by_id(&claim.turn_id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(parked.next_attempt_at.is_none());
    assert!(parked.claim_token.is_none());
}

async fn drain_dependency_scopes(store: &CrudStore) {
    // Bounded guard turns a broken EOF implementation into a clear assertion.
    for _ in 0..64 {
        if !store
            .expand_child_scope(chrono::Utc::now().timestamp() + 100, false)
            .await
            .unwrap()
        {
            return;
        }
    }
    panic!("dependency scope chain did not drain");
}

async fn assert_parked(store: &CrudStore, id: &str) {
    let row = pioneer_entity::task_child_reconcile_pending::Entity::find_by_id(id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(
        row.next_attempt_at.is_none(),
        "unrelated dependency must not wake child"
    );
    assert!(row.claim_token.is_none());
}

async fn consume_woken_capsule(store: &CrudStore, id: &str) {
    use pioneer_entity::task_child_reconcile_pending as point;
    let row = point::Entity::find_by_id(id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    assert!(
        row.next_attempt_at.is_some(),
        "reverse scope must revive parked point"
    );
    let claim = store
        .claim_child_point_with_clock(&row, &|| chrono::Utc::now().timestamp() + 100)
        .await
        .unwrap()
        .unwrap();
    assert!(store.child_point_record(&claim).await.unwrap().is_some());
    assert!(store.cleanup_claimed_child_capsule(&claim).await.unwrap());
    assert!(store.list_turn_llm_context(id).await.unwrap().is_empty());
    assert!(
        point::Entity::find_by_id(id)
            .one(&store.connection)
            .await
            .unwrap()
            .is_none(),
        "cleanup ACK must commit with deletion"
    );
}

#[tokio::test]
async fn immediate_cleanup_requires_distinct_accepted_source_and_absent_occurrence() {
    use pioneer_protocol::{TaskOccurrenceContract, TaskOccurrenceStatus};
    let (store, task) = fixture().await;
    let parent_thread = sample_thread("ws_task", "thr_task", AT);
    let creator = sample_turn("turn_task");
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &creator,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let trigger = super::sample_task_trigger(AT);
    crate::repositories::task_trigger::upsert_trigger(&store.connection, &trigger)
        .await
        .unwrap();
    let mut run = run(&store, &task, 78, TaskRunStatus::Succeeded).await;
    run.trigger_id = Some(trigger.id.clone());
    task_run::upsert_run(&store.connection, &run).await.unwrap();
    store
        .upsert_task_occurrence_contract(
            &TaskOccurrenceContract {
                occurrence_id: run.id.clone(),
                task_id: task.id.clone(),
                run_id: run.id.clone(),
                trigger_id: Some(trigger.id),
                occurrence_key: format!("immediate:{}", run.id),
                execution_generation: 79,
                agent_execution_id: None,
                work_graph_root_execution_id: None,
                root_resource_scope_id: None,
                status: TaskOccurrenceStatus::Running,
                queue_position: None,
                retry_attempt: 0,
                action_idempotency_key: format!("immediate-action:{}", run.id),
                route_id: None,
                result_return_route_id: None,
                delivery_plan: None,
                terminal_reason: None,
            },
            AT,
        )
        .await
        .unwrap();
    let link = child(
        &store,
        &run,
        "immediate_child",
        TurnStatus::Completed,
        TaskRunTurnStatus::CandidateCreated,
        AT,
    )
    .await;
    let tx = store.connection.begin().await.unwrap();
    thread_lineage::upsert_lineage(
        &tx,
        &TaskThreadLineage {
            child_thread_id: link.thread_id.clone(),
            parent_thread_id: parent_thread.id.clone(),
            root_thread_id: parent_thread.id.clone(),
            depth: 1,
            origin_kind: Some("task_run".into()),
            created_by_thread_id: Some(parent_thread.id.clone()),
            created_by_turn_id: Some(creator.id.clone()),
            created_at: AT,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    publish_capsule(&store, &link.turn_id).await;
    let claim = claimed_child(&store, &link.thread_id, &link.turn_id).await;
    assert!(
        !store.cleanup_claimed_child_capsule(&claim).await.unwrap(),
        "an immediate launch requires its accepted snapshot"
    );
    store
        .insert_task_run_conversation_snapshot_if_absent(crate::NewTaskRunConversationSnapshot {
            run_id: run.id.clone(),
            task_id: task.id,
            workspace_id: "ws_task".into(),
            conversation_thread_id: parent_thread.id.clone(),
            source_turn_id: Some("different_accepted_source".into()),
            history_json: "[]".into(),
            created_at: unix_to_datetime(AT),
        })
        .await
        .unwrap();
    assert!(
        !store.cleanup_claimed_child_capsule(&claim).await.unwrap(),
        "creator presence cannot substitute for distinct accepted source"
    );
    park_after_cutover(&store, &claim).await;
    let unrelated = sample_turn("unrelated_accepted_source");
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &unrelated,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    drain_dependency_scopes(&store).await;
    assert_parked(&store, &link.turn_id).await;
    let accepted_source = sample_turn("different_accepted_source");
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &accepted_source,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    drain_dependency_scopes(&store).await;
    consume_woken_capsule(&store, &link.turn_id).await;
    publish_capsule(&store, &link.turn_id).await;
    let next = claimed_child(&store, &link.thread_id, &link.turn_id).await;
    let occurrence = Turn {
        id: run.id,
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::ScheduledTask,
        ..sample_turn("unused")
    };
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &occurrence,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    assert!(
        !store.cleanup_claimed_child_capsule(&next).await.unwrap(),
        "an occurrence appearing after preparation invalidates the immediate absence proof"
    );
    assert!(
        !store
            .list_turn_llm_context(&link.turn_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn queue_cutover_requires_seed_completion_and_recognized_producer_generation() {
    use pioneer_entity::task_child_reconcile_pending as point;
    let (store, task) = fixture().await;
    terminal_child(
        &store,
        &task,
        79,
        "foreign_writer_child",
        TaskRunStatus::Succeeded,
        AT,
    )
    .await;
    assert!(
        !store.child_work_queues_ready().await.unwrap(),
        "schema installation alone cannot retire compatibility"
    );
    while store.expand_child_scope(AT, true).await.unwrap() {}
    assert!(store.child_work_queues_ready().await.unwrap());
    // A foreign repair writer installs an unknown producer generation after
    // startup; readiness must not be a stale process-local true cache.
    store.connection.execute_unprepared("CREATE TRIGGER task_occurrence_reconcile_unknown AFTER UPDATE ON task BEGIN SELECT 1; END").await.unwrap();
    assert!(!store.child_work_queues_ready().await.unwrap());
    point::Entity::delete_many()
        .exec(&store.connection)
        .await
        .unwrap();
    store.discover_legacy_child_work(32).await.unwrap();
    assert!(
        point::Entity::find_by_id("foreign_writer_child")
            .one(&store.connection)
            .await
            .unwrap()
            .is_some(),
        "compatibility must preserve late work from unrecognized writers"
    );
    let claim = claimed_child(&store, "th_foreign_writer_child", "foreign_writer_child").await;
    store.discover_legacy_child_work(32).await.unwrap();
    assert_eq!(
        point::Entity::find_by_id("foreign_writer_child")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap(),
        claim,
        "repeated discovery cannot invalidate an existing live claim"
    );
    store.finish_child_point(&claim, None, false).await.unwrap();
    store.discover_legacy_child_work(32).await.unwrap();
    assert!(
        point::Entity::find_by_id("foreign_writer_child")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap()
            .next_attempt_at
            .is_some(),
        "a newly proven legacy dependency can revive a parked obligation"
    );
}

#[tokio::test]
async fn taskrun_parent_terminal_receipt_arriving_after_child_close_releases_capsule() {
    let (store, task) = fixture().await;
    let run = run(&store, &task, 80, TaskRunStatus::Succeeded).await;
    let link = child(
        &store,
        &run,
        "receipt_late_child",
        TurnStatus::Completed,
        TaskRunTurnStatus::CandidateCreated,
        AT,
    )
    .await;
    let parent_thread = sample_thread("ws_task", "thr_task", AT);
    let mut parent = Turn {
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::ScheduledTask,
        ..sample_turn(&run.id)
    };
    store
        .materialize_turn_start(
            &parent_thread,
            SandboxMode::FullAccess,
            &parent,
            &[],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let tx = store.connection.begin().await.unwrap();
    thread_lineage::upsert_lineage(
        &tx,
        &TaskThreadLineage {
            child_thread_id: link.thread_id.clone(),
            parent_thread_id: parent_thread.id.clone(),
            root_thread_id: parent_thread.id.clone(),
            depth: 1,
            origin_kind: Some("task_run".into()),
            created_by_thread_id: Some(parent_thread.id.clone()),
            created_by_turn_id: Some(parent.id.clone()),
            created_at: AT,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    publish_capsule(&store, &link.turn_id).await;
    let claim = claimed_child(&store, &link.thread_id, &link.turn_id).await;
    assert!(!store.cleanup_claimed_child_capsule(&claim).await.unwrap());
    assert!(
        !store
            .list_turn_llm_context(&link.turn_id)
            .await
            .unwrap()
            .is_empty()
    );
    parent.status = TurnStatus::Completed;
    store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: "ws_task".into(),
                thread_id: parent_thread.id,
                turn: parent,
            },
            AT + 1,
        )
        .await
        .unwrap();
    assert!(
        store.cleanup_claimed_child_capsule(&claim).await.unwrap(),
        "the actual canonical terminal receipt, not queue ownership, releases the capsule"
    );
}

#[tokio::test]
async fn parked_taskrun_child_wakes_after_parent_thread_workspace_repair() {
    let (store, task) = fixture().await;
    terminal_child(
        &store,
        &task,
        81,
        "parent_workspace_child",
        TaskRunStatus::Succeeded,
        AT,
    )
    .await;
    // Keep the Task creator elsewhere: creator-thread fanout cannot rescue
    // this TaskRun parent-thread test if the parent-thread arm is lost.
    let mut independent_creator = task.clone();
    independent_creator.created_by_thread_id = Some("other_creator_thread".into());
    independent_creator.created_by_turn_id = Some("other_creator_turn".into());
    task::upsert_prepared_task(
        &store.connection,
        task::prepare_task_projection(&independent_creator).unwrap(),
    )
    .await
    .unwrap();
    let wrong = sample_thread("wrong_workspace", "thr_task", AT);
    store
        .upsert_thread_model(&wrong, PersistedActorRef::System)
        .await
        .unwrap();
    let claim = claimed_child(
        &store,
        "th_parent_workspace_child",
        "parent_workspace_child",
    )
    .await;
    assert!(!store.cleanup_claimed_child_capsule(&claim).await.unwrap());
    park_after_cutover(&store, &claim).await;
    let unrelated = sample_thread("ws_task", "unrelated_parent_thread", AT);
    store
        .upsert_thread_model(&unrelated, PersistedActorRef::System)
        .await
        .unwrap();
    drain_dependency_scopes(&store).await;
    assert_parked(&store, &claim.turn_id).await;
    let repaired = sample_thread("ws_task", "thr_task", AT);
    store
        .upsert_thread_model(&repaired, PersistedActorRef::System)
        .await
        .unwrap();
    drain_dependency_scopes(&store).await;
    consume_woken_capsule(&store, &claim.turn_id).await;
}

#[path = "task_child_owner_repair.rs"]
mod owner_repair;

#[tokio::test]
async fn shell_control_cleanup_defers_to_the_existing_child_receipt_gate() {
    let (store, task) = fixture().await;
    let active_run = run(&store, &task, 80, TaskRunStatus::Running).await;
    let pending = child(
        &store,
        &active_run,
        "pending_shell_child",
        TurnStatus::Failed,
        TaskRunTurnStatus::InProgress,
        AT,
    )
    .await;
    terminal_child(
        &store,
        &task,
        81,
        "settled_shell_child",
        TaskRunStatus::Succeeded,
        AT,
    )
    .await;
    for (turn_id, thread_id) in [
        (&pending.turn_id, &pending.thread_id),
        (
            &"settled_shell_child".to_owned(),
            &"th_settled_shell_child".to_owned(),
        ),
    ] {
        crate::repositories::turn_execution::insert_immutable(
            &store.connection,
            crate::NewTurnExecution {
                turn_id: turn_id.clone(),
                thread_id: thread_id.clone(),
                workspace_id: "ws_task".into(),
                executor_kind: crate::TurnExecutorKind::NativeAgent,
                executor_key: None,
                status: crate::TurnExecutionStatus::Running,
                owner_id: "owner".into(),
                lease_until: unix_to_datetime(AT + 10),
                created_at: unix_to_datetime(AT),
            },
        )
        .await
        .unwrap();
        crate::repositories::turn_execution::mark_terminal(
            &store.connection,
            turn_id,
            if turn_id == &pending.turn_id {
                crate::TurnExecutionStatus::Failed
            } else {
                crate::TurnExecutionStatus::Completed
            },
            unix_to_datetime(AT + 1),
        )
        .await
        .unwrap();
        store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: turn_id.clone(),
                item_id: Some("shell".into()),
                attempt_id: None,
                sequence: 1,
                source: "shell_control_v1".into(),
                tool_name: Some("write_stdin".into()),
                payload: "{}".into(),
                output_policy_snapshot: "{}".into(),
                created_at: unix_to_datetime(AT),
                expires_at: None,
            })
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .release_settled_shell_controls(&pending.turn_id)
            .await
            .unwrap(),
        0
    );
    store
        .delete_turn_llm_context_for_turn(&pending.turn_id)
        .await
        .unwrap();
    assert!(
        store
            .shell_control_receipt("ws_task", &pending.thread_id, &pending.turn_id, "shell")
            .await
            .unwrap()
            .is_some()
    );
    assert_ne!(
        store
            .cleanup_terminal_task_child_capsule(&pending.thread_id, &pending.turn_id)
            .await
            .unwrap(),
        Some(true)
    );
    let job = store
        .enqueue_recovery_job(
            "settled_shell_child".into(),
            "shell".into(),
            TurnItemType::CommandExecution,
            None,
            RecoveryTrigger::ProjectionFailure,
            RecoveryAction::RetryWithBackoff,
            None,
            None,
            None,
            None,
            1,
            2,
            serde_json::json!({}),
            serde_json::json!({}),
            AT,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .terminal_task_child_cleanup_ready("th_settled_shell_child", "settled_shell_child")
            .await
            .unwrap(),
        Some(false)
    );
    assert_eq!(
        store
            .cleanup_terminal_task_child_capsule("th_settled_shell_child", "settled_shell_child")
            .await
            .unwrap(),
        Some(false)
    );
    assert!(
        store
            .shell_control_receipt(
                "ws_task",
                "th_settled_shell_child",
                "settled_shell_child",
                "shell"
            )
            .await
            .unwrap()
            .is_some()
    );
    // The existing owner parks on this dependency. Its recovery settlement must
    // publish a new durable revision, without waiting for another Turn mutation.
    let parked = claimed_child(&store, "th_settled_shell_child", "settled_shell_child").await;
    assert!(
        store
            .finish_child_point(&parked, None, false)
            .await
            .unwrap()
    );
    assert!(
        store
            .mark_recovery_job_terminal(
                &job.id,
                RecoveryJobStatus::Blocked,
                Some("await resume".into()),
                AT + 2
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .terminal_task_child_cleanup_ready("th_settled_shell_child", "settled_shell_child")
            .await
            .unwrap(),
        Some(false),
        "unsettled blocked outbox still protects capsule"
    );
    let claims = store
        .claim_due_recovery_terminalizations(AT + 3, 45, 10)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(
        store
            .classify_claimed_recovery_terminalization(&claims[0], AT + 3)
            .await
            .unwrap(),
        crate::RecoveryTerminalizationClaimDisposition::SupersededByTerminalStatus(
            TurnStatus::Completed
        )
    );
    let republished =
        pioneer_entity::task_child_reconcile_pending::Entity::find_by_id("settled_shell_child")
            .one(&store.connection)
            .await
            .unwrap()
            .unwrap();
    assert_ne!(republished.revision, parked.revision);
    assert!(republished.next_attempt_at.is_some());
    assert!(!store.cleanup_claimed_child_capsule(&parked).await.unwrap());
    assert_eq!(
        store
            .get_recovery_job(&job.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RecoveryJobStatus::Blocked
    );
    assert!(
        !store
            .get_recovery_job(&job.id)
            .await
            .unwrap()
            .unwrap()
            .resolution_pending
    );
    // Settled audit history is insufficient while execution is still running.
    pioneer_entity::turn_execution::Entity::update_many()
        .filter(pioneer_entity::turn_execution::Column::TurnId.eq("settled_shell_child"))
        .col_expr(
            pioneer_entity::turn_execution::Column::Status,
            Expr::value("running"),
        )
        .col_expr(
            pioneer_entity::turn_execution::Column::CompletedAt,
            Expr::value(Option::<sea_orm::entity::prelude::DateTimeWithTimeZone>::None),
        )
        .exec(&store.connection)
        .await
        .unwrap();
    assert_eq!(
        store
            .terminal_task_child_cleanup_ready("th_settled_shell_child", "settled_shell_child")
            .await
            .unwrap(),
        Some(false)
    );
    pioneer_entity::turn_execution::Entity::update_many()
        .filter(pioneer_entity::turn_execution::Column::TurnId.eq("settled_shell_child"))
        .col_expr(
            pioneer_entity::turn_execution::Column::Status,
            Expr::value("completed"),
        )
        .col_expr(
            pioneer_entity::turn_execution::Column::CompletedAt,
            Expr::value(Some(unix_to_datetime(AT + 1))),
        )
        .exec(&store.connection)
        .await
        .unwrap();
    assert_eq!(
        store
            .terminal_task_child_cleanup_ready("th_settled_shell_child", "settled_shell_child")
            .await
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        store
            .cleanup_terminal_task_child_capsule("th_settled_shell_child", "settled_shell_child")
            .await
            .unwrap(),
        Some(true)
    );
    assert!(
        store
            .shell_control_receipt(
                "ws_task",
                "th_settled_shell_child",
                "settled_shell_child",
                "shell"
            )
            .await
            .unwrap()
            .is_none()
    );
    // The queued lifecycle owner must consume the same settled control capsule
    // under its claim fence and READY proof, without inventing canonical output.
    store
        .insert_turn_llm_context(NewTurnLlmContextEntry {
            turn_id: "settled_shell_child".into(),
            item_id: Some("late-shell".into()),
            attempt_id: None,
            sequence: 2,
            source: "shell_control_v1".into(),
            tool_name: Some("write_stdin".into()),
            payload: "{}".into(),
            output_policy_snapshot: "{}".into(),
            created_at: unix_to_datetime(AT),
            expires_at: None,
        })
        .await
        .unwrap();
    let claim = claimed_child(&store, "th_settled_shell_child", "settled_shell_child").await;
    assert!(store.cleanup_claimed_child_capsule(&claim).await.unwrap());
    assert!(
        store
            .list_turn_llm_context("settled_shell_child")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .shell_control_receipt("ws_task", &pending.thread_id, &pending.turn_id, "shell")
            .await
            .unwrap()
            .is_some()
    );
}
