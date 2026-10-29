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
