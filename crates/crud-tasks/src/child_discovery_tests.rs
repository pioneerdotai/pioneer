use super::*;
use crate::{
    task, task_run,
    test_support::task_projection::{sample_task, sample_task_run},
    values::unix_to_datetime,
};
use pioneer_crud_conversations::turn_writes::{
    apply_prepared_turn_upsert, prepare_turn_upsert_with_actor_columns,
};
use pioneer_crud_conversations::{
    events::TurnEventPayload,
    projection_stream, thread_lineage,
    turn_context::{self, NewTurnLlmContextEntry},
};
use pioneer_crud_execution::event_append::{PreparedTurnEvent, append_prepared_event};
use pioneer_protocol::*;
use sea_orm::DatabaseTransaction;

const AT: i64 = 1_700_000_000;

async fn upsert_thread(
    db: &SqliteDatabase,
    thread: &Thread,
    actor: PersistedActorRef,
) -> Result<bool> {
    let tx = db.begin().await?;
    let changed = pioneer_crud_conversations::thread_mutations::upsert_thread_with_creator(
        &tx,
        thread,
        &actor,
        unix_to_datetime(thread.created_at),
        unix_to_datetime(thread.updated_at),
    )
    .await?;
    tx.commit().await?;
    Ok(changed)
}

async fn upsert_turn(
    db: &DatabaseTransaction,
    id: &str,
    thread: &str,
    turn: &Turn,
    _: Option<()>,
    _: Option<()>,
    created_at: sea_orm::prelude::DateTimeWithTimeZone,
    updated_at: sea_orm::prelude::DateTimeWithTimeZone,
) -> Result<()> {
    let prepared = prepare_turn_upsert_with_actor_columns(
        db, id, thread, turn, None, None, None, None, None, false, created_at, updated_at,
    )
    .await?;
    apply_prepared_turn_upsert(db, prepared).await.map(|_| ())
}

fn sample_thread(workspace_id: &str, thread_id: &str, timestamp: i64) -> Thread {
    Thread {
        context_window: None,
        service_mode: None,
        context_authority: None,
        workspace_id: workspace_id.to_owned(),
        id: thread_id.to_owned(),
        name: None,
        preview: String::new(),
        preview_author: None,
        mode: ThreadMode::Agent,
        model: "gpt-5.4".to_owned(),
        model_provider: "openai".to_owned(),
        reasoning_effort: None,
        created_at: timestamp,
        updated_at: timestamp,
        status: ThreadStatus::Active,
        origin_kind: ThreadOriginKind::User,
        sidebar_visibility: ThreadSidebarVisibility::Visible,
        agent_nickname: None,
        agent_role: None,
        visibility: None,
        turns: Vec::new(),
    }
}
fn sample_turn(turn_id: &str) -> Turn {
    Turn {
        work_owner: Default::default(),
        id: turn_id.to_owned(),
        status: TurnStatus::InProgress,
        turn_kind: Default::default(),
        origin: Default::default(),
        mode: Default::default(),
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    }
}

async fn persist_task(store: &SqliteDatabase, task: &Task) {
    let prepared = task::prepare_task_projection(task).unwrap();
    let tx = store.begin().await.unwrap();
    task::upsert_prepared_task(&tx, prepared).await.unwrap();
    tx.commit().await.unwrap();
}

async fn persist_run(store: &SqliteDatabase, run: &TaskRun) {
    let prepared = task_run::prepare_run_projection(run).unwrap();
    let tx = store.begin().await.unwrap();
    task_run::upsert_prepared_run(&tx, prepared).await.unwrap();
    tx.commit().await.unwrap();
}

async fn fixture() -> (SqliteDatabase, Task) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = SqliteDatabase::new(
        fixture.connect_read_only().await.unwrap(),
        fixture.connection().clone(),
    );
    store.execute_unprepared("INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws_task','Discovery',TRUE,FALSE)").await.unwrap();
    let task = sample_task(AT);
    persist_task(&store, &task).await;
    (store.maintenance(), task)
}

async fn run(store: &SqliteDatabase, task: &Task, n: u32, status: TaskRunStatus) -> TaskRun {
    let mut run = sample_task_run(AT);
    run.id = format!("r{n:020}");
    run.run_group_id = run.id.clone();
    run.task_id = task.id.clone();
    run.trigger_id = None;
    run.run_number = i64::from(n) + 1;
    run.status = status;
    run.completed_at = status.is_terminal().then_some(AT);
    persist_run(store, &run).await;
    run
}

async fn child(
    store: &SqliteDatabase,
    run: &TaskRun,
    id: &str,
    status: TurnStatus,
    link_status: TaskRunTurnStatus,
    updated_at: i64,
) -> TaskRunTurn {
    let thread = sample_thread("ws_task", &format!("th_{id}"), AT);
    upsert_thread(store, &thread, PersistedActorRef::System)
        .await
        .unwrap();
    let turn = Turn {
        status,
        error: (status == TurnStatus::Failed).then(|| "child failure".to_owned()),
        ..sample_turn(id)
    };
    let tx = store.begin().await.unwrap();
    upsert_turn(
        &tx,
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
    tx.commit().await.unwrap();
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
    TaskChildWorkRepository::new(&store)
        .upsert_task_run_turn(link.clone())
        .await
        .unwrap();
    link
}

async fn discovered(store: &SqliteDatabase, limit: u64) -> Vec<String> {
    TaskChildWorkRepository::new(&store)
        .list_unreconciled_terminal_task_child_turns(limit)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.turn_id)
        .collect()
}

#[pioneer_postgres_test::postgres_test]
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
    let rows = TaskChildWorkRepository::new(&store)
        .list_unreconciled_terminal_task_child_turns(64)
        .await
        .unwrap();
    assert_eq!(rows[1].status, T::Failed);
    assert_eq!(rows[1].error.as_deref(), Some("child failure"));
    assert_eq!(rows[1].thread_id, "th_child_01");

    let mut closed_task = task;
    closed_task.status = TaskStatus::Completed;
    persist_task(&store, &closed_task).await;
    assert!(discovered(&store, 64).await.is_empty());
}

#[pioneer_postgres_test::postgres_test]
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
    persist_task(&store, &other_task).await;
    let foreign_run = run(&store, &other_task, 2, TaskRunStatus::Running).await;
    link.run_id = foreign_run.id;
    TaskChildWorkRepository::new(&store)
        .upsert_task_run_turn(link.clone())
        .await
        .unwrap();
    assert!(discovered(&store, 64).await.is_empty());

    // An old run of the same task must not borrow the active run's status.
    let historical = run(&store, &task, 3, TaskRunStatus::Succeeded).await;
    link.run_id = historical.id;
    TaskChildWorkRepository::new(&store)
        .upsert_task_run_turn(link.clone())
        .await
        .unwrap();
    assert!(discovered(&store, 64).await.is_empty());

    upsert_thread(
        &store,
        &sample_thread("ws_task", "thr_other", AT),
        PersistedActorRef::System,
    )
    .await
    .unwrap();
    link.run_id = active.id;
    link.thread_id = "thr_other".to_owned();
    TaskChildWorkRepository::new(&store)
        .upsert_task_run_turn(link.clone())
        .await
        .unwrap();
    assert!(discovered(&store, 64).await.is_empty());
    link.thread_id = "th_owned_child".to_owned();
    TaskChildWorkRepository::new(&store)
        .upsert_task_run_turn(link)
        .await
        .unwrap();
    assert_eq!(discovered(&store, 64).await, ["owned_child"]);

    pioneer_entity::turn::Entity::update_many()
        .col_expr(
            pioneer_entity::turn::Column::TurnKind,
            Expr::value("task_run"),
        )
        .filter(pioneer_entity::turn::Column::Id.eq("owned_child"))
        .exec(&store)
        .await
        .unwrap();
    assert!(discovered(&store, 64).await.is_empty());
}

async fn terminal_child(
    store: &SqliteDatabase,
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
    let created_at = unix_to_datetime(AT);
    let turn = Turn {
        status: TurnStatus::Completed,
        turn_kind: TurnKind::TaskRun,
        origin: TurnOrigin::ScheduledTask,
        ..sample_turn(&run.id)
    };
    let terminal = PreparedTurnEvent::prepare_for_storage(TurnEventPayload::TurnCompleted(
        TurnCompletedNotification {
            workspace_id: "ws_task".to_owned(),
            thread_id: parent_thread.id.clone(),
            turn: turn.clone(),
        },
    ))
    .unwrap();
    upsert_thread(store, &parent_thread, PersistedActorRef::System)
        .await
        .unwrap();
    let tx = store.begin().await.unwrap();
    upsert_turn(
        &tx,
        &turn.id,
        &parent_thread.id,
        &turn,
        None,
        None,
        created_at,
        created_at,
    )
    .await
    .unwrap();
    projection_stream::ensure_healthy(&tx, &parent_thread.id, &turn.id, created_at)
        .await
        .unwrap();
    let event = append_prepared_event(&tx, terminal, created_at)
        .await
        .unwrap();
    projection_stream::accept_terminal(&tx, &event)
        .await
        .unwrap();
    projection_stream::advance_projected_through(&tx, &turn.id, 0, event.sequence, created_at)
        .await
        .unwrap()
        .unwrap();
    thread_lineage::upsert_lineage(
        &tx,
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
    turn_context::insert(
        &tx,
        NewTurnLlmContextEntry {
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
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[pioneer_postgres_test::postgres_test]
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
    pioneer_crud_conversations::context_cleanup::delete_turn_llm_context_for_turn(
        &store,
        "b_terminal",
    )
    .await
    .unwrap();
    pioneer_entity::turn_llm_context::Entity::update_many()
        .col_expr(
            pioneer_entity::turn_llm_context::Column::Source,
            Expr::value("assistant_round"),
        )
        .filter(pioneer_entity::turn_llm_context::Column::TurnId.eq("c_terminal"))
        .exec(&store)
        .await
        .unwrap();
    assert_eq!(
        discovered(&store, 64).await,
        ["a_active", "d_active", "e_terminal", "f_terminal"]
    );
}
