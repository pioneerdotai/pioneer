use anyhow::{Context, Result};
use pioneer_crud_tasks::{child_work, task, task_run, task_run_turn, values::unix_to_datetime};
use pioneer_entity::{task_child_reconcile_pending as point, task_child_reconcile_scope as scope};
use pioneer_postgres_test::with_database;
use pioneer_protocol::{
    Task, TaskExecutorKind, TaskOwnerKind, TaskRun, TaskRunStatus, TaskRunTurnKind,
    TaskRunTurnStatus, TaskStatus,
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DbBackend, EntityTrait, QueryFilter, Select, Statement,
    TransactionTrait,
};
use std::time::Duration;

fn child_scope(kind: &str, key: &str) -> Select<scope::Entity> {
    scope::Entity::find()
        .filter(scope::Column::ScopeKind.eq(kind))
        .filter(scope::Column::ScopeKey.eq(key))
}

const AT: i64 = 1_700_000_000;

fn initial_task() -> Task {
    Task {
        id: "child-obligation".into(),
        workspace_id: "ws_task".into(),
        owner_kind: TaskOwnerKind::Thread,
        owner_id: Some("thr_task".into()),
        created_by_thread_id: Some("thr_task".into()),
        created_by_turn_id: Some("turn_task".into()),
        root_task_id: None,
        parent_task_id: None,
        executor_kind: TaskExecutorKind::Agent,
        status: TaskStatus::Scheduled,
        title: "Check weather".into(),
        goal: "Send the daily weather summary".into(),
        priority: 10,
        lifecycle_policy: None,
        delivery_policy: None,
        retry_policy: None,
        timeout_policy: None,
        concurrency_policy: None,
        metadata: None,
        result: None,
        error: None,
        revision: 0,
        created_at: AT,
        updated_at: AT,
        completed_at: None,
    }
}

fn queued_run(task_id: &str) -> TaskRun {
    TaskRun {
        id: "run_00000000000000001".into(),
        task_id: task_id.into(),
        trigger_id: Some("trg_00000000000000001".into()),
        trigger_input: None,
        parent_run_id: None,
        run_group_id: "run_00000000000000001".into(),
        attempt_number: 1,
        retry_of_run_id: None,
        ready_at: Some(AT),
        run_number: 1,
        status: TaskRunStatus::Queued,
        executor_kind: TaskExecutorKind::Agent,
        started_at: None,
        completed_at: None,
        heartbeat_at: None,
        locked_by: None,
        lock_expires_at: None,
        result: None,
        error: None,
        created_at: AT,
        updated_at: AT,
    }
}

#[tokio::test]
async fn child_task_obligation_rolls_back_with_business_projection() -> Result<()> {
    with_database(async |database| {
        let writer = database.connection();
        let observer = database.connect().await?;
        let task = initial_task();
        assert!(scope::Entity::find().one(writer).await?.is_none());

        let write = writer.begin().await?;
        task::upsert_prepared_task(&write, task::prepare_task_projection(&task)?).await?;
        assert!(child_scope("task", &task.id).one(&write).await?.is_some());
        assert!(
            child_scope("task", &task.id)
                .one(&observer)
                .await?
                .is_none()
        );

        write.rollback().await?;
        assert!(task::find_task_by_id(&observer, &task.id).await?.is_none());
        assert!(
            child_scope("task", &task.id)
                .one(&observer)
                .await?
                .is_none()
        );
        observer.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn claimed_scope_keeps_its_cursor_and_lease_during_publication() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        child_work::enqueue_scope(db, "run", "run", 100).await?;
        let claimant = db.begin().await?;
        claimant
            .execute_unprepared(
                "UPDATE task_child_reconcile_scope
             SET claim_token = 'claim', next_attempt_at = 160, pass_revision = requested_revision,
                 cursor = 'cursor', upper_watermark = 'tail'
             WHERE scope_kind = 'run' AND scope_key = 'run'",
            )
            .await?;
        let key = ("run".to_owned(), "run".to_owned());
        let before = scope::Entity::find_by_id(key.clone())
            .one(&claimant)
            .await?
            .context("claimed scope must exist")?;
        let second = database.connect().await?;
        let producer = second.begin().await?;
        let producer_pid: i32 = producer
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_backend_pid() AS pid",
            ))
            .await?
            .context("producer PID")?
            .try_get("", "pid")?;
        let publish = tokio::spawn(async move {
            child_work::enqueue_scope(&producer, "run", "run", 101).await?;
            producer.commit().await?;
            Ok::<_, anyhow::Error>(())
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked",
                        [producer_pid.into()],
                    ))
                    .await?
                    .context("producer lock state")?
                    .try_get("", "blocked")?;
                if blocked {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await??;
        claimant.commit().await?;
        publish.await??;
        let current = scope::Entity::find_by_id(key)
            .one(db)
            .await?
            .context("published scope must exist")?;
        assert_ne!(current.requested_revision, before.requested_revision);
        assert_eq!(current.incarnation_token, before.incarnation_token);
        assert_eq!(current.claim_token, before.claim_token);
        assert_eq!(current.pass_revision, before.pass_revision);
        assert_eq!(current.cursor, before.cursor);
        assert_eq!(current.upper_watermark, before.upper_watermark);
        assert_eq!(current.next_attempt_at, before.next_attempt_at);
        second.close().await?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn point_replacement_publishes_new_locator_and_clears_old_claim() -> Result<()> {
    with_database(async |database| {
        let db = database.connection();
        child_work::enqueue_point(db, "z-old-thread", "turn", 100).await?;
        db.execute_unprepared(
            "UPDATE task_child_reconcile_pending
             SET claim_token = 'old-claim', attempt_count = 2 WHERE turn_id = 'turn'",
        )
        .await?;
        let previous = point::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("old point must exist")?;

        let producer = database.connect().await?;
        let transaction = producer.begin().await?;
        child_work::enqueue_point(&transaction, "a-new-thread", "turn", 101).await?;
        assert_eq!(
            point::Entity::find_by_id("turn").one(db).await?,
            Some(previous.clone())
        );
        transaction.commit().await?;

        let replacement = point::Entity::find_by_id("turn")
            .one(db)
            .await?
            .context("new point must exist")?;
        assert_eq!(replacement.thread_id, "a-new-thread");
        assert_ne!(replacement.revision, previous.revision);
        assert_eq!(replacement.next_attempt_at, Some(101));
        assert_eq!(replacement.attempt_count, 0);
        assert!(replacement.claim_token.is_none());
        producer.close().await?;
        Ok(())
    })
    .await
}

#[pioneer_postgres_test::postgres_test]
async fn task_payload_refresh_does_not_invalidate_child_scope() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    let mut task = initial_task();
    let tx = writer.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let key = ("task".to_owned(), task.id.clone());
    let before = scope::Entity::find_by_id(key.clone())
        .one(writer)
        .await
        .unwrap()
        .unwrap();
    task.title = "changed presentation only".to_owned();
    task.updated_at += 1;
    let tx = writer.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        before,
        scope::Entity::find_by_id(key)
            .one(writer)
            .await
            .unwrap()
            .unwrap()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn run_status_projection_durably_invalidates_origin_group() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    let task = initial_task();
    let run = queued_run(&task.id);
    let tx = writer.begin().await.unwrap();
    task::upsert_prepared_task(&tx, task::prepare_task_projection(&task).unwrap())
        .await
        .unwrap();
    task_run::upsert_run(&tx, &run).await.unwrap();
    tx.commit().await.unwrap();
    let key = ("run".to_owned(), run.id.clone());
    let before = scope::Entity::find_by_id(key.clone())
        .one(writer)
        .await
        .unwrap()
        .unwrap();
    let tx = writer.begin().await.unwrap();
    task_run::update_run_status(
        &tx,
        &run.id,
        TaskRunStatus::Running,
        unix_to_datetime(AT + 1),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_ne!(
        before.requested_revision,
        scope::Entity::find_by_id(key)
            .one(writer)
            .await
            .unwrap()
            .unwrap()
            .requested_revision
    );
    assert!(
        scope::Entity::find()
            .filter(scope::Column::ScopeKind.eq("task+run-group"))
            .one(writer)
            .await
            .unwrap()
            .is_some()
    );
}

#[pioneer_postgres_test::postgres_test]
async fn moved_link_keeps_new_point_locator_when_it_sorts_before_old() {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    let link = |thread_id: &str| task_run_turn::NewTaskRunTurn {
        id: "child-move-link".into(),
        task_id: "child-move-task".into(),
        run_id: "child-move-run".into(),
        execution_id: None,
        thread_id: thread_id.into(),
        turn_id: "child-move-turn".into(),
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: AT,
        started_at: None,
        completed_at: None,
    };
    // The occurrence may be linked before its Turn row is available. The
    // producer must publish the new point without authorizing destruction.
    let write = writer.begin().await.unwrap();
    task_run_turn::upsert_turn(&write, link("z-old-thread"))
        .await
        .unwrap();
    write.commit().await.unwrap();
    let write = writer.begin().await.unwrap();
    task_run_turn::upsert_turn(&write, link("a-new-thread"))
        .await
        .unwrap();
    write.commit().await.unwrap();

    let pending = point::Entity::find_by_id("child-move-turn")
        .one(writer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.thread_id, "a-new-thread");
}
