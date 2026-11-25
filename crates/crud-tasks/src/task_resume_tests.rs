use super::*;
use crate::{
    child_repository::TaskChildWorkRepository,
    execution_store::TaskExecutionStore,
    occurrence_repository::TaskOccurrenceRepository,
    task_event_context::TaskEventContextRepository,
    test_support::{
        execution::{
            attach_test_agent_execution_contract, drain_custom_publisher_fanout,
            ensure_test_agent_identity, seed_task_owned_resume_conflict, seed_task_resume_thread,
        },
        task_projection::{NoFrozenOutput, sample_task, sample_task_run},
    },
};
use pioneer_crud_execution::{
    agent_execution::load_agent_execution, recovery_store::RecoveryStore,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::*;
use sea_orm::{ConnectionTrait, DbBackend, Set, Statement};

async fn resume_fixture(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace: &str,
) -> TaskEventStore<NoFrozenOutput> {
    let writer = fixture.connection();
    for statement in [
        Statement::from_string(
            DbBackend::Postgres,
            "UPDATE workspace SET is_current = FALSE WHERE is_current",
        ),
        Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO workspace(id,name,is_active,is_current) VALUES ($1,'Resume fixture',TRUE,TRUE)",
            [workspace.into()],
        ),
    ] {
        writer.execute_raw(statement).await.unwrap();
    }
    TaskEventStore::new(
        PostgresDatabase::new(fixture.connect_read_only().await.unwrap(), writer.clone()),
        NoFrozenOutput,
    )
}

#[tokio::test]
async fn task_owned_blocked_turn_resume_reopens_the_full_aggregate() {
    pioneer_postgres_test::with_database(async |fixture| {
        let store = resume_fixture(fixture, "ws_task_owned_resume").await;
        seed_task_resume_thread(
            &store.connection,
            "ws_task_owned_resume",
            "thread_task_owned_resume",
        )
        .await;
        let timestamp = 1_700_010_000;
        ensure_test_agent_identity(&store.connection, "ws_task_owned_resume", timestamp).await;
        let thread_id = "thread_task_owned_resume";
        let turn_id = "turn_task_owned_resume";

        let mut task = sample_task(timestamp);
        task.id = "task_task_owned_resume".to_owned();
        task.workspace_id = "ws_task_owned_resume".to_owned();
        task.status = TaskStatus::Blocked;
        let mut run = sample_task_run(timestamp);
        run.id = "run_task_owned_resume".to_owned();
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
            .expect("blocked task aggregate should project");
        let execution = TaskExecutionStore::new(&store.connection)
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, timestamp + 1)
            .await
            .expect("execution should reserve");
        attach_test_agent_execution_contract(
            &store.connection,
            task.workspace_id.as_str(),
            task.id.as_str(),
            run.id.as_str(),
            execution.id.as_str(),
            thread_id,
            timestamp + 1,
        )
        .await;
        TaskExecutionStore::new(&store.connection)
            .mark_execution_terminal(
                execution.id.as_str(),
                TaskRunExecutionStatus::Blocked,
                timestamp + 2,
                None,
                None,
            )
            .await
            .expect("execution should become blocked");
        TaskChildWorkRepository::new(&store.connection)
            .upsert_task_run_turn(TaskRunTurn {
                id: "task_run_turn_task_owned_resume".to_owned(),
                task_id: task.id.clone(),
                run_id: run.id.clone(),
                execution_id: Some(execution.id.clone()),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
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
            .expect("blocked task run turn should project");
        pioneer_entity::turn::Entity::insert(pioneer_entity::turn::ActiveModel {
            id: Set(turn_id.to_owned()),
            thread_id: Set(thread_id.to_owned()),
            status: Set("blocked".to_owned()),
            turn_kind: Set("conversation".to_owned()),
            origin: Set("system".to_owned()),
            prompt_manifest_json: Set("{}".to_owned()),
            created_at: Set(unix_to_datetime(timestamp + 2)),
            updated_at: Set(unix_to_datetime(timestamp + 3)),
            ..Default::default()
        })
        .exec(&store.connection)
        .await
        .expect("blocked child turn should persist");

        let wake = Arc::new(Notify::new());
        let recovery = RecoveryStore::new(&store.connection, &wake);
        let job = recovery
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "task_owned_resume_item".to_owned(),
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
            .expect("task recovery job should enqueue");
        recovery
            .mark_recovery_job_terminal(
                job.id.as_str(),
                RecoveryJobStatus::Blocked,
                Some("task child blocked".to_owned()),
                timestamp + 5,
            )
            .await
            .expect("task recovery job should become blocked");

        // A previous Stop can lose its terminal race to Blocked. Reopening
        // the same run ID must not carry that actor into a future cancellation.
        pioneer_entity::task_run_cancellation_actor::Entity::insert(
            pioneer_entity::task_run_cancellation_actor::ActiveModel {
                run_id: Set(run.id.clone()),
                task_id: Set(task.id.clone()),
                workspace_id: Set(task.workspace_id.clone()),
                principal_id: Set("P00000000000000000001".into()),
                origin_turn_id: Set(turn_id.into()),
            },
        )
        .exec(&store.connection)
        .await
        .unwrap();
        drain_custom_publisher_fanout(&store).await;
        let database = store.connection.clone();
        let mut fanout = database.work_signal("tasks.fanout").subscribe();
        fanout.begin_pass(&database).await.unwrap();
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_none());
        let registries = Registries::default();
        let resume_wake = Arc::new(Notify::new());
        let publication = database.begin().await.unwrap();
        crate::event_log::lock_task(&publication, &task.id)
            .await
            .unwrap();
        let pid: i32 = publication
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT pg_backend_pid()",
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get_by_index(0)
            .unwrap();
        let resume = store.resume_task_owned_turn(
            &registries,
            &resume_wake,
            thread_id,
            turn_id,
            Some(job.id.as_str()),
            timestamp + 6,
            "test-owner",
            timestamp + 51,
        );
        tokio::pin!(resume);
        let observer = fixture.connection();
        tokio::select! {
            outcome = &mut resume => panic!("resume completed before publication: {outcome:?}"),
            waiting = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let waiting: bool = observer.query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Postgres,
                        "SELECT EXISTS (SELECT FROM pg_stat_activity
                         WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))",
                        [pid.into()],
                    )).await.unwrap().unwrap().try_get_by_index(0).unwrap();
                    if waiting {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }) => waiting.expect("resume must wait for task publication"),
        }
        assert!(
            publication
                .query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT id FROM task WHERE id=$1 FOR UPDATE NOWAIT",
                    [task.id.clone().into()],
                ))
                .await
                .expect("publication can still acquire the task row")
                .is_some()
        );
        publication.commit().await.unwrap();
        let outcome = resume.await.expect("task-owned resume should commit");
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_some());
        let delivered = drain_custom_publisher_fanout(&store).await;
        assert_eq!(delivered.len(), 2);
        assert!(
            matches!(&delivered[0].payload, TaskEventPayload::TaskResumed { task: resumed, .. }
            if resumed.id == task.id)
        );
        assert!(
            matches!(&delivered[1].payload, TaskEventPayload::TaskRecovered { run_id: Some(id), .. }
            if id == &run.id)
        );
        fanout.begin_pass(&database).await.unwrap();
        assert!(matches!(
            store
                .resume_task_owned_turn(
                    &registries,
                    &resume_wake,
                    thread_id,
                    turn_id,
                    Some(job.id.as_str()),
                    timestamp + 7,
                    "test-owner",
                    timestamp + 52
                )
                .await
                .unwrap(),
            Some(TaskOwnedTurnResumeOutcome::NotFound)
        ));
        assert!(futures_util::FutureExt::now_or_never(fanout.wait(None)).is_none());
        let TaskOwnedTurnResumeOutcome::Resumed {
            task: resumed_task,
            run: resumed_run,
            task_run_turn: resumed_turn,
            execution: resumed_execution,
            ..
        } = outcome.expect("task lineage should select aggregate resume")
        else {
            panic!("expected aggregate resume outcome");
        };
        assert!(
            pioneer_entity::task_run_cancellation_actor::Entity::find_by_id(run.id.clone())
                .one(&store.connection)
                .await
                .unwrap()
                .is_none(),
            "a resumed execution cannot inherit a retired Stop actor"
        );
        assert_eq!(resumed_task.status, TaskStatus::Running);
        assert_eq!(resumed_run.status, TaskRunStatus::Running);
        assert_eq!(resumed_turn.status, TaskRunTurnStatus::InProgress);
        assert_eq!(
            resumed_execution
                .as_ref()
                .expect("execution should remain durable")
                .status,
            TaskRunExecutionStatus::Reserved
        );
        let resumed_execution_id = resumed_execution
            .expect("execution should remain durable")
            .id;
        let occurrence = TaskOccurrenceRepository::new(store.connection.clone())
            .get_task_occurrence_contract_by_run(run.id.as_str())
            .await
            .expect("resumed occurrence should load")
            .expect("resumed occurrence should exist");
        assert_eq!(occurrence.status, TaskOccurrenceStatus::Recovering);
        assert_eq!(occurrence.retry_attempt, 1);
        assert_eq!(
            load_agent_execution(&store.connection, resumed_execution_id.as_str())
                .await
                .expect("resumed AgentExecution should load")
                .expect("resumed AgentExecution should exist")
                .status,
            "recovering"
        );
        assert_eq!(
            pioneer_entity::agent_work_resource_scope::Entity::find_by_id(
                resumed_execution_id.clone()
            )
            .one(&store.connection)
            .await
            .expect("resumed Agent root scope should load")
            .expect("resumed Agent root scope should exist")
            .status,
            "active"
        );
        assert_eq!(
            pioneer_entity::turn::Entity::find_by_id(turn_id.to_owned())
                .one(&store.connection)
                .await
                .expect("child turn should load")
                .expect("child turn should exist")
                .status,
            "in_progress"
        );
        assert_eq!(
            recovery
                .get_recovery_job(job.id.as_str())
                .await
                .expect("recovery job should load")
                .expect("recovery job should exist")
                .status,
            RecoveryJobStatus::Succeeded
        );
        Ok(())
    })
    .await
    .unwrap();
}

async fn task_owned_resume_conflict_fixture(
    workspace_id: &str,
    suffix: &str,
) -> (
    TaskEventStore<NoFrozenOutput>,
    Task,
    TaskRun,
    RecoveryJobRecord,
    String,
    String,
) {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let store = resume_fixture(&fixture, workspace_id).await;
    let (task, run, job, thread_id, turn_id) =
        seed_task_owned_resume_conflict(&store.connection, workspace_id, suffix).await;
    (store, task, run, job, thread_id, turn_id)
}

#[pioneer_postgres_test::postgres_test]
async fn task_owned_resume_rejects_a_newer_terminal_successor() {
    let (store, task, run, job, thread_id, turn_id) =
        task_owned_resume_conflict_fixture("ws_task_owned_successor", "successor").await;
    let timestamp = 1_700_011_100;
    let mut successor = sample_task_run(timestamp);
    successor.id = "run_task_owned_successor_newer".to_owned();
    successor.task_id = task.id.clone();
    successor.trigger_id = None;
    successor.run_group_id = successor.id.clone();
    successor.run_number = run.run_number + 1;
    successor.status = TaskRunStatus::Succeeded;
    successor.completed_at = Some(timestamp);
    store
        .append_task_event(
            TaskEventPayload::RunCreated {
                run: successor,
                agent_spec: None,
            },
            timestamp,
        )
        .await
        .expect("newer terminal successor should persist");

    let registries = Registries::default();
    let resume_wake = Arc::new(Notify::new());
    let outcome = store
        .resume_task_owned_turn(
            &registries,
            &resume_wake,
            thread_id.as_str(),
            turn_id.as_str(),
            Some(job.id.as_str()),
            timestamp + 1,
            "test-owner",
            timestamp + 46,
        )
        .await
        .expect("successor conflict should evaluate")
        .expect("task lineage should be recognized");
    let TaskOwnedTurnResumeOutcome::Conflict { reason } = outcome else {
        panic!("newer terminal successor must fence blocked resume");
    };
    assert!(reason.contains("newer or active run"));
    assert_eq!(
        TaskEventContextRepository::new(&store.connection)
            .get_task_run(run.id.as_str())
            .await
            .expect("blocked run should load")
            .expect("blocked run should exist")
            .status,
        TaskRunStatus::Blocked
    );
}

#[pioneer_postgres_test::postgres_test]
async fn task_owned_resume_rejects_an_overlapping_cross_task_workspace_lock() {
    let (store, task, run, job, thread_id, turn_id) =
        task_owned_resume_conflict_fixture("ws_task_owned_lock", "lock").await;
    let timestamp = 1_700_011_200;
    let released_lock = TaskWriteLock {
        id: "lock_task_owned_released".to_owned(),
        workspace_id: task.workspace_id.clone(),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        scope_kind: TaskWriteLockScopeKind::Path,
        scope_path: "src".to_owned(),
        status: TaskWriteLockStatus::Released,
        acquired_at: timestamp - 20,
        expires_at: None,
        released_at: Some(timestamp - 10),
        conflict_policy: TaskConcurrencyConflictPolicy::Queue,
        reason: Some("blocked child released its lock".to_owned()),
        created_at: timestamp - 20,
        updated_at: timestamp - 10,
    };
    store
        .append_task_event(
            TaskEventPayload::WriteLockReleased {
                lock: released_lock,
                released_at: timestamp - 10,
            },
            timestamp - 10,
        )
        .await
        .expect("released owner lock should persist");

    let mut other_task = sample_task(timestamp);
    other_task.id = "task_task_owned_lock_other".to_owned();
    other_task.workspace_id = task.workspace_id.clone();
    other_task.status = TaskStatus::Running;
    let mut other_run = sample_task_run(timestamp);
    other_run.id = "run_task_owned_lock_other".to_owned();
    other_run.task_id = other_task.id.clone();
    other_run.trigger_id = None;
    other_run.run_group_id = other_run.id.clone();
    other_run.status = TaskRunStatus::Running;
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated {
                    task: other_task.clone(),
                },
                TaskEventPayload::RunCreated {
                    run: other_run.clone(),
                    agent_spec: None,
                },
                TaskEventPayload::WriteLockAcquired {
                    lock: TaskWriteLock {
                        id: "lock_task_owned_other_active".to_owned(),
                        workspace_id: task.workspace_id.clone(),
                        task_id: other_task.id,
                        run_id: other_run.id,
                        scope_kind: TaskWriteLockScopeKind::Path,
                        scope_path: "src/lib".to_owned(),
                        status: TaskWriteLockStatus::Acquired,
                        acquired_at: timestamp,
                        expires_at: Some(timestamp + 3600),
                        released_at: None,
                        conflict_policy: TaskConcurrencyConflictPolicy::Queue,
                        reason: None,
                        created_at: timestamp,
                        updated_at: timestamp,
                    },
                },
            ],
            timestamp,
        )
        .await
        .expect("cross-task active lock should persist");

    let registries = Registries::default();
    let resume_wake = Arc::new(Notify::new());
    let outcome = store
        .resume_task_owned_turn(
            &registries,
            &resume_wake,
            thread_id.as_str(),
            turn_id.as_str(),
            Some(job.id.as_str()),
            timestamp + 1,
            "test-owner",
            timestamp + 46,
        )
        .await
        .expect("lock conflict should evaluate")
        .expect("task lineage should be recognized");
    let TaskOwnedTurnResumeOutcome::Conflict { reason } = outcome else {
        panic!("overlapping cross-task lock must fence blocked resume");
    };
    assert!(reason.contains("write scope conflicts"));
    assert_eq!(
        TaskEventContextRepository::new(&store.connection)
            .get_task_run(run.id.as_str())
            .await
            .expect("blocked run should load")
            .expect("blocked run should exist")
            .status,
        TaskRunStatus::Blocked
    );
}

fn missing_mcp_activation_reason() -> String {
    format!(
        "CLI runtime recovery is blocked: frozen CLI runtime MCP projection is invalid: {}",
        CLI_MCP_ACTIVATION_CONTRACT_MISSING
    )
}

#[test]
fn revision_execution_budget_is_separate_from_the_review_round() {
    let mut revision = TaskRunTurn {
        id: "trt_revise_candidate_8".into(),
        task_id: "task".into(),
        run_id: "run".into(),
        execution_id: Some("exec".into()),
        thread_id: "thread".into(),
        turn_id: "revise_candidate_8".into(),
        kind: TaskRunTurnKind::Revision,
        round: 8,
        sequence: 9,
        status: TaskRunTurnStatus::Blocked,
        reviews_candidate_id: None,
        requested_by_candidate_id: Some("candidate".into()),
        requested_by_review_event_id: Some("review-event".into()),
        created_at: 1,
        started_at: Some(1),
        completed_at: Some(2),
    };
    let mut run = sample_task_run(1);
    run.error = Some(TaskError {
        recovery_diagnostic: None,
        code: TASK_REVISION_RETRY_PENDING.into(),
        message: "MCP activation contract missing".into(),
        class: TaskErrorClass::Provider,
        details: None,
        failed_run_id: None,
    });
    let initial_id = revision.turn_id.clone();
    assert_eq!(
        revision.revision_execution_retry(&format!("revision_retry_1_{initial_id}")),
        Some(1)
    );
    let mut legacy_manual = revision.clone();
    legacy_manual.turn_id = format!("revision_manual_retry_4_{initial_id}");
    assert!(legacy_manual.is_manual_revision_retry());
    assert_eq!(
        legacy_manual.revision_execution_retry(&legacy_manual.turn_id),
        Some(4)
    );
    let largest_automatic = revision.revision_retry_turn_id(u32::MAX, true).unwrap();
    let largest_manual = revision.revision_retry_turn_id(u32::MAX, false).unwrap();
    assert_eq!(largest_automatic.len(), 55);
    assert_eq!(largest_manual.len(), 55);
    assert_ne!(largest_automatic, largest_manual);
    assert_eq!(
        revision.revision_execution_retry(&largest_automatic),
        Some(u32::MAX)
    );
    let mut other_revision = revision.clone();
    other_revision.id = "trt_revise_other_candidate_8".into();
    assert_eq!(
        other_revision.revision_execution_retry(&largest_automatic),
        None
    );

    assert!(next_revision_execution(&revision, &run, false, 3).is_none());

    for retry in 1..=3 {
        let next = next_revision_execution(&revision, &run, true, i64::from(retry) + 2)
            .expect("automatic recovery within the budget");
        assert_eq!(next.revision_execution_retry(&next.turn_id), Some(retry));
        assert_eq!(next.id, revision.id);
        assert_eq!(next.round, revision.round);
        assert_eq!(next.sequence, revision.sequence);
        assert_eq!(
            next.requested_by_review_event_id,
            revision.requested_by_review_event_id
        );
        assert_ne!(next.turn_id, revision.turn_id);
        assert_eq!(
            next_revision_execution(&revision, &run, true, 100)
                .unwrap()
                .turn_id,
            next.turn_id,
            "the identity must survive a replay at a different timestamp"
        );
        revision = next;
        revision.status = TaskRunTurnStatus::Blocked;
    }
    assert!(next_revision_execution(&revision, &run, true, 6).is_none());
    run.error.as_mut().unwrap().code = TASK_REVISION_RETRY_REQUIRED.into();
    let manual = next_revision_execution(&revision, &run, false, 7).unwrap();
    assert_eq!(manual.revision_execution_retry(&manual.turn_id), Some(4));
    assert!(manual.is_manual_revision_retry());
    assert_eq!(
        manual.revision_execution_retry("revise_another_candidate_8"),
        None
    );
    assert!(is_missing_cli_mcp_activation_block(
        &missing_mcp_activation_reason()
    ));
    assert!(!is_missing_cli_mcp_activation_block(
        "CLI runtime recovery is blocked: frozen CLI runtime MCP projection is invalid: tool drift"
    ));
}

#[pioneer_postgres_test::postgres_test]
async fn a_settled_legacy_revision_gets_one_fresh_execution_under_concurrent_retry() {
    use pioneer_entity::{task_run_turn as link, turn as child, turn_cli_runtime_binding as cli};
    use sea_orm::{ColumnTrait, QueryFilter, sea_query::Expr};

    let suffix = "legacy_revision".repeat(7) + "12345";
    let (store, task, run, job, thread_id, turn_id) =
        task_owned_resume_conflict_fixture("ws_task_revision_retry", &suffix).await;
    assert_eq!(turn_id.len(), 126);
    let now = job.updated_at_unix + 10;
    let at = unix_to_datetime(now);
    let logical_id = format!("trt_{turn_id}");
    link::Entity::update_many()
        .filter(link::Column::RunId.eq(&run.id))
        .col_expr(link::Column::Id, Expr::value(logical_id.clone()))
        .col_expr(link::Column::Kind, Expr::value("revision"))
        .col_expr(link::Column::Round, Expr::value(3_i64))
        .exec(&store.connection)
        .await
        .unwrap();
    child::Entity::update_many()
        .filter(child::Column::Id.eq(&turn_id))
        .col_expr(
            child::Column::Error,
            Expr::value(missing_mcp_activation_reason()),
        )
        .exec(&store.connection)
        .await
        .unwrap();

    turn_execution::insert_immutable(
        &store.connection,
        turn_execution::NewTurnExecution {
            turn_id: turn_id.clone(),
            thread_id: thread_id.clone(),
            workspace_id: task.workspace_id.clone(),
            executor_kind: turn_execution::TurnExecutorKind::CliRuntime,
            executor_key: Some("codex".into()),
            status: turn_execution::TurnExecutionStatus::Starting,
            owner_id: "previous-gateway".into(),
            lease_until: unix_to_datetime(now + 20),
            created_at: at,
        },
    )
    .await
    .unwrap();
    assert!(
        turn_execution::mark_terminal(
            &store.connection,
            &turn_id,
            turn_execution::TurnExecutionStatus::Blocked,
            at,
        )
        .await
        .unwrap()
    );
    cli::Entity::insert(cli::ActiveModel {
        turn_id: Set(turn_id.clone()),
        thread_id: Set(thread_id.clone()),
        continuation_thread_id: Set(thread_id.clone()),
        workspace_id: Set(task.workspace_id.clone()),
        runtime_id: Set("codex".into()),
        runtime_kind: Set("codex".into()),
        native_thread_id: Set("previous-native-thread".into()),
        frozen_manifest_id: Set(None),
        status: Set("blocked".into()),
        created_at: Set(at),
        updated_at: Set(at),
        ..Default::default()
    })
    .exec(&store.connection)
    .await
    .unwrap();

    use pioneer_crud_execution::execution_admission_lease::{
        self as quota, ExecutionAdmissionClass, ExecutionAdmissionQuotaPolicy,
        ExecutionQuotaCeilings, NewExecutionAdmissionLease,
    };
    let ceiling = ExecutionQuotaCeilings {
        per_principal: 1,
        per_role: 1,
        per_workspace: 1,
        gateway: 1,
    };
    let mut admission = NewTaskExecutionAdmission {
        task_id: task.id.clone(),
        workspace_id: task.workspace_id.clone(),
        root_thread_id: thread_id.clone(),
        initiating_principal_id: "test-principal".to_owned(),
        authorization_context_json: "{}".to_owned(),
        created_at: at,
        execution_lease: Some(NewExecutionAdmissionLease {
            id: format!("quota:task:{}", task.id),
            subject_kind: "task".to_owned(),
            subject_id: task.id.clone(),
            operation_class: ExecutionAdmissionClass::ScheduledTask,
            principal_id: "test-principal".to_owned(),
            role_key: "test".to_owned(),
            workspace_id: task.workspace_id.clone(),
            policy_fingerprint: "test".to_owned(),
            policy: ExecutionAdmissionQuotaPolicy {
                active: ceiling,
                queued: ceiling,
                scheduled: ceiling,
            },
        }),
    };
    let tx = store.connection.begin().await.unwrap();
    task_execution_admission::insert_immutable(&tx, admission.clone())
        .await
        .unwrap();
    quota::release_by_subject(&tx, "task", &task.id, at)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // A changed schedule must not move the original quota reservation.
    admission.execution_lease.as_mut().unwrap().operation_class =
        ExecutionAdmissionClass::QueuedTask;
    let quota_status = || async {
        let lease = quota::find_by_subject(&store.connection, "task", &task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.quota_bucket, "scheduled");
        lease.status
    };

    let registries = Registries::default();
    let wake = Arc::new(Notify::new());
    let failed = store
        .retry_task_revision_execution(
            &registries,
            &wake,
            &thread_id,
            &turn_id,
            false,
            &admission,
            now,
            |_| Box::pin(async { anyhow::bail!("injected receipt commit failure") }),
        )
        .await;
    assert!(
        failed
            .unwrap_err()
            .to_string()
            .contains("injected receipt commit failure")
    );
    assert_eq!(
        task_run_turn::find_turn_by_id(&store.connection, &logical_id)
            .await
            .unwrap()
            .unwrap()
            .turn_id,
        turn_id,
    );

    assert_eq!(quota_status().await, quota::EXECUTION_LEASE_STATUS_RELEASED);
    let retry = || {
        store.retry_task_revision_execution(
            &registries,
            &wake,
            &thread_id,
            &turn_id,
            false,
            &admission,
            now,
            |_| Box::pin(async { Ok(()) }),
        )
    };
    let (one, two) = tokio::join!(retry(), retry());
    let admitted = [one.unwrap(), two.unwrap()]
        .into_iter()
        .filter(|outcome| matches!(outcome, Some(TaskOwnedTurnResumeOutcome::Resumed { .. })))
        .count();
    assert_eq!(admitted, 1);
    assert_eq!(quota_status().await, quota::EXECUTION_LEASE_STATUS_ACTIVE);
    assert!(!matches!(
        retry().await.unwrap(),
        Some(TaskOwnedTurnResumeOutcome::Resumed { .. })
    ));

    let latest = task_run_turn_from_db_model(
        task_run_turn::find_turn_by_id(&store.connection, &logical_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(latest.revision_execution_retry(&latest.turn_id), Some(1));
    assert_eq!(latest.turn_id.len(), 55);
    assert_eq!(
        latest.revision_retry_turn_id(1, false),
        Some(latest.turn_id.clone())
    );
    assert!(latest.is_manual_revision_retry());
    assert_eq!(latest.status, TaskRunTurnStatus::InProgress);
    assert_eq!(latest.round, 3);
    assert_eq!(latest.run_id, run.id);
    assert_eq!(
        turn_execution::find(&store.connection, &turn_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        turn_execution::TurnExecutionStatus::Blocked,
    );
    assert!(
        child::Entity::find_by_id(latest.turn_id)
            .one(&store.connection)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        task_actor_contract::find_task_occurrence_by_run_id(&store.connection, &run.id)
            .await
            .unwrap()
            .unwrap()
            .retry_attempt,
        1,
    );
}
