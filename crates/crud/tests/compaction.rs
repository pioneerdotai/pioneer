use pioneer_compaction::*;
use pioneer_crud::{CrudStore, compaction::*};
use sea_orm::{ConnectionTrait, DbBackend, Statement, TransactionTrait};

async fn store() -> CrudStore {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection().clone();
    let reader = fixture.connect_read_only().await.unwrap();
    let store = CrudStore::new(pioneer_postgres_pools::PostgresDatabase::new(
        reader, writer,
    ))
    .with_maintenance_access();
    let db = store.database_connection();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',TRUE,FALSE)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    store
}

#[pioneer_postgres_test::postgres_test]
async fn durable_runner_manifest_candidate_and_state_commit_together() {
    let store = store().await;
    let compaction = pioneer_crud_compaction::store::CompactionStore::new(
        store.database_connection(),
        Default::default(),
    );
    let (checkpoint, ready) =
        pioneer_crud_compaction::plan_test_support::prepare_checked_candidate(&compaction).await;
    verify_parent_materialization_after_runner_candidate(&store, checkpoint, ready).await;
}

async fn verify_parent_materialization_after_runner_candidate(
    store: &CrudStore,
    checkpoint: Checkpoint,
    ready: pioneer_compaction::runner::RunnerState,
) {
    // The real message materializer first creates and then completes the next
    // user message. That update advances the parent's broad epoch, but the new
    // item was never part of this operation's immutable manifest or coverage.
    let parent = store.get_thread_model("parent").await.unwrap().unwrap();
    let (_, mut next_turn) = store
        .get_turn("parent", "parent-turn")
        .await
        .unwrap()
        .unwrap();
    next_turn.id = "next-parent-turn".into();
    next_turn.reply_to_turn_id = None;
    store
        .materialize_turn_start(
            &parent,
            pioneer_protocol::SandboxMode::FullAccess,
            &next_turn,
            &[],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    let message = pioneer_protocol::TurnItem::UserMessage {
        id: "next-parent-message".into(),
        text: "new unselected work".into(),
        attachments: vec![],
    };
    store
        .materialize_item_started(
            pioneer_protocol::ItemStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                turn_id: next_turn.id.clone(),
                item: message.clone(),
            },
            3,
        )
        .await
        .unwrap();
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                turn_id: next_turn.id,
                item: message,
            },
            4,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "parent")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .compaction_apply_runner("runner", &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    assert_eq!(
        store
            .compaction_apply_runner("runner", &ready, None)
            .await
            .unwrap(),
        CommitOutcome::AlreadyApplied
    );
    let derived = store
        .compaction_checkpoint_source("ws", "thread", "runner-candidate")
        .await
        .unwrap()
        .unwrap();
    let coverage = store
        .compaction_checkpoint("runner-candidate")
        .await
        .unwrap()
        .unwrap()
        .coverage;
    assert_eq!(coverage.len(), 1);
    assert!(
        coverage
            .iter()
            .all(|source| !source.scope.ends_with(":next-parent-turn"))
    );
    assert_eq!(
        store
            .compaction_reference_fragment("ws", "thread", &derived, 0)
            .await
            .unwrap()
            .unwrap()
            .text,
        "saved complete candidate"
    );
    store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload=to_json('edited'::text) WHERE id='runner-source'",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .compaction_checkpoint_source("ws", "thread", "runner-candidate")
            .await
            .unwrap()
            .is_some(),
        "published checkpoint must survive an edit to its historical leaf"
    );
    // FK-on cascade must not bypass permanent checkpoint identity/ownership
    // guards. The rejected delete rolls back the entire thread cascade.
    assert!(
        store
            .database_connection()
            .execute_unprepared("DELETE FROM thread WHERE id='thread'")
            .await
            .is_err()
    );
    assert!(store.get_thread_model("thread").await.unwrap().is_some());
    assert_eq!(
        store.compaction_head(&checkpoint.owner).await.unwrap(),
        Some(checkpoint.id.clone())
    );
}

#[pioneer_postgres_test::postgres_test]
async fn task_occurrence_failures_close_command_only_after_canonical_acknowledgement() {
    for status in ["failed", "blocked", "cancelled"] {
        let store = store().await;
        let db = store.database_connection();
        for sql in [
            "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
            "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
            "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('run','thread','in_progress','task_run','scheduled_task',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        ] {
            db.execute_unprepared(sql).await.unwrap();
        }
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_run SET status=$1 WHERE id='run'",
            [status.into()],
        ))
        .await
        .unwrap();
        let before = store.compaction_history_read_fence().await.unwrap();
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &before)
                .await
                .unwrap()
                .delivered_outcome,
            "mutable {status} run without a canonical event is not closure"
        );
        assert_eq!(
            store
                .compare_and_materialize_task_run_occurrence_terminal(
                    "run",
                    chrono::Utc::now().timestamp()
                )
                .await
                .unwrap(),
            pioneer_crud::TaskRunOccurrenceTerminalizationOutcome::Changed
        );
        let after = store.compaction_history_read_fence().await.unwrap();
        assert!(
            store
                .compaction_history_causal_boundary("ws", "thread", "turn", &after)
                .await
                .unwrap()
                .delivered_outcome,
            "{status}"
        );
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &before)
                .await
                .unwrap()
                .delivered_outcome
        );
        let entries = store
            .compaction_source_page("ws", "thread", "run", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries;
        assert_eq!(entries.len(), 1, "terminal occurrence has no result item");
        let source = &entries[0].reference;
        assert_eq!(
            store
                .compaction_task_delivery_command("ws", "thread", source)
                .await
                .unwrap()
                .as_deref(),
            Some("turn")
        );
        assert!(
            store
                .compaction_task_delivery_command("other", "thread", source)
                .await
                .unwrap()
                .is_none()
        );
        let mut stale = source.clone();
        stale.version = "event-revision:999".into();
        assert!(
            store
                .compaction_task_delivery_command("ws", "thread", &stale)
                .await
                .unwrap()
                .is_none()
        );
        db.execute_unprepared("UPDATE turn SET turn_kind='conversation' WHERE id='run'")
            .await
            .unwrap();
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &after)
                .await
                .unwrap()
                .delivered_outcome
        );
        assert!(
            store
                .compaction_task_delivery_command("ws", "thread", source)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[pioneer_postgres_test::postgres_test]
async fn generic_failed_task_delivery_requires_exact_turn_identity_and_event_fence() {
    let store = store().await;
    let db = store.database_connection();
    for sql in [
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'failed','agent')",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts) VALUES ('delivery','ws','task','run','key','thread','origin_thread','thread','delivered',1,1)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let thread = store.get_thread_model("thread").await.unwrap().unwrap();
    let template = store.get_turn("thread", "turn").await.unwrap().unwrap().1;
    let before = store.compaction_history_read_fence().await.unwrap();
    for (id, expected) in [
        ("unrelated-failed-turn".to_owned(), false),
        (
            pioneer_crud::canonical_agent_id('T', "task-delivery-turn\0delivery"),
            true,
        ),
    ] {
        let mut started = template.clone();
        started.id = id.clone();
        started.status = pioneer_protocol::TurnStatus::InProgress;
        started.mode = pioneer_protocol::ThreadMode::Chat;
        started.origin = pioneer_protocol::TurnOrigin::TaskDelivery;
        started.author = None;
        let mut failed = started.clone();
        failed.status = pioneer_protocol::TurnStatus::Failed;
        failed.error = Some("fixture failure".into());
        store
            .materialize_failed_task_delivery_turn(pioneer_crud::FailedTaskDeliveryTurnWrite {
                thread: &thread,
                sandbox_mode: pioneer_protocol::SandboxMode::FullAccess,
                started_turn: &started,
                actor: pioneer_protocol::PersistedActorRef::System,
                audit_event: pioneer_protocol::TurnPermissionAuditEvent {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: id.clone(),
                    event_kind: pioneer_protocol::TurnPermissionAuditEventKind::ProfileSelected,
                    profile_mode: pioneer_protocol::TurnPermissionMode::FullAccess,
                    profile_source: pioneer_protocol::TurnPermissionProfileSource::System,
                    security_snapshot_id: None,
                    security_snapshot_version: None,
                    security_reason_code: None,
                    security_capability: None,
                    item_id: None,
                    tool_call_id: None,
                    tool_name: None,
                    action_kind: None,
                    request_key: None,
                    decision: None,
                    reason: None,
                    cached: false,
                },
                failed: pioneer_protocol::TurnFailedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn: failed,
                },
                agent_action: None,
            })
            .await
            .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE task_delivery SET delivered_turn_id=$1 WHERE id='delivery'",
            [id.clone().into()],
        ))
        .await
        .unwrap();
        let after = store.compaction_history_read_fence().await.unwrap();
        assert_eq!(
            store
                .compaction_history_causal_boundary("ws", "thread", "turn", &after)
                .await
                .unwrap()
                .delivered_outcome,
            expected
        );
        assert!(
            !store
                .compaction_history_causal_boundary("ws", "thread", "turn", &before)
                .await
                .unwrap()
                .delivered_outcome
        );
        let entries = store
            .compaction_source_page("ws", "thread", &id, PagedSource::Event, 0)
            .await
            .unwrap()
            .entries;
        for entry in entries {
            let is_failure = entry
                .payload
                .as_deref()
                .is_some_and(|p| p.contains("fixture failure"));
            assert_eq!(
                store
                    .compaction_task_delivery_command("ws", "thread", &entry.reference)
                    .await
                    .unwrap()
                    .as_deref(),
                (expected && is_failure).then_some("turn")
            );
            assert!(
                store
                    .compaction_task_delivery_command("other", "thread", &entry.reference)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

async fn admit_import_operation(
    store: &CrudStore,
    id: &str,
    thread: &str,
    turn: &str,
) -> OperationSnapshot {
    let snapshot = OperationSnapshot {
        id: id.into(),
        owner: format!("owner-{id}"),
        expected_checkpoint: None,
        projection_version: store
            .compaction_projection_version("ws", thread)
            .await
            .unwrap(),
        source_epochs: std::collections::BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(
                &ModelSelection {
                    service_mode: None,
                    context_authority: None,
                    context_window: None,
                    transport: Transport::Api,
                    instance: "p".into(),
                    model: "m".into(),
                    effort: None,
                },
                None,
                0,
            )
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
            compact: vec![],
            retain: vec![],
            coverage: vec![],
            fingerprint: id.into(),
        },
    };
    store
        .compaction_admit("ws", thread, &snapshot)
        .await
        .unwrap();
    store
        .compaction_bind_execution_turn(id, turn)
        .await
        .unwrap();
    snapshot
}

async fn ready_import_operation(
    store: &CrudStore,
    snapshot: &OperationSnapshot,
    source_thread: &str,
    source: &SourceRef,
) -> pioneer_compaction::runner::RunnerState {
    ready_operation(
        store,
        snapshot,
        &[(source_thread.to_owned(), source.clone())],
    )
    .await
}

async fn ready_operation(
    store: &CrudStore,
    snapshot: &OperationSnapshot,
    sources: &[(String, SourceRef)],
) -> pioneer_compaction::runner::RunnerState {
    ready_operation_with_references(store, snapshot, sources, &[]).await
}

async fn ready_operation_with_references(
    store: &CrudStore,
    snapshot: &OperationSnapshot,
    sources: &[(String, SourceRef)],
    references: &[(String, SourceRef)],
) -> pioneer_compaction::runner::RunnerState {
    use pioneer_compaction::runner::{RunnerState, SourceCursor};
    let op = &snapshot.id;
    // Seed the exact original legacy raw plan before runner execution. An
    // empty frozen-style plan is not a valid missing-binding contract.
    if !sources.is_empty()
        && snapshot.plan.compact.is_empty()
        && store
            .compaction_bound_source_projection(op)
            .await
            .unwrap()
            .is_none()
    {
        let mut raw = snapshot.clone();
        raw.plan.compact = (0..sources.len()).collect();
        raw.plan.coverage = sources.iter().map(|(_, source)| source.clone()).collect();
        store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "UPDATE compaction_operation SET snapshot=$2 WHERE id=$1",
                [
                    op.clone().into(),
                    serde_json::to_string(&raw).unwrap().into(),
                ],
            ))
            .await
            .unwrap();
    }
    let budget = ModelBudget::new(None, None, None);
    store
        .compaction_prepare_runner(op, &budget, sources.len() as u64, references.len() as u64)
        .await
        .unwrap();
    let manifest = sources
        .iter()
        .map(|source| (false, source))
        .chain(references.iter().map(|source| (true, source)))
        .enumerate()
        .map(
            |(ordinal, (reference_only, (thread_id, source)))| ManifestEntry {
                ordinal: ordinal as u64,
                unit: ordinal as u64,
                reference_only,
                thread_id: thread_id.clone(),
                source: source.clone(),
            },
        )
        .collect::<Vec<_>>();
    store
        .compaction_append_manifest(op, &manifest)
        .await
        .unwrap();
    let initial = RunnerState::new(snapshot.admission.deadline_ms, &budget, 1000, None).unwrap();
    store
        .compaction_activate_runner(op, &initial)
        .await
        .unwrap();
    let attempt = initial.claim(1).unwrap();
    assert!(
        store
            .compaction_runner_transition(op, initial.generation, &attempt, None)
            .await
            .unwrap()
    );
    let checkpoint = Checkpoint {
        id: format!("checkpoint-{op}"),
        operation_id: op.clone(),
        format_version: 1,
        owner: snapshot.owner.clone(),
        previous: None,
        coverage: sources.iter().map(|(_, source)| source.clone()).collect(),
        summary: "fixture summary".into(),
        selection: snapshot.admission.selection.clone(),
        projection_version: snapshot.projection_version,
    };
    let next = attempt
        .candidate(
            1,
            checkpoint.id.clone(),
            SourceCursor {
                unit: sources.len() as u64,
                ..Default::default()
            },
            true,
            2,
        )
        .unwrap();
    assert!(
        store
            .compaction_runner_transition(op, attempt.generation, &next, Some(&checkpoint))
            .await
            .unwrap()
    );
    let ready = next.candidate_checked(true).unwrap();
    assert!(
        store
            .compaction_runner_transition(op, next.generation, &ready, None)
            .await
            .unwrap()
    );
    ready
}

#[pioneer_postgres_test::postgres_test]
async fn compaction_lifecycle_after_terminal_turn_requires_exact_operation_and_generation() {
    use pioneer_crud::CanonicalTurnEventPayload as Event;
    let store = store().await;
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "lifecycle-source".into(),
                    text: "original".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
        )
        .await
        .unwrap();
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let op = admit_import_operation(&store, "lifecycle", "thread", "turn").await;
    let ready = ready_import_operation(&store, &op, "thread", &reference).await;
    let item = pioneer_protocol::TurnItem::SystemEvent {
        id: "compaction:lifecycle".into(),
        level: pioneer_protocol::SystemEventLevel::Info,
        message: "Context compaction started".into(),
        code: Some("agent_context_compaction".into()),
        details: None,
    };
    let started = pioneer_protocol::ItemStartedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item: item.clone(),
    };
    let completed = pioneer_protocol::ItemCompletedNotification {
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item,
    };
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation + 1,
                Event::ItemStarted(started.clone()),
                1
            )
            .await
            .is_err()
    );
    let mut wrong_scope = started.clone();
    wrong_scope.workspace_id = "other".into();
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemStarted(wrong_scope),
                1
            )
            .await
            .is_err()
    );
    let mut forged = started.clone();
    forged.item = pioneer_protocol::TurnItem::AgentMessage {
        id: "compaction:lifecycle".into(),
        text: "not a service event".into(),
        phase: Default::default(),
        markdown: None,
        markdown_version: None,
    };
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemStarted(forged),
                1
            )
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemCompleted(completed.clone()),
                1
            )
            .await
            .is_err(),
        "running operation cannot claim a terminal lifecycle"
    );
    let db = store.database_connection();
    db.execute_unprepared("CREATE FUNCTION abort_compaction_lifecycle() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'lifecycle append rollback'; END $$; CREATE TRIGGER abort_compaction_lifecycle BEFORE INSERT ON turn_event FOR EACH ROW WHEN (NEW.event_type='item/started') EXECUTE FUNCTION abort_compaction_lifecycle()").await.unwrap();
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemStarted(started.clone()),
                1
            )
            .await
            .is_err()
    );
    db.execute_unprepared("DROP TRIGGER abort_compaction_lifecycle ON turn_event")
        .await
        .unwrap();
    // The fixture Turn is already completed. Only the operation-owned service
    // event, not an ordinary provider callback, is admitted by this API.
    store
        .compaction_materialize_lifecycle(
            &op.id,
            ready.generation,
            Event::ItemStarted(started.clone()),
            1,
        )
        .await
        .unwrap();
    store
        .compaction_materialize_lifecycle(
            &op.id,
            ready.generation,
            Event::ItemStarted(started.clone()),
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compaction_apply_runner(&op.id, &ready, None)
            .await
            .unwrap(),
        CommitOutcome::Applied
    );
    let applied = store
        .compaction_runner_state(&op.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                applied.generation,
                Event::ItemStarted(started),
                3
            )
            .await
            .is_err()
    );
    assert!(
        store
            .compaction_materialize_lifecycle(
                &op.id,
                ready.generation,
                Event::ItemCompleted(completed.clone()),
                3
            )
            .await
            .is_err()
    );
    store
        .compaction_materialize_lifecycle(
            &op.id,
            applied.generation,
            Event::ItemCompleted(completed.clone()),
            3,
        )
        .await
        .unwrap();
    store
        .compaction_materialize_lifecycle(
            &op.id,
            applied.generation,
            Event::ItemCompleted(completed),
            4,
        )
        .await
        .unwrap();
    let row = db.query_one_raw(Statement::from_string(DbBackend::Postgres,"SELECT count(*) AS n FROM turn_event WHERE turn_id='turn' AND event_type IN ('item/started','item/completed')".to_owned())).await.unwrap().unwrap();
    assert_eq!(
        row.try_get::<i64>("", "n").unwrap(),
        3,
        "baseline plus exactly two lifecycle events; retries add no duplicates"
    );
}

#[pioneer_postgres_test::postgres_test]
async fn compaction_terminal_fence_reconciliation_persists_once_and_rejects_late_attempt() {
    use pioneer_compaction::runner::{FailureKind, RunnerPhase};
    let store = store().await;
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "terminal-source".into(),
                    text: "original".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
        )
        .await
        .unwrap();
    let reference = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    for (id, status, outcome, kind) in [
        ("stop", "cancelled", "cancelled", FailureKind::Cancelled),
        ("deadline", "failed", "deadline", FailureKind::Deadline),
    ] {
        let op = admit_import_operation(&store, id, "thread", "turn").await;
        let initial = ready_import_operation(&store, &op, "thread", &reference).await;
        let db = store.database_connection();
        let mut replay = db.work_signal("compaction").subscribe();
        replay.begin_pass(&db).await.unwrap();
        store
            .compaction_bind_execution_turn(id, "turn")
            .await
            .unwrap();
        let budget = ModelBudget::new(None, None, None);
        store
            .compaction_prepare_runner(id, &budget, 1, 0)
            .await
            .unwrap();
        let activation = pioneer_compaction::runner::RunnerState::new(
            op.admission.deadline_ms,
            &budget,
            1000,
            None,
        )
        .unwrap();
        store
            .compaction_activate_runner(id, &activation)
            .await
            .unwrap();
        assert!(
            futures_util::FutureExt::now_or_never(replay.wait(None)).is_none(),
            "idempotent runner admission must not wake discovery"
        );
        store.compaction_finish(id, status, outcome).await.unwrap();
        db.execute_unprepared("CREATE OR REPLACE FUNCTION abort_terminal_state() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'terminal state rollback'; END $$; CREATE TRIGGER abort_terminal_state BEFORE UPDATE ON compaction_runner_state FOR EACH ROW EXECUTE FUNCTION abort_terminal_state()").await.unwrap();
        assert!(store.compaction_reconcile_runner_state(id).await.is_err());
        assert_eq!(
            store
                .compaction_runner_state(id)
                .await
                .unwrap()
                .unwrap()
                .generation,
            initial.generation
        );
        db.execute_unprepared("DROP TRIGGER abort_terminal_state ON compaction_runner_state")
            .await
            .unwrap();
        let mut consumers = ["compaction", "maintenance.frozen", "maintenance.html"]
            .map(|domain| db.work_signal(domain).subscribe());
        for consumer in &mut consumers {
            consumer.begin_pass(&db).await.unwrap();
        }
        let (a, b) = tokio::join!(
            store.compaction_reconcile_runner_state(id),
            store.compaction_reconcile_runner_state(id)
        );
        for state in [a.unwrap().unwrap(), b.unwrap().unwrap()] {
            assert_eq!(state.generation, initial.generation + 1);
            assert_eq!(state.phase, RunnerPhase::Failed { kind: kind.clone() });
        }
        for consumer in &mut consumers {
            assert!(futures_util::FutureExt::now_or_never(consumer.wait(None)).is_some());
            consumer.begin_pass(&db).await.unwrap();
        }
        // A repeated terminal UPDATE and unchanged DTO must not re-enqueue work.
        store.compaction_finish(id, status, outcome).await.unwrap();
        store
            .compaction_finish_execution(id, None, op.admission.deadline_ms, status, outcome)
            .await
            .unwrap();
        assert!(
            store
                .compaction_reconcile_runner_state("absent-operation")
                .await
                .unwrap()
                .is_none()
        );
        let again = store
            .compaction_reconcile_runner_state(id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.generation, initial.generation + 1);
        assert!(
            !store
                .compaction_runner_transition(
                    id,
                    initial.generation,
                    &initial.terminate(FailureKind::Permanent).unwrap(),
                    None
                )
                .await
                .unwrap()
        );
        assert!(
            !store
                .compaction_claim_attempt(id, 0, 0, false, false)
                .await
                .unwrap()
        );
        for consumer in &mut consumers {
            assert!(
                futures_util::FutureExt::now_or_never(consumer.wait(None)).is_none(),
                "terminal ACK and rejected CAS must leave independent consumers parked"
            );
        }
        let event = pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::SystemEvent {
                    id: format!("compaction:{id}"),
                    level: pioneer_protocol::SystemEventLevel::Info,
                    message: "Compaction finished".into(),
                    code: Some("agent_context_compaction".into()),
                    details: None,
                },
            },
        );
        assert!(
            store
                .compaction_materialize_lifecycle(id, initial.generation, event.clone(), 2)
                .await
                .is_err()
        );
        store
            .compaction_materialize_lifecycle(id, again.generation, event.clone(), 2)
            .await
            .unwrap();
        store
            .compaction_materialize_lifecycle(id, again.generation, event, 3)
            .await
            .unwrap();
    }
}

// These fixtures enter optional preparation through canonical root publication,
// while retry/deadline tests below continue to own the durable worker contract.
async fn start_history_check_root(store: &CrudStore) {
    let mut thread = store.get_thread_model("thread").await.unwrap().unwrap();
    thread.updated_at = 1;
    let (_, mut turn) = store.get_turn("thread", "turn").await.unwrap().unwrap();
    store
        .database_connection()
        .execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();
    // Retire the raw fixture's creation key before reusing its Turn id.
    store
        .database_connection()
        .execute_unprepared("DELETE FROM compaction_turn_creation WHERE turn_id='turn'")
        .await
        .unwrap();
    turn.status = pioneer_protocol::TurnStatus::InProgress;
    store
        .materialize_turn_start(
            &thread,
            pioneer_protocol::SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::Principal(
                pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
            ),
        )
        .await
        .unwrap();
}

async fn publish_history_check_root(store: &CrudStore) {
    store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "root-answer".into(),
                    text: "ready root answer".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1,
        )
        .await
        .unwrap();
    let (_, mut turn) = store.get_turn("thread", "turn").await.unwrap().unwrap();
    turn.status = pioneer_protocol::TurnStatus::Completed;
    store
        .materialize_turn_completed(
            pioneer_protocol::TurnCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn,
            },
            1,
        )
        .await
        .unwrap();
}

#[pioneer_postgres_test::postgres_test]
async fn completed_cli_check_is_atomic_bounded_and_captures_settings_once() {
    let store = store().await;
    start_history_check_root(&store).await;
    let db = store.database_connection();
    db.execute_unprepared("INSERT INTO turn_cli_runtime_binding(turn_id,thread_id,continuation_thread_id,workspace_id,runtime_id,runtime_kind,native_thread_id,status,model,frozen_manifest_id) VALUES('turn','thread','thread','ws','claude','claude','native','running','sonnet',NULL)").await.unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    db.execute_unprepared(
        "UPDATE turn SET status='in_progress',reasoning_effort='high' WHERE id='turn'",
    )
    .await
    .unwrap();
    {
        use sea_orm::TransactionTrait;
        let tx = db.begin().await.unwrap();
        tx.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    publish_history_check_root(&store).await;
    let pending = store.compaction_pending_history_checks().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        store
            .compaction_history_check_is_current("turn")
            .await
            .unwrap()
    );
    assert_eq!(pending[0].reasoning_effort.as_deref(), Some("high"));
    assert_eq!(pending[0].model.as_deref(), Some("sonnet"));
    assert_eq!(
        store
            .compaction_capture_history_check("turn", r#""original selection and deadline""#)
            .await
            .unwrap()
            .as_deref(),
        Some(r#""original selection and deadline""#)
    );
    assert_eq!(
        store
            .compaction_capture_history_check("turn", r#""changed settings after restart""#)
            .await
            .unwrap()
            .as_deref(),
        Some(r#""original selection and deadline""#)
    );
    assert!(
        store
            .compaction_capture_history_check("turn", &"x".repeat(16385))
            .await
            .is_err()
    );
    store
        .compaction_stop_execution("ws", "thread", "owner", "turn")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    assert!(
        store
            .compaction_pending_history_checks()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .compaction_capture_history_check("turn", "restart")
            .await
            .unwrap()
            .is_none()
    );
}
