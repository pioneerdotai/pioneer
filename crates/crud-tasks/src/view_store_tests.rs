use super::*;
use crate::{
    event_store::TaskEventStore,
    execution_store::TaskExecutionStore,
    task_event_context::TaskEventContextRepository,
    test_support::{
        execution::ensure_test_agent_identity,
        task_projection::{NoFrozenOutput, sample_task, sample_task_run},
    },
};
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Set};

async fn postgres_store_with_workspace(
    fixture: &pioneer_postgres_test::TestDatabase,
    workspace_id: &str,
) -> PostgresDatabase {
    let connection = fixture.connection();
    let tx = connection.begin().await.expect("begin workspace fixture");
    pioneer_entity::workspace::Entity::update_many()
        .col_expr(
            pioneer_entity::workspace::Column::IsCurrent,
            Expr::value(false),
        )
        .filter(pioneer_entity::workspace::Column::IsCurrent.eq(true))
        .exec(&tx)
        .await
        .expect("clear the seeded current workspace");
    let timestamp = unix_to_datetime(1_700_000_000);
    pioneer_entity::workspace::Entity::insert(pioneer_entity::workspace::ActiveModel {
        id: Set(workspace_id.to_owned()),
        name: Set("Test Workspace".to_owned()),
        is_active: Set(true),
        is_current: Set(true),
        created_at: Set(timestamp),
        updated_at: Set(timestamp),
    })
    .exec(&tx)
    .await
    .expect("insert workspace fixture");
    tx.commit().await.expect("commit workspace fixture");
    let reader = fixture
        .connect_read_only()
        .await
        .expect("read-only PostgreSQL pool");
    PostgresDatabase::new(reader, connection.clone())
}

#[tokio::test]
async fn task_run_child_anchor_uses_accepted_task_run_turn() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db =
            postgres_store_with_workspace(fixture, "ws_task_review_anchor_accepted_turn").await;
        let store = TaskViewStore::new(&db);
        let child_work = crate::child_repository::TaskChildWorkRepository::new(&db);
        store
            .upsert_task_run_thread_binding(TaskRunThreadBinding {
                id: "binding_anchor_accepted_turn".to_owned(),
                task_id: "task_anchor_accepted_turn".to_owned(),
                run_id: "run_anchor_accepted_turn".to_owned(),
                execution_id: None,
                thread_id: "thread_anchor_accepted_turn".to_owned(),
                binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                created_at: 1_700_001_100,
            })
            .await
            .expect("binding upsert should succeed");

        for (id, turn_id, sequence) in [
            ("task_run_turn_anchor_accepted", "turn_anchor_accepted", 0),
            ("task_run_turn_anchor_latest", "turn_anchor_latest", 1),
        ] {
            child_work
                .upsert_task_run_turn(TaskRunTurn {
                    id: id.to_owned(),
                    task_id: "task_anchor_accepted_turn".to_owned(),
                    run_id: "run_anchor_accepted_turn".to_owned(),
                    execution_id: None,
                    thread_id: "thread_anchor_accepted_turn".to_owned(),
                    turn_id: turn_id.to_owned(),
                    kind: if sequence == 0 {
                        TaskRunTurnKind::Initial
                    } else {
                        TaskRunTurnKind::Revision
                    },
                    round: sequence,
                    sequence,
                    status: TaskRunTurnStatus::CandidateCreated,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_001_101 + i64::from(sequence),
                    started_at: None,
                    completed_at: None,
                })
                .await
                .expect("turn upsert should succeed");
        }

        let candidate = TaskResultCandidate {
            id: "candidate_anchor_accepted_turn".to_owned(),
            task_id: "task_anchor_accepted_turn".to_owned(),
            run_id: "run_anchor_accepted_turn".to_owned(),
            task_run_turn_id: "task_run_turn_anchor_accepted".to_owned(),
            thread_id: "thread_anchor_accepted_turn".to_owned(),
            turn_id: "stale_denormalized_candidate_turn".to_owned(),
            round: 0,
            status: TaskResultCandidateStatus::Accepted,
            result: Some(TaskResult {
                summary: Some("accepted".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_anchor_accepted_turn".to_owned()),
            }),
            extraction_error: None,
            summary: Some("accepted".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: Some("review_anchor_accepted_turn".to_owned()),
            created_at: 1_700_001_110,
            updated_at: 1_700_001_111,
            resolved_at: Some(1_700_001_111),
        };
        let candidate = task_result_candidate::prepare_protocol_candidate(&candidate).unwrap();
        task_result_candidate::upsert_candidate(&db, candidate)
            .await
            .expect("candidate upsert should succeed");

        assert_eq!(
            store
                .get_task_run_child_anchor("run_anchor_accepted_turn")
                .await
                .expect("child anchor should load"),
            TaskRunChildAnchor {
                child_thread_id: Some("thread_anchor_accepted_turn".to_owned()),
                child_turn_id: Some("turn_anchor_accepted".to_owned())
            }
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_review_target_repositories_round_trip_and_update() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = postgres_store_with_workspace(fixture, "ws_task_review_target_round_trip").await;
        let store = TaskViewStore::new(&db);
        let binding = TaskRunThreadBinding {
            id: "binding_round_trip".to_owned(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            execution_id: Some("execution_round_trip".to_owned()),
            thread_id: "thread_round_trip".to_owned(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: 1_700_000_010,
        };
        let inserted_binding = store
            .upsert_task_run_thread_binding(binding.clone())
            .await
            .expect("binding upsert should succeed");
        assert_eq!(inserted_binding, binding);

        let turn = TaskRunTurn {
            id: "task_run_turn_round_trip".to_owned(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            execution_id: Some("execution_round_trip".to_owned()),
            thread_id: "thread_round_trip".to_owned(),
            turn_id: "turn_round_trip".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::InProgress,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 1_700_000_011,
            started_at: Some(1_700_000_011),
            completed_at: None,
        };
        crate::child_repository::TaskChildWorkRepository::new(&db)
            .upsert_task_run_turn(turn.clone())
            .await
            .expect("turn upsert should succeed");
        let completed_turn = store
            .update_task_run_turn_status(
                turn.id.as_str(),
                TaskRunTurnStatus::CandidateCreated,
                Some(1_700_000_012),
            )
            .await
            .expect("turn status update should succeed")
            .expect("turn should exist");
        assert_eq!(completed_turn.status, TaskRunTurnStatus::CandidateCreated);
        assert_eq!(completed_turn.completed_at, Some(1_700_000_012));

        let candidate = TaskResultCandidate {
            id: "candidate_round_trip".to_owned(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            task_run_turn_id: turn.id.clone(),
            thread_id: "thread_round_trip".to_owned(),
            turn_id: "turn_round_trip".to_owned(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("done".to_owned()),
                data: Some(TaskValue::String("ok".to_owned())),
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_round_trip".to_owned()),
            }),
            extraction_error: None,
            summary: Some("done".to_owned()),
            diagnostics: vec!["parsed".to_owned()],
            final_review_event_id: None,
            created_at: 1_700_000_013,
            updated_at: 1_700_000_013,
            resolved_at: None,
        };
        store
            .upsert_task_result_candidate(candidate.clone())
            .await
            .expect("candidate upsert should succeed");

        let review_event = TaskResultReviewEvent {
            id: "review_event_round_trip".to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: "task_round_trip".to_owned(),
            run_id: "run_round_trip".to_owned(),
            task_run_turn_id: turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer: pioneer_protocol::TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: 1_700_000_014,
        };
        store
            .upsert_task_result_review_event(review_event.clone())
            .await
            .expect("review event upsert should succeed");
        let accepted = store
            .update_task_result_candidate_resolution(
                candidate.id.as_str(),
                TaskResultCandidateStatus::Accepted,
                Some(review_event.id.as_str()),
                Some(1_700_000_014),
                1_700_000_014,
            )
            .await
            .expect("candidate resolution update should succeed")
            .expect("candidate should exist");
        assert_eq!(accepted.status, TaskResultCandidateStatus::Accepted);
        assert_eq!(
            accepted.final_review_event_id.as_deref(),
            Some(review_event.id.as_str())
        );
        assert_eq!(accepted.resolved_at, Some(1_700_000_014));
        assert_eq!(
            store
                .get_task_result_review_event(review_event.id.as_str())
                .await
                .expect("review lookup should succeed"),
            Some(review_event)
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_review_canonical_helpers_return_target_rows_and_ordering() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = postgres_store_with_workspace(fixture, "ws_task_review_helpers").await;
        let store = TaskViewStore::new(&db);
        assert_eq!(
            store
                .get_task_run_primary_thread_binding("run_helpers")
                .await
                .expect("missing primary binding lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_task_run_thread_binding_by_thread("thread_helpers")
                .await
                .expect("missing thread binding lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_task_run_turn_by_turn("thread_helpers", "turn_helpers_initial")
                .await
                .expect("missing turn lookup should succeed"),
            None
        );
        assert!(
            store
                .list_task_run_turns("run_helpers")
                .await
                .expect("missing run turn list should succeed")
                .is_empty()
        );
        assert_eq!(
            store
                .get_latest_task_run_turn("run_helpers")
                .await
                .expect("missing latest turn lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_pending_task_result_candidate("run_helpers")
                .await
                .expect("missing pending candidate lookup should succeed"),
            None
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate("run_helpers")
                .await
                .expect("missing accepted candidate lookup should succeed"),
            None
        );
        assert!(
            store
                .list_task_result_review_events("candidate_helpers_accepted")
                .await
                .expect("missing candidate review list should succeed")
                .is_empty()
        );
        assert!(
            store
                .list_task_result_review_events_for_run("run_helpers")
                .await
                .expect("missing run review list should succeed")
                .is_empty()
        );

        let binding = TaskRunThreadBinding {
            id: "binding_helpers".to_owned(),
            task_id: "task_helpers".to_owned(),
            run_id: "run_helpers".to_owned(),
            execution_id: None,
            thread_id: "thread_helpers".to_owned(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: 1_700_001_000,
        };
        store
            .upsert_task_run_thread_binding(binding.clone())
            .await
            .expect("binding upsert should succeed");
        assert_eq!(
            store
                .get_task_run_primary_thread_binding("run_helpers")
                .await
                .expect("primary binding lookup should succeed"),
            Some(binding.clone())
        );
        assert_eq!(
            store
                .get_task_run_thread_binding_by_thread("thread_helpers")
                .await
                .expect("thread binding lookup should succeed"),
            Some(binding)
        );
        let duplicate_primary = store
            .upsert_task_run_thread_binding(TaskRunThreadBinding {
                id: "binding_helpers_duplicate_primary".to_owned(),
                task_id: "task_helpers".to_owned(),
                run_id: "run_helpers".to_owned(),
                execution_id: None,
                thread_id: "thread_helpers_duplicate_primary".to_owned(),
                binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                created_at: 1_700_001_000,
            })
            .await;
        assert!(
            duplicate_primary.is_err(),
            "duplicate primary_executor binding should be rejected"
        );

        for (index, turn_id) in ["turn_helpers_initial", "turn_helpers_revision"]
            .into_iter()
            .enumerate()
        {
            crate::child_repository::TaskChildWorkRepository::new(&db)
                .upsert_task_run_turn(TaskRunTurn {
                    id: format!("task_run_turn_helpers_{index}"),
                    task_id: "task_helpers".to_owned(),
                    run_id: "run_helpers".to_owned(),
                    execution_id: None,
                    thread_id: "thread_helpers".to_owned(),
                    turn_id: turn_id.to_owned(),
                    kind: if index == 0 {
                        TaskRunTurnKind::Initial
                    } else {
                        TaskRunTurnKind::Revision
                    },
                    round: u32::try_from(index).expect("index should fit"),
                    sequence: u32::try_from(index).expect("index should fit"),
                    status: TaskRunTurnStatus::CandidateCreated,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_001_001 + i64::try_from(index).expect("index should fit"),
                    started_at: None,
                    completed_at: None,
                })
                .await
                .expect("turn upsert should succeed");
        }
        assert!(
            crate::child_repository::TaskChildWorkRepository::new(&db)
                .upsert_task_run_turn(TaskRunTurn {
                    id: "task_run_turn_helpers_duplicate_sequence".to_owned(),
                    task_id: "task_helpers".to_owned(),
                    run_id: "run_helpers".to_owned(),
                    execution_id: None,
                    thread_id: "thread_helpers".to_owned(),
                    turn_id: "turn_helpers_duplicate_sequence".to_owned(),
                    kind: TaskRunTurnKind::Review,
                    round: 99,
                    sequence: 1,
                    status: TaskRunTurnStatus::ReviewRecorded,
                    reviews_candidate_id: None,
                    requested_by_candidate_id: None,
                    requested_by_review_event_id: None,
                    created_at: 1_700_001_009,
                    started_at: None,
                    completed_at: None,
                })
                .await
                .is_err(),
            "duplicate run/sequence turn should be rejected"
        );
        let turns = store
            .list_task_run_turns("run_helpers")
            .await
            .expect("turn list should succeed");
        assert_eq!(
            turns
                .iter()
                .map(|turn| turn.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn_helpers_initial", "turn_helpers_revision"]
        );
        assert_eq!(
            store
                .get_latest_task_run_turn("run_helpers")
                .await
                .expect("latest turn lookup should succeed")
                .map(|turn| turn.turn_id),
            Some("turn_helpers_revision".to_owned())
        );

        let pending = TaskResultCandidate {
            id: "candidate_helpers_pending".to_owned(),
            task_id: "task_helpers".to_owned(),
            run_id: "run_helpers".to_owned(),
            task_run_turn_id: "task_run_turn_helpers_0".to_owned(),
            thread_id: "thread_helpers".to_owned(),
            turn_id: "turn_helpers_initial".to_owned(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("pending".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: None,
            }),
            extraction_error: None,
            summary: Some("pending".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: 1_700_001_010,
            updated_at: 1_700_001_010,
            resolved_at: None,
        };
        let accepted = TaskResultCandidate {
            id: "candidate_helpers_accepted".to_owned(),
            status: TaskResultCandidateStatus::Accepted,
            task_run_turn_id: "task_run_turn_helpers_1".to_owned(),
            turn_id: "turn_helpers_revision".to_owned(),
            round: 1,
            final_review_event_id: Some("review_helpers_accept".to_owned()),
            created_at: 1_700_001_020,
            updated_at: 1_700_001_021,
            resolved_at: Some(1_700_001_021),
            ..pending.clone()
        };
        store
            .upsert_task_result_candidate(pending.clone())
            .await
            .expect("pending candidate upsert should succeed");
        store
            .upsert_task_result_candidate(accepted.clone())
            .await
            .expect("accepted candidate upsert should succeed");
        assert_eq!(
            store
                .get_pending_task_result_candidate("run_helpers")
                .await
                .expect("pending candidate lookup should succeed")
                .map(|candidate| candidate.id),
            Some(pending.id)
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate("run_helpers")
                .await
                .expect("accepted candidate lookup should succeed")
                .map(|candidate| candidate.id),
            Some(accepted.id.clone())
        );
        assert_eq!(
            store
                .get_task_run_child_anchor("run_helpers")
                .await
                .expect("child anchor should load"),
            TaskRunChildAnchor {
                child_thread_id: Some("thread_helpers".to_owned()),
                child_turn_id: Some("turn_helpers_revision".to_owned())
            }
        );

        for (id, created_at) in [
            ("review_helpers_first", 1_700_001_030),
            ("review_helpers_second", 1_700_001_031),
        ] {
            store
                .upsert_task_result_review_event(TaskResultReviewEvent {
                    id: id.to_owned(),
                    candidate_id: accepted.id.clone(),
                    task_id: "task_helpers".to_owned(),
                    run_id: "run_helpers".to_owned(),
                    task_run_turn_id: "task_run_turn_helpers_1".to_owned(),
                    reviewer_kind: TaskResultReviewerKind::ParentAgent,
                    reviewer: pioneer_protocol::TaskResultReviewerRef::AgentExecution(
                        pioneer_protocol::AgentExecutionId::new("R".repeat(21))
                            .expect("parent reviewer execution id"),
                    ),
                    reviewer_thread_id: Some("parent_thread_helpers".to_owned()),
                    reviewer_turn_id: Some("parent_turn_helpers".to_owned()),
                    reviewer_user_id: None,
                    reviewer_agent_spec_id: None,
                    event_kind: TaskResultReviewEventKind::Decision,
                    decision: TaskResultReviewDecision::Accept,
                    feedback_text: None,
                    feedback: None,
                    confidence: None,
                    supersedes_review_event_id: None,
                    next_task_run_turn_id: None,
                    created_at,
                })
                .await
                .expect("review event upsert should succeed");
        }
        assert_eq!(
            store
                .list_task_result_review_events(accepted.id.as_str())
                .await
                .expect("candidate reviews should list")
                .into_iter()
                .map(|event| event.id)
                .collect::<Vec<_>>(),
            vec!["review_helpers_first", "review_helpers_second"]
        );
        assert_eq!(
            store
                .list_task_result_review_events_for_run("run_helpers")
                .await
                .expect("run reviews should list")
                .into_iter()
                .map(|event| event.id)
                .collect::<Vec<_>>(),
            vec!["review_helpers_first", "review_helpers_second"]
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_review_projector_replays_new_runtime_events_idempotently() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = postgres_store_with_workspace(fixture, "ws_task_review_projector").await;
        let store = TaskViewStore::new(&db);
        let events = TaskEventStore::new(db.clone(), NoFrozenOutput);
        let timestamp = 1_700_002_000;
        ensure_test_agent_identity(&db, "ws_task_review_projector", timestamp).await;
        let mut task = sample_task(timestamp);
        task.id = "task_projector".to_owned();
        task.workspace_id = "ws_task_review_projector".to_owned();
        let mut run = sample_task_run(timestamp);
        run.id = "run_projector".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();

        events
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
        let execution = TaskExecutionStore::new(&db)
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, timestamp + 1)
            .await
            .expect("execution should reserve");

        let binding = TaskRunThreadBinding {
            id: "binding_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id.clone()),
            thread_id: "thread_projector".to_owned(),
            binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
            created_at: timestamp + 1,
        };
        let turn = TaskRunTurn {
            id: "task_run_turn_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: Some(execution.id.clone()),
            thread_id: binding.thread_id.clone(),
            turn_id: "turn_projector".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 2,
            started_at: Some(timestamp + 2),
            completed_at: Some(timestamp + 3),
        };
        let candidate = TaskResultCandidate {
            id: "candidate_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: turn.id.clone(),
            thread_id: binding.thread_id.clone(),
            turn_id: turn.turn_id.clone(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("candidate".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some(run.id.clone()),
            }),
            extraction_error: None,
            summary: Some("candidate".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: timestamp + 4,
            updated_at: timestamp + 4,
            resolved_at: None,
        };
        let review_event = TaskResultReviewEvent {
            id: "review_projector_accept".to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer: pioneer_protocol::TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Accept,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: None,
            next_task_run_turn_id: None,
            created_at: timestamp + 5,
        };
        let accepted_candidate = TaskResultCandidate {
            status: TaskResultCandidateStatus::Accepted,
            final_review_event_id: Some(review_event.id.clone()),
            updated_at: timestamp + 5,
            resolved_at: Some(timestamp + 5),
            ..candidate.clone()
        };

        let payloads = vec![
            TaskEventPayload::TaskRunThreadBindingCreated {
                binding: binding.clone(),
            },
            TaskEventPayload::TaskRunTurnStarted {
                task_run_turn: TaskRunTurn {
                    status: TaskRunTurnStatus::InProgress,
                    completed_at: None,
                    ..turn.clone()
                },
            },
            TaskEventPayload::TaskRunTurnCompleted {
                task_run_turn: turn.clone(),
            },
            TaskEventPayload::TaskResultCandidateCreated {
                candidate: candidate.clone(),
            },
            TaskEventPayload::TaskRunEnteredReview {
                task_id: run.task_id.clone(),
                run_id: run.id.clone(),
                candidate_id: candidate.id.clone(),
                entered_at: timestamp + 4,
            },
            TaskEventPayload::TaskResultReviewEventRecorded {
                review_event: review_event.clone(),
            },
            TaskEventPayload::TaskResultCandidateAccepted {
                candidate: accepted_candidate.clone(),
                review_event_id: review_event.id.clone(),
            },
        ];

        events
            .append_task_events(payloads.clone(), timestamp + 10)
            .await
            .expect("new runtime events should project");
        events
            .append_task_events(payloads, timestamp + 10)
            .await
            .expect("replay should be idempotent");

        assert_eq!(
            store
                .get_task_run_primary_thread_binding(run.id.as_str())
                .await
                .expect("primary binding should load"),
            Some(binding)
        );
        assert_eq!(
            store
                .list_task_run_turns(run.id.as_str())
                .await
                .expect("turns should list"),
            vec![turn]
        );
        assert_eq!(
            store
                .get_accepted_task_result_candidate(run.id.as_str())
                .await
                .expect("accepted candidate should load")
                .map(|candidate| candidate.final_review_event_id),
            Some(Some(review_event.id.clone()))
        );
        assert_eq!(
            store
                .list_task_result_review_events(candidate.id.as_str())
                .await
                .expect("review events should list"),
            vec![review_event]
        );
        assert_eq!(
            TaskEventContextRepository::new(&db)
                .get_task_run(run.id.as_str())
                .await
                .expect("run lookup should succeed")
                .expect("run should exist")
                .status,
            TaskRunStatus::WaitingReview
        );
        assert_eq!(
            store
                .get_task(task.id.as_str())
                .await
                .expect("task lookup should succeed")
                .expect("task should exist")
                .task
                .status,
            TaskStatus::WaitingReview
        );
        assert_eq!(
            TaskExecutionStore::new(&db)
                .load_execution_for_run(run.id.as_str())
                .await
                .expect("execution lookup should succeed")
                .expect("execution should exist")
                .status,
            TaskRunExecutionStatus::WaitingReview
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_review_projector_replays_legacy_child_thread_linked_json() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = postgres_store_with_workspace(fixture, "ws_task_review_legacy_projector").await;
        let store = TaskViewStore::new(&db);
        let events = TaskEventStore::new(db.clone(), NoFrozenOutput);
        let timestamp = 1_700_003_000;
        let mut run = sample_task_run(timestamp);
        run.id = "run_legacy_projector".to_owned();
        run.task_id = "task_legacy_projector".to_owned();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();

        let legacy_link_json = serde_json::json!({
            "kind": "child_thread_linked",
            "payload": {
                "lineage": {
                    "childThreadId": "child_thread_legacy_projector",
                    "childTurnId": "child_turn_legacy_projector",
                    "parentThreadId": "parent_thread_legacy_projector",
                    "parentTurnId": "parent_turn_legacy_projector",
                    "taskId": run.task_id.clone(),
                    "taskRunId": run.id.clone(),
                    "rootThreadId": "root_thread_legacy_projector",
                    "depth": 1,
                    "createdAt": timestamp + 1
                }
            }
        });
        let legacy_link: TaskEventPayload =
            serde_json::from_value(legacy_link_json).expect("legacy child link should decode");

        let completed_result = TaskResult {
            summary: Some("legacy accepted".to_owned()),
            data: Some(TaskValue::String("legacy".to_owned())),
            artifacts: Vec::new(),
            completed_by_run_id: Some(run.id.clone()),
        };
        let payloads = vec![
            TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: None,
            },
            legacy_link,
            TaskEventPayload::RunCompleted {
                task_id: run.task_id.clone(),
                run_id: run.id.clone(),
                result: Some(completed_result.clone()),
                completed_at: timestamp + 2,
            },
        ];
        events
            .append_task_events(payloads.clone(), timestamp + 10)
            .await
            .expect("legacy events should project");
        events
            .append_task_events(payloads, timestamp + 10)
            .await
            .expect("legacy replay should be idempotent");
        let binding = store
            .get_task_run_primary_thread_binding(run.id.as_str())
            .await
            .expect("legacy binding should load")
            .expect("legacy binding should exist");
        assert_eq!(binding.execution_id, None);
        assert_eq!(binding.thread_id, "child_thread_legacy_projector");

        let turns = store
            .list_task_run_turns(run.id.as_str())
            .await
            .expect("legacy turns should list");
        assert_eq!(turns.len(), 1);
        assert_eq!(turns[0].turn_id, "child_turn_legacy_projector");
        assert_eq!(turns[0].status, TaskRunTurnStatus::InProgress);

        let accepted = store
            .get_accepted_task_result_candidate(run.id.as_str())
            .await
            .expect("legacy accepted candidate should load")
            .expect("legacy accepted candidate should exist");
        assert_eq!(accepted.result, Some(completed_result));
        assert_eq!(
            store
                .list_task_result_review_events(accepted.id.as_str())
                .await
                .expect("legacy auto review should list")
                .into_iter()
                .map(|event| (event.event_kind, event.reviewer_kind, event.decision))
                .collect::<Vec<_>>(),
            vec![(
                TaskResultReviewEventKind::SystemAuto,
                TaskResultReviewerKind::RuntimeAuto,
                TaskResultReviewDecision::Accept
            )]
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_review_projector_replays_rejection_revision_and_failed_turn_events() {
    pioneer_postgres_test::with_database(async |fixture| {
        let db = postgres_store_with_workspace(fixture, "ws_task_review_revision_projector").await;
        let store = TaskViewStore::new(&db);
        let events = TaskEventStore::new(db.clone(), NoFrozenOutput);
        let timestamp = 1_700_004_000;
        let mut task = sample_task(timestamp);
        task.id = "task_revision_projector".to_owned();
        task.workspace_id = "ws_task_review_revision_projector".to_owned();
        let mut run = sample_task_run(timestamp);
        run.id = "run_revision_projector".to_owned();
        run.task_id = task.id.clone();
        run.trigger_id = None;
        run.run_group_id = run.id.clone();

        let initial_turn = TaskRunTurn {
            id: "task_run_turn_revision_initial".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: "thread_revision_projector".to_owned(),
            turn_id: "turn_revision_initial".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: timestamp + 1,
            started_at: Some(timestamp + 1),
            completed_at: Some(timestamp + 2),
        };
        let candidate = TaskResultCandidate {
            id: "candidate_revision_projector".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: initial_turn.id.clone(),
            thread_id: initial_turn.thread_id.clone(),
            turn_id: initial_turn.turn_id.clone(),
            round: 0,
            status: TaskResultCandidateStatus::PendingReview,
            result: Some(TaskResult {
                summary: Some("needs changes".to_owned()),
                data: None,
                artifacts: Vec::new(),
                completed_by_run_id: Some(run.id.clone()),
            }),
            extraction_error: None,
            summary: Some("needs changes".to_owned()),
            diagnostics: Vec::new(),
            final_review_event_id: None,
            created_at: timestamp + 3,
            updated_at: timestamp + 3,
            resolved_at: None,
        };
        let review_event = TaskResultReviewEvent {
            id: "review_revision_projector".to_owned(),
            candidate_id: candidate.id.clone(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            task_run_turn_id: initial_turn.id.clone(),
            reviewer_kind: TaskResultReviewerKind::ParentAgent,
            reviewer: pioneer_protocol::TaskResultReviewerRef::AgentExecution(
                pioneer_protocol::AgentExecutionId::new("R".repeat(21))
                    .expect("parent reviewer execution id"),
            ),
            reviewer_thread_id: Some("parent_thread_revision_projector".to_owned()),
            reviewer_turn_id: Some("parent_turn_revision_projector".to_owned()),
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::Decision,
            decision: TaskResultReviewDecision::RequestChanges,
            feedback_text: Some("tighten result".to_owned()),
            feedback: None,
            confidence: Some(0.8),
            supersedes_review_event_id: None,
            next_task_run_turn_id: Some("task_run_turn_revision_retry".to_owned()),
            created_at: timestamp + 4,
        };
        let rejected_candidate = TaskResultCandidate {
            status: TaskResultCandidateStatus::Rejected,
            final_review_event_id: Some(review_event.id.clone()),
            updated_at: timestamp + 4,
            resolved_at: Some(timestamp + 4),
            ..candidate.clone()
        };
        let failed_revision_turn = TaskRunTurn {
            id: "task_run_turn_revision_retry".to_owned(),
            task_id: run.task_id.clone(),
            run_id: run.id.clone(),
            execution_id: None,
            thread_id: initial_turn.thread_id.clone(),
            turn_id: "turn_revision_retry".to_owned(),
            kind: TaskRunTurnKind::Revision,
            round: 1,
            sequence: 1,
            status: TaskRunTurnStatus::Failed,
            reviews_candidate_id: None,
            requested_by_candidate_id: Some(candidate.id.clone()),
            requested_by_review_event_id: Some(review_event.id.clone()),
            created_at: timestamp + 5,
            started_at: Some(timestamp + 5),
            completed_at: Some(timestamp + 6),
        };

        events
            .append_task_events(
                vec![
                    TaskEventPayload::TaskCreated { task: task.clone() },
                    TaskEventPayload::RunCreated {
                        run: run.clone(),
                        agent_spec: None,
                    },
                    TaskEventPayload::TaskRunTurnCompleted {
                        task_run_turn: initial_turn,
                    },
                    TaskEventPayload::TaskResultCandidateCreated {
                        candidate: candidate.clone(),
                    },
                    TaskEventPayload::TaskResultReviewEventRecorded {
                        review_event: review_event.clone(),
                    },
                    TaskEventPayload::TaskResultCandidateRejected {
                        candidate: rejected_candidate,
                        review_event_id: review_event.id.clone(),
                    },
                    TaskEventPayload::TaskRevisionRequested {
                        task_id: run.task_id.clone(),
                        run_id: run.id.clone(),
                        previous_candidate_id: candidate.id.clone(),
                        requested_by_review_event_id: review_event.id.clone(),
                        task_run_turn_id: failed_revision_turn.id.clone(),
                        thread_id: failed_revision_turn.thread_id.clone(),
                        turn_id: failed_revision_turn.turn_id.clone(),
                        round: failed_revision_turn.round,
                        feedback: "tighten result".to_owned(),
                        requested_at: timestamp + 5,
                    },
                    TaskEventPayload::TaskRunTurnFailed {
                        task_run_turn: failed_revision_turn.clone(),
                        error: None,
                    },
                ],
                timestamp + 10,
            )
            .await
            .expect("revision events should project");

        let rejected = store
            .get_task_result_candidate(candidate.id.as_str())
            .await
            .expect("candidate lookup should succeed")
            .expect("candidate should exist");
        assert_eq!(rejected.status, TaskResultCandidateStatus::Rejected);
        assert_eq!(
            rejected.final_review_event_id.as_deref(),
            Some(review_event.id.as_str())
        );
        assert_eq!(
            store
                .get_latest_task_run_turn(run.id.as_str())
                .await
                .expect("latest turn should load"),
            Some(failed_revision_turn)
        );
        Ok(())
    })
    .await
    .unwrap();
}
