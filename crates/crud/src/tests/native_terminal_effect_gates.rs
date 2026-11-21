use super::*;
use crate::{ClaimedNativeTerminalEffectRecord, DB_ID_LEN};
use pioneer_crud_execution::native_terminal_effect_outbox as outbox;
use pioneer_crud_tasks::task_result_candidate;
use pioneer_entity::native_terminal_effect_outbox as effect;
use pioneer_postgres_test::{TestDatabase, with_database};
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass,
    SqliteWriteEvent, SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{ActiveModelTrait, DatabaseBackend, IntoActiveModel, QueryTrait};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

const NOW: i64 = 1_900_000_000;

#[derive(Default)]
struct Routes {
    reads: AtomicUsize,
    writes: AtomicUsize,
    queued: AtomicUsize,
}
impl SqliteReadObserver for Routes {
    fn observe(&self, event: SqliteReadEvent) {
        if matches!(
            event,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                ..
            }
        ) {
            self.reads.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl SqliteWriteObserver for Routes {
    fn observe(&self, event: SqliteWriteEvent) {
        match event {
            SqliteWriteEvent::Acquired {
                class: SqliteWriteClass::Maintenance,
                ..
            } => {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
            SqliteWriteEvent::Enqueued {
                class: SqliteWriteClass::Maintenance,
                ..
            } => {
                self.queued.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }
}
struct Fixture<'a> {
    store: CrudStore,
    routes: Arc<Routes>,
    database: &'a TestDatabase,
}
impl<'a> Fixture<'a> {
    async fn open(database: &'a TestDatabase) -> Self {
        database.connection().execute_unprepared(
            "UPDATE workspace SET is_current=false WHERE is_current;
             INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws-gates','Gates',true,true)"
        ).await.unwrap();
        let routes = Arc::new(Routes::default());
        let store = Self::open_store(database, &routes).await;
        Self {
            store,
            routes,
            database,
        }
    }
    async fn open_store(database: &TestDatabase, routes: &Arc<Routes>) -> CrudStore {
        // Queue the scheduling cases behind a deliberately occupied writer slot.
        let writer = database.connect_with_limit(1).await.unwrap();
        let reader = database.connect_read_only().await.unwrap();
        let db = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, routes.clone()),
            routes.clone(),
        );
        db.validate_reader().await.unwrap();
        CrudStore::new(db)
    }
    async fn restart(&mut self) {
        self.store.database_connection().close().await.unwrap();
        self.store = Self::open_store(self.database, &self.routes).await;
    }
    async fn effect(
        &self,
        suffix: &str,
        gate: pioneer_protocol::NativeTerminalEffectGate,
        committed: bool,
    ) -> String {
        let thread_id = format!("thr_{suffix}");
        let turn_id = format!("turn_{suffix}");
        let (_, _, mut turn) =
            start_test_turn(self.store.clone(), "ws-gates", &thread_id, &turn_id).await;
        let effect_id = format!("{turn_id}:terminal-effect:post-turn");
        self.store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: format!("batch_{suffix}"),
                    workspace_id: "ws-gates".into(),
                    thread_id: thread_id.clone(),
                    turn_id: turn_id.clone(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.clone(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                            request: serde_json::json!({"phase":"turn.post_turn"}),
                            runtime_snapshot: serde_json::json!({"schema_version":1}),
                        },
                        max_attempts: 3,
                    }],
                },
                NOW - 1,
            )
            .await
            .unwrap();
        if committed {
            turn.status = TurnStatus::Completed;
            self.store
                .materialize_turn_completed(
                    TurnCompletedNotification {
                        workspace_id: "ws-gates".into(),
                        thread_id,
                        turn,
                    },
                    NOW,
                )
                .await
                .unwrap();
        }
        effect_id
    }
    async fn row(&self, id: &str) -> effect::Model {
        effect::Entity::find_by_id(id)
            .one(&self.store.connection)
            .await
            .unwrap()
            .unwrap()
    }
    async fn claim(&self, now: i64) -> Vec<ClaimedNativeTerminalEffectRecord> {
        self.store
            .claim_due_native_terminal_effects_with_clock(now, 10, 8, &|| now)
            .await
            .unwrap()
            .records
    }
}

pub(super) async fn candidate_fixture(
    store: &CrudStore,
    thread_id: &str,
    turn_id: &str,
    suffix: &str,
    status: TaskResultCandidateStatus,
    at: i64,
) -> TaskResultCandidate {
    let mut task = sample_task(at);
    task.id = format!("task_{suffix}");
    let thread = pioneer_entity::thread::Entity::find_by_id(thread_id)
        .one(&store.connection)
        .await
        .unwrap()
        .unwrap();
    task.workspace_id = thread.workspace_id;
    let mut run = sample_task_run(at);
    run.id = format!("run_{suffix}");
    run.task_id = task.id.clone();
    run.trigger_id = None;
    run.run_group_id = run.id.clone();
    store
        .append_task_events(
            vec![
                TaskEventPayload::TaskCreated { task: task.clone() },
                TaskEventPayload::RunCreated {
                    run: run.clone(),
                    agent_spec: None,
                },
            ],
            at,
        )
        .await
        .unwrap();
    // Canonical TaskRun Turn ownership is unique. Restored candidate metadata
    // can still name the same independent gate locator: the repository does
    // not enforce equality of those denormalized columns with its parent.
    let existing_parent = pioneer_entity::task_run_turn::Entity::find()
        .filter(pioneer_entity::task_run_turn::Column::TurnId.eq(turn_id))
        .one(&store.connection)
        .await
        .unwrap();
    let (parent_thread_id, parent_turn_id) = if existing_parent.is_some() {
        let parent_thread_id = format!("source_thread_{suffix}");
        let parent_turn_id = format!("source_turn_{suffix}");
        start_test_turn(
            store.clone(),
            &task.workspace_id,
            &parent_thread_id,
            &parent_turn_id,
        )
        .await;
        (parent_thread_id, parent_turn_id)
    } else {
        (thread_id.to_owned(), turn_id.to_owned())
    };
    let run_turn = TaskRunTurn {
        id: format!("trt_{suffix}"),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        execution_id: None,
        thread_id: parent_thread_id,
        turn_id: parent_turn_id,
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::CandidateCreated,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at: at,
        started_at: Some(at),
        completed_at: Some(at),
    };
    store.upsert_task_run_turn(run_turn.clone()).await.unwrap();
    TaskResultCandidate {
        id: format!("candidate_{suffix}"),
        task_id: task.id,
        run_id: run.id,
        task_run_turn_id: run_turn.id,
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        round: 0,
        status,
        result: Some(TaskResult {
            summary: Some("result".into()),
            data: None,
            artifacts: vec![],
            completed_by_run_id: None,
        }),
        extraction_error: None,
        summary: None,
        diagnostics: vec![],
        final_review_event_id: Some(format!("review_{suffix}")),
        created_at: at,
        updated_at: at,
        resolved_at: Some(at),
    }
}

async fn insert_history(store: &CrudStore, candidate: &TaskResultCandidate) {
    // Repository insertion models a supported snapshot restore that the probe
    // must reconcile; the normal CrudStore writer is exercised separately.
    task_result_candidate::upsert_candidate(
        &store.connection,
        task_result_candidate::prepare_protocol_candidate(candidate).unwrap(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn waiting_due_prefix_progress_restart_backoff_and_stale_probe_tokens() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let mut ids = vec![];
        for i in 0..19 {
            ids.push(
                f.effect(
                    &format!("waiting{i:02}"),
                    pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                    true,
                )
                .await,
            );
        }
        assert!(f.claim(NOW).await.is_empty());
        let mut reserved = vec![];
        for id in &ids {
            let row = f.row(id).await;
            if row.gate_probe_attempts == 1 {
                reserved.push(row);
            }
        }
        assert_eq!(reserved.len(), 8);
        assert!(reserved.iter().all(|r| r.gate_probe_at == NOW + 5
            && r.attempt_count == 0
            && r.status == "waiting_acceptance"));
        // Future rows leave the due prefix; another store after restart finds the next eight.
        let restarted = CrudStore::new(f.store.database_connection());
        assert!(
            restarted
                .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
                .await
                .unwrap()
                .records
                .is_empty()
        );
        assert_eq!(
            outbox::discover_gate_probes(&f.store.connection, NOW, 8)
                .await
                .unwrap()
                .len(),
            3
        );
        let old = reserved.remove(0);
        let new = outbox::reserve_gate_probe(
            &f.store.with_maintenance_access().connection,
            &old,
            Some("new-reservation".into()),
            &|| NOW + 5,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(new.gate_probe_at, NOW + 15);
        outbox::defer_gate_probe(&f.store.connection, &old, true, &|| NOW + 500)
            .await
            .unwrap();
        assert_eq!(
            f.row(&old.effect_id).await.gate_probe_token,
            new.gate_probe_token
        );
        assert_eq!(f.row(&old.effect_id).await.gate_probe_at, NOW + 15);
        // Normal candidate writes bypass backoff and clear the reservation token.
        let candidate = candidate_fixture(
            &f.store,
            &new.thread_id,
            &new.turn_id,
            "late",
            TaskResultCandidateStatus::Accepted,
            NOW + 6,
        )
        .await;
        f.store
            .upsert_task_result_candidate(candidate.clone())
            .await
            .unwrap();
        assert_eq!(f.row(&new.effect_id).await.status, "ready");
        assert_eq!(f.row(&new.effect_id).await.gate_probe_token, None);
        outbox::probe_waiting_gate(&f.store.connection, &old, &|| NOW + 7)
            .await
            .unwrap();
        outbox::defer_gate_probe(&f.store.connection, &new, true, &|| NOW + 500)
            .await
            .unwrap();
        assert_eq!(
            f.row(&new.effect_id).await.accepted_candidate_id.as_deref(),
            Some(candidate.id.as_str())
        );
        assert_eq!(f.row(&new.effect_id).await.status, "ready");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn task_event_batch_applies_candidate_gates_immediately_and_replay_is_idempotent() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "events",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        f.claim(NOW).await;
        let candidate = candidate_fixture(
            &f.store,
            "thr_events",
            "turn_events",
            "events",
            TaskResultCandidateStatus::Accepted,
            NOW + 1,
        )
        .await;
        let mut pending = candidate.clone();
        pending.status = TaskResultCandidateStatus::PendingReview;
        pending.resolved_at = None;
        pending.final_review_event_id = None;
        let events = vec![
            TaskEventPayload::TaskResultCandidateCreated { candidate: pending },
            TaskEventPayload::TaskResultCandidateAccepted {
                candidate: candidate.clone(),
                review_event_id: candidate.final_review_event_id.clone().unwrap(),
            },
        ];
        let appended = f
            .store
            .append_task_events_once(events.clone(), NOW + 1)
            .await
            .unwrap();
        assert!(
            appended
                .iter()
                .all(|event| event.append_status.is_inserted())
        );
        assert_eq!(f.row(&id).await.status, "ready");
        assert_eq!(f.row(&id).await.gate_probe_token, None);
        let replay = f.store.append_task_events(events, NOW + 2).await.unwrap();
        assert!(
            replay
                .iter()
                .all(|event| !event.append_status.is_inserted())
        );
        assert_eq!(
            f.row(&id).await.accepted_candidate_id.as_deref(),
            Some(candidate.id.as_str())
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn late_effect_uses_latest_rejection_and_superseding_review_before_terminal_commit() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let (_, _, mut turn) =
            start_test_turn(f.store.clone(), "ws-gates", "thr_late", "turn_late").await;
        let mut candidate = candidate_fixture(
            &f.store,
            "thr_late",
            "turn_late",
            "late_review",
            TaskResultCandidateStatus::Accepted,
            NOW - 5,
        )
        .await;
        f.store
            .upsert_task_result_candidate(candidate.clone())
            .await
            .unwrap();
        let review = TaskResultReviewEvent {
            id: "review_late_override".into(),
            candidate_id: candidate.id.clone(),
            task_id: candidate.task_id.clone(),
            run_id: candidate.run_id.clone(),
            task_run_turn_id: candidate.task_run_turn_id.clone(),
            reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
            reviewer: pioneer_protocol::TaskResultReviewerRef::RuntimePolicy,
            reviewer_thread_id: None,
            reviewer_turn_id: None,
            reviewer_user_id: None,
            reviewer_agent_spec_id: None,
            event_kind: TaskResultReviewEventKind::SystemAuto,
            decision: TaskResultReviewDecision::Reject,
            feedback_text: None,
            feedback: None,
            confidence: None,
            supersedes_review_event_id: candidate.final_review_event_id.clone(),
            next_task_run_turn_id: None,
            created_at: NOW - 1,
        };
        f.store
            .upsert_task_result_review_event(review.clone())
            .await
            .unwrap();
        candidate.status = TaskResultCandidateStatus::Rejected;
        candidate.final_review_event_id = Some(review.id);
        candidate.updated_at = NOW - 1;
        candidate.resolved_at = Some(NOW - 1);
        f.store
            .upsert_task_result_candidate(candidate)
            .await
            .unwrap();
        let effect_id = "turn_late:terminal-effect:post-turn";
        f.store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: "late_batch".into(),
                    workspace_id: "ws-gates".into(),
                    thread_id: "thr_late".into(),
                    turn_id: "turn_late".into(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.into(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                            request: serde_json::json!({}),
                            runtime_snapshot: serde_json::json!({}),
                        },
                        max_attempts: 3,
                    }],
                },
                NOW,
            )
            .await
            .unwrap();
        assert_eq!(f.row(effect_id).await.status, "prepared");
        turn.status = TurnStatus::Completed;
        f.store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: "ws-gates".into(),
                    thread_id: "thr_late".into(),
                    turn,
                },
                NOW,
            )
            .await
            .unwrap();
        assert_eq!(f.row(effect_id).await.status, "discarded");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn resolution_writer_uses_latest_terminal_and_event_cancellation_bypasses_backoff() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "resolution",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let mut pending = candidate_fixture(
            &f.store,
            "thr_resolution",
            "turn_resolution",
            "older",
            TaskResultCandidateStatus::PendingReview,
            NOW - 20,
        )
        .await;
        pending.final_review_event_id = None;
        pending.resolved_at = None;
        f.store
            .upsert_task_result_candidate(pending.clone())
            .await
            .unwrap();
        let rejected = candidate_fixture(
            &f.store,
            "thr_resolution",
            "turn_resolution",
            "newer",
            TaskResultCandidateStatus::Rejected,
            NOW + 20,
        )
        .await;
        insert_history(&f.store, &rejected).await;
        f.store
            .update_task_result_candidate_resolution(
                &pending.id,
                TaskResultCandidateStatus::Accepted,
                Some("review_older"),
                Some(NOW),
                NOW,
            )
            .await
            .unwrap();
        assert_eq!(f.row(&id).await.status, "discarded");
        assert_eq!(f.row(&id).await.accepted_candidate_id, None);

        let id = f
            .effect(
                "cancelled",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        f.claim(NOW).await;
        let mut candidate = candidate_fixture(
            &f.store,
            "thr_cancelled",
            "turn_cancelled",
            "cancelled",
            TaskResultCandidateStatus::PendingReview,
            NOW,
        )
        .await;
        candidate.final_review_event_id = None;
        candidate.resolved_at = None;
        f.store
            .upsert_task_result_candidate(candidate.clone())
            .await
            .unwrap();
        candidate.status = TaskResultCandidateStatus::Cancelled;
        candidate.updated_at = NOW + 1;
        candidate.resolved_at = Some(NOW + 1);
        candidate.final_review_event_id = Some("review_cancelled".into());
        f.store
            .append_task_event(
                TaskEventPayload::TaskResultCandidateCancelled {
                    candidate,
                    review_event_id: "review_cancelled".into(),
                },
                NOW + 1,
            )
            .await
            .unwrap();
        assert_eq!(f.row(&id).await.status, "discarded");
        assert_eq!(f.row(&id).await.gate_probe_token, None);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn legacy_run_completed_creates_or_uses_existing_candidate_and_resolves_during_backoff() {
    for existing in [false, true] {
        with_database(async |database| {
            let f = Fixture::open(database).await;
            let id = f
                .effect(
                    "legacy",
                    pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                    true,
                )
                .await;
            f.claim(NOW).await;
            let candidate = candidate_fixture(
                &f.store,
                "thr_legacy",
                "turn_legacy",
                "legacy",
                TaskResultCandidateStatus::Accepted,
                NOW,
            )
            .await;
            if existing {
                insert_history(&f.store, &candidate).await;
            }
            let event = TaskEventPayload::RunCompleted {
                task_id: candidate.task_id.clone(),
                run_id: candidate.run_id.clone(),
                result: candidate.result.clone(),
                completed_at: NOW + 1,
            };
            f.store
                .append_task_event(event.clone(), NOW + 1)
                .await
                .unwrap();
            assert_eq!(f.row(&id).await.status, "ready");
            assert_eq!(f.row(&id).await.gate_probe_token, None);
            let expected_id = if existing {
                candidate.id
            } else {
                format!("trc_{}", candidate.run_id)
            };
            assert_eq!(
                f.row(&id).await.accepted_candidate_id.as_deref(),
                Some(expected_id.as_str())
            );
            f.store.append_task_event(event, NOW + 2).await.unwrap();
            assert_eq!(
                f.row(&id).await.accepted_candidate_id.as_deref(),
                Some(expected_id.as_str())
            );
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn terminal_event_and_idempotent_resolution_fence_persisted_candidate_timestamp() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "timestamps",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let mut candidate = candidate_fixture(
            &f.store,
            "thr_timestamps",
            "turn_timestamps",
            "timestamps",
            TaskResultCandidateStatus::Accepted,
            NOW,
        )
        .await;
        candidate.resolved_at = Some(NOW + 1);
        f.store
            .append_task_event(
                TaskEventPayload::TaskResultCandidateAccepted {
                    candidate: candidate.clone(),
                    review_event_id: candidate.final_review_event_id.clone().unwrap(),
                },
                NOW + 1,
            )
            .await
            .unwrap();
        assert_eq!(f.row(&id).await.status, "ready");
        let stored = f
            .store
            .update_task_result_candidate_resolution(
                &candidate.id,
                TaskResultCandidateStatus::Accepted,
                candidate.final_review_event_id.as_deref(),
                candidate.resolved_at,
                NOW + 100,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.updated_at, NOW);
        assert_eq!(
            f.row(&id).await.accepted_candidate_id.as_deref(),
            Some(candidate.id.as_str())
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn accepted_event_without_resolution_time_uses_projector_event_timestamp() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "default_resolution",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let mut candidate = candidate_fixture(
            &f.store,
            "thr_default_resolution",
            "turn_default_resolution",
            "default_resolution",
            TaskResultCandidateStatus::PendingReview,
            NOW,
        )
        .await;
        candidate.resolved_at = None;
        candidate.final_review_event_id = None;
        let candidate_id = candidate.id.clone();
        f.store
            .append_task_event(
                TaskEventPayload::TaskResultCandidateAccepted {
                    candidate,
                    review_event_id: "review_default_resolution".into(),
                },
                NOW + 20,
            )
            .await
            .unwrap();
        assert_eq!(f.row(&id).await.status, "ready");
        assert_eq!(
            f.row(&id).await.accepted_candidate_id.as_deref(),
            Some(candidate_id.as_str())
        );
        let candidate =
            task_result_candidate::find_candidate_by_id(&f.store.connection, &candidate_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(candidate.updated_at, unix_to_datetime(NOW + 20));
        assert_eq!(candidate.resolved_at, Some(unix_to_datetime(NOW + 20)));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn legacy_completion_observes_parent_projected_earlier_in_atomic_batch() {
    with_database(async |database| {
        let f = Fixture::open(database).await;
        let id = f
            .effect(
                "legacy_batch",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        f.claim(NOW).await;
        let candidate = candidate_fixture(
            &f.store,
            "thr_legacy_batch",
            "turn_legacy_batch",
            "legacy_batch",
            TaskResultCandidateStatus::Accepted,
            NOW,
        )
        .await;
        let parent = pioneer_entity::task_run_turn::Entity::find_by_id(&candidate.task_run_turn_id)
            .one(&f.store.connection)
            .await
            .unwrap()
            .unwrap();
        let parent = pioneer_crud_tasks::task_run_turn::task_run_turn_from_db_model(parent).unwrap();
        // Leave real Task/run/Turn parents, then create their TaskRunTurn through A.
        // B must see A without serializing the legacy candidate under the writer.
        pioneer_entity::task_run_turn::Entity::delete_by_id(&parent.id)
            .exec(&f.store.connection)
            .await
            .unwrap();
        f.store
            .append_task_events_once(
                vec![
                    TaskEventPayload::TaskRunTurnCompleted {
                        task_run_turn: parent,
                    },
                    TaskEventPayload::RunCompleted {
                        task_id: candidate.task_id,
                        run_id: candidate.run_id.clone(),
                        result: candidate.result,
                        completed_at: NOW + 1,
                    },
                ],
                NOW + 1,
            )
            .await
            .unwrap();
        assert_eq!(f.row(&id).await.status, "ready");
        assert_eq!(
            f.row(&id).await.accepted_candidate_id,
            Some(format!("trc_{}", candidate.run_id))
        );
        assert_eq!(f.row(&id).await.gate_probe_token, None);
        Ok(())
    })
    .await
    .unwrap();
}
