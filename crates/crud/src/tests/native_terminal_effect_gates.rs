use super::*;
use crate::repositories::native_terminal_effect_outbox as outbox;
use crate::repositories::task_result_candidate;
use crate::{ClaimedNativeTerminalEffectRecord, DB_ID_LEN};
use pioneer_entity::native_terminal_effect_outbox as effect;
use pioneer_sqlite::SqliteDatabase;
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass, SqliteWriteEvent,
    SqliteWriteExecutor, SqliteWriteObserver, sqlite_read_only_connection_url,
};
use sea_orm::{ActiveModelTrait, IntoActiveModel};
use sea_orm::{ConnectOptions, Database, DatabaseBackend, QueryTrait};
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};
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
struct Fixture {
    store: CrudStore,
    routes: Arc<Routes>,
    _directory: tempfile::TempDir,
}
impl Fixture {
    async fn open() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("gates.sqlite");
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
        options
            .max_connections(1)
            .map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "ON"));
        let writer = Database::connect(options).await.unwrap();
        Migrator::up(&writer, None).await.unwrap();
        let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path));
        options.max_connections(4).map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
        let reader = Database::connect(options).await.unwrap();
        let routes = Arc::new(Routes::default());
        let db = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, routes.clone()),
            routes.clone(),
        );
        db.validate_reader().await.unwrap();
        let store = CrudStore::new(db);
        store.connection.execute_unprepared("INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws-gates','Gates',1,1)").await.unwrap();
        Self {
            store,
            routes,
            _directory: directory,
        }
    }
    async fn restart(&mut self) {
        self.store.database_connection().close().await.unwrap();
        let path = self._directory.path().join("gates.sqlite");
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
        options
            .max_connections(1)
            .map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "ON"));
        let writer = Database::connect(options).await.unwrap();
        let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path));
        options.max_connections(4).map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
        let reader = Database::connect(options).await.unwrap();
        self.store = CrudStore::new(SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, self.routes.clone()),
            self.routes.clone(),
        ));
        self.store.connection.validate_reader().await.unwrap();
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
async fn latest_terminal_candidate_all_statuses_ties_and_large_payloads() {
    for status in [
        TaskResultCandidateStatus::Rejected,
        TaskResultCandidateStatus::Superseded,
        TaskResultCandidateStatus::Cancelled,
    ] {
        let f = Fixture::open().await;
        let id = f
            .effect(
                "latest",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        let mut older = candidate_fixture(
            &f.store,
            "thr_latest",
            "turn_latest",
            "a",
            TaskResultCandidateStatus::Accepted,
            NOW - 20,
        )
        .await;
        older.summary = Some("x".repeat(256 * 1024));
        insert_history(&f.store, &older).await;
        let latest =
            candidate_fixture(&f.store, "thr_latest", "turn_latest", "z", status, NOW - 10).await;
        insert_history(&f.store, &latest).await;
        assert!(f.claim(NOW).await.is_empty());
        assert_eq!(f.row(&id).await.status, "discarded");
        assert_eq!(f.row(&id).await.accepted_candidate_id, None);
    }
    // Equal timestamps choose id DESC across statuses, then across accepted IDs.
    let f = Fixture::open().await;
    let id = f
        .effect(
            "tie",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    let rejected = candidate_fixture(
        &f.store,
        "thr_tie",
        "turn_tie",
        "a",
        TaskResultCandidateStatus::Rejected,
        NOW,
    )
    .await;
    let accepted = candidate_fixture(
        &f.store,
        "thr_tie",
        "turn_tie",
        "z",
        TaskResultCandidateStatus::Accepted,
        NOW,
    )
    .await;
    insert_history(&f.store, &rejected).await;
    insert_history(&f.store, &accepted).await;
    assert_eq!(f.claim(NOW).await.len(), 1);
    assert_eq!(
        f.row(&id).await.accepted_candidate_id.as_deref(),
        Some(accepted.id.as_str())
    );
}

#[tokio::test]
async fn terminal_metadata_ties_choose_greatest_id_within_each_status() {
    for status in [
        TaskResultCandidateStatus::Accepted,
        TaskResultCandidateStatus::Rejected,
        TaskResultCandidateStatus::Superseded,
        TaskResultCandidateStatus::Cancelled,
    ] {
        let f = Fixture::open().await;
        let id = f
            .effect(
                "same_status_tie",
                pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
                true,
            )
            .await;
        for suffix in ["z", "a"] {
            let candidate = candidate_fixture(
                &f.store,
                "thr_same_status_tie",
                "turn_same_status_tie",
                suffix,
                status,
                NOW,
            )
            .await;
            insert_history(&f.store, &candidate).await;
        }
        let metadata = outbox::candidate_metadata_seek(
            "thr_same_status_tie",
            "turn_same_status_tie",
            &crate::convention::task_result_candidate_status_to_db(status),
        )
        .into_model::<outbox::CandidateGateMetadata>()
        .one(&f.store.connection)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(metadata.id, "candidate_z");
        let records = f.claim(NOW).await;
        if status == TaskResultCandidateStatus::Accepted {
            assert_eq!(records.len(), 1);
            assert_eq!(
                f.row(&id).await.accepted_candidate_id.as_deref(),
                Some("candidate_z")
            );
        } else {
            assert!(records.is_empty());
            assert_eq!(f.row(&id).await.status, "discarded");
        }
    }
}

#[tokio::test]
async fn latest_gate_revalidation_rejects_newer_rejection_after_preparation() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "stale",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            false,
        )
        .await;
    let accepted = candidate_fixture(
        &f.store,
        "thr_stale",
        "turn_stale",
        "a",
        TaskResultCandidateStatus::Accepted,
        NOW,
    )
    .await;
    insert_history(&f.store, &accepted).await;
    let prepared = outbox::prepare_activation_for_terminal(&f.store.connection, "turn_stale")
        .await
        .unwrap();
    let rejected = candidate_fixture(
        &f.store,
        "thr_stale",
        "turn_stale",
        "z",
        TaskResultCandidateStatus::Rejected,
        NOW,
    )
    .await;
    insert_history(&f.store, &rejected).await;
    let tx = f.store.connection.begin().await.unwrap();
    assert!(
        outbox::activate_prepared_for_terminal(&tx, prepared, unix_to_datetime(NOW))
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    assert_eq!(f.row(&id).await.status, "prepared");
}

#[tokio::test]
async fn waiting_due_prefix_progress_restart_backoff_and_stale_probe_tokens() {
    let f = Fixture::open().await;
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
}

#[tokio::test]
async fn poison_gate_reservation_failure_is_deferred_and_healthy_rows_progress() {
    let f = Fixture::open().await;
    let poison = f
        .effect(
            "a_poison",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    let healthy = f
        .effect(
            "z_healthy",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_probe_once BEFORE UPDATE OF gate_probe_token ON native_terminal_effect_outbox WHEN NEW.gate_probe_token IS NOT NULL AND NEW.effect_id LIKE 'turn_a_poison:%' BEGIN SELECT RAISE(ABORT, 'probe failure'); END").await.unwrap();
    assert!(f.claim(NOW).await.is_empty());
    let poison_row = f.row(&poison).await;
    assert_eq!(poison_row.gate_probe_at, NOW + 5);
    assert_eq!(poison_row.gate_probe_token, None);
    assert_eq!(poison_row.attempt_count, 0);
    assert_eq!(f.row(&healthy).await.gate_probe_attempts, 1);
    f.store
        .connection
        .execute_unprepared("DROP TRIGGER fail_probe_once")
        .await
        .unwrap();
    let mut at = NOW + 5;
    for delay in [
        10, 20, 40, 80, 160, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300, 300,
    ] {
        let row = f.row(&poison).await;
        let next = outbox::reserve_gate_probe(
            &f.store.connection,
            &row,
            Some(generate_id(DB_ID_LEN)),
            &|| at,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(next.gate_probe_at, at + delay);
        assert!(next.gate_probe_attempts <= 16);
        assert_eq!(next.attempt_count, 0);
        at += delay;
    }
    assert_eq!(f.row(&poison).await.gate_probe_attempts, 16);
}

#[tokio::test]
async fn unavailable_probe_bookkeeping_reports_storage_error_after_other_rows_progress() {
    let f = Fixture::open().await;
    let poison = f
        .effect(
            "a_storage",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    let healthy = f
        .effect(
            "z_storage",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    let ready = f
        .effect(
            "storage_ready",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_probe_bookkeeping BEFORE UPDATE OF gate_probe_at ON native_terminal_effect_outbox WHEN NEW.effect_id LIKE 'turn_a_storage:%' BEGIN SELECT RAISE(ABORT, 'storage failure'); END").await.unwrap();
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(outcome.records.len(), 1);
    assert_eq!(outcome.records[0].effect_id, ready);
    assert_eq!(f.row(&poison).await.gate_probe_attempts, 0);
    assert_eq!(f.row(&healthy).await.gate_probe_attempts, 1);
    assert_eq!(f.row(&ready).await.status, "running");
    assert_eq!(f.row(&ready).await.attempt_count, 1);
    assert!(
        f.store
            .complete_native_terminal_effect(&ready, &outcome.records[0].claim_token, NOW + 1)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn task_event_batch_applies_candidate_gates_immediately_and_replay_is_idempotent() {
    let f = Fixture::open().await;
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
}

#[tokio::test]
async fn late_effect_uses_latest_rejection_and_superseding_review_before_terminal_commit() {
    let f = Fixture::open().await;
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
}

#[tokio::test]
async fn exhausted_final_running_lease_and_three_claim_pages_share_eight_inputs() {
    let f = Fixture::open().await;
    for status in ["ready", "retry_wait", "running"] {
        for i in 0..10 {
            let id = f
                .effect(
                    &format!("mixed_{status}_{i:02}"),
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await;
            let mut row = f.row(&id).await.into_active_model();
            row.status = Set(status.into());
            // Four exhausted rows in each due prefix must consume input slots.
            row.attempt_count = Set(if i < 4 { 3 } else { 1 });
            if status == "running" {
                row.claim_token = Set(Some(format!("old_{i}")));
                row.claim_expires_at = Set(Some(unix_to_datetime(NOW)));
            }
            row.update(&f.store.connection).await.unwrap();
        }
    }
    assert!(f.claim(NOW).await.is_empty());
    let unresolved = effect::Entity::find()
        .filter(effect::Column::Status.eq("unresolved"))
        .all(&f.store.connection)
        .await
        .unwrap();
    assert_eq!(unresolved.len(), 8);
    for status in ["ready", "retry_wait", "running"] {
        assert!(unresolved.iter().any(|r| r.effect_id.contains(status)));
    }
    assert!(unresolved.iter().all(
        |r| r.last_error_code.as_deref() == Some("retry_exhausted") && r.claim_token.is_none()
    ));
    let records = f.claim(NOW + 2).await;
    assert!(records.len() <= 8);
    assert!(!records.is_empty());
    assert!(records.iter().all(|r| r.attempt_count <= r.max_attempts));
    for row in &unresolved {
        assert!(
            !f.store
                .complete_native_terminal_effect(&row.effect_id, "old_0", NOW + 2)
                .await
                .unwrap()
        );
    }
}

#[tokio::test]
async fn total_quantum_is_sixteen_inputs_with_malformed_payload_and_checkpoint_progress() {
    let f = Fixture::open().await;
    for i in 0..10 {
        f.effect(
            &format!("probe_{i}"),
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    }
    for i in 0..10 {
        let id = f
            .effect(
                &format!("execute_{i}"),
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await;
        if i < 2 {
            let mut row = f.row(&id).await.into_active_model();
            if i == 0 {
                row.payload_json = Set("malformed".into());
            } else {
                row.handler_checkpoint_json = Set(Some("{}".into()));
                row.handler_checkpoint_sha256 = Set(Some("0".repeat(64)));
            }
            row.update(&f.store.connection).await.unwrap();
        }
    }
    let records = f.claim(NOW).await;
    assert!(!records.is_empty());
    assert!(records.len() <= 6);
    let rows = effect::Entity::find()
        .all(&f.store.connection)
        .await
        .unwrap();
    assert_eq!(rows.iter().filter(|r| r.gate_probe_attempts > 0).count(), 8);
    let execution_inputs = rows.iter().filter(|r| r.attempt_count > 0).count();
    assert!(execution_inputs <= 8);
    assert!(execution_inputs + rows.iter().filter(|r| r.gate_probe_attempts > 0).count() <= 16);
    assert_eq!(rows.iter().filter(|r| r.status == "unresolved").count(), 2);
    assert!(f.routes.reads.load(Ordering::SeqCst) > 0);
    assert!(f.routes.writes.load(Ordering::SeqCst) > 0);
    let db = f.store.with_maintenance_access().connection;
    assert_eq!(db.read_class(), SqliteReadClass::Maintenance);
    assert_eq!(db.write_class(), SqliteWriteClass::Maintenance);
    db.validate_reader().await.unwrap();
}

#[tokio::test]
async fn clock_is_read_after_writer_wait_and_cancellation_preserves_reservation() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "clock",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    let row = f.row(&id).await;
    let maintenance = f.store.with_maintenance_access().connection;
    let tx = f.store.connection.begin().await.unwrap();
    let now = Arc::new(AtomicI64::new(NOW));
    let queued_before = f.routes.queued.load(Ordering::SeqCst);
    let reservation = {
        let db = maintenance.clone();
        let row = row.clone();
        let now = now.clone();
        tokio::spawn(async move {
            outbox::reserve_gate_probe(&db, &row, Some("clock-token".into()), &|| {
                now.load(Ordering::SeqCst)
            })
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while f.routes.queued.load(Ordering::SeqCst) == queued_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    now.store(NOW + 100, Ordering::SeqCst);
    tx.commit().await.unwrap();
    let reserved = reservation.await.unwrap().unwrap().unwrap();
    assert_eq!(reserved.gate_probe_at, NOW + 105);
    // Cancellation after the durable reservation does not reset its deadline.
    let tx = f.store.connection.begin().await.unwrap();
    let probe = {
        let db = maintenance.clone();
        let reserved = reserved.clone();
        tokio::spawn(
            async move { outbox::defer_gate_probe(&db, &reserved, true, &|| NOW + 200).await },
        )
    };
    tokio::task::yield_now().await;
    probe.abort();
    assert!(probe.await.unwrap_err().is_cancelled());
    tx.rollback().await.unwrap();
    assert_eq!(f.row(&id).await.gate_probe_at, NOW + 105);
    let ready = f
        .effect(
            "lease_clock",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let tx = f.store.connection.begin().await.unwrap();
    let queued_before = f.routes.queued.load(Ordering::SeqCst);
    let claim = {
        let db = maintenance.clone();
        let now = now.clone();
        tokio::spawn(async move {
            outbox::claim_due(
                &db,
                unix_to_datetime(NOW),
                90,
                8,
                || generate_id(DB_ID_LEN),
                &|| now.load(Ordering::SeqCst),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while f.routes.queued.load(Ordering::SeqCst) == queued_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    now.store(NOW + 200, Ordering::SeqCst);
    tx.commit().await.unwrap();
    let claims = claim.await.unwrap().unwrap().claimed;
    assert_eq!(claims.len(), 1);
    assert_eq!(
        f.row(&ready).await.claim_expires_at,
        Some(unix_to_datetime(NOW + 290))
    );
}

#[tokio::test]
async fn planner_uses_four_covering_seeks_gate_partial_range_and_three_claim_ranges() {
    let f = Fixture::open().await;
    let db = f.store.with_maintenance_access().connection;
    for status in ["accepted", "rejected", "superseded", "cancelled"] {
        let statement = outbox::candidate_metadata_seek("thread", "turn", status)
            .build(DatabaseBackend::Sqlite);
        assert!(!statement.sql.contains("result_json"));
        assert!(!statement.sql.contains("diagnostics_json"));
        assert!(!statement.sql.contains("summary"));
        assert!(statement.sql.contains("LIMIT"));
        assert_plan(
            &db,
            statement,
            "COVERING INDEX idx_task_result_candidate_gate",
            &["thread_id=?", "turn_id=?", "status=?"],
        )
        .await;
    }
    assert_plan(
        &db,
        outbox::gate_due_page(NOW, 8).build(DatabaseBackend::Sqlite),
        "idx_native_terminal_effect_gate_due",
        &["gate_probe_at<?"],
    )
    .await;
    for status in ["ready", "retry_wait", "running"] {
        let statement =
            outbox::claim_page(status, unix_to_datetime(NOW), 3).build(DatabaseBackend::Sqlite);
        assert!(!statement.sql.contains("attempt_count\" <"));
        let (index, range) = if status == "running" {
            ("idx_native_terminal_effect_expired", "claim_expires_at<?")
        } else {
            ("idx_native_terminal_effect_due", "next_run_at<?")
        };
        assert_plan(&db, statement, index, &["status=?", range]).await;
    }
}
async fn assert_plan(
    db: &SqliteDatabase,
    mut statement: sea_orm::Statement,
    index: &str,
    keys: &[&str],
) {
    statement.sql = format!("EXPLAIN QUERY PLAN {}", statement.sql);
    let details = db
        .query_all_raw(statement)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.try_get::<String>("", "detail").unwrap())
        .collect::<Vec<_>>();
    assert!(
        details.iter().any(|d| d.contains("SEARCH")
            && d.contains(index)
            && keys.iter().all(|key| d.contains(key))),
        "{details:?}"
    );
    assert!(
        !details
            .iter()
            .any(|d| d.contains("USE TEMP B-TREE") || d.contains("SCAN ")),
        "{details:?}"
    );
}

#[tokio::test]
async fn resolution_writer_uses_latest_terminal_and_event_cancellation_bypasses_backoff() {
    let f = Fixture::open().await;
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
}

#[tokio::test]
async fn point_claim_failure_preserves_healthy_claims_before_and_after_poison() {
    let f = Fixture::open().await;
    let mut ids = Vec::new();
    for suffix in ["a_claim", "b_claim", "c_claim"] {
        ids.push(
            f.effect(
                suffix,
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await,
        );
    }
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_second_claim BEFORE UPDATE OF claim_token ON native_terminal_effect_outbox WHEN NEW.effect_id LIKE 'turn_b_claim:%' AND NEW.status = 'running' BEGIN SELECT RAISE(ABORT, 'claim failure'); END").await.unwrap();
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(
        outcome
            .records
            .iter()
            .map(|record| &record.effect_id)
            .collect::<Vec<_>>(),
        vec![&ids[0], &ids[2]]
    );
    assert_eq!(f.row(&ids[0]).await.status, "running");
    assert_eq!(f.row(&ids[0]).await.attempt_count, 1);
    assert_eq!(f.row(&ids[1]).await.status, "ready");
    assert_eq!(f.row(&ids[1]).await.attempt_count, 0);
    assert_eq!(
        f.row(&ids[1]).await.next_run_at,
        Some(unix_to_datetime(NOW + 5))
    );
    // The fault is permanent; a later quantum can still confirm other rows.
    assert!(f.claim(NOW + 1).await.is_empty());
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW + 5, 10, 8, &|| NOW + 5)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert!(outcome.records.is_empty());
    assert_eq!(f.row(&ids[1]).await.attempt_count, 0);
}

#[test]
fn preparation_keeps_effect_count_and_payload_byte_bounds() {
    let spec = pioneer_protocol::NativeTerminalEffectSpec {
        effect_id: "bound-effect".into(),
        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
        gate: pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
            request: serde_json::json!({}),
            runtime_snapshot: serde_json::json!({}),
        },
        max_attempts: 3,
    };
    let mut preparation = pioneer_protocol::NativeTerminalEffectPreparation {
        batch_id: "bound-batch".into(),
        workspace_id: "ws-gates".into(),
        thread_id: "bound-thread".into(),
        turn_id: "bound-turn".into(),
        runtime_generation: 1,
        effects: vec![spec.clone(); outbox::MAX_EFFECTS_PER_TURN + 1],
    };
    assert!(outbox::prepare_input(preparation.clone()).is_err());
    preparation.effects = vec![spec];
    preparation.effects[0].payload = pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
        request: serde_json::json!({"large":"x".repeat(outbox::MAX_EFFECT_PAYLOAD_BYTES + 1)}),
        runtime_snapshot: serde_json::json!({}),
    };
    assert!(outbox::prepare_input(preparation).is_err());
}

#[tokio::test]
async fn failed_claim_commit_never_returns_dispatchable_effects() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "commit_failure",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    // Mutation/reload succeed; the deferred FK makes the commit fail. The
    // API must never return the records prepared before this failed commit.
    f.store.connection.execute_unprepared("CREATE TABLE claim_commit_check (workspace_id TEXT REFERENCES workspace(id) DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_claim_commit AFTER UPDATE OF claim_token ON native_terminal_effect_outbox WHEN NEW.status = 'running' BEGIN INSERT INTO claim_commit_check VALUES('missing-workspace'); END").await.unwrap();
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert!(outcome.records.is_empty());
    assert_eq!(f.row(&id).await.status, "ready");
    assert_eq!(f.row(&id).await.attempt_count, 0);
    f.store
        .connection
        .execute_unprepared("DROP TRIGGER fail_claim_commit")
        .await
        .unwrap();
    assert_eq!(f.claim(NOW + 5).await.len(), 1);
}

#[tokio::test]
async fn legacy_run_completed_creates_or_uses_existing_candidate_and_resolves_during_backoff() {
    for existing in [false, true] {
        let f = Fixture::open().await;
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
    }
}

#[tokio::test]
async fn terminal_event_and_idempotent_resolution_fence_persisted_candidate_timestamp() {
    let f = Fixture::open().await;
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
}

#[tokio::test]
async fn accepted_event_without_resolution_time_uses_projector_event_timestamp() {
    let f = Fixture::open().await;
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
    let candidate = task_result_candidate::find_candidate_by_id(&f.store.connection, &candidate_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(candidate.updated_at, unix_to_datetime(NOW + 20));
    assert_eq!(candidate.resolved_at, Some(unix_to_datetime(NOW + 20)));
}

#[tokio::test]
async fn legacy_completion_observes_parent_projected_earlier_in_atomic_batch() {
    let f = Fixture::open().await;
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
    let parent = crate::task_run_turn_from_db_model(parent).unwrap();
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
}

#[tokio::test]
async fn permanent_failed_reservations_back_off_across_restart_without_double_increment() {
    let mut f = Fixture::open().await;
    let id = f
        .effect(
            "permanent_probe",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_every_reservation BEFORE UPDATE OF gate_probe_token ON native_terminal_effect_outbox WHEN NEW.gate_probe_token IS NOT NULL BEGIN SELECT RAISE(ABORT, 'reservation failure'); END").await.unwrap();
    let mut now = NOW;
    for attempt in 1..=18 {
        if attempt == 4 {
            f.restart().await;
        }
        let outcome = f
            .store
            .claim_due_native_terminal_effects_with_clock(now, 10, 8, &|| now)
            .await
            .unwrap();
        assert!(outcome.storage_failed);
        assert!(outcome.records.is_empty());
        let row = f.row(&id).await;
        let delay = [5, 10, 20, 40, 80, 160, 300][usize::min(attempt - 1, 6)];
        assert_eq!(
            row.gate_probe_attempts,
            i64::try_from(usize::min(attempt, 16)).unwrap()
        );
        assert_eq!(row.gate_probe_at, now + delay);
        assert_eq!(row.gate_probe_token, None);
        assert_eq!(row.attempt_count, 0);
        now += delay;
    }
    f.store
        .connection
        .execute_unprepared("DROP TRIGGER fail_every_reservation")
        .await
        .unwrap();
    let before = f.row(&id).await;
    let reserved = outbox::reserve_gate_probe(
        &f.store.connection,
        &before,
        Some("committed-probe".into()),
        &|| now,
    )
    .await
    .unwrap()
    .unwrap();
    outbox::defer_gate_probe(&f.store.connection, &reserved, true, &|| now + 10)
        .await
        .unwrap();
    assert_eq!(
        f.row(&id).await.gate_probe_attempts,
        reserved.gate_probe_attempts
    );
    outbox::defer_gate_probe(&f.store.connection, &before, false, &|| now + 1000)
        .await
        .unwrap();
    assert_eq!(f.row(&id).await.gate_probe_token, reserved.gate_probe_token);
    assert_eq!(f.row(&id).await.gate_probe_attempts, 16);
}

#[tokio::test]
async fn committed_probe_failure_deferral_does_not_increment_again() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "committed_failure",
            pioneer_protocol::NativeTerminalEffectGate::AcceptedTaskResult,
            true,
        )
        .await;
    let candidate = candidate_fixture(
        &f.store,
        "thr_committed_failure",
        "turn_committed_failure",
        "committed_failure",
        TaskResultCandidateStatus::Accepted,
        NOW,
    )
    .await;
    insert_history(&f.store, &candidate).await;
    // Reservation commits successfully; only the subsequent gate resolution
    // fails. Conditional deferral must keep its already incremented counter.
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_probe_resolution BEFORE UPDATE OF status ON native_terminal_effect_outbox WHEN OLD.status='waiting_acceptance' AND NEW.status='ready' BEGIN SELECT RAISE(ABORT,'probe resolution failure'); END").await.unwrap();
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(f.row(&id).await.gate_probe_attempts, 1);
    assert_eq!(f.row(&id).await.gate_probe_at, NOW + 5);
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW + 5, 10, 8, &|| NOW + 5)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(f.row(&id).await.gate_probe_attempts, 2);
    assert_eq!(f.row(&id).await.gate_probe_at, NOW + 15);
    assert_eq!(f.row(&id).await.attempt_count, 0);
}

#[tokio::test]
async fn failed_prefix_claim_uses_one_input_and_does_not_hide_large_backlog() {
    let f = Fixture::open().await;
    let mut ids = Vec::new();
    for i in 0..12 {
        ids.push(
            f.effect(
                &format!("prefix_{i:02}"),
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await,
        );
    }
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_prefix BEFORE UPDATE OF claim_token ON native_terminal_effect_outbox WHEN NEW.effect_id LIKE 'turn_prefix_00:%' BEGIN SELECT RAISE(ABORT, 'claim failure'); END").await.unwrap();
    // Rotation puts ready last; the two empty pages donate all eight slots.
    let at = NOW + 4;
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(at, 10, 8, &|| at)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(outcome.records.len(), 7);
    assert_eq!(f.row(&ids[0]).await.attempt_count, 0);
    assert_eq!(
        f.row(&ids[0]).await.next_run_at,
        Some(unix_to_datetime(at + 5))
    );
    for id in &ids[8..] {
        assert_eq!(f.row(id).await.attempt_count, 0);
    }
    let second = f
        .store
        .claim_due_native_terminal_effects_with_clock(at, 10, 8, &|| at)
        .await
        .unwrap();
    assert!(!second.storage_failed);
    assert_eq!(second.records.len(), 4);
}

#[tokio::test]
async fn exhausted_transition_failure_and_unavailable_deferral_preserve_other_claims() {
    let f = Fixture::open().await;
    let poison = f
        .effect(
            "a_exhausted",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let healthy = f
        .effect(
            "z_after_exhausted",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let mut row = f.row(&poison).await.into_active_model();
    row.attempt_count = Set(3);
    row.update(&f.store.connection).await.unwrap();
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_exhausted BEFORE UPDATE OF status ON native_terminal_effect_outbox WHEN NEW.effect_id LIKE 'turn_a_exhausted:%' AND NEW.status = 'unresolved' BEGIN SELECT RAISE(ABORT, 'exhausted failure'); END").await.unwrap();
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(outcome.records.len(), 1);
    assert_eq!(outcome.records[0].effect_id, healthy);
    assert_eq!(f.row(&poison).await.status, "ready");
    assert_eq!(f.row(&poison).await.attempt_count, 3);
    assert_eq!(
        f.row(&poison).await.next_run_at,
        Some(unix_to_datetime(NOW + 5))
    );
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_execution_deferral BEFORE UPDATE OF next_run_at ON native_terminal_effect_outbox WHEN NEW.effect_id LIKE 'turn_a_exhausted:%' BEGIN SELECT RAISE(ABORT, 'deferral failure'); END").await.unwrap();
    let next = f
        .effect(
            "healthy_next",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW + 5, 10, 8, &|| NOW + 5)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(outcome.records.len(), 1);
    assert_eq!(outcome.records[0].effect_id, next);
    assert_eq!(f.row(&poison).await.attempt_count, 3);
}

#[tokio::test]
async fn execution_failure_deferral_is_fenced_against_claim_completion_and_new_owner() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "stale_execution",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let before = f.row(&id).await;
    let first = f.claim(NOW).await.pop().unwrap();
    assert!(
        !outbox::defer_execution_claim(&f.store.connection, &before, &|| NOW + 100)
            .await
            .unwrap()
    );
    let running = f.row(&id).await;
    let second = f.claim(NOW + 10).await.pop().unwrap();
    let current = f.row(&id).await;
    assert!(
        !outbox::defer_execution_claim(&f.store.connection, &running, &|| NOW + 200)
            .await
            .unwrap()
    );
    assert_eq!(f.row(&id).await.claim_token, current.claim_token);
    assert_eq!(f.row(&id).await.claim_expires_at, current.claim_expires_at);
    assert!(
        !f.store
            .complete_native_terminal_effect(&id, &first.claim_token, NOW + 11)
            .await
            .unwrap()
    );
    assert!(
        f.store
            .complete_native_terminal_effect(&id, &second.claim_token, NOW + 11)
            .await
            .unwrap()
    );
    assert!(
        !outbox::defer_execution_claim(&f.store.connection, &current, &|| NOW + 300)
            .await
            .unwrap()
    );
    assert_eq!(f.row(&id).await.status, "succeeded");
    assert_eq!(f.row(&id).await.claim_token, None);
}

#[tokio::test]
async fn failed_commit_of_one_claim_keeps_independently_confirmed_neighbors() {
    let f = Fixture::open().await;
    let mut ids = Vec::new();
    for suffix in ["a_confirmed", "b_commit_error", "c_confirmed"] {
        ids.push(
            f.effect(
                suffix,
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await,
        );
    }
    f.store.connection.execute_unprepared("CREATE TABLE point_commit_check (workspace_id TEXT REFERENCES workspace(id) DEFERRABLE INITIALLY DEFERRED)").await.unwrap();
    f.store.connection.execute_unprepared("CREATE TRIGGER fail_point_commit AFTER UPDATE OF claim_token ON native_terminal_effect_outbox WHEN NEW.effect_id LIKE 'turn_b_commit_error:%' AND NEW.status = 'running' BEGIN INSERT INTO point_commit_check VALUES('missing-workspace'); END").await.unwrap();
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(
        outcome
            .records
            .iter()
            .map(|record| &record.effect_id)
            .collect::<Vec<_>>(),
        vec![&ids[0], &ids[2]]
    );
    assert_eq!(f.row(&ids[1]).await.status, "ready");
    assert_eq!(f.row(&ids[1]).await.attempt_count, 0);
    assert_eq!(
        f.row(&ids[1]).await.next_run_at,
        Some(unix_to_datetime(NOW + 5))
    );
    assert!(f.row(&ids[0]).await.claim_token.is_some());
    assert!(f.row(&ids[2]).await.claim_token.is_some());
}

#[tokio::test]
async fn expired_running_failure_deferral_reads_clock_after_writer_wait_and_preserves_owner() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "running_deferral",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let claim = f.claim(NOW).await.pop().unwrap();
    let snapshot = f.row(&id).await;
    let tx = f.store.connection.begin().await.unwrap();
    let queued_before = f.routes.queued.load(Ordering::SeqCst);
    let clock = Arc::new(AtomicI64::new(NOW + 10));
    let task = {
        let db = f.store.with_maintenance_access().connection;
        let snapshot = snapshot.clone();
        let clock = clock.clone();
        tokio::spawn(async move {
            outbox::defer_execution_claim(&db, &snapshot, &|| clock.load(Ordering::SeqCst)).await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while f.routes.queued.load(Ordering::SeqCst) == queued_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    clock.store(NOW + 100, Ordering::SeqCst);
    tx.commit().await.unwrap();
    assert!(task.await.unwrap().unwrap());
    let deferred = f.row(&id).await;
    assert_eq!(deferred.claim_expires_at, Some(unix_to_datetime(NOW + 105)));
    assert_eq!(
        deferred.claim_token.as_deref(),
        Some(claim.claim_token.as_str())
    );
    assert_eq!(deferred.attempt_count, 1);
    assert_eq!(deferred.updated_at, snapshot.updated_at);
    assert_eq!(deferred.next_run_at, snapshot.next_run_at);
    assert!(f.claim(NOW + 104).await.is_empty());
    // Deferring recovery does not revoke the handler's existing ownership.
    assert!(
        f.store
            .complete_native_terminal_effect(&id, &claim.claim_token, NOW + 104)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn lost_claim_acknowledgement_does_not_allow_snapshot_repair_or_redispatch() {
    let f = Fixture::open().await;
    let id = f
        .effect(
            "a_lost_ack",
            pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
            true,
        )
        .await;
    let snapshot = f.row(&id).await;
    // Model the durable side of an ambiguous commit: the row committed, but
    // its acknowledgement/record was lost before dispatch. No mock writer or
    // alternative production claim path is needed to exercise recovery CAS.
    let confirmed = outbox::claim_due(
        &f.store.with_maintenance_access().connection,
        unix_to_datetime(NOW),
        10,
        1,
        || "lost-ack-token".into(),
        &|| NOW,
    )
    .await
    .unwrap();
    assert_eq!(confirmed.claimed.len(), 1);
    drop(confirmed);
    let committed = f.row(&id).await;
    assert!(
        !outbox::defer_execution_claim(&f.store.connection, &snapshot, &|| NOW + 100)
            .await
            .unwrap()
    );
    assert_eq!(f.row(&id).await.claim_token, committed.claim_token);
    assert_eq!(
        f.row(&id).await.claim_expires_at,
        committed.claim_expires_at
    );
    assert_eq!(f.row(&id).await.attempt_count, 1);
    let mut healthy = Vec::new();
    for suffix in ["b_after_lost_ack", "c_after_lost_ack"] {
        healthy.push(
            f.effect(
                suffix,
                pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                true,
            )
            .await,
        );
    }
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(NOW, 10, 8, &|| NOW)
        .await
        .unwrap();
    assert!(!outcome.storage_failed);
    assert_eq!(
        outcome
            .records
            .iter()
            .map(|record| &record.effect_id)
            .collect::<Vec<_>>(),
        healthy.iter().collect::<Vec<_>>()
    );
    assert!(outcome.records.iter().all(|record| record.effect_id != id));
    assert_eq!(f.row(&id).await.claim_token, committed.claim_token);
    assert_eq!(f.row(&id).await.attempt_count, 1);
}

#[tokio::test]
async fn failed_claim_discovery_page_keeps_its_input_quota_and_other_statuses_progress() {
    let f = Fixture::open().await;
    for status in ["ready", "retry_wait", "running"] {
        for i in 0..8 {
            let id = f
                .effect(
                    &format!("discovery_{status}_{i:02}"),
                    pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                    true,
                )
                .await;
            let mut row = f.row(&id).await.into_active_model();
            row.status = Set(status.into());
            if status == "running" {
                row.attempt_count = Set(1);
                row.claim_token = Set(Some(format!("discovery-owner-{i}")));
                row.claim_expires_at = Set(Some(unix_to_datetime(NOW)));
            }
            row.update(&f.store.connection).await.unwrap();
        }
    }
    // SQL returns three ready inputs, but typed decoding cannot return the
    // page. Its full quota still counts, leaving only five independent inputs.
    f.store.connection.execute_unprepared("UPDATE native_terminal_effect_outbox SET updated_at='invalid-time' WHERE effect_id LIKE 'turn_discovery_ready_00:%'").await.unwrap();
    let at = NOW + 2; // ready first, then retry_wait, then running
    let outcome = f
        .store
        .claim_due_native_terminal_effects_with_clock(at, 10, 8, &|| at)
        .await
        .unwrap();
    assert!(outcome.storage_failed);
    assert_eq!(outcome.records.len(), 5);
    assert_eq!(
        outcome
            .records
            .iter()
            .filter(|record| record.effect_id.contains("retry_wait"))
            .count(),
        3
    );
    assert_eq!(
        outcome
            .records
            .iter()
            .filter(|record| record.effect_id.contains("running"))
            .count(),
        2
    );
    assert!(
        outcome
            .records
            .iter()
            .all(|record| !record.effect_id.contains("ready"))
    );
}
