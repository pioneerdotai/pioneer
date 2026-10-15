//! New foreground executions reuse only compatible, durable compaction progress.
use super::*;
use pioneer_compaction::runner::{AttemptPurpose, SourceCursor};

async fn progress_fixture(correction: bool) -> (Fixture, PreparedOperation) {
    let text = if correction {
        "small".into()
    } else {
        "漢字🌍".repeat(4_000)
    };
    let replies = if correction {
        vec![Reply::Success, Reply::InvalidCompletion]
    } else {
        vec![Reply::Success, Reply::Success, Reply::InvalidCompletion]
    };
    let f = fixture(&text, replies, !correction, false).await;
    let manifest = f
        .store
        .compaction_manifest_page("operation", false, 0, 0)
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert!(
        !history.is_empty(),
        "the frozen input must prove the selected sources"
    );
    let projection = super::super::frozen::capture(
        &f.store,
        "ws",
        "thread",
        &std::collections::BTreeSet::from(["thread".into()]),
        &history,
    )
    .await
    .unwrap();
    let version = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    let prepared = PreparedOperation {
        owner: "resume-owner".into(),
        execution_turn: "turn".into(),
        user_initiated: true,
        source_projection: Some(projection),
        expected_checkpoint: None,
        summary_basis: None,
        operation_deadline_ms: None,
        projection_version: version,
        source_epochs: std::collections::BTreeMap::from([("thread".into(), version)]),
        plan: f.runner.snapshot.plan.clone(),
        manifest,
        target_identity: "resume-target".into(),
        target_tokens: 500,
    };
    (f, prepared)
}

async fn failed_progress(
    correction: bool,
) -> (Fixture, PreparedOperation, OperationSnapshot, RunnerState) {
    failed_progress_with_status(correction, "failed").await
}

async fn failed_progress_with_status(
    correction: bool,
    old_status: &str,
) -> (Fixture, PreparedOperation, OperationSnapshot, RunnerState) {
    let (f, prepared) = progress_fixture(correction).await;
    let snapshot = admit(&f, prepared.clone(), 10).await.unwrap();
    let runner = runner(&f, snapshot.clone(), !correction, "turn");
    assert_eq!(
        runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::InvalidCompletion)
    );
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    let state = f
        .store
        .compaction_runner_state(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.cursor > SourceCursor::default());
    assert!(state.previous_checkpoint.is_some());
    assert_eq!(state.attempts, if correction { 2 } else { 3 });
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn SET status=? WHERE id='turn'",
            [old_status.into()],
        ))
        .await
        .unwrap();
    f.store.database_connection().execute_unprepared(
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at)
         VALUES ('new-turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    (f, prepared, snapshot, state)
}

async fn admit(f: &Fixture, prepared: PreparedOperation, now: u64) -> Result<OperationSnapshot> {
    admit_operation(
        &f.store,
        "ws",
        "thread",
        &CompactionSettings::default(),
        &f.runner.snapshot.admission.selection,
        None,
        f.runner.summarizer.as_ref(),
        prepared,
        now,
    )
    .await
}

fn runner(f: &Fixture, snapshot: OperationSnapshot, fits: bool, turn: &str) -> CompactionRunner {
    CompactionRunner::new(
        f.store.clone(),
        "ws".into(),
        "thread".into(),
        snapshot,
        f.runner.summarizer.clone(),
        Arc::new(Target(fits)),
        f.observer.clone(),
        f.clock.clone(),
    )
    .with_execution_turn(turn.into())
}

#[tokio::test]
async fn new_user_admission_resumes_only_unfinished_portion_and_preserves_chain() {
    for (correction, same_deadline, old_status) in [
        (false, false, "failed"),
        (true, false, "failed"),
        (false, true, "completed"),
        // CLI delivery-plan compilation leaves the old turn Blocked.
        (false, false, "blocked"),
        (true, false, "blocked"),
    ] {
        let (f, mut prepared, snapshot, before) =
            failed_progress_with_status(correction, old_status).await;
        let checkpoint = f
            .store
            .compaction_checkpoint(before.previous_checkpoint.as_deref().unwrap())
            .await
            .unwrap()
            .unwrap();
        if !correction {
            assert!(
                before.cursor.character > 0,
                "exercise an intra-source cursor"
            );
            assert!(
                checkpoint.previous.is_some(),
                "several accepted checkpoints must survive"
            );
        }
        // A process restart / background polling / same-turn admission has no new budget.
        assert_eq!(
            runner(&f, snapshot.clone(), true, "turn")
                .run(CancellationToken::new())
                .await
                .unwrap(),
            CompactionExit::Failed(FailureKind::InvalidCompletion)
        );
        prepared.user_initiated = false;
        prepared.execution_turn = "new-turn".into();
        assert_eq!(
            serde_json::to_value(admit(&f, prepared.clone(), 20).await.unwrap()).unwrap(),
            serde_json::to_value(&snapshot).unwrap()
        );
        let mut incompatible_background = prepared.clone();
        incompatible_background.target_identity = "changed-background-target".into();
        assert!(
            admit(&f, incompatible_background, 20)
                .await
                .unwrap_err()
                .to_string()
                .contains("plan changed")
        );
        prepared.user_initiated = true;
        prepared.execution_turn = "turn".into();
        assert!(
            admit(&f, prepared.clone(), 20)
                .await
                .unwrap_err()
                .to_string()
                .contains("new user execution")
        );
        assert_eq!(
            f.store
                .compaction_runner_state(&snapshot.id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
        prepared.execution_turn = "new-turn".into();
        // The request owns this budget; it may even be shorter than the old request's budget.
        prepared.operation_deadline_ms =
            Some(before.deadline_ms - if same_deadline { 0 } else { 100 });
        let resumed = admit(&f, prepared, 20).await.unwrap();
        assert_eq!(resumed.id, snapshot.id);
        let expected_deadline = before.deadline_ms - if same_deadline { 0 } else { 100 };
        assert_eq!(resumed.admission.deadline_ms, expected_deadline);
        let after = f
            .store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.cursor, before.cursor);
        assert_eq!(after.previous_checkpoint, before.previous_checkpoint);
        assert_eq!(after.deadline_ms, expected_deadline);
        assert_eq!(
            (
                after.attempts,
                after.retries,
                after.corrections,
                after.target_tokens
            ),
            (
                before.attempts,
                before.retries,
                before.corrections,
                before.target_tokens
            )
        );
        assert_eq!(
            after.phase,
            RunnerPhase::Ready {
                purpose: if correction {
                    AttemptPurpose::Correction
                } else {
                    AttemptPurpose::Portion
                }
            }
        );
        // Late calls and reconciliation from the old request cannot execute or stop the new one.
        let stale = runner(&f, snapshot.clone(), true, "turn");
        let starts = f.observer.ids.lock().unwrap().len();
        assert!(stale.run(CancellationToken::new()).await.is_err());
        assert!(stale.reconcile(FailureKind::Deadline).await.is_err());
        assert_eq!(f.observer.ids.lock().unwrap().len(), starts);
        assert_eq!(
            f.store
                .compaction_operation(&snapshot.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "running"
        );
        assert!(matches!(
            runner(&f, resumed, true, "new-turn")
                .run(CancellationToken::new())
                .await
                .unwrap(),
            CompactionExit::Applied(_)
        ));
        let calls = f.provider.calls.lock().unwrap();
        let failed = before.attempts as usize - 1;
        let failed_input: SummaryInput =
            serde_json::from_str(&calls[failed].messages[1].content).unwrap();
        let resumed_input: SummaryInput =
            serde_json::from_str(&calls[failed + 1].messages[1].content).unwrap();
        assert_eq!(
            serde_json::to_value(&failed_input).unwrap(),
            serde_json::to_value(&resumed_input).unwrap()
        );
        assert_eq!(resumed_input.previous_summary, checkpoint.summary);
        assert_eq!(resumed_input.compact_units.is_empty(), correction);
        drop(calls);
        let next = f
            .store
            .compaction_checkpoint(&format!("{}:summary:{}", snapshot.id, before.attempts + 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(next.previous, before.previous_checkpoint);
        let saved_status = pioneer_entity::turn::Entity::find_by_id("turn")
            .one(&f.store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .status;
        assert_eq!(saved_status, old_status);
        let observations = f.store.database_connection().query_all_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "SELECT observation FROM compaction_attempt_observation WHERE operation_id=? ORDER BY attempt", [snapshot.id.into()])).await.unwrap();
        assert!(observations.len() > before.attempts as usize);
        let failed_observation: pioneer_compaction::runner::AttemptObservation =
            serde_json::from_str(
                &observations[failed]
                    .try_get::<String>("", "observation")
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            failed_observation.failure,
            Some(FailureKind::InvalidCompletion)
        );
    }
}

#[tokio::test]
async fn invalid_completion_resume_refuses_incompatible_or_missing_evidence_without_mutation() {
    for case in [
        "plan",
        "source",
        "checkpoint",
        "snapshot",
        "missing-binding",
        "manifest",
        "legacy-correction",
        "stop",
        "head",
        "old-active",
        "old-interrupted",
        "blocked-stop",
        "blocked-deleted",
        "system-turn",
        "finished-turn",
    ] {
        let (f, mut prepared, snapshot, before) =
            failed_progress(case == "legacy-correction").await;
        prepared.execution_turn = "new-turn".into();
        let (sql, expected) = match case {
            "plan" => {
                prepared.target_identity = "changed".into();
                (None, "plan changed")
            }
            "source" => (
                Some("UPDATE turn_event SET payload='{}' WHERE id='source'"),
                "sources changed",
            ),
            "checkpoint" => (
                // Closed checkpoints are immutable. Model a missing reference
                // in saved runner state without deleting retained evidence.
                Some(
                    "UPDATE compaction_runner_state SET state=json_set(state,'$.previous_checkpoint','missing-checkpoint') WHERE operation_id IN (SELECT id FROM compaction_operation WHERE owner='resume-owner')",
                ),
                "checkpoint is unavailable",
            ),
            "snapshot" => (
                Some(
                    "UPDATE compaction_frozen_history SET expired=1 WHERE id IN (SELECT manifest_id FROM compaction_operation_projection WHERE operation_id IN (SELECT id FROM compaction_operation WHERE owner='resume-owner'))",
                ),
                "snapshot is unavailable",
            ),
            "missing-binding" => (
                Some(
                    "DELETE FROM compaction_operation_projection WHERE operation_id IN (SELECT id FROM compaction_operation WHERE owner='resume-owner')",
                ),
                "snapshot is unavailable",
            ),
            "manifest" => (
                Some(
                    "DELETE FROM compaction_manifest WHERE operation_id IN (SELECT id FROM compaction_operation WHERE owner='resume-owner')",
                ),
                "plan is unavailable",
            ),
            "legacy-correction" => (
                Some(
                    "UPDATE compaction_runner_state SET state=json_remove(state,'$.resume_phase') WHERE operation_id IN (SELECT id FROM compaction_operation WHERE owner='resume-owner')",
                ),
                "phase is unsafe",
            ),
            "stop" => (
                Some(
                    "INSERT INTO compaction_execution_stop(owner,turn_id) VALUES ('resume-owner','turn')",
                ),
                "fence changed",
            ),
            "head" => (
                Some("UPDATE compaction_context SET head='other' WHERE owner='resume-owner'"),
                "fence changed",
            ),
            "old-active" => (
                Some("UPDATE turn SET status='in_progress' WHERE id='turn'"),
                "fence changed",
            ),
            "old-interrupted" => (
                Some("UPDATE turn SET status='interrupted' WHERE id='turn'"),
                "fence changed",
            ),
            "blocked-stop" => (
                Some(
                    "UPDATE turn SET status='blocked' WHERE id='turn'; INSERT INTO compaction_execution_stop(owner,turn_id) VALUES ('resume-owner','turn')",
                ),
                "fence changed",
            ),
            "blocked-deleted" => (
                Some(
                    "UPDATE turn SET status='blocked',message_deleted_at=CURRENT_TIMESTAMP WHERE id='turn'",
                ),
                "fence changed",
            ),
            "system-turn" => (
                Some("UPDATE turn SET origin='system' WHERE id='new-turn'"),
                "fence changed",
            ),
            "finished-turn" => (
                Some("UPDATE turn SET status='completed' WHERE id='new-turn'"),
                "fence changed",
            ),
            _ => unreachable!(),
        };
        if let Some(sql) = sql {
            f.store
                .database_connection()
                .execute_unprepared(sql)
                .await
                .unwrap();
        }
        let state_before = f
            .store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        let operation_before = f
            .store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        // Source edits can also change the next fingerprint; direct admission still must refuse.
        let error = admit(&f, prepared, 20).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{case}: {error}");
        assert_eq!(
            f.store
                .compaction_runner_state(&snapshot.id)
                .await
                .unwrap()
                .unwrap(),
            state_before
        );
        let record = f
            .store
            .compaction_operation(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (record.status, record.snapshot, record.execution_turn),
            (
                operation_before.status,
                operation_before.snapshot,
                operation_before.execution_turn
            )
        );
        assert_eq!(*f.provider.count.borrow() as u64, before.attempts);
    }
}

#[tokio::test]
async fn invalid_completion_resume_is_atomic_and_has_only_one_winner() {
    let (f, mut prepared, snapshot, before) = failed_progress(false).await;
    let operation_before = f
        .store
        .compaction_operation(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    prepared.execution_turn = "new-turn".into();
    f.store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_resume BEFORE UPDATE ON compaction_runner_state BEGIN SELECT RAISE(ABORT,'fixture'); END",
    ).await.unwrap();
    let error = admit(&f, prepared, 20).await.unwrap_err();
    assert!(error.to_string().contains("fixture"), "{error:#}");
    let failed = f
        .store
        .compaction_operation(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.execution_turn.as_deref(), Some("turn"));
    assert_eq!(failed.snapshot, operation_before.snapshot);
    assert_eq!(failed.deadline_ms, operation_before.deadline_ms);
    assert_eq!(failed.outcome, operation_before.outcome);
    assert_eq!(
        f.store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    f.store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_resume")
        .await
        .unwrap();
    let deadline = before.deadline_ms + 10;
    let (one, two) = tokio::join!(
        f.store
            .compaction_resume_invalid_completion(&snapshot.id, "new-turn", deadline),
        f.store
            .compaction_resume_invalid_completion(&snapshot.id, "new-turn", deadline),
    );
    assert_ne!(one.unwrap(), two.unwrap());
    let after = f
        .store
        .compaction_runner_state(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.generation, before.generation + 1);
    assert_eq!(after.previous_checkpoint, before.previous_checkpoint);
    assert_eq!(after.cursor, before.cursor);
}

#[tokio::test]
async fn invalid_completion_resume_keeps_saved_admission_immutable() {
    let (f, mut prepared, snapshot, before) = failed_progress(false).await;
    let record = pioneer_entity::compaction_operation::Entity::find_by_id(snapshot.id.clone())
        .one(&f.store.database_connection())
        .await
        .unwrap()
        .unwrap();
    assert!(
        record.next_portion > 0,
        "exercise the closed-admission guard"
    );
    let deadline = before.deadline_ms - 100;
    for case in [
        "plan",
        "fingerprint",
        "same-turn",
        "unbound-turn",
        "deadline",
    ] {
        let mut changed = snapshot.clone();
        changed.admission.deadline_ms = if case == "deadline" {
            deadline + 1
        } else {
            deadline
        };
        let mut fingerprint = record.fingerprint.clone();
        let turn = match case {
            "plan" => {
                changed.plan.fingerprint = "different-plan".into();
                Some("new-turn")
            }
            "fingerprint" => {
                fingerprint = "different-fingerprint".into();
                Some("new-turn")
            }
            "same-turn" => Some("turn"),
            "unbound-turn" => None,
            "deadline" => Some("new-turn"),
            _ => unreachable!(),
        };
        let error = f
            .store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_operation SET status='running',outcome=NULL,
                 deadline_ms=?,snapshot=?,fingerprint=?,execution_turn=? WHERE id=?",
                [
                    i64::try_from(deadline).unwrap().into(),
                    serde_json::to_string(&changed).unwrap().into(),
                    fingerprint.into(),
                    turn.into(),
                    snapshot.id.clone().into(),
                ],
            ))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("prepared admission immutable"),
            "{case}: {error}"
        );
        let unchanged =
            pioneer_entity::compaction_operation::Entity::find_by_id(snapshot.id.clone())
                .one(&f.store.database_connection())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(unchanged.status, record.status);
        assert_eq!(unchanged.outcome, record.outcome);
        assert_eq!(unchanged.snapshot, record.snapshot);
        assert_eq!(unchanged.fingerprint, record.fingerprint);
        assert_eq!(unchanged.deadline_ms, record.deadline_ms);
        assert_eq!(unchanged.execution_turn, record.execution_turn);
        assert_eq!(
            f.store
                .compaction_runner_state(&snapshot.id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
    }
    prepared.execution_turn = "new-turn".into();
    prepared.operation_deadline_ms = Some(deadline);
    assert_eq!(admit(&f, prepared, 20).await.unwrap().id, snapshot.id);
    let resumed = f
        .store
        .compaction_operation(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    let mut expected: serde_json::Value = serde_json::from_str(&record.snapshot).unwrap();
    expected["admission"]["deadline_ms"] = deadline.into();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&resumed.snapshot).unwrap(),
        expected
    );
    assert_eq!(*f.provider.count.borrow() as u64, before.attempts);
}

#[tokio::test]
async fn legacy_portion_resumes_but_other_terminal_kinds_do_not() {
    let (f, mut prepared, snapshot, before) = failed_progress(false).await;
    for kind in [
        FailureKind::Cancelled,
        FailureKind::Permanent,
        FailureKind::Transient,
        FailureKind::InsufficientEffect,
        FailureKind::Deadline,
    ] {
        let mut other = before.clone();
        other.phase = RunnerPhase::Failed { kind };
        f.store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE compaction_runner_state SET state=? WHERE operation_id=?",
                [
                    serde_json::to_string(&other).unwrap().into(),
                    snapshot.id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        assert!(
            !f.store
                .compaction_resume_invalid_completion(
                    &snapshot.id,
                    "new-turn",
                    before.deadline_ms + 100
                )
                .await
                .unwrap()
        );
        assert_eq!(
            f.store
                .compaction_runner_state(&snapshot.id)
                .await
                .unwrap()
                .unwrap(),
            other
        );
        assert_eq!(
            f.store
                .compaction_operation(&snapshot.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "failed"
        );
    }
    let mut legacy = serde_json::to_value(&before).unwrap();
    legacy.as_object_mut().unwrap().remove("resume_phase");
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_runner_state SET state=? WHERE operation_id=?",
            [legacy.to_string().into(), snapshot.id.clone().into()],
        ))
        .await
        .unwrap();
    prepared.execution_turn = "new-turn".into();
    let resumed = admit(&f, prepared, 20).await.unwrap();
    assert_eq!(resumed.id, snapshot.id);
    let after = f
        .store
        .compaction_runner_state(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.phase,
        RunnerPhase::Ready {
            purpose: AttemptPurpose::Portion
        }
    );
    assert_eq!(after.cursor, before.cursor);
    assert_eq!(after.previous_checkpoint, before.previous_checkpoint);
}

#[tokio::test]
async fn refreshed_source_plan_cannot_silently_recompute_saved_progress() {
    let (f, mut prepared, snapshot, before) = failed_progress(false).await;
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload=? WHERE id='source'",
            [
                canonical_agent_message_event_payload("fixture-agent-message", "edited history")
                    .into(),
            ],
        ))
        .await
        .unwrap();
    let sources = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let reference = sources.entries[0].reference.clone();
    assert_ne!(reference, prepared.manifest[0].source);
    prepared.manifest[0].source = reference.clone();
    prepared.plan.coverage = vec![reference];
    prepared.projection_version = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    prepared
        .source_epochs
        .insert("thread".into(), prepared.projection_version);
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    prepared.source_projection = Some(
        super::super::frozen::capture(
            &f.store,
            "ws",
            "thread",
            &std::collections::BTreeSet::from(["thread".into()]),
            &history,
        )
        .await
        .unwrap(),
    );
    prepared.execution_turn = "new-turn".into();
    assert!(
        admit(&f, prepared, 20)
            .await
            .unwrap_err()
            .to_string()
            .contains("sources changed")
    );
    assert_eq!(
        f.store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .unwrap(),
        before
    );
    assert_eq!(*f.provider.count.borrow() as u64, before.attempts);
    let operations: i64 = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_operation WHERE owner='resume-owner'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(operations, 1);
}

#[tokio::test]
async fn historical_invalid_completion_does_not_block_admission_after_head_advances() {
    let (f, prepared_a) = progress_fixture(false).await;
    let a = admit(&f, prepared_a.clone(), 10).await.unwrap();
    // B is independently admitted on the same head while A is still running.
    // Its raw plan also lets this test expire A's frozen input independently.
    let mut prepared_b = prepared_a.clone();
    prepared_b.target_identity = "already-admitted-B".into();
    prepared_b.source_projection = None;
    let b = admit(&f, prepared_b, 11).await.unwrap();
    assert_ne!(a.id, b.id);
    assert_eq!(
        runner(&f, a.clone(), true, "turn")
            .run(CancellationToken::new())
            .await
            .unwrap(),
        CompactionExit::Failed(FailureKind::InvalidCompletion)
    );
    let state_a = f
        .store
        .compaction_runner_state(&a.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state_a.cursor > SourceCursor::default());
    assert_eq!(f.store.compaction_head(&a.owner).await.unwrap(), None);

    // Apply B through the real runner, including its normal atomic head CAS.
    let CompactionExit::Applied(head) = runner(&f, b.clone(), true, "turn")
        .run(CancellationToken::new())
        .await
        .unwrap()
    else {
        panic!("B did not apply");
    };
    assert_eq!(
        f.store.compaction_head(&a.owner).await.unwrap().as_deref(),
        Some(head.as_str())
    );
    assert_eq!(
        f.store
            .compaction_operation(&b.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "UPDATE compaction_frozen_history SET expired=1 WHERE id=(SELECT manifest_id FROM compaction_operation_projection WHERE operation_id=?)",
        [a.id.clone().into()],
    )).await.unwrap();
    assert!(
        f.store
            .compaction_bound_source_projection(&a.id)
            .await
            .is_err()
    );

    // C uses B as its previous summary and selects only newly appended material.
    f.store.database_connection().execute_unprepared(
        "UPDATE turn SET status='completed' WHERE id='turn';
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at)
         VALUES ('new-turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let event = pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
        pioneer_protocol::ItemCompletedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "new-turn".into(),
            item: pioneer_protocol::TurnItem::AgentMessage {
                id: "next-source".into(),
                text: "漢字🌍".repeat(4_000),
                phase: Default::default(),
                markdown: None,
                markdown_version: None,
            },
        },
    );
    let payload = serde_json::to_string(&event).unwrap();
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at)
         VALUES ('next-source','thread','new-turn',1,?,?,CURRENT_TIMESTAMP)",
            [event.event_type().into(), payload.into()],
        ))
        .await
        .unwrap();
    let sources = f
        .store
        .compaction_source_page("ws", "thread", "new-turn", PagedSource::Event, 0)
        .await
        .unwrap();
    let source = sources.entries[0].reference.clone();
    let mut prepared_c = prepared_a;
    prepared_c.execution_turn = "new-turn".into();
    prepared_c.expected_checkpoint = Some(head.clone());
    prepared_c.summary_basis = Some(head.clone());
    prepared_c.source_projection = None;
    prepared_c.target_identity = "new-plan-C".into();
    prepared_c.plan.coverage = vec![source.clone()];
    prepared_c.manifest = vec![ManifestEntry {
        ordinal: 0,
        unit: 0,
        reference_only: false,
        thread_id: "thread".into(),
        source,
    }];
    prepared_c.projection_version = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    prepared_c
        .source_epochs
        .insert("thread".into(), prepared_c.projection_version);
    let calls_before_c = *f.provider.count.borrow();
    let c = admit(&f, prepared_c.clone(), 20).await.unwrap();
    assert_ne!(c.id, a.id);
    assert_ne!(c.id, b.id);
    assert_eq!(c.expected_checkpoint.as_deref(), Some(head.as_str()));
    assert_eq!(
        f.store
            .compaction_operation(&c.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    let state_c = f
        .store
        .compaction_runner_state(&c.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state_c.cursor, SourceCursor::default());
    assert_eq!(state_c.attempts, 0);
    assert_eq!(state_c.previous_checkpoint.as_deref(), Some(head.as_str()));
    assert_eq!(*f.provider.count.borrow(), calls_before_c);
    assert_eq!(
        f.store
            .compaction_operation(&a.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "failed"
    );
    assert_eq!(
        f.store
            .compaction_runner_state(&a.id)
            .await
            .unwrap()
            .unwrap(),
        state_a
    );
    assert_eq!(
        f.store.compaction_head(&a.owner).await.unwrap().as_deref(),
        Some(head.as_str())
    );

    // Progress on B's current head must still fail fast for an incompatible plan.
    f.provider.replies.lock().unwrap().extend([
        Reply::Success,
        Reply::Success,
        Reply::InvalidCompletion,
    ]);
    assert_eq!(
        runner(&f, c.clone(), true, "new-turn")
            .run(CancellationToken::new())
            .await
            .unwrap(),
        CompactionExit::Failed(FailureKind::InvalidCompletion)
    );
    let failed_c = f
        .store
        .compaction_runner_state(&c.id)
        .await
        .unwrap()
        .unwrap();
    assert!(failed_c.cursor > SourceCursor::default());
    f.store.database_connection().execute_unprepared(
        "UPDATE turn SET status='blocked' WHERE id='new-turn';
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at)
         VALUES ('next-turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    prepared_c.execution_turn = "next-turn".into();
    prepared_c.target_identity = "incompatible-current-plan".into();
    let calls_after_c = *f.provider.count.borrow();
    assert!(
        admit(&f, prepared_c, 30)
            .await
            .unwrap_err()
            .to_string()
            .contains("plan changed")
    );
    assert_eq!(*f.provider.count.borrow(), calls_after_c);
    assert_eq!(
        f.store
            .compaction_runner_state(&c.id)
            .await
            .unwrap()
            .unwrap(),
        failed_c
    );
    let operations: i64 = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_operation WHERE owner='resume-owner'",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(operations, 3);
}
