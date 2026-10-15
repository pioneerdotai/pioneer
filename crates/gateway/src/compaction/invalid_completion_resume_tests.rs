//! New foreground executions reuse only compatible, durable compaction progress.
use super::*;
use pioneer_compaction::runner::{AttemptPurpose, SourceCursor};

async fn failed_progress(
    correction: bool,
) -> (Fixture, PreparedOperation, OperationSnapshot, RunnerState) {
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
    f.store.database_connection().execute_unprepared(
        "UPDATE turn SET status='failed' WHERE id='turn';
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at)
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
    for (correction, same_deadline, completed_turn) in [
        (false, false, false),
        (true, false, false),
        (false, true, true),
    ] {
        let (f, mut prepared, snapshot, before) = failed_progress(correction).await;
        if completed_turn {
            // A failed background operation may be attached to a completed turn.
            // Only the later foreground execution may resume it.
            f.store
                .database_connection()
                .execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
                .await
                .unwrap();
        }
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
        let after = f
            .store
            .compaction_runner_state(&snapshot.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.cursor, before.cursor);
        assert_eq!(after.previous_checkpoint, before.previous_checkpoint);
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
        let old_status = pioneer_entity::turn::Entity::find_by_id("turn")
            .one(&f.store.database_connection())
            .await
            .unwrap()
            .unwrap()
            .status;
        assert_eq!(
            old_status,
            if completed_turn {
                "completed"
            } else {
                "failed"
            }
        );
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
                Some("DELETE FROM compaction_checkpoint WHERE owner='resume-owner'"),
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
    prepared.execution_turn = "new-turn".into();
    f.store.database_connection().execute_unprepared(
        "CREATE TRIGGER reject_resume BEFORE UPDATE ON compaction_runner_state BEGIN SELECT RAISE(ABORT,'fixture'); END",
    ).await.unwrap();
    assert!(admit(&f, prepared, 20).await.is_err());
    let failed = f
        .store
        .compaction_operation(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, "failed");
    assert_eq!(failed.execution_turn.as_deref(), Some("turn"));
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
