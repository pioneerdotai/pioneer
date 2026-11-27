use super::*;
use pioneer_compaction::summary::INSTRUCTIONS;

async fn saved(f: &Fixture) -> RunnerState {
    f.store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap()
}

async fn retry_time(f: &Fixture, calls: usize) -> u64 {
    f.provider.wait_calls(calls).await;
    wait_backoff(&f.store).await;
    let state = saved(f).await;
    let RunnerPhase::Backoff { not_before_ms, .. } = state.phase else {
        panic!("expected backoff");
    };
    assert_eq!(state.attempts, calls as u64);
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    not_before_ms
}

fn start(f: &Fixture) -> tokio::task::JoinHandle<Result<CompactionExit>> {
    let runner = f.runner.clone();
    tokio::spawn(async move { runner.run(CancellationToken::new()).await })
}

async fn finish(task: tokio::task::JoinHandle<Result<CompactionExit>>) -> CompactionExit {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

fn assert_retry_requests(f: &Fixture, start: usize, feedback: &[Option<&str>]) {
    let calls = f.provider.calls.lock().unwrap();
    let expected = &calls[start];
    assert_eq!(expected.messages[0].content, INSTRUCTIONS);
    for (call, feedback) in calls[start..start + feedback.len()].iter().zip(feedback) {
        if let Some(detail) = feedback {
            let instructions = &call.messages[0].content;
            assert!(
                instructions
                    .lines()
                    .any(|line| line.strip_prefix("Retry: ") == Some(*detail))
            );
            assert_eq!(instructions.matches("Retry: ").count(), 1);
            let headings: Vec<_> = instructions
                .lines()
                .filter(|line| line.starts_with("## "))
                .collect();
            assert_eq!(headings, HEADINGS);
            let mut system = call.messages[0].clone();
            system.content.clone_from(&expected.messages[0].content);
            assert_eq!(system, expected.messages[0]);
            assert_eq!(call.messages[1..], expected.messages[1..]);
        } else {
            assert_eq!(call.messages, expected.messages);
        }
        assert!(
            !serde_json::to_string(&call.messages)
                .unwrap()
                .contains("private invalid model response")
        );
        assert_request_options(call, expected);
    }
}

pub(super) fn assert_same_request(actual: &ChatRequest, expected: &ChatRequest) {
    assert_eq!(actual.messages, expected.messages);
    assert_request_options(actual, expected);
}

fn assert_request_options(call: &ChatRequest, expected: &ChatRequest) {
    assert_eq!(call.model, expected.model);
    assert_eq!(call.max_tokens, expected.max_tokens);
    assert_eq!(call.reasoning, expected.reasoning);
    assert_eq!(call.temperature, expected.temperature);
    assert_eq!(call.compiled_prompt, expected.compiled_prompt);
    assert_eq!(call.parallel_tool_calls, expected.parallel_tool_calls);
    assert!(call.tools.is_none());
    assert!(call.tool_choice.is_none());
    assert!(call.tool_declarations.is_none());
    assert_eq!(call.service_mode, expected.service_mode);
}

const START_WITH_FIRST: &str = "Start with ## Goal and constraints.";
const MISSING_SECOND: &str = "Recognized 1/6; next: ## Decisions and rationale.";

#[pioneer_postgres_test::postgres_test]
async fn two_invalid_completions_then_success_survive_runner_restart() {
    let f = fixture(
        "small",
        vec![
            Reply::MissingSummarySections,
            Reply::Invalid,
            Reply::Success,
        ],
        true,
        false,
    )
    .await;
    let mut task = start(&f);
    let expected_explanations = [
        "Summary ended before required section 2 was recognized",
        "Nonempty content appeared before the first recognized summary heading",
    ];
    for (index, explanation) in expected_explanations.iter().enumerate() {
        let calls = index + 1;
        let retry_at = retry_time(&f, calls).await;
        let before = saved(&f).await;
        assert_eq!(before.portion_attempts as usize, calls);
        assert!(
            !serde_json::to_string(&before)
                .unwrap()
                .contains("private invalid model response")
        );
        assert_eq!(before.cursor, SourceCursor::default());
        assert_eq!(before.previous_checkpoint, None);
        assert!(f.store.compaction_head("owner").await.unwrap().is_none());
        let diagnostic = before
            .observation
            .as_ref()
            .unwrap()
            .diagnostic
            .as_ref()
            .unwrap();
        assert_eq!(diagnostic.stage, "summary_validation");
        assert_eq!(diagnostic.code, "invalid_completion");
        assert_eq!(diagnostic.explanation.as_str(), *explanation);
        assert_eq!(
            before.observation.as_ref().unwrap().failure,
            Some(FailureKind::InvalidCompletion)
        );
        // Drop this driver, then let run() reload the durable state.
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(saved(&f).await, before);
        task = start(&f);
        f.clock.advance(retry_at);
    }
    assert!(matches!(finish(task).await, CompactionExit::Applied(_)));
    assert_eq!(f.provider.calls.lock().unwrap().len(), 3);
    assert_retry_requests(&f, 0, &[None, Some(MISSING_SECOND), Some(START_WITH_FIRST)]);
    let state = saved(&f).await;
    assert_eq!(state.retries, 0);
    assert_eq!(state.attempts, 3);
    assert_eq!(state.deadline_ms, f.runner.snapshot.admission.deadline_ms);
    let observations = attempt_history(&f).await;
    assert_eq!(observations.len(), 3);
    for (index, (encoded, explanation)) in
        observations.iter().zip(expected_explanations).enumerate()
    {
        let observation: pioneer_compaction::runner::AttemptObservation =
            serde_json::from_str(encoded).unwrap();
        assert_eq!(observation.number, (index + 1) as u64);
        assert_eq!(observation.failure, Some(FailureKind::InvalidCompletion));
        let diagnostic = observation.diagnostic.unwrap();
        assert_eq!(diagnostic.stage, "summary_validation");
        assert_eq!(diagnostic.code, "invalid_completion");
        assert_eq!(diagnostic.explanation, explanation);
        assert!(!encoded.contains("private invalid model response"));
    }
}

#[pioneer_postgres_test::postgres_test]
async fn third_invalid_completion_stops_without_checkpoint_or_fourth_call() {
    let f = fixture(
        "small",
        vec![
            Reply::Invalid,
            Reply::Invalid,
            Reply::Invalid,
            Reply::Success,
        ],
        true,
        false,
    )
    .await;
    let task = start(&f);
    for calls in 1..=2 {
        let retry_at = retry_time(&f, calls).await;
        f.clock.advance(retry_at);
    }
    assert_eq!(
        finish(task).await,
        CompactionExit::Failed(FailureKind::InvalidCompletion)
    );
    assert_eq!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::InvalidCompletion)
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), 3);
    let state = saved(&f).await;
    assert_retry_requests(
        &f,
        0,
        &[None, Some(START_WITH_FIRST), Some(START_WITH_FIRST)],
    );
    assert_eq!(state.portion_attempts, 3);
    assert_eq!(state.previous_checkpoint, None);
    assert_eq!(state.cursor, SourceCursor::default());
    let row = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT COUNT(*) AS count FROM compaction_checkpoint WHERE operation_id='operation'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "count").unwrap(), 0);
}

#[pioneer_postgres_test::postgres_test]
async fn invalid_portion_exhaustion_preserves_previous_summary_and_cursor() {
    let f = fixture(
        &"漢字🌍".repeat(4000),
        vec![
            Reply::Success,
            Reply::Invalid,
            Reply::Invalid,
            Reply::Invalid,
        ],
        true,
        false,
    )
    .await;
    let task = start(&f);
    let mut retry_at = retry_time(&f, 2).await;
    let accepted = saved(&f).await;
    let checkpoint_id = accepted.previous_checkpoint.as_ref().unwrap();
    let checkpoint = f
        .store
        .compaction_checkpoint(checkpoint_id)
        .await
        .unwrap()
        .unwrap();
    assert!(accepted.cursor > SourceCursor::default());
    assert!(!checkpoint.summary.is_empty());
    assert_eq!(accepted.portion_attempts, 1);
    f.clock.advance(retry_at);
    retry_at = retry_time(&f, 3).await;
    let second = saved(&f).await;
    assert_eq!(second.cursor, accepted.cursor);
    assert_eq!(second.previous_checkpoint, accepted.previous_checkpoint);
    f.clock.advance(retry_at);
    assert_eq!(
        finish(task).await,
        CompactionExit::Failed(FailureKind::InvalidCompletion)
    );
    let failed = saved(&f).await;
    assert_eq!(failed.cursor, accepted.cursor);
    assert_eq!(failed.previous_checkpoint, accepted.previous_checkpoint);
    assert_eq!(
        f.store
            .compaction_checkpoint(checkpoint_id)
            .await
            .unwrap()
            .unwrap()
            .summary,
        checkpoint.summary
    );
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
    assert_eq!(f.provider.calls.lock().unwrap().len(), 4);
    assert_retry_requests(
        &f,
        1,
        &[None, Some(START_WITH_FIRST), Some(START_WITH_FIRST)],
    );
    let calls = f.provider.calls.lock().unwrap();
    let retried: SummaryInput = serde_json::from_str(&calls[1].messages[1].content).unwrap();
    assert_eq!(retried.previous_summary, checkpoint.summary);
}

#[pioneer_postgres_test::postgres_test]
async fn next_portion_has_its_own_attempt_budget_after_third_call_succeeds() {
    let f = fixture(
        &"漢字🌍".repeat(4000),
        vec![
            Reply::Invalid,
            Reply::Invalid,
            Reply::Success,
            Reply::Invalid,
            Reply::Invalid,
            Reply::Success,
        ],
        true,
        false,
    )
    .await;
    let task = start(&f);
    for calls in [1, 2, 4, 5] {
        let retry_at = retry_time(&f, calls).await;
        let state = saved(&f).await;
        if calls <= 2 {
            assert_eq!(state.cursor, SourceCursor::default());
            assert_eq!(state.portion_attempts as usize, calls);
        } else {
            assert!(state.cursor > SourceCursor::default());
            assert!(state.previous_checkpoint.is_some());
            assert_eq!(state.portion_attempts as usize, calls - 3);
        }
        f.clock.advance(retry_at);
    }
    assert!(matches!(finish(task).await, CompactionExit::Applied(_)));
    for start in [0, 3] {
        assert_retry_requests(
            &f,
            start,
            &[None, Some(START_WITH_FIRST), Some(START_WITH_FIRST)],
        );
    }
    assert_eq!(saved(&f).await.retries, 0);
    assert_eq!(saved(&f).await.corrections, 0);
}

#[pioneer_postgres_test::postgres_test]
async fn mixed_failures_cannot_make_a_fourth_call_for_the_same_portion() {
    for (replies, feedback) in [
        (
            vec![
                Reply::Transient,
                Reply::Invalid,
                Reply::Invalid,
                Reply::Success,
            ],
            [None, None, Some(START_WITH_FIRST)],
        ),
        (
            vec![
                Reply::Invalid,
                Reply::Transient,
                Reply::Invalid,
                Reply::Success,
            ],
            [None, Some(START_WITH_FIRST), Some(START_WITH_FIRST)],
        ),
        (
            vec![
                Reply::Invalid,
                Reply::Invalid,
                Reply::Transient,
                Reply::Success,
            ],
            [None, Some(START_WITH_FIRST), Some(START_WITH_FIRST)],
        ),
    ] {
        let transport_retry = replies[..2]
            .iter()
            .position(|reply| matches!(reply, Reply::Transient));
        let f = fixture("small", replies, true, false).await;
        let task = start(&f);
        for calls in 1..=2 {
            let retry_at = retry_time(&f, calls).await;
            f.clock.advance(retry_at);
        }
        assert!(matches!(
            finish(task).await,
            CompactionExit::Failed(FailureKind::InvalidCompletion | FailureKind::Transient)
        ));
        assert_eq!(f.provider.calls.lock().unwrap().len(), 3);
        assert_eq!(saved(&f).await.portion_attempts, 3);
        assert_retry_requests(&f, 0, &feedback);
        if let Some(failed) = transport_retry {
            let calls = f.provider.calls.lock().unwrap();
            assert_same_request(&calls[failed + 1], &calls[failed]);
        }
    }
}

#[pioneer_postgres_test::postgres_test]
async fn deadline_and_stop_during_invalid_completion_backoff_prevent_the_next_call() {
    for stop in [false, true] {
        let f = fixture("small", vec![Reply::Invalid, Reply::Success], true, false).await;
        let cancel = CancellationToken::new();
        let task = tokio::spawn({
            let runner = f.runner.clone();
            let cancel = cancel.clone();
            async move { runner.run(cancel).await }
        });
        retry_time(&f, 1).await;
        let reason = if stop {
            f.store
                .compaction_finish("operation", "cancelled", "user_stop")
                .await
                .unwrap();
            cancel.cancel();
            FailureKind::Cancelled
        } else {
            f.clock.advance(f.runner.snapshot.admission.deadline_ms);
            FailureKind::Deadline
        };
        assert_eq!(
            finish(task).await,
            CompactionExit::Reconcile(reason.clone())
        );
        assert_eq!(
            f.runner.reconcile(reason.clone()).await.unwrap(),
            CompactionExit::Failed(reason)
        );
        assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
        assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
        assert_eq!(saved(&f).await.previous_checkpoint, None);
    }
}

async fn attempt_history(f: &Fixture) -> Vec<String> {
    f.store
        .database_connection()
        .query_all_raw(Statement::from_string(
            DbBackend::Postgres,
            "SELECT observation::text AS observation FROM compaction_attempt_observation WHERE operation_id='operation' ORDER BY attempt",
        ))
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("", "observation").unwrap())
        .collect()
}

#[pioneer_postgres_test::postgres_test]
async fn deadline_resume_executes_after_third_portion_or_correction_call_was_interrupted() {
    for correction in [false, true] {
        let text = if correction {
            "small".into()
        } else {
            "漢字🌍".repeat(4000)
        };
        let f = fixture(
            &text,
            vec![
                Reply::Success,
                Reply::Transient,
                Reply::Invalid,
                Reply::Hang,
                Reply::Success,
            ],
            !correction,
            false,
        )
        .await;
        f.store
            .compaction_bind_execution_turn("operation", "turn")
            .await
            .unwrap();
        let task = start(&f);
        for calls in [2, 3] {
            let retry_at = retry_time(&f, calls).await;
            f.clock.advance(retry_at);
        }
        f.provider.wait_calls(4).await;
        let interrupted = saved(&f).await;
        let purpose = if correction {
            AttemptPurpose::Correction
        } else {
            AttemptPurpose::Portion
        };
        assert!(
            matches!(interrupted.phase, RunnerPhase::Attempt { purpose: actual, .. } if actual == purpose)
        );
        assert_eq!(interrupted.portion_attempts, 3);
        assert_eq!(interrupted.attempts, 4);
        assert_eq!(interrupted.retries, 1);
        assert_eq!(interrupted.corrections, u8::from(correction));
        assert!(interrupted.cursor > SourceCursor::default());
        let checkpoint_id = interrupted.previous_checkpoint.as_ref().unwrap();
        let checkpoint = f
            .store
            .compaction_checkpoint(checkpoint_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!checkpoint.summary.is_empty());
        assert_eq!(checkpoint_id, "operation:summary:1");

        f.clock.advance(f.runner.snapshot.admission.deadline_ms);
        assert_eq!(
            finish(task).await,
            CompactionExit::Reconcile(FailureKind::Deadline)
        );
        assert_eq!(
            f.runner.reconcile(FailureKind::Deadline).await.unwrap(),
            CompactionExit::Failed(FailureKind::Deadline)
        );
        let expired = saved(&f).await;
        assert_eq!(expired.portion_attempts, 3);
        assert_eq!(expired.cursor, interrupted.cursor);
        assert_eq!(expired.previous_checkpoint, interrupted.previous_checkpoint);
        assert_eq!(expired.resume_phase, Some(RunnerPhase::Ready { purpose }));
        assert!(expired.can_resume_deadline());
        assert_eq!(expired.observation.as_ref().unwrap().failure, None);
        assert_eq!(
            serde_json::from_str::<RunnerState>(&serde_json::to_string(&expired).unwrap()).unwrap(),
            expired
        );
        // Re-running the old execution does not refresh the exhausted budget.
        assert_eq!(
            f.runner.run(CancellationToken::new()).await.unwrap(),
            CompactionExit::Reconcile(FailureKind::Deadline)
        );
        assert_eq!(saved(&f).await, expired);
        assert_eq!(f.provider.calls.lock().unwrap().len(), 4);
        let history = attempt_history(&f).await;
        assert_eq!(history.len(), 4);

        if correction {
            // The saved summary is sufficient: the resumed correction must not
            // read, validate or regenerate its already processed raw history.
            f.store
                .database_connection()
                .execute_unprepared("DELETE FROM turn_event WHERE id='source'")
                .await
                .unwrap();
        }
        let deadline = expired.deadline_ms + pioneer_compaction::OPERATION_MILLIS;
        assert!(
            f.store
                .compaction_resume_deadline("operation", "turn", deadline)
                .await
                .unwrap()
        );
        let resumed = saved(&f).await;
        assert_eq!(resumed.portion_attempts, 0);
        assert_eq!(resumed.phase, RunnerPhase::Ready { purpose });
        assert_eq!(resumed.generation, expired.generation + 1);
        assert_eq!(resumed.attempts, expired.attempts);
        assert_eq!(resumed.retries, expired.retries);
        assert_eq!(resumed.corrections, expired.corrections);
        assert_eq!(resumed.target_tokens, expired.target_tokens);
        assert_eq!(resumed.cursor, expired.cursor);
        assert_eq!(resumed.previous_checkpoint, expired.previous_checkpoint);
        assert_eq!(
            resumed.source_text_projection_version,
            expired.source_text_projection_version
        );
        assert_eq!(attempt_history(&f).await, history);
        let operation = f
            .store
            .compaction_operation("operation")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(operation.status, "running");
        assert_eq!(operation.deadline_ms as u64, deadline);
        assert_eq!(operation.attempts as u64, expired.attempts);
        assert_eq!(operation.transient_retries as u8, expired.retries);
        assert_eq!(operation.correction as u8, expired.corrections);
        let snapshot = serde_json::from_str(&operation.snapshot).unwrap();
        let runner = Arc::new(CompactionRunner::new(
            f.store.clone(),
            "ws".into(),
            "thread".into(),
            snapshot,
            f.runner.summarizer.clone(),
            f.runner.target.clone(),
            f.observer.clone(),
            f.clock.clone(),
        ));
        let gate = f.provider.pause_next();
        let task = tokio::spawn(async move { runner.run(CancellationToken::new()).await });
        f.provider.wait_calls(5).await;
        let claimed = saved(&f).await;
        assert_eq!(claimed.attempts, expired.attempts + 1);
        assert_eq!(claimed.portion_attempts, 1);
        assert_eq!(claimed.cursor, expired.cursor);
        assert_eq!(claimed.previous_checkpoint, expired.previous_checkpoint);
        assert!(
            matches!(claimed.phase, RunnerPhase::Attempt { purpose: actual, .. } if actual == purpose)
        );
        assert_retry_requests(
            &f,
            1,
            &[None, None, Some(START_WITH_FIRST), Some(START_WITH_FIRST)],
        );
        let retried: SummaryInput = {
            let calls = f.provider.calls.lock().unwrap();
            assert_same_request(&calls[4], &calls[3]);
            serde_json::from_str(&calls[4].messages[1].content).unwrap()
        };
        assert_eq!(retried.previous_summary, checkpoint.summary);
        if correction {
            assert!(retried.compact_units.is_empty());
            assert!(retried.reference_only.is_empty());
        } else {
            let first: SummaryInput =
                serde_json::from_str(&f.provider.calls.lock().unwrap()[0].messages[1].content)
                    .unwrap();
            assert_ne!(
                serde_json::to_value(&first.compact_units).unwrap(),
                serde_json::to_value(&retried.compact_units).unwrap()
            );
        }
        gate.add_permits(1);
        let exit = finish(task).await;
        if correction {
            assert_eq!(
                exit,
                CompactionExit::Failed(FailureKind::InsufficientEffect)
            );
            assert_eq!(
                f.provider.calls.lock().unwrap().len(),
                5,
                "a second correction must not be admitted"
            );
        } else {
            assert!(matches!(exit, CompactionExit::Applied(_)));
        }
        let completed = saved(&f).await;
        assert_eq!(
            completed.attempts as usize,
            f.provider.calls.lock().unwrap().len()
        );
        assert!(completed.attempts > expired.attempts);
        assert_eq!(completed.retries, expired.retries);
        assert_eq!(completed.corrections, expired.corrections);
        assert_eq!(completed.target_tokens, expired.target_tokens);
        assert_eq!(&attempt_history(&f).await[..history.len()], &history);
        assert_eq!(
            f.store
                .compaction_checkpoint(checkpoint_id)
                .await
                .unwrap()
                .unwrap()
                .summary,
            checkpoint.summary
        );
    }
}
