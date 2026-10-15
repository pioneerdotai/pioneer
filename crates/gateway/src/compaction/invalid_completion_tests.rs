use super::*;

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

fn assert_same_request(f: &Fixture, start: usize, end: usize) {
    let calls = f.provider.calls.lock().unwrap();
    let expected = &calls[start];
    for call in &calls[start + 1..end] {
        assert_eq!(call.messages, expected.messages);
        assert_eq!(call.model, expected.model);
        assert_eq!(call.max_tokens, expected.max_tokens);
        assert_eq!(call.reasoning, expected.reasoning);
        assert_eq!(call.temperature, expected.temperature);
        assert_eq!(call.compiled_prompt, expected.compiled_prompt);
        assert_eq!(call.parallel_tool_calls, expected.parallel_tool_calls);
        assert!(call.tools.is_none());
        assert!(call.tool_choice.is_none());
    }
}

#[tokio::test]
async fn two_invalid_completions_then_success_survive_runner_restart() {
    let f = fixture(
        "small",
        vec![Reply::Invalid, Reply::Invalid, Reply::Success],
        true,
        false,
    )
    .await;
    let mut task = start(&f);
    for calls in 1..=2 {
        let retry_at = retry_time(&f, calls).await;
        let before = saved(&f).await;
        assert_eq!(before.portion_attempts as usize, calls);
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
        assert_eq!(
            diagnostic.explanation,
            "Summary sections are missing, duplicated or out of order"
        );
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
    assert_same_request(&f, 0, 3);
    let state = saved(&f).await;
    assert_eq!(state.retries, 0);
    assert_eq!(state.attempts, 3);
    assert_eq!(state.deadline_ms, f.runner.snapshot.admission.deadline_ms);
    let observations = f.store.database_connection().query_all_raw(Statement::from_string(
        DbBackend::Sqlite,
        "SELECT observation FROM compaction_attempt_observation WHERE operation_id='operation' ORDER BY attempt",
    )).await.unwrap();
    assert_eq!(observations.len(), 3);
    for observation in &observations[..2] {
        let encoded: String = observation.try_get("", "observation").unwrap();
        let observation: pioneer_compaction::runner::AttemptObservation =
            serde_json::from_str(&encoded).unwrap();
        assert_eq!(observation.failure, Some(FailureKind::InvalidCompletion));
        assert_eq!(observation.diagnostic.unwrap().code, "invalid_completion");
        assert!(!encoded.contains("private invalid model response"));
    }
}

#[tokio::test]
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
    assert_eq!(state.portion_attempts, 3);
    assert_eq!(state.previous_checkpoint, None);
    assert_eq!(state.cursor, SourceCursor::default());
    let row = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT COUNT(*) AS count FROM compaction_checkpoint WHERE operation_id='operation'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "count").unwrap(), 0);
}

#[tokio::test]
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
    assert_same_request(&f, 1, 4);
    let calls = f.provider.calls.lock().unwrap();
    let retried: SummaryInput = serde_json::from_str(&calls[1].messages[1].content).unwrap();
    assert_eq!(retried.previous_summary, checkpoint.summary);
}

#[tokio::test]
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
    assert_same_request(&f, 0, 3);
    assert_same_request(&f, 3, 6);
    assert_eq!(saved(&f).await.retries, 0);
    assert_eq!(saved(&f).await.corrections, 0);
}

#[tokio::test]
async fn mixed_failures_cannot_make_a_fourth_call_for_the_same_portion() {
    for replies in [
        vec![
            Reply::Transient,
            Reply::Invalid,
            Reply::Invalid,
            Reply::Success,
        ],
        vec![
            Reply::Invalid,
            Reply::Transient,
            Reply::Invalid,
            Reply::Success,
        ],
        vec![
            Reply::Invalid,
            Reply::Invalid,
            Reply::Transient,
            Reply::Success,
        ],
    ] {
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
        assert_same_request(&f, 0, 3);
    }
}

#[tokio::test]
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
