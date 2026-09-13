use super::*;
use migration::{Migrator, MigratorTrait};
use pioneer_compaction::summary::{HEADINGS, SummaryInput};
use pioneer_compaction::{
    CompactionMode, CompactionPlan, CompactionSettings, ModelBudget, ModelSelection, Transport,
};
use pioneer_crud::compaction::{CanonicalSource, ManifestEntry, PagedSource};
use pioneer_protocol::ProviderFailureClass;
use pioneer_provider::{
    ChatRequest, ChatResponse, Provider, ProviderFailureClassification, ProviderTermination,
    StreamChunk,
};
use sea_orm::{ConnectionTrait, Database, DbBackend, Statement};
use std::{
    collections::VecDeque,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

struct ManualClock(tokio::sync::watch::Sender<u64>);
impl ManualClock {
    fn new() -> Self {
        Self(tokio::sync::watch::channel(0).0)
    }
    fn advance(&self, value: u64) {
        assert!(value >= self.now_ms());
        self.0.send_replace(value);
    }
}
#[async_trait]
impl CompactionClock for ManualClock {
    fn now_ms(&self) -> u64 {
        *self.0.borrow()
    }
    async fn sleep_until(&self, deadline: u64) {
        let mut rx = self.0.subscribe();
        loop {
            if *rx.borrow_and_update() >= deadline {
                return;
            }
            rx.changed().await.unwrap();
        }
    }
}
#[derive(Clone, Copy)]
enum Reply {
    Success,
    Transient,
    Hang,
}
struct ProviderFixture {
    replies: Mutex<VecDeque<Reply>>,
    calls: Mutex<Vec<ChatRequest>>,
    count: tokio::sync::watch::Sender<usize>,
    active: AtomicUsize,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ProviderFixture {
    fn new(replies: Vec<Reply>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            calls: Mutex::new(vec![]),
            count: tokio::sync::watch::channel(0).0,
            active: AtomicUsize::new(0),
        }
    }
    async fn wait_calls(&self, count: usize) {
        let mut rx = self.count.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if *rx.borrow_and_update() >= count {
                    return;
                }
                rx.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}
#[async_trait]
impl Provider for ProviderFixture {
    fn name(&self) -> &str {
        "fixture"
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.active);
        let count = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(request);
            calls.len()
        };
        self.count.send_replace(count);
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Reply::Success);
        match reply {
            Reply::Transient => anyhow::bail!("synthetic rate limit"),
            Reply::Hang => std::future::pending::<()>().await,
            Reply::Success => {}
        }
        Ok(ChatResponse {
            text: HEADINGS.iter().map(|h| format!("{h}\nState.\n")).collect(),
            usage: None,
            termination: ProviderTermination::Complete,
            reasoning_content: None,
            tool_calls: vec![],
            provider_replay_state: None,
        })
    }
    async fn stream_chat(
        &self,
        _: ChatRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<StreamChunk>>> {
        anyhow::bail!("not used")
    }
    fn classify_failure(&self, _: &anyhow::Error) -> Option<ProviderFailureClassification> {
        Some(ProviderFailureClassification {
            class: ProviderFailureClass::RateLimit,
            http_status: Some(429),
            provider_code: None,
            retry_after_ms: Some(12_000),
        })
    }
}
struct Target(bool);
#[async_trait]
impl CompactionTarget for Target {
    async fn fits(&self, _: &str) -> Result<bool> {
        Ok(self.0)
    }
}
#[derive(Default)]
struct Observer {
    ids: Mutex<Vec<String>>,
    fail: bool,
}
#[async_trait]
impl CompactionObserver for Observer {
    async fn started(&self, operation: &str, _: &RunnerState) -> Result<()> {
        self.ids.lock().unwrap().push(operation.into());
        if self.fail {
            anyhow::bail!("synthetic observer failure")
        }
        Ok(())
    }
    fn heartbeat(&self, _: &str) {}
    async fn terminal(&self, operation: &str, _: &RunnerState) -> Result<()> {
        self.ids.lock().unwrap().push(operation.into());
        if self.fail {
            anyhow::bail!("synthetic observer failure")
        }
        Ok(())
    }
}
struct Fixture {
    store: CrudStore,
    runner: Arc<CompactionRunner>,
    provider: Arc<ProviderFixture>,
    clock: Arc<ManualClock>,
    observer: Arc<Observer>,
    payload: String,
}
async fn fixture(
    text: &str,
    replies: Vec<Reply>,
    target_fits: bool,
    observer_fails: bool,
) -> Fixture {
    super::load_test_catalog();
    let db = Database::connect("sqlite::memory:").await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let store = CrudStore::new(db);
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','in_progress','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        store
            .database_connection()
            .execute_unprepared(sql)
            .await
            .unwrap();
    }
    let payload = serde_json::json!({"text":text}).to_string();
    store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,"INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('source','thread','turn',1,'fixture',?,CURRENT_TIMESTAMP)",[payload.clone().into()])).await.unwrap();
    let source = store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "fixture-instance".into(),
        model: "fixture-model".into(),
        effort: None,
    };
    let snapshot = OperationSnapshot {
        id: "operation".into(),
        owner: "owner".into(),
        expected_checkpoint: None,
        projection_version: 0,
        source_epochs: std::collections::BTreeMap::new(),
        admission: CompactionSettings::default()
            .admit(&selection, None, 0)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            compact: vec![],
            retain: vec![],
            coverage: vec![],
            fingerprint: "fixture-fingerprint".into(),
        },
    };
    store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    let budget = ModelBudget::new(Some(4096), None, None);
    store
        .compaction_prepare_runner("operation", &budget, 1, 0)
        .await
        .unwrap();
    store
        .compaction_append_manifest(
            "operation",
            &[ManifestEntry {
                ordinal: 0,
                unit: 0,
                reference_only: false,
                thread_id: "thread".into(),
                source,
            }],
        )
        .await
        .unwrap();
    store
        .compaction_activate_runner(
            "operation",
            &RunnerState::new(900_000, &budget, 500, None).unwrap(),
        )
        .await
        .unwrap();
    let provider = Arc::new(ProviderFixture::new(replies));
    let summarizer = Arc::new(
        pioneer_agent::compaction::NativeSummarizer::new(provider.clone(), selection, budget)
            .unwrap(),
    );
    let observer = Arc::new(Observer {
        fail: observer_fails,
        ..Default::default()
    });
    let clock = Arc::new(ManualClock::new());
    let runner = Arc::new(CompactionRunner::new(
        store.clone(),
        "ws".into(),
        "thread".into(),
        snapshot,
        summarizer,
        Arc::new(Target(target_fits)),
        observer.clone(),
        clock.clone(),
    ));
    Fixture {
        store,
        runner,
        provider,
        clock,
        observer,
        payload,
    }
}
async fn wait_backoff(store: &CrudStore) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(
                store
                    .compaction_runner_state("operation")
                    .await
                    .unwrap()
                    .unwrap()
                    .phase,
                RunnerPhase::Backoff { .. }
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn compaction_runner_portions_cover_huge_source_once_and_reuse_committed_result() {
    let f = fixture(&"漢字🌍".repeat(4000), vec![], true, true).await;
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Applied(_)
    ));
    let head = f.store.compaction_head("owner").await.unwrap();
    let head = head.unwrap_or_else(|| panic!("fixture owner must have an applied head"));
    let reference = f
        .store
        .compaction_checkpoint_source("ws", "thread", &head)
        .await
        .unwrap()
        .unwrap();
    let leaves = super::coverage::checkpoint_leaves(
        &f.store,
        "ws",
        &std::collections::BTreeSet::from(["thread".into()]),
        &reference,
    )
    .await
    .unwrap();
    assert_eq!(
        leaves.len(),
        1,
        "the published chain retains complete-source coverage across all portions"
    );
    let calls = f.provider.calls.lock().unwrap();
    assert!(calls.len() > 2);
    let mut restored = String::new();
    for (index, call) in calls.iter().enumerate() {
        let input: SummaryInput = serde_json::from_str(&call.messages[1].content).unwrap();
        assert!(index == 0 || !input.previous_summary.is_empty());
        for part in input.compact_units {
            restored.push_str(&part.text);
        }
        assert_eq!(call.max_tokens, Some(500));
        assert!(call.tools.is_none());
    }
    assert_eq!(restored, f.payload);
    let count = calls.len();
    drop(calls);
    let row = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_coverage",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 1);
    let row = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_checkpoint",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), count as i64);
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Applied(_)
    ));
    assert_eq!(f.provider.calls.lock().unwrap().len(), count);
    assert!(
        f.observer
            .ids
            .lock()
            .unwrap()
            .iter()
            .all(|id| id == "operation")
    );
}
#[tokio::test]
async fn compaction_retry_jitter_is_bounded_durable_and_respects_retry_after() {
    let f = fixture("small", vec![], true, false).await;
    let state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap()
        .claim(0)
        .unwrap();
    let jittered = f
        .runner
        .attempt_failed(&state, FailureKind::Transient, None)
        .unwrap();
    let RunnerPhase::Backoff { not_before_ms, .. } = jittered.phase else {
        panic!("missing retry");
    };
    assert!((2000..=2250).contains(&not_before_ms));
    assert!(not_before_ms > 2000, "fixture must exercise actual jitter");
    let restored: RunnerState =
        serde_json::from_str(&serde_json::to_string(&jittered).unwrap()).unwrap();
    assert_eq!(
        restored.action(not_before_ms - 1),
        RunnerAction::WaitUntil(not_before_ms)
    );
    assert_eq!(restored.retries, 1);
    assert_eq!(restored.deadline_ms, state.deadline_ms);
    let delayed = f
        .runner
        .attempt_failed(&state, FailureKind::Transient, Some(12000))
        .unwrap();
    assert_eq!(delayed.action(0), RunnerAction::WaitUntil(12000));
    let exhausted = f
        .runner
        .attempt_failed(&state, FailureKind::Transient, Some(state.deadline_ms))
        .unwrap();
    assert!(matches!(
        exhausted.phase,
        RunnerPhase::Failed {
            kind: FailureKind::Deadline
        }
    ));
}

#[tokio::test]
async fn compaction_runner_retry_after_and_stop_keep_durable_limits() {
    let f = fixture("small", vec![Reply::Transient, Reply::Success], true, false).await;
    let task = tokio::spawn({
        let runner = f.runner.clone();
        async move { runner.run(CancellationToken::new()).await }
    });
    f.provider.wait_calls(1).await;
    wait_backoff(&f.store).await;
    f.clock.advance(11_999);
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
    f.clock.advance(12_000);
    assert!(matches!(
        task.await.unwrap().unwrap(),
        CompactionExit::Applied(_)
    ));
    let state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((state.attempts, state.retries), (2, 1));

    let f = fixture("small", vec![Reply::Hang, Reply::Success], true, false).await;
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let task = tokio::spawn({
        let runner = f.runner.clone();
        async move { runner.run(token).await }
    });
    f.provider.wait_calls(1).await;
    // The owning Stop control path records cancellation before cancelling service work.
    f.store
        .compaction_finish("operation", "cancelled", "user_stop")
        .await
        .unwrap();
    cancel.cancel();
    assert_eq!(
        task.await.unwrap().unwrap(),
        CompactionExit::Reconcile(FailureKind::Cancelled)
    );
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
    assert_eq!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::Cancelled)
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn compaction_runner_attempt_and_operation_deadlines_drop_transport() {
    let f = fixture(
        "small",
        vec![Reply::Hang, Reply::Hang, Reply::Hang],
        true,
        false,
    )
    .await;
    let task = tokio::spawn({
        let runner = f.runner.clone();
        async move { runner.run(CancellationToken::new()).await }
    });
    f.provider.wait_calls(1).await;
    f.clock.advance(300_000);
    wait_backoff(&f.store).await;
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    f.clock.advance(302_250);
    f.provider.wait_calls(2).await;
    f.clock.advance(900_000);
    assert_eq!(
        task.await.unwrap().unwrap(),
        CompactionExit::Reconcile(FailureKind::Deadline)
    );
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
    assert_eq!(
        f.store
            .compaction_runner_state("operation")
            .await
            .unwrap()
            .unwrap()
            .deadline_ms,
        900_000
    );
}
#[tokio::test]
async fn compaction_runner_corrects_once_and_failed_fingerprint_does_not_regenerate() {
    let f = fixture("small", vec![], false, false).await;
    assert_eq!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::InsufficientEffect)
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), 2);
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
    assert_eq!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::InsufficientEffect)
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn admission_resumes_exact_plan_without_resetting_deadline() {
    let f = fixture("source", vec![], true, false).await;
    let entries = f
        .store
        .compaction_manifest_page("operation", false, 0, 0)
        .await
        .unwrap();
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let projection = super::frozen::capture(&f.store, "ws", "thread", &allowed, &[])
        .await
        .unwrap();
    let mut prepared = PreparedOperation {
        owner: "admission-owner".into(),
        execution_turn: "turn".into(),
        source_projection: Some(projection.clone()),
        expected_checkpoint: None,
        summary_basis: None,
        operation_deadline_ms: None,
        projection_version: 0,
        source_epochs: std::collections::BTreeMap::new(),
        plan: f.runner.snapshot.plan.clone(),
        manifest: entries,
        target_identity: "complete-target-identity".into(),
        target_tokens: 500,
    };
    let settings = CompactionSettings::default();
    let selection = &f.runner.snapshot.admission.selection;
    let first = admit_operation(
        &f.store,
        "ws",
        "thread",
        &settings,
        selection,
        None,
        f.runner.summarizer.as_ref(),
        prepared.clone(),
        10,
    )
    .await
    .unwrap();
    let recaptured = super::frozen::capture(&f.store, "ws", "thread", &allowed, &[])
        .await
        .unwrap();
    assert_ne!(projection.manifest_id, recaptured.manifest_id);
    assert_eq!(projection.identity_sha256, recaptured.identity_sha256);
    prepared.source_projection = Some(recaptured);
    prepared.operation_deadline_ms = Some(900100);
    let resumed = admit_operation(
        &f.store,
        "ws",
        "thread",
        &settings,
        selection,
        None,
        f.runner.summarizer.as_ref(),
        prepared.clone(),
        100,
    )
    .await
    .unwrap();
    assert_eq!(first.id, resumed.id);
    assert_eq!(resumed.admission.deadline_ms, 900010);
    let mut changed = prepared.clone();
    changed.target_identity = "changed-target".into();
    let changed = admit_operation(
        &f.store,
        "ws",
        "thread",
        &settings,
        selection,
        None,
        f.runner.summarizer.as_ref(),
        changed,
        100,
    )
    .await
    .unwrap();
    assert_ne!(first.id, changed.id);
    assert_eq!(changed.admission.deadline_ms, 900100);
    f.store
        .compaction_finish(&changed.id, "failed", "test_cleanup")
        .await
        .unwrap();
    f.store
        .compaction_finish(&first.id, "failed", "ineffective")
        .await
        .unwrap();
    prepared.operation_deadline_ms = Some(901000);
    prepared.source_projection = Some(
        super::frozen::capture(&f.store, "ws", "thread", &allowed, &[])
            .await
            .unwrap(),
    );
    let failed = admit_operation(
        &f.store,
        "ws",
        "thread",
        &settings,
        selection,
        None,
        f.runner.summarizer.as_ref(),
        prepared,
        1000,
    )
    .await
    .unwrap();
    assert_eq!(failed.id, first.id);
    assert_eq!(
        f.store
            .compaction_operation(&failed.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        "failed"
    );
    assert_eq!(*f.provider.count.borrow(), 0);
}

#[tokio::test]
async fn control_reconciliation_preserves_applied_result_after_deadline_and_stop() {
    let f = fixture("source", vec![], true, false).await;
    let applied = f.runner.run(CancellationToken::new()).await.unwrap();
    f.clock.advance(900001);
    assert_eq!(
        f.runner.reconcile(FailureKind::Deadline).await.unwrap(),
        applied
    );
    let cancel = CancellationToken::new();
    f.runner
        .store
        .compaction_finish(&f.runner.snapshot.id, "cancelled", "cancelled")
        .await
        .unwrap();
    cancel.cancel();
    assert!(cancel.is_cancelled());
    assert_eq!(
        f.runner.reconcile(FailureKind::Cancelled).await.unwrap(),
        applied
    );
    assert_eq!(*f.provider.count.borrow(), 1);

    let f = fixture("source", vec![], true, false).await;
    f.clock.advance(900001);
    assert_eq!(
        f.runner.reconcile(FailureKind::Deadline).await.unwrap(),
        CompactionExit::Failed(FailureKind::Deadline)
    );
    assert_eq!(
        f.store
            .compaction_operation("operation")
            .await
            .unwrap()
            .unwrap()
            .outcome
            .as_deref(),
        Some("deadline")
    );
    assert_eq!(*f.provider.count.borrow(), 0);
}

#[tokio::test]
async fn saved_candidate_survives_restart_without_regenerating() {
    struct PausedTarget(tokio::sync::Notify);
    #[async_trait]
    impl CompactionTarget for PausedTarget {
        async fn fits(&self, _: &str) -> Result<bool> {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    let f = fixture("source", vec![], true, false).await;
    let target = Arc::new(PausedTarget(tokio::sync::Notify::new()));
    let runner = Arc::new(CompactionRunner::new(
        f.store.clone(),
        "ws".into(),
        "thread".into(),
        f.runner.snapshot.clone(),
        f.runner.summarizer.clone(),
        target.clone(),
        f.observer.clone(),
        f.clock.clone(),
    ));
    let task = tokio::spawn(async move { runner.run(CancellationToken::new()).await });
    tokio::time::timeout(Duration::from_secs(10), target.0.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(
        f.store
            .compaction_runner_state("operation")
            .await
            .unwrap()
            .unwrap()
            .phase,
        RunnerPhase::Candidate { .. }
    ));
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Applied(_)
    ));
    assert_eq!(*f.provider.count.borrow(), 1);
}

#[tokio::test]
async fn interrupted_portion_resumes_from_saved_summary_with_same_retry_budget() {
    let f = fixture(
        &"漢字🌍".repeat(4000),
        vec![Reply::Success, Reply::Hang],
        true,
        false,
    )
    .await;
    let task = tokio::spawn({
        let runner = f.runner.clone();
        async move { runner.run(CancellationToken::new()).await }
    });
    f.provider.wait_calls(2).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(f.provider.active.load(Ordering::SeqCst), 0);
    let before = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    assert!(before.previous_checkpoint.is_some());
    assert_eq!(before.attempts, 2);
    let task = tokio::spawn({
        let runner = f.runner.clone();
        async move { runner.run(CancellationToken::new()).await }
    });
    wait_backoff(&f.store).await;
    f.clock.advance(2250);
    assert!(matches!(
        task.await.unwrap().unwrap(),
        CompactionExit::Applied(_)
    ));
    let calls = f.provider.calls.lock().unwrap();
    let interrupted: SummaryInput = serde_json::from_str(&calls[1].messages[1].content).unwrap();
    let resumed: SummaryInput = serde_json::from_str(&calls[2].messages[1].content).unwrap();
    assert!(!resumed.previous_summary.is_empty());
    assert_eq!(
        serde_json::to_value(interrupted).unwrap(),
        serde_json::to_value(resumed).unwrap()
    );
    drop(calls);
    let state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.retries, 1);
    let observations=f.store.database_connection().query_all_raw(Statement::from_string(DbBackend::Sqlite,
        "SELECT observation FROM compaction_attempt_observation WHERE operation_id='operation' ORDER BY attempt")).await.unwrap();
    assert_eq!(observations.len() as u64, state.attempts);
    let failed: pioneer_compaction::runner::AttemptObservation = serde_json::from_str(
        &observations[1]
            .try_get::<String>("", "observation")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(failed.failure, Some(FailureKind::Transient));
    assert_eq!(failed.input_tokens, None);
    assert!(failed.estimated_input_tokens > 0);
}

#[tokio::test]
async fn native_target_rechecks_complete_request_before_checkpoint_publication() {
    use pioneer_agent::compaction::request::NativeRequestProjection;
    use pioneer_provider::{ChatMessage, CompiledPromptPayload};
    let f = fixture("old history", vec![], true, false).await;
    let request = ChatRequest {
        model: "fixture-model".into(),
        messages: vec![
            ChatMessage::user("old history"),
            ChatMessage::user("protected current input"),
        ],
        temperature: None,
        max_tokens: Some(1024),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: Some(CompiledPromptPayload {
            stable_system_text: "fixed instruction ".repeat(4000),
            dynamic_system_text: String::new(),
            boundary_marker: String::new(),
            full_system_text: String::new(),
        }),
    };
    let projection = NativeRequestProjection::new(
        request,
        [0],
        vec![],
        ModelBudget::new(Some(4096), None, None),
        false,
    )
    .unwrap();
    let runner = CompactionRunner::new(
        f.store.clone(),
        "ws".into(),
        "thread".into(),
        f.runner.snapshot.clone(),
        f.runner.summarizer.clone(),
        Arc::new(NativeRequestTarget(projection)),
        f.observer.clone(),
        f.clock.clone(),
    );
    assert_eq!(
        runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::InsufficientEffect)
    );
    assert_eq!(*f.provider.count.borrow(), 2);
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
}

#[tokio::test]
async fn compaction_small_window_reduces_saved_tool_text_without_losing_call_or_original() {
    use pioneer_agent::compaction::request::NativeRequestProjection;
    use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef, ProviderToolCall};
    let f = fixture("fixture", vec![], true, false).await;
    let original = format!("BEGIN {} END", "long result ".repeat(6000));
    let payload = serde_json::json!({"storage":{"kind":"shell","stdout":original}}).to_string();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('sized-result','turn','sized-item','command_execution','completed',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [payload.clone().into()])).await.unwrap();
    let source = f
        .store
        .compaction_tool_item_reference("ws", "thread", "turn", "sized-item")
        .await
        .unwrap()
        .unwrap();
    let mut tool = ChatMessage::tool_result("call", "shell", &original);
    tool.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "turn:round".into(),
        complete: true,
        protected_input: false,
        inherited: false,
        sources: vec![MessageSourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        }],
    });
    let request = ChatRequest {
        model: "small".into(),
        messages: vec![
            ChatMessage::user("Keep this input exactly"),
            ChatMessage::assistant_tool_calls(
                None::<String>,
                vec![ProviderToolCall {
                    id: "call".into(),
                    name: "shell".into(),
                    arguments: "{}".into(),
                }],
            ),
            tool,
        ],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    let budget = ModelBudget::new(Some(4096), None, None);
    let full = NativeRequestProjection::full(request, vec![], budget.clone(), false).unwrap();
    assert!(!full.fits);
    let reduced = super::result_budget::shrink_results(
        &f.store.with_maintenance_access(),
        "ws",
        &full,
        &[],
        &budget,
        false,
    )
    .await
    .unwrap()
    .unwrap();
    assert!(reduced.fits);
    assert_eq!(&reduced.request.messages[..2], &full.request.messages[..2]);
    let result = &reduced.request.messages[2];
    assert_eq!(result.tool_call_id.as_deref(), Some("call"));
    assert_eq!(result.provenance, full.request.messages[2].provenance);
    assert!(result.content.starts_with("BEGIN") && result.content.ends_with("END"));
    assert!(
        result.content.contains("sized-item")
            && result.content.contains("threads_tools_result_read")
    );
    assert!(pioneer_compaction::text_tokens(&serde_json::to_string(result).unwrap()) < 4096);
    assert_eq!(
        super::history::reference_payload(&f.store, "ws", "thread", &source)
            .await
            .unwrap(),
        payload
    );
    let mut stale = full;
    stale.request.messages[2]
        .provenance
        .as_mut()
        .unwrap()
        .sources[0]
        .version = "item-revision:999".into();
    assert!(
        super::result_budget::shrink_results(&f.store, "ws", &stale, &[], &budget, false)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        super::result_budget::shrink_results(
            &f.store,
            "other-workspace",
            &stale,
            &[],
            &budget,
            false
        )
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(*f.provider.count.borrow(), 0);
}

#[tokio::test]
async fn pending_runtime_origins_resolve_versions_with_scope_and_whole_round_identity() {
    use pioneer_agent::compaction::history::{
        NativeHistoryLayout, PendingOriginKind, pending_origin,
    };
    use pioneer_provider::{ChatMessage, ProviderToolCall};
    let f = fixture("fixture", vec![], true, false).await;
    let db = f.store.database_connection();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input','turn',0,'text','accepted','{\"type\":\"text\",\"text\":\"accepted\"}',CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('assistant-source','turn','round',1,'assistant_round','{}','{}',CURRENT_TIMESTAMP)").await.unwrap();
    db.execute_unprepared("INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ('full-item','turn','tool-item','command_execution','completed','{\"storage\":{\"kind\":\"shell\",\"stdout\":\"full\"}}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let mut input = ChatMessage::user("accepted");
    input.provenance = Some(pending_origin(
        "ws",
        "thread",
        "turn",
        "input",
        PendingOriginKind::Input,
        "turn",
    ));
    let mut assistant = ChatMessage::assistant_tool_calls(
        None::<String>,
        vec![ProviderToolCall {
            id: "call".into(),
            name: "tool".into(),
            arguments: "{}".into(),
        }],
    );
    assistant.provenance = Some(pending_origin(
        "ws",
        "thread",
        "turn",
        "round",
        PendingOriginKind::Assistant,
        "round",
    ));
    let mut tool = ChatMessage::tool_result("call", "tool", "bounded output");
    tool.provenance = Some(pending_origin(
        "ws",
        "thread",
        "turn",
        "round",
        PendingOriginKind::ToolItem,
        "tool-item",
    ));
    let mut messages = vec![input, assistant, tool];
    assert!(
        !NativeHistoryLayout::from_messages("ws", "thread", &messages, &[10, 10, 10])
            .unwrap()
            .units[1]
            .complete
    );
    assert!(
        origins::resolve_message_origins(
            &f.store,
            "ws",
            "thread",
            "turn",
            &Default::default(),
            &mut messages
        )
        .await
        .is_err()
    );
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    origins::resolve_message_origins(&f.store, "ws", "thread", "turn", &allowed, &mut messages)
        .await
        .unwrap();
    assert_eq!(
        messages[0].provenance.as_ref().unwrap().sources[0].version,
        "input-revision:1"
    );
    assert_eq!(
        messages[1].provenance.as_ref().unwrap().sources[0].id,
        "assistant-source"
    );
    assert_eq!(
        messages[2].provenance.as_ref().unwrap().sources[0].id,
        "full-item"
    );
    assert_eq!(messages[2].content, "bounded output");
    let layout =
        NativeHistoryLayout::from_messages("ws", "thread", &messages, &[10, 10, 10]).unwrap();
    assert!(layout.units[0].protected_input);
    assert!(layout.units[1].complete);
    assert_eq!(layout.message_indexes[1], vec![1, 2]);
    assert_eq!(*f.provider.count.borrow(), 0);
}

#[tokio::test]
async fn interrupted_execution_turn_fences_summary_commit_before_service_cancellation() {
    struct Paused(tokio::sync::Notify);
    #[async_trait]
    impl CompactionTarget for Paused {
        async fn fits(&self, _: &str) -> Result<bool> {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    let f = fixture("source", vec![], true, false).await;
    f.store
        .compaction_bind_execution_turn("operation", "turn")
        .await
        .unwrap();
    assert!(
        f.store
            .compaction_bind_execution_turn("operation", "other-turn")
            .await
            .is_err()
    );
    let target = Arc::new(Paused(tokio::sync::Notify::new()));
    let runner = CompactionRunner::new(
        f.store.clone(),
        "ws".into(),
        "thread".into(),
        f.runner.snapshot.clone(),
        f.runner.summarizer.clone(),
        target.clone(),
        f.observer.clone(),
        f.clock.clone(),
    );
    let task = tokio::spawn(async move { runner.run(CancellationToken::new()).await });
    tokio::time::timeout(Duration::from_secs(10), target.0.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // This is the durable Stop boundary used by Gateway. The compaction row
    // intentionally remains running to exercise the publication race itself.
    f.store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='interrupted' WHERE id='turn'")
        .await
        .unwrap();
    assert_eq!(
        f.store
            .compaction_operation("operation")
            .await
            .unwrap()
            .unwrap()
            .status,
        "running"
    );
    assert_eq!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::Cancelled)
    );
    assert!(f.store.compaction_head("owner").await.unwrap().is_none());
    assert_eq!(*f.provider.count.borrow(), 1);
}

#[tokio::test]
async fn stale_head_is_cas_guard_but_not_summary_basis_during_rebuild() {
    let f = fixture("old source", vec![], true, false).await;
    let CompactionExit::Applied(head) = f.runner.run(CancellationToken::new()).await.unwrap()
    else {
        panic!("first checkpoint missing");
    };
    let entries = f
        .store
        .compaction_manifest_page("operation", false, 0, 0)
        .await
        .unwrap();
    let mut prepared = PreparedOperation {
        owner: "owner".into(),
        execution_turn: "turn".into(),
        source_projection: None,
        expected_checkpoint: Some(head.clone()),
        summary_basis: None,
        operation_deadline_ms: None,
        projection_version: 0,
        source_epochs: std::collections::BTreeMap::new(),
        plan: f.runner.snapshot.plan.clone(),
        manifest: entries,
        target_identity: "rebuild-target".into(),
        target_tokens: 500,
    };
    let settings = CompactionSettings::default();
    let selection = &f.runner.snapshot.admission.selection;
    // A live summary cannot silently be discarded from the same owner.
    assert!(
        admit_operation(
            &f.store,
            "ws",
            "thread",
            &settings,
            selection,
            None,
            f.runner.summarizer.as_ref(),
            prepared.clone(),
            0
        )
        .await
        .is_err()
    );
    f.store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload='{\"text\":\"corrected source\"}' WHERE id='source'",
        )
        .await
        .unwrap();
    assert!(
        f.store
            .compaction_checkpoint_source("ws", "thread", &head)
            .await
            .unwrap()
            .is_none()
    );
    prepared.projection_version = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    prepared.manifest[0].source = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let snapshot = admit_operation(
        &f.store,
        "ws",
        "thread",
        &settings,
        selection,
        None,
        f.runner.summarizer.as_ref(),
        prepared,
        0,
    )
    .await
    .unwrap();
    assert_eq!(snapshot.expected_checkpoint, Some(head.clone()));
    let state = f
        .store
        .compaction_runner_state(&snapshot.id)
        .await
        .unwrap()
        .unwrap();
    assert!(state.previous_checkpoint.is_none());
    let runner = CompactionRunner::new(
        f.store.clone(),
        "ws".into(),
        "thread".into(),
        snapshot,
        f.runner.summarizer.clone(),
        Arc::new(Target(true)),
        f.observer.clone(),
        f.clock.clone(),
    );
    let CompactionExit::Applied(new_head) = runner.run(CancellationToken::new()).await.unwrap()
    else {
        panic!("rebuilt checkpoint missing");
    };
    assert_ne!(head, new_head);
    let checkpoint = f
        .store
        .compaction_checkpoint(&new_head)
        .await
        .unwrap()
        .unwrap();
    assert!(checkpoint.previous.is_none());
    assert_eq!(f.provider.calls.lock().unwrap().len(), 2);
    let calls = f.provider.calls.lock().unwrap();
    let input: SummaryInput = serde_json::from_str(&calls[1].messages[1].content).unwrap();
    assert!(input.previous_summary.is_empty());
    assert!(input.compact_units[0].text.contains("corrected source"));
    drop(calls);
    assert!(
        f.store
            .compaction_checkpoint(&head)
            .await
            .unwrap()
            .is_some()
    );
}

struct SmallWindowMain;
#[async_trait]
impl Provider for SmallWindowMain {
    fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
        use pioneer_provider::{InputTypeSupport, ProviderCapabilities, ProviderInputCapabilities};
        ProviderCapabilities {
            streaming: true,
            vision: true,
            tool_calling: true,
            embeddings: false,
            transcription: false,
            input_types: ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::native_inline_only(),
                image: InputTypeSupport::native_inline_only(),
                audio: InputTypeSupport::native_inline_only(),
                video: InputTypeSupport::native_inline_only(),
            },
        }
    }
    fn name(&self) -> &str {
        "openai"
    }
    async fn chat(&self, _: ChatRequest) -> Result<ChatResponse> {
        anyhow::bail!("main model must not run during preparation")
    }
    async fn stream_chat(
        &self,
        _: ChatRequest,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<StreamChunk>>> {
        anyhow::bail!("main model must not run during preparation")
    }
    async fn list_models(&self) -> Result<Vec<pioneer_protocol::ProviderModelInfo>> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn native_preparation_applies_real_runner_and_reuses_checkpoint_without_generation() {
    use pioneer_agent::compaction::controller::NativeContext;
    use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef, ProviderRegistry};
    let old = "old fact ".repeat(4000);
    let f = fixture(&old, vec![], true, false).await;
    let source = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let providers = ProviderRegistry::new(|_| "fixture-key".into());
    providers
        .insert("summary-fixture", f.provider.clone())
        .unwrap();
    providers
        .insert("main-fixture", Arc::new(SmallWindowMain))
        .unwrap();
    let context = NativeContext {
        overflow_recovery: false,
        recovery_deadline_ms: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        conversation_thread_id: None,
        provider_instance: "main-fixture".into(),
        provider: providers
            .get_or_create_for_workspace("ws", "main-fixture")
            .unwrap(),
        events: Arc::new(ExecutionEventHub::new()),
        cancellation: CancellationToken::new(),
    };
    let mut history = ChatMessage::assistant(old);
    history.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "old-work".into(),
        sources: vec![MessageSourceRef {
            scope: source.scope,
            id: source.id,
            version: source.version,
        }],
        complete: true,
        protected_input: false,
        inherited: false,
    });
    use base64::Engine;
    let mut current = ChatMessage::user("Continue");
    current
        .content_parts
        .push(pioneer_provider::MessageContentPart::file(
            pioneer_provider::MessageAttachment {
                mime_type: "text/plain".into(),
                name: Some("retained.txt".into()),
                size_bytes: None,
                sha256: None,
                artifact: None,
                source: pioneer_provider::AttachmentDataSource::Bytes {
                    base64_data: base64::engine::general_purpose::STANDARD
                        .encode("retained media evidence ".repeat(100)),
                },
            },
        ));
    let request = ChatRequest {
        model: "gpt-4".into(),
        messages: vec![history, current],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    let settings = CompactionSettings {
        selection: Some(ModelSelection {
            transport: Transport::Api,
            instance: "summary-fixture".into(),
            model: "summary-model".into(),
            effort: None,
        }),
    };
    let raw_request = request.clone();
    let prepared = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &settings,
        &context,
        request,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await
    .unwrap();
    assert_eq!(prepared.request.max_tokens, Some(2048));
    assert_eq!(prepared.request.messages.len(), 2);
    assert_eq!(prepared.request.messages[1].content, "Continue");
    assert_eq!(prepared.request.messages[1].content_parts.len(), 1);
    let pioneer_provider::MessageContentPart::File { file } =
        &prepared.request.messages[1].content_parts[0]
    else {
        panic!("lost retained media")
    };
    assert!(file.sha256.is_some());
    assert!(prepared.request.messages[0].content.contains(HEADINGS[0]));
    assert!(prepared.receipt.identity.checkpoint.is_some());
    let count = f.provider.calls.lock().unwrap().len();
    assert!(count > 0);
    let checkpoint_ref = f
        .store
        .compaction_checkpoint_source(
            "ws",
            "thread",
            prepared.receipt.identity.checkpoint.as_deref().unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let leaves = super::coverage::checkpoint_leaves(
        &f.store,
        "ws",
        &std::collections::BTreeSet::from(["thread".into()]),
        &checkpoint_ref,
    )
    .await
    .unwrap();
    assert_eq!(leaves.len(), 1);
    assert_eq!(leaves.first().unwrap().thread, "thread");
    assert!(
        super::coverage::checkpoint_leaves(
            &f.store,
            "ws",
            &std::collections::BTreeSet::from(["other-thread".into()]),
            &checkpoint_ref,
        )
        .await
        .is_err()
    );
    assert!(
        f.provider
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.model == "summary-model" && r.tools.is_none() && r.reasoning.is_none())
    );
    let again = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &settings,
        &context,
        prepared.request,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        again.receipt.identity.checkpoint,
        prepared.receipt.identity.checkpoint
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), count);
    let restored = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &settings,
        &context,
        raw_request.clone(),
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await
    .unwrap();
    assert_eq!(restored.request.messages, again.request.messages);
    assert_eq!(f.provider.calls.lock().unwrap().len(), count);

    // An already projected head must not bypass coverage normalization. Keep
    // equal-source originals out of the request and re-read the durable summary
    // instead of trusting a caller's body attached to a valid head reference.
    let mut duplicated = restored.request.clone();
    duplicated.messages[0].content = "non-authoritative cached summary".into();
    duplicated
        .messages
        .insert(1, raw_request.messages[0].clone());
    let normalized = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &settings,
        &context,
        duplicated,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await
    .unwrap();
    assert_eq!(normalized.request.messages, restored.request.messages);
    assert_eq!(f.provider.calls.lock().unwrap().len(), count);

    let mut earlier_boundary = raw_request.clone();
    earlier_boundary.messages.remove(0);
    let incompatible = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &settings,
        &context,
        earlier_boundary,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await;
    assert!(
        incompatible
            .err()
            .unwrap()
            .to_string()
            .contains("selected history boundary")
    );
    // A versioned origin cannot silently pass the cheap fitting path after edit.
    let id = &raw_request.messages[0].provenance.as_ref().unwrap().sources[0].id;
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload='changed source' WHERE id=?",
            [id.clone().into()],
        ))
        .await
        .unwrap();
    let stale = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &settings,
        &context,
        raw_request,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await;
    assert!(stale.err().unwrap().to_string().contains("source changed"));
    assert!(
        super::coverage::checkpoint_leaves(
            &f.store,
            "ws",
            &std::collections::BTreeSet::from(["thread".into()]),
            &checkpoint_ref,
        )
        .await
        .is_err()
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), count);
}

#[tokio::test]
async fn compaction_empty_task_policy_never_reads_unselected_parent_payload() {
    // This existing fixture source is deliberately not a typed Turn event.
    // An Empty/Custom context must not materialize it merely to discard it.
    let f = fixture("unparseable unselected parent payload", vec![], true, false).await;
    for mode in [
        pioneer_protocol::TaskAgentContextMode::Empty,
        pioneer_protocol::TaskAgentContextMode::Custom,
    ] {
        let policy = pioneer_protocol::TaskAgentContextPolicy {
            mode,
            ..super::frozen::default_task_context_policy()
        };
        let json = super::test_support::capture_selected_line_json(
            &f.store,
            "ws",
            "thread",
            None,
            Some(&policy),
        )
        .await
        .unwrap();
        let history = crate::turn_runtime_snapshot::restore_history_json(
            &f.store,
            "ws",
            &std::collections::BTreeSet::from(["thread".into()]),
            &json,
        )
        .await
        .unwrap();
        assert!(history.is_empty());
    }
    assert!(
        super::test_support::capture_line_json(&f.store, "ws", "thread", None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn canonical_line_snapshot_keeps_completed_rounds_and_exact_ui_aliases() {
    use pioneer_crud::NewTurnLlmContextEntry;
    use pioneer_protocol::{
        ItemCompletedNotification, ItemStartedNotification, PersistedActorRef, SandboxMode,
        TurnItem, UserInput,
    };
    use pioneer_provider::{
        CanonicalProviderRoundEnvelope, ChatMessage, ProviderCallIdentity, ProviderToolCall, Role,
    };
    let f = fixture("unused", vec![], true, false).await;
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    let thread = f.store.get_thread_model("thread").await.unwrap().unwrap();
    let (_, turn) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
    // The runner fixture seeded only a scope shell. Build an actual new Turn
    // through the projector so its immutable initiator is recorded normally.
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();

    f.store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[UserInput::Text {
                text: "original request".into(),
                text_elements: vec![],
            }],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_unprepared("UPDATE turn SET send_mode='agent' WHERE id='turn'")
        .await
        .unwrap();
    for (round, item, sequence) in [("round-one", "item-one", 1), ("round-two", "item-two", 3)] {
        f.store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: TurnItem::Reasoning {
                        id: round.into(),
                        summary: vec![],
                        content: vec![],
                    },
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
        let envelope = CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: round.into(),
            termination: ProviderTermination::ToolCalls,
            message: ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                Some("recorded reasoning"),
                vec![ProviderToolCall {
                    id: "reused-provider-id".into(),
                    name: "read_file".into(),
                    arguments: "{}".into(),
                }],
                None,
            ),
            calls: vec![ProviderCallIdentity {
                provider_call_id: "reused-provider-id".into(),
                turn_item_id: item.into(),
                ordinal: 0,
            }],
        };
        f.store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: "turn".into(),
                item_id: Some(round.into()),
                attempt_id: None,
                sequence,
                source: "assistant_round".into(),
                tool_name: None,
                payload: serde_json::to_string(&envelope).unwrap(),
                output_policy_snapshot: "{}".into(),
                created_at: chrono::Utc::now().fixed_offset(),
                expires_at: None,
            })
            .await
            .unwrap();
        f.store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: TurnItem::Reasoning {
                        id: round.into(),
                        summary: vec![],
                        content: vec!["recorded reasoning".into()],
                    },
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
        if sequence == 1 {
            let view = pioneer_tools::ToolResultView::Json {
                value: serde_json::to_value(ChatMessage::tool_result(
                    "reused-provider-id",
                    "read_file",
                    "first completed result",
                ))
                .unwrap(),
                truncated: false,
            };
            f.store
                .insert_turn_llm_context(NewTurnLlmContextEntry {
                    turn_id: "turn".into(),
                    item_id: Some(item.into()),
                    attempt_id: None,
                    sequence: 2,
                    source: "tool_result_v2".into(),
                    tool_name: Some("read_file".into()),
                    payload: serde_json::to_string(&view).unwrap(),
                    output_policy_snapshot: "{}".into(),
                    created_at: chrono::Utc::now().fixed_offset(),
                    expires_at: None,
                })
                .await
                .unwrap();
        }
    }
    for id in ["observation-one", "observation-two"] {
        f.store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: TurnItem::AgentMessage {
                        id: id.into(),
                        text: "same observed text".into(),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
    }
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let view = pioneer_tools::ToolResultView::Json {
        value: serde_json::to_value(ChatMessage::tool_result(
            "reused-provider-id",
            "read_file",
            "late result",
        ))
        .unwrap(),
        truncated: false,
    };
    f.store
        .insert_turn_llm_context(NewTurnLlmContextEntry {
            turn_id: "turn".into(),
            item_id: Some("item-two".into()),
            attempt_id: None,
            sequence: 4,
            source: "tool_result_v2".into(),
            tool_name: Some("read_file".into()),
            payload: serde_json::to_string(&view).unwrap(),
            output_policy_snapshot: "{}".into(),
            created_at: chrono::Utc::now().fixed_offset(),
            expires_at: None,
        })
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    let frozen = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert_eq!(frozen[0].content, "original request");
    assert_eq!(
        frozen[0].provenance.as_ref().unwrap().sources[0].scope,
        "input:turn"
    );
    assert_eq!(
        frozen
            .iter()
            .filter(|message| message.role == Role::Tool)
            .count(),
        1
    );
    assert_eq!(
        frozen
            .iter()
            .filter(|message| message.content == "same observed text")
            .count(),
        2
    );
    assert_eq!(
        frozen
            .iter()
            .filter(|message| message.reasoning_content.is_some())
            .count(),
        1
    );
    assert!(frozen.iter().all(|message| message.provenance.is_some()));
    assert!(
        !frozen
            .iter()
            .any(|message| message.content == "late result")
    );
    let current_fence = f.store.compaction_history_read_fence().await.unwrap();
    let leaves = frozen
        .iter()
        .flat_map(|message| &message.provenance.as_ref().unwrap().sources)
        .map(|source| SourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        })
        .collect();
    let exact =
        super::history::load_exact_line_history(&f.store, "ws", "thread", &current_fence, &leaves)
            .await
            .unwrap();
    assert_eq!(
        exact, frozen,
        "exact reconstruction keeps the completed call/result unit and excludes the later round result"
    );
    let current = super::history::load_line_history(&f.store, "ws", "thread", None, &current_fence)
        .await
        .unwrap();
    assert_eq!(
        current
            .iter()
            .filter(|message| message.role == Role::Tool)
            .count(),
        2
    );
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    // New TaskRun writers use this entry point: no transcript in the descriptor,
    // exact current line on restoration, and the launch turn can be excluded.
    let current_json = super::test_support::capture_line_json(&f.store, "ws", "thread", None)
        .await
        .unwrap();
    assert!(!current_json.contains("original request"));
    assert!(!current_json.contains("late result"));
    let captured =
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &current_json)
            .await
            .unwrap();
    assert_eq!(captured, current);
    // Policy applies to the separately delivered history, and never cuts a
    // canonical tool round into an orphan call or result.
    for mode in [
        pioneer_protocol::TaskAgentContextMode::LastNTurns,
        pioneer_protocol::TaskAgentContextMode::InheritParent,
        pioneer_protocol::TaskAgentContextMode::Empty,
        pioneer_protocol::TaskAgentContextMode::Custom,
        pioneer_protocol::TaskAgentContextMode::SummaryOnly,
    ] {
        let policy = pioneer_protocol::TaskAgentContextPolicy {
            mode,
            max_turns: Some(1),
            ..super::frozen::default_task_context_policy()
        };
        let json = super::test_support::capture_selected_line_json(
            &f.store,
            "ws",
            "thread",
            None,
            Some(&policy),
        )
        .await
        .unwrap();
        assert!(!json.contains("original request"));
        let restored =
            crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &json)
                .await
                .unwrap();
        match mode {
            pioneer_protocol::TaskAgentContextMode::LastNTurns
            | pioneer_protocol::TaskAgentContextMode::InheritParent => {
                assert_eq!(restored, current)
            }
            _ => assert!(restored.is_empty()),
        }
    }
    let excluded = super::test_support::capture_line_json(&f.store, "ws", "thread", Some("turn"))
        .await
        .unwrap();
    assert!(
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &excluded,)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        super::test_support::capture_line_json(&f.store, "foreign-workspace", "thread", None)
            .await
            .is_err()
    );
    let snapshot = super::frozen::capture(&f.store, "ws", "thread", &allowed, &frozen)
        .await
        .unwrap();
    let descriptor_json = serde_json::to_string(&snapshot).unwrap();
    assert!(!descriptor_json.contains("original request"));
    assert!(!descriptor_json.contains("first completed result"));
    // Exercise the production runtime-snapshot reader, including its explicit
    // compatibility branch for already accepted legacy arrays.
    let mut runtime = crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
        "thread",
        "ws",
        "turn",
        pioneer_protocol::ThreadMode::Agent,
        &pioneer_agent::AgentTurnHookRuntimeContext::default(),
        "fixture-model",
        "fixture-instance",
        None,
        &std::collections::HashMap::new(),
        &[],
        &[],
        &[],
        &std::collections::HashMap::new(),
        &[],
        &[],
    )
    .unwrap();
    runtime.history_json = descriptor_json;
    f.store.upsert_turn_runtime_snapshot(runtime).await.unwrap();
    let stored = f
        .store
        .get_turn_runtime_snapshot("turn")
        .await
        .unwrap()
        .unwrap();
    let (_, runtime_history) =
        crate::turn_runtime_snapshot::restored_conversation_scope_from_snapshot(&f.store, &stored)
            .await
            .unwrap();
    assert_eq!(runtime_history, frozen);
    let legacy = serde_json::to_string(&vec![pioneer_provider::ChatMessage::user(
        "accepted legacy projection",
    )])
    .unwrap();
    let legacy =
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &legacy)
            .await
            .unwrap();
    assert_eq!(legacy[0].content, "accepted legacy projection");
    assert!(
        legacy[0].provenance.is_none(),
        "legacy text must not acquire invented source coverage"
    );
    let restored = super::frozen::restore(&f.store, "ws", &allowed, &snapshot)
        .await
        .unwrap();
    assert_eq!(restored, frozen);
    for (restored, original) in restored.iter().zip(&frozen) {
        assert_eq!(restored.provenance, original.provenance);
    }
    assert!(
        super::frozen::restore(
            &f.store,
            "ws",
            &std::collections::BTreeSet::new(),
            &snapshot
        )
        .await
        .is_err()
    );
    assert!(
        super::frozen::restore(&f.store, "foreign", &allowed, &snapshot)
            .await
            .is_err()
    );
    let refs = f
        .store
        .compaction_frozen_history_page("ws", "thread", &snapshot.manifest_id, 0)
        .await
        .unwrap();
    assert!(
        !serde_json::to_string(&refs)
            .unwrap()
            .contains("recorded reasoning")
    );
    // Appends visible to a new line read never extend an accepted snapshot.
    assert!(current.len() > restored.len());
    let input = &frozen[0].provenance.as_ref().unwrap().sources[0];
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "UPDATE turn_input SET payload=replace(payload,'original request','changed request') WHERE id=?",[input.id.clone().into()])).await.unwrap();
    assert!(
        super::frozen::restore(&f.store, "ws", &allowed, &snapshot)
            .await
            .is_err(),
        "edited source must not silently rewrite a frozen execution context"
    );
    assert!(f.provider.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn frozen_fork_uses_compatible_ancestor_without_importing_future_work() {
    use pioneer_crud::compaction::{CommitOutcome, SourceAssertion};
    use pioneer_provider::ChatMessage;
    async fn publish(
        store: &CrudStore,
        owner: &str,
        end: usize,
        previous: Option<&str>,
        start: usize,
    ) -> String {
        let page = store
            .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap();
        let assertions = page.entries[start..end]
            .iter()
            .map(|row| SourceAssertion {
                revision: Some(
                    row.reference
                        .version
                        .strip_prefix("event-revision:")
                        .unwrap()
                        .parse()
                        .unwrap(),
                ),
                kind: CanonicalSource::Event,
                turn_id: "turn".into(),
                id: row.reference.id.clone(),
                payload: row.payload.clone().expect("small fixture source is inline"),
            })
            .collect::<Vec<_>>();
        let selection = ModelSelection {
            transport: Transport::Api,
            instance: "fixture".into(),
            model: "fixture".into(),
            effort: None,
        };
        let version = store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap();
        let id = format!("operation-{end}");
        let coverage = assertions
            .iter()
            .map(SourceAssertion::reference)
            .collect::<Vec<_>>();
        let snapshot = OperationSnapshot {
            id: id.clone(),
            owner: owner.into(),
            expected_checkpoint: previous.map(str::to_owned),
            projection_version: version,
            source_epochs: std::collections::BTreeMap::from([("thread".into(), version)]),
            admission: CompactionSettings::default()
                .admit(&selection, None, 0)
                .unwrap(),
            plan: CompactionPlan {
                mode: CompactionMode::Normal,
                compact: (start..end).collect(),
                retain: vec![],
                coverage: coverage.clone(),
                fingerprint: id.clone(),
            },
        };
        store
            .compaction_admit("ws", "thread", &snapshot)
            .await
            .unwrap();
        let checkpoint = Checkpoint {
            id: format!("checkpoint-{end}"),
            operation_id: id,
            format_version: 1,
            owner: owner.into(),
            previous: previous.map(str::to_owned),
            coverage,
            summary: HEADINGS
                .iter()
                .map(|heading| format!("{heading}\nState through {end}.\n"))
                .collect(),
            selection,
            projection_version: version,
        };
        store
            .compaction_save_candidate(&checkpoint, 0)
            .await
            .unwrap();
        assert_eq!(
            store
                .compaction_apply(&checkpoint, previous, &assertions)
                .await
                .unwrap(),
            CommitOutcome::Applied
        );
        checkpoint.id
    }
    let f = fixture("unused seed", vec![], true, false).await;
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    let owner = super::native::native_owner("ws", "thread");
    let mut first = None;
    let mut at_sixty = None;
    for n in 1..=100 {
        f.store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: format!("work-{n:03}"),
                        text: format!("work {n}"),
                        phase: Default::default(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
        if n == 40 {
            first = Some(publish(&f.store, &owner, 40, None, 0).await);
        }
        if n == 60 {
            at_sixty = Some(f.store.compaction_history_read_fence().await.unwrap());
        }
    }
    let first = first.unwrap();
    let head = publish(&f.store, &owner, 100, Some(&first), 40).await;
    let raw =
        super::history::load_task_line_history(&f.store, "ws", "thread", None, &at_sixty.unwrap())
            .await
            .unwrap();
    assert_eq!(raw.len(), 60);
    let mut selected = raw.clone();
    assert_eq!(
        super::checkpoint::project_compatible_checkpoint(
            &f.store,
            "ws",
            "thread",
            &owner,
            &head,
            &std::collections::BTreeSet::from(["thread".to_owned()]),
            &mut selected,
        )
        .await
        .unwrap(),
        Some(first.clone())
    );
    assert_eq!(selected.len(), 21);
    assert!(selected[0].content.contains("State through 40"));
    assert_eq!(
        selected[1..]
            .iter()
            .map(|message| message.content.clone())
            .collect::<Vec<_>>(),
        (41..=60).map(|n| format!("work {n}")).collect::<Vec<_>>()
    );
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let replacement_source = &raw[19].provenance.as_ref().unwrap().sources[0];
    let replaced = super::frozen::remove_delivered_projection(
        &f.store,
        "ws",
        "thread",
        &allowed,
        selected.clone(),
        &[SourceRef {
            scope: replacement_source.scope.clone(),
            id: replacement_source.id.clone(),
            version: replacement_source.version.clone(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(
        replaced
            .iter()
            .map(|message| message.content.clone())
            .collect::<Vec<_>>(),
        (1..=60)
            .filter(|n| *n != 20)
            .map(|n| format!("work {n}"))
            .collect::<Vec<_>>(),
        "a covered transport copy is removed through exact original projection, preserving every other source"
    );
    let mut inherited = selected.clone();
    for message in &mut inherited {
        message.provenance.as_mut().unwrap().inherited = true;
    }
    let disjoint = super::frozen::compose_frozen_basis(
        &f.store,
        "ws",
        "thread",
        &allowed,
        &inherited,
        &raw[40..],
    )
    .await
    .unwrap();
    assert_eq!(disjoint.len(), 21);
    assert!(
        disjoint[0].content.contains("State through 40"),
        "disjoint prepared summary remains intact"
    );
    let joined =
        super::frozen::compose_frozen_basis(&f.store, "ws", "thread", &allowed, &inherited, &raw)
            .await
            .unwrap();
    assert_eq!(
        joined.len(),
        60,
        "overlapping summary is replaced by exact originals once"
    );
    assert_eq!(
        joined
            .iter()
            .map(|message| message.content.clone())
            .collect::<Vec<_>>(),
        (1..=60).map(|n| format!("work {n}")).collect::<Vec<_>>()
    );
    assert!(
        joined
            .iter()
            .all(|message| !message.provenance.as_ref().unwrap().inherited)
    );
    let joined_snapshot = super::frozen::capture(&f.store, "ws", "thread", &allowed, &joined)
        .await
        .unwrap();
    assert_eq!(
        super::frozen::restore(&f.store, "ws", &allowed, &joined_snapshot)
            .await
            .unwrap(),
        joined
    );
    assert_eq!(
        f.store.compaction_head(&owner).await.unwrap(),
        Some(head.clone())
    );
    let frozen = super::frozen::capture(&f.store, "ws", "thread", &allowed, &selected)
        .await
        .unwrap();
    assert_eq!(
        super::frozen::restore(&f.store, "ws", &allowed, &frozen)
            .await
            .unwrap(),
        selected
    );
    assert_eq!(
        f.store.compaction_head(&owner).await.unwrap(),
        Some(head.clone())
    );
    let policy = pioneer_protocol::TaskAgentContextPolicy {
        mode: pioneer_protocol::TaskAgentContextMode::SummaryOnly,
        ..super::frozen::default_task_context_policy()
    };
    let summary_json = super::test_support::capture_selected_line_json(
        &f.store,
        "ws",
        "thread",
        None,
        Some(&policy),
    )
    .await
    .unwrap();
    let summary_history =
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &summary_json)
            .await
            .unwrap();
    assert_eq!(summary_history.len(), 1);
    assert_eq!(
        summary_history[0].provenance.as_ref().unwrap().sources[0].id,
        head
    );
    let no_summary = pioneer_protocol::TaskAgentContextPolicy {
        include_parent_summary: false,
        ..policy
    };
    let json = super::test_support::capture_selected_line_json(
        &f.store,
        "ws",
        "thread",
        None,
        Some(&no_summary),
    )
    .await
    .unwrap();
    assert!(
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &json,)
            .await
            .unwrap()
            .is_empty()
    );
    let mut earlier: Vec<ChatMessage> = raw[..20].to_vec();
    assert_eq!(
        super::checkpoint::project_compatible_checkpoint(
            &f.store,
            "ws",
            "thread",
            &owner,
            &head,
            &allowed,
            &mut earlier,
        )
        .await
        .unwrap(),
        None
    );
    assert_eq!(earlier, raw[..20]);
    assert!(
        super::checkpoint::project_compatible_checkpoint(
            &f.store,
            "ws",
            "thread",
            "foreign-owner",
            &head,
            &allowed,
            &mut earlier,
        )
        .await
        .is_err()
    );
    // Later unrelated payload damage must not prevent reconstruction of the
    // accepted 1..60 sources or pull later work into the frozen projection.
    f.store.database_connection().execute_unprepared(
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('later-malformed','thread','turn',101,'item_completed','{invalid',CURRENT_TIMESTAMP)"
    ).await.unwrap();
    let after_unrelated_damage =
        super::frozen::compose_frozen_basis(&f.store, "ws", "thread", &allowed, &inherited, &raw)
            .await
            .unwrap();
    assert_eq!(after_unrelated_damage, joined);
    assert!(
        f.provider.calls.lock().unwrap().is_empty(),
        "compatible selection needs no new generation"
    );
}

#[tokio::test]
async fn native_media_preparation_materializes_full_request_without_main_provider_call() {
    use base64::Engine;
    use pioneer_agent::compaction::controller::NativeContext;
    use pioneer_provider::{
        AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart, ProviderRegistry,
    };
    let f = fixture("completed", vec![], true, false).await;
    let providers = ProviderRegistry::with_provider("media-fixture", Arc::new(SmallWindowMain));
    let provider = providers
        .get_or_create_for_workspace("ws", "media-fixture")
        .unwrap();
    let context = NativeContext {
        overflow_recovery: false,
        recovery_deadline_ms: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        conversation_thread_id: None,
        provider_instance: "media-fixture".into(),
        provider,
        events: Arc::new(ExecutionEventHub::new()),
        cancellation: CancellationToken::new(),
    };
    let mut message = ChatMessage::user("Inspect these inputs");
    let image = MessageAttachment {
        mime_type: "image/png".into(), name: Some("pixel.png".into()), size_bytes: None, sha256: None, artifact: None,
        source: AttachmentDataSource::Bytes { base64_data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jU1cAAAAASUVORK5CYII=".into() } };
    message.content_parts.push(MessageContentPart::image(image));
    message
        .content_parts
        .push(MessageContentPart::file(MessageAttachment {
            mime_type: "text/plain".into(),
            name: Some("evidence.txt".into()),
            size_bytes: None,
            sha256: None,
            artifact: None,
            source: AttachmentDataSource::Bytes {
                base64_data: base64::engine::general_purpose::STANDARD
                    .encode("Доказательство 🦀".repeat(100)),
            },
        }));
    let request = ChatRequest {
        model: "gpt-4o".into(),
        messages: vec![message],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    let prepared = super::test_support::prepare_native_request(
        &f.store,
        &providers,
        &CompactionSettings::default(),
        &context,
        request,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await
    .unwrap();
    assert_eq!(prepared.request.messages.len(), 1);
    for part in &prepared.request.messages[0].content_parts {
        let attachment = match part {
            MessageContentPart::Image { image } => image,
            MessageContentPart::File { file } => file,
            _ => panic!("unexpected media"),
        };
        assert!(matches!(
            attachment.source,
            AttachmentDataSource::Bytes { .. }
        ));
        assert!(attachment.sha256.is_some());
        assert!(attachment.size_bytes.unwrap() > 0);
    }
    assert!(
        f.store
            .compaction_head(&super::native::native_owner("ws", "thread"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn nested_task_basis_uses_accepted_h_manifest_without_live_ancestor_append() {
    check_nested_task_basis(false).await;
}

#[tokio::test]
async fn active_legacy_task_basis_continues_by_reference_without_rewriting_snapshot() {
    check_nested_task_basis(true).await;
}

async fn check_nested_task_basis(legacy: bool) {
    use pioneer_protocol::{PersistedActorRef, SandboxMode, UserInput};
    use std::collections::BTreeSet;
    let f = fixture("unused", vec![], true, false).await;
    let db = f.store.database_connection();
    let root = f.store.get_thread_model("thread").await.unwrap().unwrap();
    let (_, template) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM turn WHERE id='turn'")
        .await
        .unwrap();
    f.store
        .materialize_turn_start(
            &root,
            SandboxMode::FullAccess,
            &template,
            &[UserInput::Text {
                text: "accepted root H".into(),
                text_elements: vec![],
            }],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let h = if legacy {
        serde_json::to_string(&vec![
            pioneer_provider::ChatMessage::user("accepted root H"),
            pioneer_provider::ChatMessage::assistant("independent identical observation"),
            pioneer_provider::ChatMessage::assistant("independent identical observation"),
        ])
        .unwrap()
    } else {
        super::test_support::capture_line_json(&f.store, "ws", "thread", None)
            .await
            .unwrap()
    };
    for sql in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('child','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task','ws','thread','thread','thread','turn','agent','running','Task','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run','task','run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt','task','run','child','child-turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('child','thread','thread',1,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES ('run','task','ws','thread','turn',?,CURRENT_TIMESTAMP)", [h.clone().into()])).await.unwrap();
    let child = f.store.get_thread_model("child").await.unwrap().unwrap();
    let mut child_turn = template.clone();
    child_turn.id = "child-turn".into();
    f.store
        .materialize_turn_start(
            &child,
            SandboxMode::FullAccess,
            &child_turn,
            &[UserInput::Text {
                text: "child own command".into(),
                text_elements: vec![],
            }],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let mut later = template.clone();
    later.id = "later-root".into();
    f.store
        .materialize_turn_start(
            &root,
            SandboxMode::FullAccess,
            &later,
            &[UserInput::Text {
                text: "ancestor append after admission".into(),
                text_elements: vec![],
            }],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let nested = super::frozen::capture_execution_basis_json(
        &f.store,
        "ws",
        "child",
        Some("child-turn"),
        None,
        None,
    )
    .await
    .unwrap();
    let scopes = super::frozen::accepted_history_scopes(&f.store, "ws", "child", &nested)
        .await
        .unwrap();
    assert_eq!(scopes, BTreeSet::from(["thread".into(), "child".into()]));
    assert!(
        super::frozen::accepted_history_scopes(&f.store, "ws", "thread", &nested)
            .await
            .is_err()
    );
    let history =
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &scopes, &nested)
            .await
            .unwrap();
    let without_creator =
        super::frozen::capture_execution_basis_json(&f.store, "ws", "child", None, None, None)
            .await
            .unwrap();
    let restored_without_creator = crate::turn_runtime_snapshot::restore_history_json(
        &f.store,
        "ws",
        &scopes,
        &without_creator,
    )
    .await
    .unwrap();
    assert_eq!(restored_without_creator, history);
    if legacy {
        assert_eq!(
            history
                .iter()
                .filter(|message| message.content == "independent identical observation")
                .count(),
            2
        );
        assert!(!nested.contains("accepted root H"));
        assert!(
            history
                .iter()
                .filter(|message| message.content == "accepted root H")
                .all(
                    |message| message.provenance.as_ref().unwrap().sources[0].scope
                        == "task-basis:run"
                )
        );
    }

    assert_eq!(
        history
            .iter()
            .filter(|m| m.content.contains("accepted root H"))
            .count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|m| m.content.contains("child own command"))
            .count(),
        1
    );
    assert!(
        history
            .iter()
            .all(|m| !m.content.contains("ancestor append after admission"))
    );
    assert_eq!(
        f.store
            .compaction_task_basis_snapshot("ws", "child", "child-turn")
            .await
            .unwrap()
            .unwrap()
            .history_json,
        h
    );
    for sql in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('grand','ws','','agent','m','p','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('grand-task','ws','thread','child','child','child-turn','agent','running','Nested','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('grand-run','grand-task','grand-run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('grand-rt','grand-task','grand-run','grand','grand-turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('grand','child','thread',2,CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES ('grand-run','grand-task','ws','child','child-turn',?,CURRENT_TIMESTAMP)", [nested.clone().into()])).await.unwrap();
    let grand_scopes =
        super::frozen::execution_history_scopes(&f.store, "ws", "grand", "grand-turn", None)
            .await
            .unwrap();
    assert_eq!(
        grand_scopes,
        BTreeSet::from(["thread".into(), "child".into(), "grand".into()])
    );
    let inherited =
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &grand_scopes, &nested)
            .await
            .unwrap();
    let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
        "ws",
        "grand",
        &inherited,
        &vec![1; inherited.len()],
    )
    .unwrap();
    assert!(
        layout
            .units
            .iter()
            .all(|unit| unit.role == pioneer_compaction::SourceRole::Inherited)
    );
    db.execute_unprepared(if legacy {
        "UPDATE task_run_conversation_snapshot SET history_json=history_json||' ' WHERE run_id='run'"
    } else {
        "UPDATE turn_input SET text='edited source' WHERE turn_id='turn'"
    })
        .await
        .unwrap();
    assert!(
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &grand_scopes, &nested)
            .await
            .is_err()
    );
    assert!(
        !super::frozen::execution_history_scopes(&f.store, "other", "child", "child-turn", None)
            .await
            .unwrap()
            .contains("thread")
    );
}

#[tokio::test]
async fn frozen_failed_event_preserves_old_wire_form_and_new_terminal_status() {
    use pioneer_provider::ChatMessage;
    let f = fixture("irrelevant", vec![], true, false).await;
    // This fixture's raw synthetic event has no canonical projector ACK.
    // Remove it before exercising the actual ordered event materializer.
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    for (id, code, message) in [
        (
            "source-blocker",
            "permission_denied",
            "recorded permission blocker",
        ),
        (
            "compaction-progress",
            "agent_context_compaction",
            "technical summary progress",
        ),
    ] {
        f.store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: pioneer_protocol::TurnItem::SystemEvent {
                        id: id.into(),
                        level: SystemEventLevel::Info,
                        message: message.into(),
                        code: Some(code.into()),
                        details: None,
                    },
                },
                chrono::Utc::now().timestamp(),
            )
            .await
            .unwrap();
    }
    let mut turn = f.store.get_turn("thread", "turn").await.unwrap().unwrap().1;
    turn.status = pioneer_protocol::TurnStatus::Interrupted;
    turn.error = None;
    f.store
        .materialize_turn_failed(
            pioneer_protocol::TurnFailedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn,
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert!(
        history
            .iter()
            .any(|message| message.content.contains("recorded permission blocker"))
    );
    assert!(
        history
            .iter()
            .any(|message| message.content == "Historical turn Interrupted: None")
    );
    assert!(
        history
            .iter()
            .all(|message| !message.content.contains("technical summary progress"))
    );
    let reference = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.source_type == pioneer_protocol::constants::events::TURN_FAILED)
        .unwrap()
        .reference;
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    for content in [
        "Historical turn failed: None",
        "Historical turn Interrupted: None",
    ] {
        let mut message = ChatMessage::user(content);
        message.provenance = Some(pioneer_provider::MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            context_thread: None,
            unit_id: "failure".into(),
            sources: vec![pioneer_provider::MessageSourceRef {
                scope: reference.scope.clone(),
                id: reference.id.clone(),
                version: reference.version.clone(),
            }],
            inherited: false,
            complete: true,
            protected_input: false,
        });
        let descriptor =
            super::frozen::capture(&f.store, "ws", "thread", &allowed, &[message.clone()])
                .await
                .unwrap();
        let restored = super::frozen::restore(&f.store, "ws", &allowed, &descriptor)
            .await
            .unwrap();
        assert_eq!(restored, vec![message]);
    }
}

#[tokio::test]
async fn compaction_compatible_input_overlap_keeps_exact_steering_rows_once() {
    let f = fixture("unused seed", vec![], true, false).await;
    let db = f.store.database_connection();
    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    let mut earlier = None;
    for n in 1..=3 {
        let input = pioneer_protocol::UserInput::Text {
            text: format!("input {n}"),
            text_elements: vec![],
        };
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,'turn',?,'text',?,?,CURRENT_TIMESTAMP)",
            [format!("input-{n}").into(), (n as i64).into(), format!("input {n}").into(), serde_json::to_string(&input).unwrap().into()]
        )).await.unwrap();
        if n == 2 {
            let fence = f.store.compaction_history_read_fence().await.unwrap();
            earlier = Some(
                super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
                    .await
                    .unwrap(),
            );
        }
    }
    let mut earlier = earlier.unwrap();
    assert_eq!(earlier.len(), 1);
    for message in &mut earlier {
        message.provenance.as_mut().unwrap().inherited = true;
    }
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let later = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('later-broken','turn',4,'text','','{invalid',CURRENT_TIMESTAMP)").await.unwrap();
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let joined =
        super::frozen::compose_frozen_basis(&f.store, "ws", "thread", &allowed, &earlier, &later)
            .await
            .unwrap();
    assert_eq!(
        joined
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>(),
        vec!["input 1", "input 2", "input 3"]
    );
    let selected = earlier[0]
        .provenance
        .as_ref()
        .unwrap()
        .sources
        .iter()
        .map(|source| SourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        })
        .collect();
    let current_fence = f.store.compaction_history_read_fence().await.unwrap();
    let exact = super::history::load_exact_line_history(
        &f.store,
        "ws",
        "thread",
        &current_fence,
        &selected,
    )
    .await
    .unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].content, earlier[0].content);
    assert_eq!(
        exact[0].provenance.as_ref().unwrap().sources,
        earlier[0].provenance.as_ref().unwrap().sources
    );
    let snapshot = super::frozen::capture(&f.store, "ws", "thread", &allowed, &joined)
        .await
        .unwrap();
    assert_eq!(
        super::frozen::restore(&f.store, "ws", &allowed, &snapshot)
            .await
            .unwrap(),
        joined
    );
}

#[tokio::test]
async fn compaction_checkpoint_projects_accepted_foreign_own_dag_without_covering_h() {
    use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef};
    use std::collections::BTreeSet;
    let f = fixture("completed A", vec![Reply::Success], true, false).await;
    let CompactionExit::Applied(a_id) = f.runner.run(CancellationToken::new()).await.unwrap()
    else {
        panic!("A checkpoint must apply");
    };
    let a = f.store.compaction_checkpoint(&a_id).await.unwrap().unwrap();
    let a_source = f
        .store
        .compaction_checkpoint_source("ws", "thread", &a_id)
        .await
        .unwrap()
        .unwrap();
    let db = f.store.database_connection();
    db.execute_unprepared("INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('c','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await.unwrap();
    let mut snapshot = f.runner.snapshot.clone();
    snapshot.id = "c-operation".into();
    snapshot.owner = "c-owner".into();
    snapshot.plan.fingerprint = "c-plan".into();
    f.store
        .compaction_admit("ws", "c", &snapshot)
        .await
        .unwrap();
    let c = Checkpoint {
        id: "c-checkpoint".into(),
        operation_id: snapshot.id,
        owner: snapshot.owner,
        previous: None,
        coverage: vec![a_source.clone()],
        summary: "C prepared work".into(),
        selection: a.selection.clone(),
        projection_version: 0,
        format_version: 1,
    };
    f.store.compaction_save_candidate(&c, 0).await.unwrap();
    // Persisted historical fixture for projection. The operation/import CAS
    // transition is exercised separately against real accepted Task metadata
    // by the CRUD frozen_own_imports regression.
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='c-checkpoint'",
    )
    .await
    .unwrap();
    let mut own = ChatMessage::assistant("A original");
    own.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: Some("c".into()),
        unit_id: "A-work".into(),
        sources: a
            .coverage
            .iter()
            .map(|s| MessageSourceRef {
                scope: s.scope.clone(),
                id: s.id.clone(),
                version: s.version.clone(),
            })
            .collect(),
        complete: true,
        protected_input: false,
        inherited: false,
    });
    let mut h = ChatMessage::user("H remains inherited");
    h.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "H".into(),
        sources: vec![MessageSourceRef {
            scope: "event:turn".into(),
            id: "h-source".into(),
            version: "event-revision:1".into(),
        }],
        complete: true,
        protected_input: false,
        inherited: true,
    });
    db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('h-source','thread','turn',2,'fixture','{}',CURRENT_TIMESTAMP)").await.unwrap();
    let allowed = BTreeSet::from(["thread".into(), "c".into()]);
    let original = vec![h.clone(), own.clone()];
    let mut denied = original.clone();
    assert!(
        super::checkpoint::project_checkpoint(
            &f.store,
            "ws",
            "c",
            "c-owner",
            &c.id,
            &BTreeSet::from(["c".into()]),
            &mut denied
        )
        .await
        .is_err()
    );
    assert_eq!(denied, original);
    let mut projected = original.clone();
    super::checkpoint::project_checkpoint(
        &f.store,
        "ws",
        "c",
        "c-owner",
        &c.id,
        &allowed,
        &mut projected,
    )
    .await
    .unwrap();
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0], h);
    assert!(projected[1].content.contains("C prepared work"));
    assert_eq!(projected[1].provenance.as_ref().unwrap().thread_id, "c");
    assert_eq!(
        serde_json::to_value(f.store.compaction_checkpoint(&a_id).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(&a).unwrap()
    );
    // A retained A summary is equally eligible; physical source owner A and
    // working context owner C must not be confused when expanding coverage.
    let mut summarized = own;
    summarized.provenance.as_mut().unwrap().sources = vec![MessageSourceRef {
        scope: a_source.scope,
        id: a_source.id,
        version: a_source.version,
    }];
    let mut summarized = vec![h, summarized];
    super::checkpoint::project_checkpoint(
        &f.store,
        "ws",
        "c",
        "c-owner",
        &c.id,
        &allowed,
        &mut summarized,
    )
    .await
    .unwrap();
    assert_eq!(summarized, projected);
    db.execute_unprepared("UPDATE turn_event SET payload='edited A' WHERE id='source'")
        .await
        .unwrap();
    let mut stale = original;
    assert!(
        super::checkpoint::project_checkpoint(
            &f.store, "ws", "c", "c-owner", &c.id, &allowed, &mut stale
        )
        .await
        .is_err()
    );
}

struct CleanupGateSummarizer {
    inner: Arc<dyn Summarizer>,
    active: std::sync::atomic::AtomicBool,
    entered: tokio::sync::watch::Sender<usize>,
    release: tokio::sync::Semaphore,
}
impl CleanupGateSummarizer {
    async fn wait_cleanup(&self, count: usize) {
        let mut receiver = self.entered.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            while *receiver.borrow_and_update() < count {
                receiver.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}
#[async_trait]
impl Summarizer for CleanupGateSummarizer {
    fn model_budget(&self) -> ModelBudget {
        self.inner.model_budget()
    }
    fn input_tokens(&self, request: &SummaryRequest) -> Result<u64> {
        self.inner.input_tokens(request)
    }
    async fn summarize(
        &self,
        request: SummaryRequest,
    ) -> std::result::Result<
        pioneer_compaction::summary::SummaryCompletion,
        pioneer_compaction::summary::SummaryFailure,
    > {
        assert!(!self.active.swap(true, Ordering::SeqCst));
        self.inner.summarize(request).await
    }
    async fn cleanup(
        &self,
    ) -> std::result::Result<(), pioneer_compaction::summary::SummaryFailure> {
        if self.active.load(Ordering::SeqCst) {
            self.entered.send_modify(|count| *count += 1);
            self.release.acquire().await.unwrap().forget();
            self.active.store(false, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
async fn service_cleanup_precedes_publication_backoff_and_stop_reconciliation() {
    for scenario in 0..3 {
        let mut f = fixture(
            "completed history",
            vec![if scenario == 2 {
                Reply::Hang
            } else {
                Reply::Success
            }],
            true,
            false,
        )
        .await;
        let cleanup = Arc::new(CleanupGateSummarizer {
            inner: f.runner.summarizer.clone(),
            active: std::sync::atomic::AtomicBool::new(false),
            entered: tokio::sync::watch::channel(0).0,
            release: tokio::sync::Semaphore::new(0),
        });
        Arc::get_mut(&mut f.runner).unwrap().summarizer = cleanup.clone();
        let cancel = CancellationToken::new();
        let work = tokio::spawn({
            let runner = f.runner.clone();
            let cancel = cancel.clone();
            async move { runner.run(cancel).await }
        });
        f.provider.wait_calls(1).await;
        if scenario == 2 {
            f.clock.advance(pioneer_compaction::ATTEMPT_MILLIS);
        }
        cleanup.wait_cleanup(1).await;
        if scenario == 1 {
            cancel.cancel();
            // Cancellation drops drive's cleanup future. run must reacquire
            // the same resource owner and finish cleanup before returning.
            cleanup.wait_cleanup(2).await;
        }
        assert!(f.store.compaction_head("owner").await.unwrap().is_none());
        assert!(matches!(
            f.store
                .compaction_runner_state("operation")
                .await
                .unwrap()
                .unwrap()
                .phase,
            RunnerPhase::Attempt { .. }
        ));
        assert_eq!(*f.provider.count.borrow(), 1);
        assert!(!work.is_finished());
        cleanup.release.add_permits(1);
        if scenario == 2 {
            wait_backoff(&f.store).await;
            cancel.cancel();
        }
        let exit = tokio::time::timeout(Duration::from_secs(10), work)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!cleanup.active.load(Ordering::SeqCst));
        if scenario == 0 {
            assert!(matches!(exit, CompactionExit::Applied(_)));
        } else {
            assert!(matches!(
                exit,
                CompactionExit::Reconcile(FailureKind::Cancelled)
            ));
            f.runner.reconcile(FailureKind::Cancelled).await.unwrap();
            assert!(f.store.compaction_head("owner").await.unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn canonical_attachment_metadata_keeps_all_recorded_versions_without_repeating_user_text() {
    use pioneer_protocol::{
        ArtifactKind, ArtifactRef, ArtifactStatus, ItemCompletedNotification, TurnItem,
        UserMessageAttachment,
    };
    let f = fixture("unused", vec![], true, false).await;
    let db = f.store.database_connection();
    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input','turn',0,'text','unique user text','{\"type\":\"text\",\"text\":\"unique user text\"}',CURRENT_TIMESTAMP)").await.unwrap();
    let attachments = (0..48)
        .map(|index| UserMessageAttachment::Artifact {
            artifact: ArtifactRef {
                artifact_id: format!("historical-artifact-{index}"),
                version_id: Some(format!("recorded-version-{index}")),
                display_name: format!("file-{index}.png"),
                kind: ArtifactKind::Image,
                mime_type: Some("image/png".into()),
                size_bytes: Some(42),
                sha256: None,
                status: ArtifactStatus::Ready,
                preview: None,
            },
        })
        .collect();
    f.store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: TurnItem::UserMessage {
                    id: "user-item".into(),
                    text: "unique user text".into(),
                    attachments,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    let text = history
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(text.matches("unique user text").count(), 1);
    for index in 0..48 {
        assert!(text.contains(&format!("recorded-version-{index}")));
    }
    assert!(
        history
            .iter()
            .all(|message| message.content_parts.is_empty())
    );
    let allowed = std::collections::BTreeSet::from(["thread".into()]);
    let frozen = super::frozen::capture(&f.store, "ws", "thread", &allowed, &history)
        .await
        .unwrap();
    let restored = super::frozen::restore(&f.store, "ws", &allowed, &frozen)
        .await
        .unwrap();
    assert_eq!(history, restored);
    assert_eq!(f.provider.count.borrow().clone(), 0);
}

#[tokio::test]
async fn old_thread_summary_is_ignored_without_originals() {
    let f = fixture("unused", vec![], true, false).await;
    f.store
        .update_thread_summary("thread", "old facts must not enter the prompt", 900)
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::history::load_line_history(&f.store, "ws", "thread", Some("turn"), &fence)
        .await
        .unwrap();
    assert!(history.is_empty());
    assert_eq!(f.provider.count.borrow().clone(), 0);
}

#[tokio::test]
async fn old_thread_summary_is_ignored_with_available_originals() {
    let f = fixture("unused", vec![], true, false).await;
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    f.store
        .update_thread_summary("thread", "unverified legacy text", 999)
        .await
        .unwrap();
    f.store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "retained-original".into(),
                    text: "available original".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert!(
        history
            .iter()
            .any(|message| message.content.contains("available original"))
    );
    assert!(
        history
            .iter()
            .all(|message| !message.content.contains("unverified legacy text"))
    );
    assert_eq!(
        f.store
            .get_thread_summary("thread")
            .await
            .unwrap()
            .unwrap()
            .0,
        "unverified legacy text"
    );
}

#[test]
fn stopped_compaction_item_is_cancelled_with_the_same_lifecycle_identity() {
    let observer = HubCompactionObserver {
        hub: Arc::new(ExecutionEventHub::new()),
        processor: std::sync::Weak::new(),
        workspace: "ws".into(),
        thread: "thread".into(),
        turn: "turn".into(),
    };
    let mut state = RunnerState::new(
        900000,
        &ModelBudget::new(Some(128000), None, Some(16384)),
        1000,
        None,
    )
    .unwrap();
    let started = observer.item("operation", &state, false);
    state.phase = RunnerPhase::Failed {
        kind: FailureKind::Cancelled,
    };
    let cancelled = observer.item("operation", &state, true);
    for (item, expected) in [(started, "started"), (cancelled, "cancelled")] {
        let pioneer_protocol::TurnItem::SystemEvent {
            id,
            details,
            code,
            level,
            ..
        } = item
        else {
            panic!("wrong lifecycle item")
        };
        assert_eq!(id, "compaction:operation");
        assert_eq!(code.as_deref(), Some("agent_context_compaction"));
        assert_eq!(details.unwrap()["status"], expected);
        assert_eq!(level, SystemEventLevel::Info);
    }
}
