use super::*;
use migration::{Migrator, MigratorTrait};
use pioneer_compaction::summary::{HEADINGS, SummaryInput};
use pioneer_compaction::{
    CompactionMode, CompactionPlan, CompactionSettings, ModelBudget, ModelSelection, Transport,
};
use pioneer_crud::compaction::{
    CanonicalSource, ManifestEntry, PagedSource, PublicationTestPause, arm_publication_test_hook,
};
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

#[derive(Default)]
struct HistoryReadObserver {
    reads: Mutex<Vec<pioneer_sqlite::SqliteReadEvent>>,
    writes: Mutex<Vec<pioneer_sqlite::SqliteWriteEvent>>,
}

impl pioneer_sqlite::SqliteReadObserver for HistoryReadObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteReadEvent) {
        self.reads.lock().unwrap().push(event);
    }
}

impl pioneer_sqlite::SqliteWriteObserver for HistoryReadObserver {
    fn observe(&self, event: pioneer_sqlite::SqliteWriteEvent) {
        self.writes.lock().unwrap().push(event);
    }
}

impl HistoryReadObserver {
    fn reads(&self) -> Vec<pioneer_sqlite::SqliteReadEvent> {
        self.reads.lock().unwrap().clone()
    }

    fn writes(&self) -> Vec<pioneer_sqlite::SqliteWriteEvent> {
        self.writes.lock().unwrap().clone()
    }
}

struct ManualClock {
    now: tokio::sync::watch::Sender<u64>,
    sleeps: tokio::sync::broadcast::Sender<u64>,
}
impl ManualClock {
    fn new() -> Self {
        Self {
            now: tokio::sync::watch::channel(0).0,
            sleeps: tokio::sync::broadcast::channel(64).0,
        }
    }
    fn advance(&self, value: u64) {
        assert!(value >= self.now_ms());
        self.now.send_replace(value);
    }
    fn subscribe_sleeps(&self) -> tokio::sync::broadcast::Receiver<u64> {
        self.sleeps.subscribe()
    }
}
#[async_trait]
impl CompactionClock for ManualClock {
    fn now_ms(&self) -> u64 {
        *self.now.borrow()
    }
    async fn sleep_until(&self, deadline: u64) {
        let _ = self.sleeps.send(deadline);
        let mut rx = self.now.subscribe();
        loop {
            if *rx.borrow_and_update() >= deadline {
                return;
            }
            rx.changed().await.unwrap();
        }
    }
}

async fn wait_for_sleep(sleeps: &mut tokio::sync::broadcast::Receiver<u64>, expected: u64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if sleeps.recv().await.unwrap() == expected {
                return;
            }
        }
    })
    .await
    .expect("runner did not enter the expected validation backoff");
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
    gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
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
            gate: Mutex::new(None),
            active: AtomicUsize::new(0),
        }
    }
    fn pause_next(&self) -> Arc<tokio::sync::Semaphore> {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
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
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
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

#[test]
fn indexed_runner_payload_keeps_unicode_character_cursor_stable() {
    let source = pioneer_compaction::SourceRef {
        scope: "event:turn".into(),
        id: "unicode".into(),
        version: "event-revision:1".into(),
    };
    let text = "a漢🌍é".repeat(12_345);
    let indexed = IndexedPayload::new(text.clone());
    let mut rebuilt = String::new();
    let mut cursor = 0;
    loop {
        let fragment = indexed.fragment(&source, cursor).unwrap();
        let repeated = indexed.fragment(&source, cursor).unwrap();
        assert_eq!(fragment.reference, repeated.reference);
        assert_eq!(fragment.text, repeated.text);
        assert_eq!(fragment.next_character, repeated.next_character);
        rebuilt.push_str(&fragment.text);
        let Some(next) = fragment.next_character else {
            break;
        };
        assert_eq!(next, cursor + fragment.text.chars().count() as u64);
        cursor = next;
    }
    assert_eq!(rebuilt, text);
}

#[tokio::test]
async fn runner_keeps_large_compressed_active_payload_while_loading_reference_only_excerpt() {
    let f = fixture(&"漢🌍".repeat(40_000), vec![], true, false).await;
    let reference_payload = serde_json::json!({"text":"reference ".repeat(20_000)}).to_string();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES('reference','thread','turn',2,'fixture',?,CURRENT_TIMESTAMP)",
        [reference_payload.into()],
    )).await.unwrap();
    let reference = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 1)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES('operation',1,0,1,'thread',?,?,?)",
        [reference.scope.clone().into(), reference.id.clone().into(), reference.version.clone().into()],
    )).await.unwrap();
    let config = serde_json::json!({
        "table":"turn_event", "column":"payload", "compression_level":3,
        "dict_chooser":"'[nodict]'"
    });
    f.store
        .database_connection()
        .query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [config.to_string().into()],
        ))
        .await
        .unwrap();
    let raw: String = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT payload FROM _turn_event_zstd WHERE id='source'".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "payload")
        .unwrap();
    let compressed = pioneer_sqlite::zstd::compress_column_value(raw.as_bytes(), 3, None).unwrap();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE _turn_event_zstd SET payload=?,_payload_dict=-1 WHERE id='source' AND payload=?",
        [compressed.into(), raw.into()],
    )).await.unwrap();

    let source = f
        .store
        .compaction_manifest_page("operation", false, 0, 0)
        .await
        .unwrap()[0]
        .source
        .clone();
    let first = f
        .runner
        .active_payload_fragment("thread", &source, 0)
        .await
        .unwrap();
    assert!(first.next_character.is_some());
    let before = {
        let cache = f.runner.active_payload.lock().await;
        Arc::as_ptr(&cache.as_ref().unwrap().payload)
    };
    assert_eq!(f.runner.reference_excerpts().await.unwrap().len(), 1);
    let second = f
        .runner
        .active_payload_fragment("thread", &source, first.next_character.unwrap())
        .await
        .unwrap();
    assert!(!second.text.is_empty());
    let after = {
        let cache = f.runner.active_payload.lock().await;
        Arc::as_ptr(&cache.as_ref().unwrap().payload)
    };
    assert_eq!(
        before, after,
        "reference-only loading evicted the active payload"
    );
    assert_eq!(
        f.runner.active_payload_loads.load(Ordering::SeqCst),
        1,
        "the active compressed object was materialized more than once"
    );
}

#[tokio::test]
async fn classified_technical_event_does_not_load_its_compressed_payload() {
    let f = fixture("unused", vec![], true, false).await;
    f.store.database_connection().execute_unprepared(
        "UPDATE turn SET status='completed' WHERE id='turn';
         UPDATE compaction_event_revision SET projection_revision=revision,projection_kind='technical' WHERE source_id='source'",
    ).await.unwrap();
    let config = serde_json::json!({
        "table":"turn_event", "column":"payload", "compression_level":3,
        "dict_chooser":"'[nodict]'"
    });
    f.store
        .database_connection()
        .query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [config.to_string().into()],
        ))
        .await
        .unwrap();
    // Metadata remains readable, but touching the body would ask sqlite-zstd to
    // decode deliberately invalid compressed bytes and fail the load.
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE _turn_event_zstd SET payload=?,_payload_dict=-1 WHERE id='source'",
            [vec![0_u8, 1, 2, 3].into()],
        ))
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let history = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert!(history.is_empty());
}

#[tokio::test]
async fn canonical_inputs_and_contexts_release_each_bounded_raw_payload_batch() {
    use pioneer_provider::{
        CanonicalProviderRoundEnvelope, ChatMessage, ProviderCallIdentity, ProviderToolCall,
    };
    let f = fixture("unused", vec![], true, false).await;
    let mut total_raw_bytes = 0usize;
    let mut largest_payload_bytes = 0usize;
    for index in 0..129_i64 {
        let input = pioneer_protocol::UserInput::Text {
            text: format!("input-{index}-🧪"),
            text_elements: vec![],
        };
        let payload = serde_json::to_string(&input).unwrap();
        total_raw_bytes += payload.len();
        f.store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES(?, 'turn', ?, 'text', '', ?, CURRENT_TIMESTAMP)",
            [format!("batched-input-{index}").into(), index.into(), payload.into()],
        )).await.unwrap();
    }
    for sequence in 1..=130_i64 {
        let round = match sequence {
            1 | 2 => Some(1),
            65 | 66 => Some(2),
            129 | 130 => Some(3),
            _ => None,
        };
        let (source, item_id, payload) = if let Some(round) = round
            && sequence % 2 == 1
        {
            let envelope = CanonicalProviderRoundEnvelope {
                version: 1,
                round_id: format!("large-round-{round}"),
                termination: ProviderTermination::ToolCalls,
                message: ChatMessage::assistant_tool_calls(
                    None::<String>,
                    vec![ProviderToolCall {
                        id: format!("large-call-{round}"),
                        name: "large_tool".into(),
                        arguments: "{}".into(),
                    }],
                ),
                calls: vec![ProviderCallIdentity {
                    provider_call_id: format!("large-call-{round}"),
                    turn_item_id: format!("large-item-{round}"),
                    ordinal: 0,
                }],
            };
            (
                "assistant_round",
                Some(format!("large-round-{round}")),
                serde_json::to_string(&envelope).unwrap(),
            )
        } else if let Some(round) = round {
            let view = pioneer_tools::ToolResultView::Json {
                value: serde_json::to_value(ChatMessage::tool_result(
                    format!("large-call-{round}"),
                    "large_tool",
                    format!(
                        "large-{round}-{}",
                        "漢🌍".repeat(pioneer_crud::compaction::SOURCE_PAGE_BYTES / 7 + 100)
                    ),
                ))
                .unwrap(),
                truncated: false,
            };
            (
                "tool_result_v2",
                Some(format!("large-item-{round}")),
                serde_json::to_string(&view).unwrap(),
            )
        } else {
            ("legacy", None, format!("context-{sequence}-é"))
        };
        total_raw_bytes += payload.len();
        largest_payload_bytes = largest_payload_bytes.max(payload.len());
        f.store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES(?, 'turn', ?, ?, ?, ?, '{}', CURRENT_TIMESTAMP)",
            [format!("batched-context-{sequence}").into(), item_id.into(), sequence.into(), source.into(), payload.into()],
        )).await.unwrap();
    }
    f.store.database_connection().execute_unprepared(
        "UPDATE turn SET status='completed' WHERE id='turn';
         UPDATE compaction_event_revision SET projection_revision=revision,projection_kind='technical' WHERE source_id='source'",
    ).await.unwrap();

    let (prepared, stats) = super::history::with_payload_batch_stats(
        super::frozen::capture_execution_basis_prepared(&f.store, "ws", "thread", None, None, None),
    )
    .await;
    let prepared = prepared.unwrap();
    assert!(
        stats.calls >= 7,
        "input/context loading did not cross several batches"
    );
    assert_eq!(
        stats.max_rows,
        pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize,
        "the 128-row boundary was not exercised"
    );
    assert!(
        stats.max_returned_raw_bytes >= largest_payload_bytes,
        "an oversized source was not admitted as one bounded object"
    );
    assert!(
        stats.max_returned_raw_bytes < total_raw_bytes,
        "one returned batch unexpectedly contains the complete raw history"
    );
    assert_eq!(stats.current_raw_bytes, 0, "raw batch lease leaked");
    assert_eq!(
        stats.peak_concurrent_raw_bytes, stats.max_returned_raw_bytes,
        "a previous raw payload batch remained live while the next batch was loaded"
    );
    assert!(prepared.messages[0].content.contains("input-0-🧪"));
    let source_position = |id: &str| {
        prepared
            .messages
            .iter()
            .position(|message| {
                message
                    .provenance
                    .as_ref()
                    .is_some_and(|origin| origin.sources.iter().any(|source| source.id == id))
            })
            .unwrap()
    };
    let first = source_position("batched-context-2");
    let last = source_position("batched-context-130");
    assert!(first < last, "context order changed across payload batches");
}

#[tokio::test]
async fn frozen_capture_rejects_foreign_scope_before_legacy_registration() {
    use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef};
    use pioneer_sqlite::{SqliteDatabase, SqliteWriteExecutor};
    use sea_orm::{ConnectOptions, TransactionTrait};

    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("foreign-scope.sqlite");
    let writer_url = format!("sqlite://{}?mode=rwc", path.display());
    let mut writer_options = ConnectOptions::new(writer_url);
    writer_options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(writer_options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    writer.execute_unprepared(
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1);
         INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ).await.unwrap();
    let reader_url = format!("sqlite://{}?mode=ro", path.display());
    let mut reader_options = ConnectOptions::new(reader_url);
    reader_options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(reader_options).await.unwrap();
    let observer = Arc::new(HistoryReadObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer,
    );
    let store = CrudStore::new(database);
    store.database_connection().execute_unprepared(
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES('legacy-forbidden','turn','legacy-forbidden','command_execution','completed',0,'{invalid',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         DELETE FROM compaction_item_revision WHERE source_id='legacy-forbidden'",
    ).await.unwrap();
    let mut message = ChatMessage::user("must not be inspected");
    message.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "foreign-workspace".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "turn:foreign".into(),
        complete: true,
        protected_input: false,
        inherited: false,
        sources: vec![MessageSourceRef {
            scope: "item:turn".into(),
            id: "legacy-forbidden".into(),
            version: "item-revision:1".into(),
        }],
    });
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let held_writer = store.database_connection().begin().await.unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        super::frozen::capture(&store, "ws", "thread", &allowed, &[message]),
    )
    .await
    .expect("foreign capture waited for the serialized writer")
    .unwrap_err();
    held_writer.rollback().await.unwrap();
    assert!(error.to_string().contains("outside the accepted context"));
    let registered: i64 = store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS n FROM compaction_item_revision WHERE source_id='legacy-forbidden'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "n")
        .unwrap();
    assert_eq!(registered, 0);

    let input = pioneer_protocol::UserInput::Text {
        text: "accepted".into(),
        text_elements: vec![],
    };
    let payload = serde_json::to_string(&input).unwrap();
    store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES('restore-input','turn',0,'text','accepted',?,CURRENT_TIMESTAMP)",
        [payload.into()],
    )).await.unwrap();
    let mut accepted = super::history::input_message(&[input]).unwrap();
    accepted.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "turn:accepted".into(),
        complete: true,
        protected_input: false,
        inherited: false,
        sources: vec![MessageSourceRef {
            scope: "input:turn".into(),
            id: "restore-input".into(),
            version: "input-revision:1".into(),
        }],
    });
    let descriptor = super::frozen::capture(
        &store,
        "ws",
        "thread",
        &allowed,
        std::slice::from_ref(&accepted),
    )
    .await
    .unwrap();
    store.database_connection().execute_unprepared(
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES('other','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES('other-turn','other','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO turn_item(id,turn_id,item_id,item_type,status,active_attempt_number,payload,created_at,updated_at) VALUES('restore-forbidden','other-turn','restore-forbidden','command_execution','completed',0,'{invalid',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         DELETE FROM compaction_item_revision WHERE source_id='restore-forbidden'",
    ).await.unwrap();
    let mut forbidden = store
        .compaction_frozen_history_page("ws", "thread", &descriptor.manifest_id, 0)
        .await
        .unwrap()
        .remove(0);
    forbidden.source_thread = "other".into();
    forbidden.sources = vec![pioneer_compaction::SourceRef {
        scope: "item:other-turn".into(),
        id: "restore-forbidden".into(),
        version: "item-revision:1".into(),
    }];
    let reference_json = serde_json::to_string(&forbidden).unwrap();
    let storage_manifest = store.database_connection().query_one_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT source_manifest FROM compaction_frozen_span WHERE manifest_id=? AND kind=0 AND start<=0 AND end>0 LIMIT 1",
        [descriptor.manifest_id.clone().into()],
    )).await.unwrap().map(|row| row.try_get::<String>("", "source_manifest").unwrap())
        .unwrap_or_else(|| descriptor.manifest_id.clone());
    store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_frozen_message_data SET reference_json=?,bytes=? WHERE manifest_id=? AND ordinal=0",
        [reference_json.clone().into(), (reference_json.len() as i64).into(), storage_manifest.into()],
    )).await.unwrap();
    let held_writer = store.database_connection().begin().await.unwrap();
    let restore_error = tokio::time::timeout(
        Duration::from_secs(1),
        super::frozen::restore(&store, "ws", &allowed, &descriptor),
    )
    .await
    .expect("foreign restore waited for the serialized writer")
    .unwrap_err();
    held_writer.rollback().await.unwrap();
    assert!(
        restore_error
            .to_string()
            .contains("outside the accepted context")
    );
    let restore_registered: i64 = store.database_connection().query_one_raw(Statement::from_string(
        DbBackend::Sqlite,
        "SELECT count(*) AS n FROM compaction_item_revision WHERE source_id='restore-forbidden'".to_owned(),
    )).await.unwrap().unwrap().try_get("", "n").unwrap();
    assert_eq!(restore_registered, 0);
}

/// Manual end-to-end scenario for the dedicated performance stage. Unlike the
/// CRUD microbenchmark, this exercises frozen capture, frozen restore and the
/// real runner. Durations are observations, never test thresholds.
#[tokio::test]
#[ignore = "manual end-to-end history benchmark; run only in the dedicated benchmark stage"]
async fn benchmark_frozen_capture_restore_and_compressed_runner() {
    use pioneer_provider::ChatMessage;

    let f = fixture("runner seed", vec![], true, false).await;
    for index in 0..1_000_i64 {
        let id = format!("benchmark-input-{index}");
        let text = format!("small history message {index}");
        let input = pioneer_protocol::UserInput::Text {
            text: text.clone(),
            text_elements: vec![],
        };
        let payload = serde_json::to_string(&input).unwrap();
        f.store.database_connection().execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES(?, 'turn', ?, 'text', ?, ?, CURRENT_TIMESTAMP)",
            [id.clone().into(), index.into(), text.into(), payload.into()],
        )).await.unwrap();
    }
    f.store.database_connection().execute_unprepared(
        "UPDATE turn SET status='completed' WHERE id='turn';
         UPDATE compaction_event_revision SET projection_revision=revision,projection_kind='technical' WHERE source_id='source'",
    ).await.unwrap();
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let capture_started = std::time::Instant::now();
    let (prepared, payload_stats) = super::history::with_payload_batch_stats(
        super::frozen::capture_execution_basis_prepared(&f.store, "ws", "thread", None, None, None),
    )
    .await;
    let prepared = prepared.unwrap();
    let capture_elapsed = capture_started.elapsed();
    let restore_started = std::time::Instant::now();
    let restored = super::frozen::restore(&f.store, "ws", &allowed, &prepared.descriptor)
        .await
        .unwrap();
    let restore_elapsed = restore_started.elapsed();
    assert_eq!(prepared.messages, restored);

    let large = serde_json::json!({
        "type":"json", "value":{"role":"tool","content":"漢🌍".repeat(1_000_000)},
        "truncated":false
    })
    .to_string();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES('benchmark-large-result','turn',1,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)",
        [large.into()],
    )).await.unwrap();
    let large_source = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::ProviderContext, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let reference_only = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_manifest SET source_scope=?,source_id=?,source_version=? WHERE operation_id='operation' AND ordinal=0",
        [large_source.scope.into(), large_source.id.into(), large_source.version.into()],
    )).await.unwrap();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO compaction_manifest(operation_id,ordinal,unit_ordinal,reference_only,source_thread,source_scope,source_id,source_version) VALUES('operation',1,0,1,'thread',?,?,?)",
        [reference_only.scope.into(), reference_only.id.into(), reference_only.version.into()],
    )).await.unwrap();
    let config = serde_json::json!({
        "table":"turn_llm_context", "column":"payload", "compression_level":3,
        "dict_chooser":"'[nodict]'"
    });
    f.store
        .database_connection()
        .query_one_write_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT zstd_enable_transparent(?)",
            [config.to_string().into()],
        ))
        .await
        .unwrap();
    let raw: String = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT payload FROM _turn_llm_context_zstd WHERE id='benchmark-large-result'"
                .to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get("", "payload")
        .unwrap();
    let compressed = pioneer_sqlite::zstd::compress_column_value(raw.as_bytes(), 3, None).unwrap();
    f.store.database_connection().execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE _turn_llm_context_zstd SET payload=?,_payload_dict=-1 WHERE id='benchmark-large-result' AND payload=?",
        [compressed.into(), raw.into()],
    )).await.unwrap();
    let runner_started = std::time::Instant::now();
    let outcome = f.runner.run(CancellationToken::new()).await.unwrap();
    let runner_elapsed = runner_started.elapsed();
    assert!(matches!(outcome, CompactionExit::Applied(_)));
    eprintln!(
        "history_e2e_benchmark canonical_rows=1000 projected_messages={} measured_capture_payload_batch_calls={} measured_max_payload_batch_rows={} measured_max_returned_raw_payload_batch_bytes={} measured_peak_concurrent_raw_payload_bytes={} measured_capture_ms={} measured_restore_ms={} measured_runner_ms={} measured_runner_provider_calls={} measured_runner_full_payload_loads={} measured_restored_utf8_bytes={}",
        restored.len(),
        payload_stats.calls,
        payload_stats.max_rows,
        payload_stats.max_returned_raw_bytes,
        payload_stats.peak_concurrent_raw_bytes,
        capture_elapsed.as_millis(),
        restore_elapsed.as_millis(),
        runner_elapsed.as_millis(),
        *f.provider.count.borrow(),
        f.runner.active_payload_loads.load(Ordering::SeqCst),
        restored
            .iter()
            .map(|message: &ChatMessage| message.content.len())
            .sum::<usize>(),
    );
}
async fn fixture(
    text: &str,
    replies: Vec<Reply>,
    target_fits: bool,
    observer_fails: bool,
) -> Fixture {
    super::load_test_catalog();
    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
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
        source_epochs: std::collections::BTreeMap::from([("thread".into(), 0)]),
        admission: CompactionSettings::default()
            .admit(&selection, None, 0)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
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

async fn install_publication_retry_probe(fixture: &Fixture) {
    fixture
        .store
        .database_connection()
        .execute_unprepared(
            "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) \
         VALUES ('publication-retry-probe','thread','turn',2,'fixture','{}',CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn gateway_retry_validation_reuses_candidate_and_provider_budget() {
    let f = fixture("publication source", vec![Reply::Success], true, false).await;
    install_publication_retry_probe(&f).await;
    let mut sleeps = f.clock.subscribe_sleeps();
    let mut hook =
        arm_publication_test_hook(&f.store, "operation", PublicationTestPause::BeforeWriter);
    let runner = f.runner.clone();
    let run = tokio::spawn(async move { runner.run(CancellationToken::new()).await.unwrap() });
    f.provider.wait_calls(1).await;
    hook.reached().await;
    let commit_state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    let checkpoint = match &commit_state.phase {
        RunnerPhase::Commit { checkpoint } => checkpoint.clone(),
        _ => panic!("publication hook was reached before Commit"),
    };
    let candidate = f
        .store
        .compaction_checkpoint(&checkpoint)
        .await
        .unwrap()
        .unwrap();
    f.store
        .database_connection()
        .execute_unprepared(
            "UPDATE turn_event SET payload='{\"race\":1}' WHERE id='publication-retry-probe'",
        )
        .await
        .unwrap();
    hook.release();
    wait_for_sleep(&mut sleeps, 10).await;

    let during_backoff = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(during_backoff.phase, commit_state.phase);
    assert_eq!(during_backoff.attempts, commit_state.attempts);
    assert_eq!(during_backoff.retries, commit_state.retries);
    assert_eq!(
        f.store
            .compaction_checkpoint(&checkpoint)
            .await
            .unwrap()
            .unwrap()
            .summary,
        candidate.summary
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        let reader = f.store.database_connection().begin_read().await.unwrap();
        reader.rollback().await.unwrap();
        f.store
            .database_connection()
            .execute_unprepared(
                "UPDATE turn_event SET payload='{\"race\":2}' WHERE id='publication-retry-probe'",
            )
            .await
            .unwrap();
    })
    .await
    .expect("validation backoff retained a database reservation");

    f.clock.advance(10);
    assert!(matches!(run.await.unwrap(), CompactionExit::Applied(_)));
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn gateway_retry_validation_cancellation_and_deadline_interrupt_backoff() {
    let f = fixture("publication source", vec![Reply::Success], true, false).await;
    install_publication_retry_probe(&f).await;
    let mut sleeps = f.clock.subscribe_sleeps();
    let mut hook =
        arm_publication_test_hook(&f.store, "operation", PublicationTestPause::BeforeWriter);
    let cancel = CancellationToken::new();
    let runner = f.runner.clone();
    let child_cancel = cancel.clone();
    let run = tokio::spawn(async move { runner.run(child_cancel).await.unwrap() });
    hook.reached().await;
    f.store.database_connection().execute_unprepared(
        "UPDATE turn_event SET payload='{\"cancel_race\":true}' WHERE id='publication-retry-probe'",
    ).await.unwrap();
    hook.release();
    wait_for_sleep(&mut sleeps, 10).await;
    cancel.cancel();
    assert!(matches!(
        run.await.unwrap(),
        CompactionExit::Reconcile(FailureKind::Cancelled)
    ));
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);

    let f = fixture("publication source", vec![Reply::Success], true, false).await;
    install_publication_retry_probe(&f).await;
    let mut sleeps = f.clock.subscribe_sleeps();
    let mut first_hook =
        arm_publication_test_hook(&f.store, "operation", PublicationTestPause::BeforeWriter);
    let runner = f.runner.clone();
    let run = tokio::spawn(async move { runner.run(CancellationToken::new()).await.unwrap() });
    first_hook.reached().await;
    f.store.database_connection().execute_unprepared(
        "UPDATE turn_event SET payload='{\"deadline_race\":1}' WHERE id='publication-retry-probe'",
    ).await.unwrap();
    first_hook.release();
    wait_for_sleep(&mut sleeps, 10).await;
    let mut second_hook =
        arm_publication_test_hook(&f.store, "operation", PublicationTestPause::BeforeWriter);
    f.clock.advance(10);
    second_hook.reached().await;
    f.store.database_connection().execute_unprepared(
        "UPDATE turn_event SET payload='{\"deadline_race\":2}' WHERE id='publication-retry-probe'",
    ).await.unwrap();
    second_hook.release();
    wait_for_sleep(&mut sleeps, 30).await;
    f.clock.advance(900_000);
    assert!(matches!(
        run.await.unwrap(),
        CompactionExit::Reconcile(FailureKind::Deadline)
    ));
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn gateway_restart_from_commit_reprepares_publication_proof() {
    let f = fixture("publication source", vec![Reply::Success], true, false).await;
    install_publication_retry_probe(&f).await;
    let mut sleeps = f.clock.subscribe_sleeps();
    let mut hook =
        arm_publication_test_hook(&f.store, "operation", PublicationTestPause::BeforeWriter);
    let runner = f.runner.clone();
    let run = tokio::spawn(async move { runner.run(CancellationToken::new()).await.unwrap() });
    hook.reached().await;
    f.store.database_connection().execute_unprepared(
        "UPDATE turn_event SET payload='{\"restart_race\":true}' WHERE id='publication-retry-probe'",
    ).await.unwrap();
    hook.release();
    wait_for_sleep(&mut sleeps, 10).await;
    let before_restart = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    let commit_checkpoint = match &before_restart.phase {
        RunnerPhase::Commit { checkpoint } => checkpoint.clone(),
        _ => panic!("validation retry did not preserve Commit phase"),
    };
    let summary = f
        .store
        .compaction_checkpoint(&commit_checkpoint)
        .await
        .unwrap()
        .unwrap()
        .summary;
    run.abort();
    let _ = run.await;

    let exit = f.runner.run(CancellationToken::new()).await.unwrap();
    assert!(matches!(exit, CompactionExit::Applied(_)));
    assert_eq!(f.provider.calls.lock().unwrap().len(), 1);
    let applied = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied.attempts, before_restart.attempts);
    assert_eq!(applied.retries, before_restart.retries);
    let RunnerPhase::Applied { checkpoint } = &applied.phase else {
        panic!("restarted Commit did not publish its candidate")
    };
    assert_eq!(checkpoint, &commit_checkpoint);
    assert_eq!(
        f.store
            .compaction_checkpoint(checkpoint)
            .await
            .unwrap()
            .unwrap()
            .summary,
        summary
    );
}

#[tokio::test]
async fn admitted_history_applies_after_real_new_message_materialization() {
    let f = fixture(&"accepted history H ".repeat(4000), vec![], true, false).await;
    let gate = f.provider.pause_next();
    let runner = f.runner.clone();
    let task = tokio::spawn(async move { runner.run(CancellationToken::new()).await });
    f.provider.wait_calls(1).await;

    let parent = f.store.get_thread_model("thread").await.unwrap().unwrap();
    let (_, mut next_turn) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
    next_turn.id = "new-work-turn".into();
    next_turn.reply_to_turn_id = None;
    f.store
        .materialize_turn_start(
            &parent,
            pioneer_protocol::SandboxMode::FullAccess,
            &next_turn,
            &[pioneer_protocol::UserInput::Text {
                text: "new work N".into(),
                text_elements: vec![],
            }],
            pioneer_protocol::PersistedActorRef::System,
        )
        .await
        .unwrap();
    let message = pioneer_protocol::TurnItem::UserMessage {
        id: "new-work-n".into(),
        text: "new work N".into(),
        attachments: vec![],
    };
    f.store
        .materialize_item_started(
            pioneer_protocol::ItemStartedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: next_turn.id.clone(),
                item: message.clone(),
            },
            1,
        )
        .await
        .unwrap();
    f.store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: next_turn.id,
                item: message,
            },
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        f.store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        1
    );
    gate.add_permits(1);

    let exit = task.await.unwrap().unwrap();
    let CompactionExit::Applied(checkpoint) = exit else {
        let state = f.store.compaction_runner_state("operation").await.unwrap();
        panic!("accepted H was not published after appending N: {exit:?}; state={state:?}");
    };
    assert!(
        f.provider.calls.lock().unwrap().len() > 1,
        "the fixture must persist intermediate checkpoints"
    );
    let checkpoint = f
        .store
        .compaction_checkpoint(&checkpoint)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.coverage.len(), 1);
    assert!(
        checkpoint
            .coverage
            .iter()
            .all(|source| source.id != "new-work-n")
    );
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
    assert_eq!(projection.manifest_id, recaptured.manifest_id);
    assert_eq!(projection.identity_sha256, recaptured.identity_sha256);
    // Old releases could store equivalent snapshots under different IDs.
    // Preserve the admission/recovery coverage for those existing aliases.
    let legacy_alias = pioneer_compaction::frozen::FrozenHistoryRef {
        manifest_id: "legacy-recaptured-projection".into(),
        ..recaptured
    };
    f.store
        .compaction_begin_frozen_history("ws", "thread", &legacy_alias)
        .await
        .unwrap();
    assert!(
        f.store
            .compaction_finish_frozen_history("ws", "thread", &legacy_alias)
            .await
            .unwrap()
    );
    prepared.source_projection = Some(legacy_alias);
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
async fn native_discovers_working_context_head_published_after_inherited_snapshot() {
    use pioneer_agent::compaction::controller::NativeContext;
    use pioneer_crud::compaction::{CommitOutcome, SourceAssertion};
    use pioneer_protocol::{PersistedActorRef, SandboxMode};
    use pioneer_provider::{ChatMessage, ProviderRegistry};

    // Large enough to overflow gpt-4's input budget, but still one bounded
    // canonical source assertion for the atomic publication fixture below.
    let inherited_text = "accepted inherited fact ".repeat(4_000);
    let f = fixture("unused", vec![], true, false).await;
    let db = f.store.database_connection();
    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    f.store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "accepted-inherited-work".into(),
                    text: inherited_text,
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    super::history::prepare_history(&f.store, "ws", "thread")
        .await
        .unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let mut accepted = super::history::load_line_history(&f.store, "ws", "thread", None, &fence)
        .await
        .unwrap();
    assert_eq!(accepted.len(), 1);
    accepted[0].provenance.as_mut().unwrap().inherited = true;
    let accepted_threads = std::collections::BTreeSet::from(["thread".to_owned()]);
    let frozen = super::frozen::capture(&f.store, "ws", "thread", &accepted_threads, &accepted)
        .await
        .unwrap();
    let parent_projection_json = serde_json::to_string(&frozen).unwrap();

    // C accepts the parent-owned immutable basis before K exists. The TaskRun
    // binding is the authority used by the later child-owned recapture.
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-c','ws','','agent','gpt-4','main-fixture','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-c','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-c','context-c','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task-c','ws','thread','thread','thread','turn','agent','running','Task C','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-c','task-c','run-c',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-c','task-c','run-c','context-c','turn-c','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES ('run-c','task-c','ws','thread','turn',?,CURRENT_TIMESTAMP)",
        [parent_projection_json.clone().into()],
    ))
    .await
    .unwrap();
    assert!(
        super::frozen::accepted_history_scopes(
            &f.store,
            "ws",
            "context-c",
            &parent_projection_json,
        )
        .await
        .is_err(),
        "a parent manifest is not a ready execution projection for C"
    );

    // Before A publishes K, another admitted child must be able to compact the
    // same raw inherited H through the complete Native runner/commit path.
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-d','ws','','agent','gpt-4','main-fixture','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-d','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-d','context-d','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task-d','ws','thread','thread','thread','turn','agent','running','Task D','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-d','task-d','run-d',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-d','task-d','run-d','context-d','turn-d','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES ('run-d','task-d','ws','thread','turn',?,CURRENT_TIMESTAMP)",
        [parent_projection_json.clone().into()],
    ))
    .await
    .unwrap();
    let d_projection_json = super::frozen::capture_execution_basis_json(
        &f.store,
        "ws",
        "context-d",
        Some("turn-d"),
        Some("turn-d"),
        None,
    )
    .await
    .unwrap();
    let d_projection: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(&d_projection_json).unwrap();
    let d_allowed =
        super::frozen::accepted_history_scopes(&f.store, "ws", "context-d", &d_projection_json)
            .await
            .unwrap();
    let d_history = super::frozen::restore(&f.store, "ws", &d_allowed, &d_projection)
        .await
        .unwrap();
    assert!(d_history[0].provenance.as_ref().unwrap().inherited);
    let providers = ProviderRegistry::new(|_| "fixture-key".into());
    providers
        .insert("summary-fixture", f.provider.clone())
        .unwrap();
    providers
        .insert("main-fixture", Arc::new(SmallWindowMain))
        .unwrap();
    let settings = CompactionSettings {
        selection: Some(ModelSelection {
            transport: Transport::Api,
            instance: "summary-fixture".into(),
            model: "summary-model".into(),
            effort: None,
        }),
    };
    let d_context = NativeContext {
        overflow_recovery: false,
        recovery_deadline_ms: None,
        workspace_id: "ws".into(),
        thread_id: "context-d".into(),
        turn_id: "turn-d".into(),
        conversation_thread_id: Some("thread".into()),
        provider_instance: "main-fixture".into(),
        provider: providers
            .get_or_create_for_workspace("ws", "main-fixture")
            .unwrap(),
        events: Arc::new(ExecutionEventHub::new()),
        cancellation: CancellationToken::new(),
    };
    let parent_thread = f.store.get_thread_model("thread").await.unwrap().unwrap();
    let (_, mut next_parent_turn) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
    next_parent_turn.id = "next-parent-turn".into();
    next_parent_turn.reply_to_turn_id = None;
    let parent_epoch_before_n = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    let barrier = f.provider.pause_next();
    let preparation = super::native::prepare_native_projection(
        &f.store,
        &providers,
        &settings,
        &d_context,
        ChatRequest {
            model: "gpt-4".into(),
            messages: vec![d_history[0].clone(), ChatMessage::user("Continue D")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        },
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
        Some(d_projection),
        None,
    );
    let append_new_parent_work = async {
        f.provider.wait_calls(1).await;
        f.store
            .materialize_turn_start(
                &parent_thread,
                SandboxMode::FullAccess,
                &next_parent_turn,
                &[pioneer_protocol::UserInput::Text {
                    text: "new parent work N".into(),
                    text_elements: vec![],
                }],
                PersistedActorRef::System,
            )
            .await
            .unwrap();
        let message = pioneer_protocol::TurnItem::UserMessage {
            id: "new-parent-work-n".into(),
            text: "new parent work N".into(),
            attachments: vec![],
        };
        f.store
            .materialize_item_started(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: next_parent_turn.id.clone(),
                    item: message.clone(),
                },
                1,
            )
            .await
            .unwrap();
        f.store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: next_parent_turn.id.clone(),
                    item: message,
                },
                2,
            )
            .await
            .unwrap();
        let parent_epoch_after_n = f
            .store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap();
        barrier.add_permits(1);
        assert!(
            parent_epoch_after_n > parent_epoch_before_n,
            "real parent Turn materialization must register N as new work"
        );
    };
    let (d_prepared, ()) = tokio::join!(preparation, append_new_parent_work);
    let d_prepared = d_prepared.unwrap();
    assert!(d_prepared.receipt.identity.checkpoint.is_some());
    assert!(
        d_prepared.request.messages[0]
            .provenance
            .as_ref()
            .unwrap()
            .inherited
    );
    let summarizer_calls = f.provider.calls.lock().unwrap().len();
    assert!(summarizer_calls > 0);

    // A later child accepts the already compacted H together with the new,
    // separately captured N. Capture/restore and Native preparation must keep
    // both exactly once and must not invoke the summarizer again.
    super::history::prepare_history(&f.store, "ws", "thread")
        .await
        .unwrap();
    let parent_fence = f.store.compaction_history_read_fence().await.unwrap();
    let parent_after_n =
        super::history::load_line_history(&f.store, "ws", "thread", None, &parent_fence)
            .await
            .unwrap();
    let n = parent_after_n
        .into_iter()
        .find(|message| message.content.contains("new parent work N"))
        .expect("new parent turn must materialize N");
    let n_sources = n
        .provenance
        .as_ref()
        .unwrap()
        .sources
        .iter()
        .map(|source| {
            (
                source.scope.clone(),
                source.id.clone(),
                source.version.clone(),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let d_checkpoint = f
        .store
        .compaction_checkpoint(d_prepared.receipt.identity.checkpoint.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(d_checkpoint.coverage.iter().all(|source| {
        !n_sources.contains(&(
            source.scope.clone(),
            source.id.clone(),
            source.version.clone(),
        ))
    }));
    let d_manifest = f
        .store
        .compaction_manifest_page(&d_checkpoint.operation_id, false, 0, 0)
        .await
        .unwrap();
    assert!(d_manifest.iter().all(|entry| {
        !n_sources.contains(&(
            entry.source.scope.clone(),
            entry.source.id.clone(),
            entry.source.version.clone(),
        ))
    }));
    // D completes a small own contribution and publishes it through the same
    // immutable output/delivery path used by a real TaskRun. E's basis is then
    // captured from the parent plus that accepted delivery; it is not assembled
    // from the prepared request or granted an ad-hoc checkpoint scope.
    f.store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "context-d".into(),
                turn_id: "turn-d".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "d-own-result".into(),
                    text: "accepted own work D".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    for statement in [
        "UPDATE turn SET status='completed' WHERE id='turn-d'",
        "UPDATE task_run SET status='succeeded' WHERE id='run-d'",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    let d_task_turn = f.store.get_task_run_turn("rt-d").await.unwrap().unwrap();
    let d_output = super::frozen::capture_task_output(&f.store, "ws", &d_task_turn)
        .await
        .unwrap();
    for statement in [
        "INSERT INTO task_result_candidate(id,task_id,run_id,task_run_turn_id,thread_id,turn_id,round,status,created_at,updated_at) VALUES ('candidate-d','task-d','run-d','rt-d','context-d','turn-d',0,'accepted',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('delivery-turn-d','thread','completed','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task_delivery(id,workspace_id,task_id,run_id,delivery_key,mode,thread_target,target_thread_id,status,attempt_count,max_attempts,delivered_turn_id) VALUES ('delivery-d','ws','task-d','run-d','delivery-d','thread','origin_thread','thread','delivered',1,1,'delivery-turn-d')",
        "INSERT INTO compaction_delivery_output(delivery_id,candidate_id,task_run_turn_id) VALUES ('delivery-d','candidate-d','rt-d')",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    f.store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "delivery-turn-d".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: pioneer_protocol::task_delivery_result_item_id("delivery-d"),
                    text: "accepted own work D".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let acknowledgement = f
        .store
        .compaction_source_page("ws", "thread", "delivery-turn-d", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries[0]
        .reference
        .clone();
    let delivered = f
        .store
        .compaction_delivery_output("ws", "delivery-d")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.output, d_output);
    let source_threads = super::frozen::accepted_history_scopes(
        &f.store,
        "ws",
        &delivered.output.source_thread,
        &serde_json::to_string(&delivered.output.history).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        source_threads,
        std::collections::BTreeSet::from(["context-d".to_owned()]),
        "the delivered output grants only D's own immutable result"
    );
    let delivery_fence = f.store.compaction_history_read_fence().await.unwrap();
    let mut source_epochs = std::collections::BTreeMap::from([(
        "thread".into(),
        f.store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
    )]);
    for source_thread in &source_threads {
        source_epochs.insert(
            source_thread.clone(),
            f.store
                .compaction_projection_version("ws", source_thread)
                .await
                .unwrap(),
        );
    }
    let accepted_output = super::delivered::AuthorizedOutputSet {
        workspace: "ws".into(),
        destination: "thread".into(),
        fence: delivery_fence,
        authorization_revision: 0,
        source_epochs,
        branches: vec![super::delivered::AuthorizedOutputBranch {
            snapshot: delivered,
            acknowledgement: acknowledgement.clone(),
            acknowledgements: vec![acknowledgement],
            source_threads,
        }],
    };
    let next_parent_projection_json = super::frozen::capture_execution_basis_with_outputs(
        &f.store,
        "ws",
        "thread",
        Some("next-parent-turn"),
        None,
        None,
        Some(&accepted_output),
    )
    .await
    .unwrap();
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('context-e','ws','','agent','gpt-4','main-fixture','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('context-e','thread','thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn-e','context-e','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('task-e','ws','thread','thread','thread','next-parent-turn','agent','running','Task E','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('run-e','task-e','run-e',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('rt-e','task-e','run-e','context-e','turn-e','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(statement).await.unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES ('run-e','task-e','ws','thread','next-parent-turn',?,CURRENT_TIMESTAMP)",
        [next_parent_projection_json.into()],
    ))
    .await
    .unwrap();
    let e_projection_json = super::frozen::capture_execution_basis_json(
        &f.store,
        "ws",
        "context-e",
        Some("turn-e"),
        Some("turn-e"),
        None,
    )
    .await
    .unwrap();
    let e_projection: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(&e_projection_json).unwrap();
    let e_allowed =
        super::frozen::accepted_history_scopes(&f.store, "ws", "context-e", &e_projection_json)
            .await
            .unwrap();
    let e_history = super::frozen::restore(&f.store, "ws", &e_allowed, &e_projection)
        .await
        .unwrap();
    assert_eq!(
        e_history
            .iter()
            .filter(|message| message.content.contains("new parent work N"))
            .count(),
        1
    );
    assert_eq!(
        e_history
            .iter()
            .filter(|message| message.content.contains("accepted own work D"))
            .count(),
        1
    );
    assert!(e_history.iter().all(|message| {
        message
            .provenance
            .as_ref()
            .unwrap()
            .sources
            .iter()
            .all(|source| {
                Some(source.id.as_str()) != d_prepared.receipt.identity.checkpoint.as_deref()
            })
    }));
    let e_context = NativeContext {
        overflow_recovery: false,
        recovery_deadline_ms: None,
        workspace_id: "ws".into(),
        thread_id: "context-e".into(),
        turn_id: "turn-e".into(),
        conversation_thread_id: Some("thread".into()),
        provider_instance: "main-fixture".into(),
        provider: providers
            .get_or_create_for_workspace("ws", "main-fixture")
            .unwrap(),
        events: Arc::new(ExecutionEventHub::new()),
        cancellation: CancellationToken::new(),
    };
    let e_prepared = super::native::prepare_native_projection(
        &f.store,
        &providers,
        &settings,
        &e_context,
        ChatRequest {
            model: "gpt-4".into(),
            messages: e_history
                .into_iter()
                .chain(std::iter::once(ChatMessage::user("Continue E")))
                .collect(),
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        },
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
        Some(e_projection),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        e_prepared
            .request
            .messages
            .iter()
            .filter(|message| message.content.contains("new parent work N"))
            .count(),
        1
    );
    assert_eq!(
        e_prepared
            .request
            .messages
            .iter()
            .filter(|message| message.content.contains("accepted own work D"))
            .count(),
        1,
        "the accepted own contribution from D must survive checkpoint reuse"
    );
    assert_eq!(
        e_prepared
            .request
            .messages
            .iter()
            .flat_map(|message| {
                message
                    .provenance
                    .iter()
                    .flat_map(|origin| origin.sources.iter())
            })
            .filter(|source| {
                Some(source.id.as_str()) == d_prepared.receipt.identity.checkpoint.as_deref()
            })
            .count(),
        1,
        "H must remain represented by one compatible checkpoint"
    );
    assert_eq!(
        e_prepared
            .request
            .messages
            .iter()
            .flat_map(|message| {
                message
                    .provenance
                    .iter()
                    .flat_map(|origin| origin.sources.iter())
            })
            .filter(|source| {
                d_checkpoint.coverage.iter().any(|covered| {
                    covered.scope == source.scope
                        && covered.id == source.id
                        && covered.version == source.version
                })
            })
            .count(),
        0,
        "checkpoint D must replace H instead of accompanying duplicate raw sources"
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), summarizer_calls);

    // K does not exist when C's immutable basis is captured.
    let source = accepted[0].provenance.as_ref().unwrap().sources[0].clone();
    let source = SourceRef {
        scope: source.scope,
        id: source.id,
        version: source.version,
    };
    let (kind, turn_id) = if let Some(turn) = source.scope.strip_prefix("item:") {
        (CanonicalSource::ToolItem, turn)
    } else if let Some(turn) = source.scope.strip_prefix("event:") {
        (CanonicalSource::Event, turn)
    } else {
        panic!("unexpected accepted source scope")
    };
    let assertion = SourceAssertion {
        revision: Some(source.version.rsplit_once(':').unwrap().1.parse().unwrap()),
        kind,
        turn_id: turn_id.into(),
        id: source.id.clone(),
        payload: super::history::reference_payload(&f.store, "ws", "thread", &source)
            .await
            .unwrap(),
    };
    let owner = super::native::native_owner("ws", "thread");
    let version = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    let selection = ModelSelection {
        transport: Transport::Api,
        instance: "summary-fixture".into(),
        model: "summary-model".into(),
        effort: None,
    };
    let operation = OperationSnapshot {
        id: "late-working-context-operation".into(),
        owner: owner.clone(),
        expected_checkpoint: None,
        projection_version: version,
        source_epochs: std::collections::BTreeMap::from([("thread".into(), version)]),
        admission: CompactionSettings::default()
            .admit(&selection, None, 0)
            .unwrap(),
        plan: CompactionPlan {
            mode: CompactionMode::Normal,
            coverage_domain: pioneer_compaction::CoverageDomain::WorkingContext,
            compact: vec![0],
            retain: vec![],
            coverage: vec![source.clone()],
            fingerprint: "late-working-context-plan".into(),
        },
    };
    f.store
        .compaction_admit("ws", "thread", &operation)
        .await
        .unwrap();
    let checkpoint = Checkpoint {
        id: "late-working-context-checkpoint".into(),
        operation_id: operation.id.clone(),
        owner: owner.clone(),
        previous: None,
        format_version: 1,
        coverage: vec![source],
        summary: HEADINGS
            .iter()
            .map(|heading| format!("{heading}\nAccepted state.\n"))
            .collect(),
        selection,
        projection_version: version,
    };
    f.store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
    assert_eq!(
        f.store
            .compaction_apply(&checkpoint, None, &[assertion])
            .await
            .unwrap(),
        CommitOutcome::Applied
    );

    // Production refresh recaptures the accepted Task basis for the current
    // execution. The new descriptor belongs to C, but still contains raw H;
    // discovering K remains the responsibility of request preparation.
    let execution_projection_json = super::frozen::capture_execution_basis_json(
        &f.store,
        "ws",
        "context-c",
        Some("turn-c"),
        Some("turn-c"),
        None,
    )
    .await
    .unwrap();
    let execution_projection: pioneer_compaction::frozen::FrozenHistoryRef =
        serde_json::from_str(&execution_projection_json).unwrap();
    let allowed = super::frozen::accepted_history_scopes(
        &f.store,
        "ws",
        "context-c",
        &execution_projection_json,
    )
    .await
    .unwrap();
    assert_eq!(
        f.store
            .compaction_frozen_history_owner("ws", &execution_projection)
            .await
            .unwrap()
            .as_deref(),
        Some("context-c")
    );
    let restored = super::frozen::restore(&f.store, "ws", &allowed, &execution_projection)
        .await
        .unwrap();
    assert!(restored[0].provenance.as_ref().unwrap().inherited);
    assert!(
        restored[0]
            .provenance
            .as_ref()
            .unwrap()
            .sources
            .iter()
            .all(|source| !source.scope.starts_with("checkpoint:")),
        "child recapture must leave raw H for checkpoint discovery"
    );

    // Exercise the same discovery/projection used by post-turn preparation.
    let mut projected = restored.clone();
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "context-c",
        &allowed,
        &mut projected,
    )
    .await
    .unwrap();
    assert_eq!(projected.len(), 1);
    let projected_origin = projected[0].provenance.as_ref().unwrap();
    assert_eq!(projected_origin.sources[0].id, checkpoint.id);
    assert!(projected_origin.inherited);

    let context = NativeContext {
        overflow_recovery: false,
        recovery_deadline_ms: None,
        workspace_id: "ws".into(),
        thread_id: "context-c".into(),
        turn_id: "turn-c".into(),
        conversation_thread_id: Some("thread".into()),
        provider_instance: "main-fixture".into(),
        provider: providers
            .get_or_create_for_workspace("ws", "main-fixture")
            .unwrap(),
        events: Arc::new(ExecutionEventHub::new()),
        cancellation: CancellationToken::new(),
    };
    let request = ChatRequest {
        model: "gpt-4".into(),
        messages: vec![restored[0].clone(), ChatMessage::user("Continue")],
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    let limits = pioneer_provider::catalog::model_catalog()
        .unwrap()
        .limits("openai", &request.model);
    let budget = ModelBudget::new(
        Some(limits.context_window),
        limits.max_input,
        limits.max_output,
    );
    let raw_projection = pioneer_agent::compaction::request::NativeRequestProjection::full(
        request.clone(),
        vec![],
        budget.clone(),
        false,
    )
    .unwrap();
    assert!(!budget.fits(
        raw_projection.estimated_input_tokens,
        raw_projection.output_reserve,
        false
    ));
    let compact_request = ChatRequest {
        messages: vec![projected[0].clone(), ChatMessage::user("Continue")],
        ..request.clone()
    };
    let compact_projection = pioneer_agent::compaction::request::NativeRequestProjection::full(
        compact_request,
        vec![],
        budget.clone(),
        false,
    )
    .unwrap();
    assert!(budget.fits(
        compact_projection.estimated_input_tokens,
        compact_projection.output_reserve,
        false
    ));
    let prepared = super::native::prepare_native_projection(
        &f.store,
        &providers,
        &settings,
        &context,
        request.clone(),
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
        Some(execution_projection.clone()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(prepared.request.messages.len(), 2);
    assert_eq!(
        prepared.request.messages[0]
            .provenance
            .as_ref()
            .unwrap()
            .sources[0]
            .id,
        checkpoint.id
    );
    assert!(
        prepared.request.messages[0]
            .provenance
            .as_ref()
            .unwrap()
            .inherited
    );
    assert!(
        f.provider.calls.lock().unwrap().len() == summarizer_calls,
        "a compatible late-published checkpoint avoids another summary"
    );

    let repeated = super::native::prepare_native_projection(
        &f.store,
        &providers,
        &settings,
        &context,
        request,
        None,
        false,
        f.observer.clone(),
        f.clock.clone(),
        Some(execution_projection),
        None,
    )
    .await
    .unwrap();
    assert_eq!(repeated.request.messages, prepared.request.messages);
    assert!(
        repeated.request.messages[0]
            .provenance
            .as_ref()
            .unwrap()
            .inherited
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), summarizer_calls);

    let mut foreign = restored;
    foreign[0].provenance.as_mut().unwrap().thread_id = "foreign".into();
    assert!(
        super::checkpoint::project_accepted_checkpoints(
            &f.store,
            "ws",
            "context-c",
            &allowed,
            &mut foreign,
        )
        .await
        .is_err()
    );
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
    let preparation_work = super::coverage::observe_preparation_work(&f.store, "ws");
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let mut resolver = super::coverage::CheckpointGraphResolver::default();
    let first_graph = resolver
        .resolve(&f.store, "ws", Some(&allowed), &checkpoint_ref)
        .await
        .unwrap()
        .unwrap();
    let first_edge_loads = preparation_work.edge_loads();
    let first_closure_builds = preparation_work.closure_builds();
    assert!(first_edge_loads > 0);
    assert_eq!(first_closure_builds, 1);
    let first_projection = resolver
        .projection_metadata(&f.store, "ws", &first_graph)
        .await
        .unwrap();
    let first_body = resolver
        .projection_body(&f.store, "ws", &checkpoint_ref)
        .await
        .unwrap();
    let first_body_loads = preparation_work.body_loads();
    assert_eq!(first_body.id, checkpoint_ref.id);
    assert_eq!(
        first_projection.coverage_domain,
        pioneer_compaction::CoverageDomain::OwnContribution
    );
    assert!(first_body_loads > 0);
    assert!(resolver.cached_payload_bytes() > 0);
    let repeated_graph = resolver
        .resolve(&f.store, "ws", Some(&allowed), &checkpoint_ref)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_graph.leaves, repeated_graph.leaves);
    assert_eq!(
        preparation_work.edge_loads(),
        first_edge_loads,
        "one preparation reloaded immutable checkpoint edges"
    );
    assert_eq!(preparation_work.closure_builds(), first_closure_builds);
    assert!(preparation_work.revalidations() > 0);
    let repeated_projection = resolver
        .projection_metadata(&f.store, "ws", &repeated_graph)
        .await
        .unwrap();
    let repeated_body = resolver
        .projection_body(&f.store, "ws", &checkpoint_ref)
        .await
        .unwrap();
    assert!(std::sync::Arc::ptr_eq(&first_body, &repeated_body));
    assert_eq!(
        repeated_projection.coverage_domain,
        first_projection.coverage_domain
    );
    assert_eq!(
        preparation_work.body_loads(),
        first_body_loads,
        "one preparation reloaded checkpoint bodies and coverage"
    );
    assert!(
        resolver
            .resolve(
                &f.store,
                "ws",
                Some(&std::collections::BTreeSet::from([
                    "other-thread".to_owned()
                ])),
                &checkpoint_ref,
            )
            .await
            .is_err(),
        "prepared graph metadata became authority for a narrower scope"
    );
    assert!(
        resolver
            .resolve(&f.store, "other-workspace", Some(&allowed), &checkpoint_ref)
            .await
            .unwrap()
            .is_none(),
        "prepared graph metadata crossed its workspace boundary"
    );
    let mut wrong_scope = checkpoint_ref.clone();
    wrong_scope.scope = "checkpoint:other-owner".into();
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &wrong_scope)
            .await
            .unwrap()
            .is_none(),
        "an equal checkpoint id in another scope reused prepared metadata"
    );
    let mut stale_identity = checkpoint_ref.clone();
    stale_identity.version.push_str("-stale");
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &stale_identity)
            .await
            .unwrap()
            .is_none(),
        "an equal checkpoint id with another version reused prepared metadata"
    );
    let mut next_preparation = super::coverage::CheckpointGraphResolver::default();
    next_preparation
        .resolve(&f.store, "ws", Some(&allowed), &checkpoint_ref)
        .await
        .unwrap()
        .unwrap();
    assert!(
        preparation_work.edge_loads() > first_edge_loads
            && preparation_work.closure_builds() > first_closure_builds,
        "an independent preparation retained the previous request's graph"
    );
    let leaves = super::coverage::checkpoint_leaves(&f.store, "ws", &allowed, &checkpoint_ref)
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
    let edited_source = &raw_request.messages[0].provenance.as_ref().unwrap().sources[0];
    let edited_source = SourceRef {
        scope: edited_source.scope.clone(),
        id: edited_source.id.clone(),
        version: edited_source.version.clone(),
    };
    let original_revision: i64 = edited_source
        .version
        .strip_prefix("event-revision:")
        .unwrap()
        .parse()
        .unwrap();
    let original_payload =
        super::history::reference_payload(&f.store, "ws", "thread", &edited_source)
            .await
            .unwrap();
    let cold_body = super::coverage::pause_body_load(&f.store, "ws", &checkpoint_ref.id);
    let projection_store = f.store.clone();
    let projection_head = checkpoint_ref.id.clone();
    let projection_owner = super::native::native_owner("ws", "thread");
    let original_messages = raw_request.messages.clone();
    let projected_messages = original_messages.clone();
    let projection = tokio::spawn(async move {
        let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
        let mut messages = projected_messages;
        let mut resolver = super::coverage::CheckpointGraphResolver::default();
        let result = super::checkpoint::project_checkpoint_with_resolver(
            &projection_store,
            super::checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: "thread",
                owner: &projection_owner,
                allowed: &allowed,
            },
            &projection_head,
            &mut messages,
            &mut resolver,
        )
        .await;
        (result, messages)
    });
    cold_body.reached().await;
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload='changed during cold checkpoint body load' WHERE id=?",
            [edited_source.id.clone().into()],
        ))
        .await
        .unwrap();
    cold_body.release();
    let (projection_result, messages_after_rejection) = projection.await.unwrap();
    assert!(
        projection_result.is_err(),
        "checkpoint projection accepted a leaf changed during cold body loading"
    );
    assert_eq!(
        messages_after_rejection, original_messages,
        "failed checkpoint projection modified the original messages"
    );
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload='changed source' WHERE id=?",
            [edited_source.id.clone().into()],
        ))
        .await
        .unwrap();
    let changed_source = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.reference.id == edited_source.id)
        .unwrap()
        .reference;
    let changed_revision: i64 = changed_source
        .version
        .strip_prefix("event-revision:")
        .unwrap()
        .parse()
        .unwrap();
    assert!(changed_revision > original_revision);
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &checkpoint_ref)
            .await
            .unwrap()
            .is_none(),
        "cached graph metadata hid an edited canonical leaf"
    );
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
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload=? WHERE id=?",
            [original_payload.into(), edited_source.id.clone().into()],
        ))
        .await
        .unwrap();
    let restored_source = f
        .store
        .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.reference.id == edited_source.id)
        .unwrap()
        .reference;
    let restored_revision: i64 = restored_source
        .version
        .strip_prefix("event-revision:")
        .unwrap()
        .parse()
        .unwrap();
    assert!(restored_revision > changed_revision);
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &checkpoint_ref)
            .await
            .unwrap()
            .is_none(),
        "restoring equal text must not revive a stale revision"
    );
    assert_eq!(f.provider.calls.lock().unwrap().len(), count);
}

#[tokio::test]
async fn prepared_graph_revalidates_other_thread_edit_and_delete_independently() {
    let f = fixture("current leaf", vec![], true, false).await;
    f.store.database_connection().execute_unprepared(
        r#"INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('source-thread','ws','','chat','fixture','fixture','active','user','private',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('source-turn','source-thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
         INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('edit-leaf','source-turn',0,'text','edit leaf','{"type":"text","text":"edit leaf"}',CURRENT_TIMESTAMP),('delete-leaf','source-turn',1,'text','delete leaf','{"type":"text","text":"delete leaf"}',CURRENT_TIMESTAMP)"#
    ).await.unwrap();
    let fence = f.store.compaction_history_read_fence().await.unwrap();
    let mut parent_basis =
        super::history::load_line_history(&f.store, "ws", "source-thread", None, &fence)
            .await
            .unwrap();
    for message in &mut parent_basis {
        message.provenance.as_mut().unwrap().inherited = true;
    }
    let parent_descriptor = super::frozen::capture(
        &f.store,
        "ws",
        "source-thread",
        &std::collections::BTreeSet::from(["source-thread".to_owned()]),
        &parent_basis,
    )
    .await
    .unwrap();
    for statement in [
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('edit-root-thread','ws','','agent','fixture','fixture','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('edit-root-turn','edit-root-thread','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('edit-root-thread','source-thread','source-thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('edit-root-task','ws','thread','source-thread','source-thread','source-turn','agent','running','Edit root','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('edit-root-run','edit-root-task','edit-root-run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('edit-root-run-turn','edit-root-task','edit-root-run','edit-root-thread','edit-root-turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('delete-root-thread','ws','','agent','fixture','fixture','active','task_run','internal',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('delete-root-turn','delete-root-thread','in_progress','conversation','system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('delete-root-thread','source-thread','source-thread',1,CURRENT_TIMESTAMP)",
        "INSERT INTO task(id,workspace_id,owner_kind,owner_id,created_by_thread_id,created_by_turn_id,executor_kind,status,title,goal) VALUES ('delete-root-task','ws','thread','source-thread','source-thread','source-turn','agent','running','Delete root','fixture')",
        "INSERT INTO task_run(id,task_id,run_group_id,attempt_number,run_number,status,executor_kind) VALUES ('delete-root-run','delete-root-task','delete-root-run',1,1,'running','agent')",
        "INSERT INTO task_run_turn(id,task_id,run_id,thread_id,turn_id,kind,round,sequence,status,created_at) VALUES ('delete-root-run-turn','delete-root-task','delete-root-run','delete-root-thread','delete-root-turn','initial',0,1,'in_progress',CURRENT_TIMESTAMP)",
    ] {
        f.store
            .database_connection()
            .execute_unprepared(statement)
            .await
            .unwrap();
    }
    let parent_json = serde_json::to_string(&parent_descriptor).unwrap();
    for (run, task) in [
        ("edit-root-run", "edit-root-task"),
        ("delete-root-run", "delete-root-task"),
    ] {
        f.store
            .database_connection()
            .execute_raw(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO task_run_conversation_snapshot(run_id,task_id,workspace_id,conversation_thread_id,source_turn_id,history_json,created_at) VALUES (?,?,'ws','source-thread','source-turn',?,CURRENT_TIMESTAMP)",
                [run.into(), task.into(), parent_json.clone().into()],
            ))
            .await
            .unwrap();
    }

    async fn publish(
        fixture: &Fixture,
        source_id: &str,
        root_thread: &str,
        operation_id: &str,
    ) -> SourceRef {
        let source = fixture
            .store
            .compaction_source_page("ws", "source-thread", "source-turn", PagedSource::Input, 0)
            .await
            .unwrap()
            .entries
            .into_iter()
            .find(|entry| entry.reference.id == source_id)
            .unwrap()
            .reference;
        let execution_turn = match root_thread {
            "edit-root-thread" => "edit-root-turn",
            "delete-root-thread" => "delete-root-turn",
            _ => panic!("unexpected cross-thread root"),
        };
        let accepted_basis = super::frozen::capture_execution_basis_prepared(
            &fixture.store,
            "ws",
            root_thread,
            Some(execution_turn),
            Some(execution_turn),
            None,
        )
        .await
        .unwrap();
        let selection = ModelSelection {
            transport: Transport::Api,
            instance: "fixture".into(),
            model: "fixture".into(),
            effort: None,
        };
        let root_epoch = accepted_basis.source_epochs[root_thread];
        let operation = OperationSnapshot {
            id: operation_id.into(),
            owner: super::native::native_owner("ws", root_thread),
            expected_checkpoint: None,
            projection_version: root_epoch,
            source_epochs: accepted_basis.source_epochs.clone(),
            admission: CompactionSettings::default()
                .admit(&selection, None, 0)
                .unwrap(),
            plan: CompactionPlan {
                mode: CompactionMode::Normal,
                coverage_domain: pioneer_compaction::CoverageDomain::WorkingContext,
                compact: vec![0],
                retain: vec![],
                coverage: vec![source.clone()],
                fingerprint: format!("{operation_id}-plan"),
            },
        };
        fixture
            .store
            .compaction_admit_for_turn("ws", root_thread, &operation, Some(execution_turn))
            .await
            .unwrap();
        fixture
            .store
            .compaction_bind_source_projection(operation_id, &accepted_basis.descriptor)
            .await
            .unwrap();
        let budget = ModelBudget::new(Some(4096), None, None);
        fixture
            .store
            .compaction_prepare_runner(operation_id, &budget, 1, 0)
            .await
            .unwrap();
        fixture
            .store
            .compaction_append_manifest(
                operation_id,
                &[ManifestEntry {
                    ordinal: 0,
                    unit: 0,
                    reference_only: false,
                    thread_id: "source-thread".into(),
                    source,
                }],
            )
            .await
            .unwrap();
        fixture
            .store
            .compaction_activate_runner(
                operation_id,
                &RunnerState::new(900_000, &budget, 500, None).unwrap(),
            )
            .await
            .unwrap();
        let summarizer = Arc::new(
            pioneer_agent::compaction::NativeSummarizer::new(
                fixture.provider.clone(),
                selection,
                budget,
            )
            .unwrap(),
        );
        let runner = CompactionRunner::new(
            fixture.store.clone(),
            "ws".into(),
            root_thread.into(),
            operation,
            summarizer,
            Arc::new(Target(true)),
            fixture.observer.clone(),
            fixture.clock.clone(),
        );
        let CompactionExit::Applied(checkpoint_id) =
            runner.run(CancellationToken::new()).await.unwrap()
        else {
            panic!("cross-thread checkpoint did not apply")
        };
        fixture
            .store
            .compaction_checkpoint_source("ws", root_thread, &checkpoint_id)
            .await
            .unwrap()
            .unwrap()
    }

    let edit_root = publish(&f, "edit-leaf", "edit-root-thread", "edit-leaf-operation").await;
    let delete_root = publish(
        &f,
        "delete-leaf",
        "delete-root-thread",
        "delete-leaf-operation",
    )
    .await;
    let allowed = std::collections::BTreeSet::from([
        "edit-root-thread".to_owned(),
        "delete-root-thread".to_owned(),
        "source-thread".to_owned(),
    ]);
    assert_eq!(
        f.store
            .compaction_reference_thread("ws", &edit_root)
            .await
            .unwrap()
            .as_deref(),
        Some("edit-root-thread")
    );
    assert_eq!(
        f.store
            .compaction_reference_thread("ws", &delete_root)
            .await
            .unwrap()
            .as_deref(),
        Some("delete-root-thread")
    );
    let mut resolver = super::coverage::CheckpointGraphResolver::default();
    for root in [&edit_root, &delete_root] {
        let graph = resolver
            .resolve(&f.store, "ws", Some(&allowed), root)
            .await
            .unwrap()
            .expect("cross-thread graph must be valid before mutation");
        assert_eq!(graph.leaves.len(), 1);
        assert_eq!(
            graph.leaves.iter().next().unwrap().thread.as_str(),
            "source-thread"
        );
    }
    let source_epoch_before_edit = f
        .store
        .compaction_projection_version("ws", "source-thread")
        .await
        .unwrap();
    let edit_root_epoch = f
        .store
        .compaction_projection_version("ws", "edit-root-thread")
        .await
        .unwrap();
    let delete_root_epoch = f
        .store
        .compaction_projection_version("ws", "delete-root-thread")
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_unprepared(
            r#"UPDATE turn_input SET text='edited',payload='{"type":"text","text":"edited"}' WHERE id='edit-leaf'"#,
        )
        .await
        .unwrap();
    assert!(
        f.store
            .compaction_projection_version("ws", "source-thread")
            .await
            .unwrap()
            > source_epoch_before_edit
    );
    assert_eq!(
        f.store
            .compaction_projection_version("ws", "edit-root-thread")
            .await
            .unwrap(),
        edit_root_epoch
    );
    assert_eq!(
        f.store
            .compaction_projection_version("ws", "delete-root-thread")
            .await
            .unwrap(),
        delete_root_epoch
    );
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &edit_root)
            .await
            .unwrap()
            .is_none(),
        "cached graph hid an edited canonical leaf"
    );
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &delete_root)
            .await
            .unwrap()
            .is_some(),
        "independent delete graph was not current immediately before delete"
    );
    let source_epoch_before_delete = f
        .store
        .compaction_projection_version("ws", "source-thread")
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_input WHERE id='delete-leaf'")
        .await
        .unwrap();
    assert!(
        f.store
            .compaction_projection_version("ws", "source-thread")
            .await
            .unwrap()
            > source_epoch_before_delete
    );
    assert_eq!(
        f.store
            .compaction_projection_version("ws", "edit-root-thread")
            .await
            .unwrap(),
        edit_root_epoch
    );
    assert_eq!(
        f.store
            .compaction_projection_version("ws", "delete-root-thread")
            .await
            .unwrap(),
        delete_root_epoch
    );
    assert!(
        resolver
            .resolve(&f.store, "ws", Some(&allowed), &delete_root)
            .await
            .unwrap()
            .is_none(),
        "cached graph hid deletion of its current canonical leaf"
    );
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
async fn prepared_empty_manifest_keeps_only_manifest_scopes_and_epochs() {
    let f = fixture("unused", vec![], true, false).await;
    let build_scopes = std::collections::BTreeSet::from([
        "thread".to_owned(),
        "authorized-but-unselected".to_owned(),
    ]);
    let prepared = super::frozen::capture_with_imports_prepared(
        &f.store,
        "ws",
        "thread",
        &build_scopes,
        &[],
        &std::collections::BTreeMap::new(),
        super::coverage::CheckpointGraphResolver::default(),
    )
    .await
    .unwrap();
    assert!(prepared.messages.is_empty());
    let descriptor_json = serde_json::to_string(&prepared.descriptor).unwrap();
    let restored_scopes =
        super::frozen::accepted_history_scopes(&f.store, "ws", "thread", &descriptor_json)
            .await
            .unwrap();
    assert_eq!(
        prepared.accepted_scopes,
        std::collections::BTreeSet::from(["thread".to_owned()])
    );
    assert_eq!(prepared.accepted_scopes, restored_scopes);
    let captured_epochs = std::collections::BTreeMap::from([
        ("thread".to_owned(), 7_u64),
        ("authorized-but-unselected".to_owned(), 11_u64),
    ]);
    let downstream_epochs =
        super::frozen::prepared_source_epochs(&prepared.accepted_scopes, &captured_epochs).unwrap();
    assert_eq!(
        downstream_epochs,
        std::collections::BTreeMap::from([("thread".to_owned(), 7_u64)])
    );
}

#[tokio::test]
async fn native_prepared_history_keeps_captured_epoch_until_admission() {
    use pioneer_agent::compaction::controller::NativeContext;
    use pioneer_protocol::{PersistedActorRef, SandboxMode, UserInput};
    use pioneer_provider::{ChatMessage, ProviderRegistry};

    let f = fixture("unused synthetic event", vec![], true, false).await;
    f.store
        .database_connection()
        .execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    f.store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: pioneer_protocol::TurnItem::AgentMessage {
                    id: "captured-canonical-history".into(),
                    text: "captured canonical history ".repeat(4_000),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    let thread = f.store.get_thread_model("thread").await.unwrap().unwrap();
    let (_, mut mutation_turn) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
    mutation_turn.id = "epoch-mutation-turn".into();
    mutation_turn.reply_to_turn_id = None;
    f.store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &mutation_turn,
            &[UserInput::Text {
                text: "source excluded from prepared context".into(),
                text_elements: vec![],
            }],
            PersistedActorRef::System,
        )
        .await
        .unwrap();
    let mutation_source = f
        .store
        .compaction_source_page("ws", "thread", &mutation_turn.id, PagedSource::Input, 0)
        .await
        .unwrap()
        .entries
        .into_iter()
        .next()
        .expect("materialized mutation input is missing")
        .reference;
    let providers = ProviderRegistry::new(|_| "fixture-key".into());
    providers
        .insert("summary-fixture", f.provider.clone())
        .unwrap();
    providers
        .insert("main-fixture", Arc::new(SmallWindowMain))
        .unwrap();
    let settings = CompactionSettings {
        selection: Some(ModelSelection {
            transport: Transport::Api,
            instance: "summary-fixture".into(),
            model: "summary-model".into(),
            effort: None,
        }),
    };
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
    let prepared = super::frozen::capture_execution_basis_prepared(
        &f.store,
        "ws",
        "thread",
        None,
        Some(&mutation_turn.id),
        None,
    )
    .await
    .unwrap();
    assert!(
        prepared
            .messages
            .iter()
            .map(|message| message.content.len())
            .sum::<usize>()
            > 32_000,
        "canonical prepared history is too small to require compaction"
    );
    assert!(prepared.messages.iter().all(|message| {
        message.provenance.as_ref().is_none_or(|origin| {
            origin
                .sources
                .iter()
                .all(|source| source.id != mutation_source.id)
        })
    }));
    let captured_epoch = prepared.source_epochs["thread"];
    let owner = super::native::native_owner("ws", "thread");
    let head_before = f.store.compaction_head(&owner).await.unwrap();
    let operations_before = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS count FROM compaction_operation".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "count")
        .unwrap();
    let changed_input = UserInput::Text {
        text: "changed source excluded from prepared context".into(),
        text_elements: vec![],
    };
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_input SET text=?,payload=? WHERE id=?",
            [
                "changed source excluded from prepared context".into(),
                serde_json::to_string(&changed_input).unwrap().into(),
                mutation_source.id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let current_epoch = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    assert!(current_epoch > captured_epoch);
    let captured_sources = prepared
        .messages
        .iter()
        .flat_map(|message| message.provenance.iter())
        .flat_map(|origin| origin.sources.iter())
        .map(|source| SourceRef {
            scope: source.scope.clone(),
            id: source.id.clone(),
            version: source.version.clone(),
        })
        .collect::<Vec<_>>();
    assert!(!captured_sources.is_empty());
    assert!(
        f.store
            .compaction_references_current("ws", "thread", &captured_sources)
            .await
            .unwrap(),
        "unrelated epoch mutation invalidated an exact prepared reference"
    );
    let request = ChatRequest {
        model: "gpt-4".into(),
        messages: prepared
            .messages
            .iter()
            .cloned()
            .chain(std::iter::once(ChatMessage::user("Continue")))
            .collect(),
        temperature: None,
        max_tokens: None,
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    };
    let error = super::native::prepare_native_projection_from_history(
        &f.store,
        &providers,
        &settings,
        &context,
        request,
        prepared,
        f.observer.clone(),
        f.clock.clone(),
    )
    .await
    .err()
    .expect("stale captured epoch unexpectedly reached a successful native preparation");
    assert!(
        error
            .to_string()
            .contains("source scope or epoch changed before admission"),
        "native preparation failed before captured epochs reached admission: {error:#}"
    );
    let operations_after = f
        .store
        .database_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT count(*) AS count FROM compaction_operation".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "count")
        .unwrap();
    assert_eq!(operations_after, operations_before);
    assert_eq!(
        f.store.compaction_head(&owner).await.unwrap(),
        head_before,
        "stale prepared context changed the published head"
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
    let prepared =
        super::frozen::capture_execution_basis_prepared(&f.store, "ws", "thread", None, None, None)
            .await
            .unwrap();
    let restored_prepared = super::frozen::restore(&f.store, "ws", &allowed, &prepared.descriptor)
        .await
        .unwrap();
    assert_eq!(prepared.messages, restored_prepared);
    assert_eq!(prepared.messages, current);
    let prepared_scopes = super::frozen::accepted_history_scopes(
        &f.store,
        "ws",
        "thread",
        &serde_json::to_string(&prepared.descriptor).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(prepared.accepted_scopes, prepared_scopes);
    assert!(
        prepared
            .source_epochs
            .keys()
            .eq(prepared.accepted_scopes.iter())
    );
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
    let reused = super::frozen::capture(&f.store, "ws", "thread", &allowed, &frozen)
        .await
        .unwrap();
    assert_eq!(snapshot, reused);
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
    // A view-only fixture misses the production failure: the background worker
    // physically compresses sources AFTER the immutable snapshot was captured.
    let epoch = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    assert!(
        crate::database::compress_history_payloads_for_test(&f.store)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(
        f.store
            .compaction_projection_version("ws", "thread")
            .await
            .unwrap(),
        epoch
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
    fn identities(messages: &[ChatMessage]) -> Vec<Vec<(String, String, String)>> {
        messages
            .iter()
            .map(|message| {
                message
                    .provenance
                    .as_ref()
                    .unwrap()
                    .sources
                    .iter()
                    .map(|source| {
                        (
                            source.scope.clone(),
                            source.id.clone(),
                            source.version.clone(),
                        )
                    })
                    .collect()
            })
            .collect()
    }
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
        let assertions = (start + 1..=end)
            .map(|number| {
                let item = format!("work-{number:03}");
                page.entries
                    .iter()
                    .find(|row| row.item_id.as_deref() == Some(item.as_str()))
                    .unwrap_or_else(|| panic!("missing exact work source {number}"))
            })
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
                coverage_domain: pioneer_compaction::CoverageDomain::OwnContribution,
                compact: (0..assertions.len()).collect(),
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
    let epoch_after_seed_delete = f
        .store
        .compaction_projection_version("ws", "thread")
        .await
        .unwrap();
    let owner = super::native::native_owner("ws", "thread");
    let parent = f.store.get_thread_model("thread").await.unwrap().unwrap();
    let (_, mut next_turn) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
    next_turn.id = "new-parent-work-turn".into();
    next_turn.reply_to_turn_id = None;
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
        if n == 41 {
            f.store
                .materialize_turn_start(
                    &parent,
                    pioneer_protocol::SandboxMode::FullAccess,
                    &next_turn,
                    &[pioneer_protocol::UserInput::Text {
                        text: "new work after checkpoint H".into(),
                        text_elements: vec![],
                    }],
                    pioneer_protocol::PersistedActorRef::System,
                )
                .await
                .unwrap();
            let message = pioneer_protocol::TurnItem::UserMessage {
                id: "new-parent-work".into(),
                text: "new work after checkpoint H".into(),
                attachments: vec![],
            };
            f.store
                .materialize_item_started(
                    pioneer_protocol::ItemStartedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "thread".into(),
                        turn_id: next_turn.id.clone(),
                        item: message.clone(),
                    },
                    1,
                )
                .await
                .unwrap();
            f.store
                .materialize_item_completed(
                    pioneer_protocol::ItemCompletedNotification {
                        workspace_id: "ws".into(),
                        thread_id: "thread".into(),
                        turn_id: next_turn.id.clone(),
                        item: message,
                    },
                    2,
                )
                .await
                .unwrap();
            assert_eq!(
                f.store
                    .compaction_projection_version("ws", "thread")
                    .await
                    .unwrap(),
                epoch_after_seed_delete + 1
            );
            let checkpoint = f
                .store
                .compaction_checkpoint_source("ws", "thread", first.as_deref().unwrap())
                .await
                .unwrap()
                .expect("new work must not retire checkpoint H");
            assert!(
                f.store
                    .compaction_reference_fragment("ws", "thread", &checkpoint, 0)
                    .await
                    .unwrap()
                    .unwrap()
                    .text
                    .contains("State through 40"),
                "checkpoint text remains readable after harmless epoch growth"
            );
        }
        if n == 60 {
            at_sixty = Some(f.store.compaction_history_read_fence().await.unwrap());
        }
    }
    let first = first.unwrap();
    let seventy = publish(&f.store, &owner, 70, Some(&first), 40).await;
    let eighty_five = publish(&f.store, &owner, 85, Some(&seventy), 70).await;
    let head = publish(&f.store, &owner, 100, Some(&eighty_five), 85).await;
    let head_source = f
        .store
        .compaction_checkpoint_source("ws", "thread", &head)
        .await
        .unwrap()
        .unwrap();
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let mut prepared_graph = super::coverage::CheckpointGraphResolver::default();
    prepared_graph
        .resolve(&f.store, "ws", Some(&allowed), &head_source)
        .await
        .unwrap()
        .unwrap();
    let first_checkpoint = f
        .store
        .compaction_checkpoint(&first)
        .await
        .unwrap()
        .unwrap();
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_checkpoint SET status='failed' WHERE id=?",
            [first.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        prepared_graph
            .resolve(&f.store, "ws", Some(&allowed), &head_source)
            .await
            .is_err(),
        "cached graph metadata hid a changed intermediate checkpoint"
    );
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_checkpoint SET status='retained' WHERE id=?",
            [first.clone().into()],
        ))
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM compaction_checkpoint WHERE id=?",
            [first.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        prepared_graph
            .resolve(&f.store, "ws", Some(&allowed), &head_source)
            .await
            .is_err(),
        "cached graph metadata hid a deleted intermediate checkpoint"
    );
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_operation SET status='running' WHERE id=?",
            [first_checkpoint.operation_id.clone().into()],
        ))
        .await
        .unwrap();
    f.store
        .compaction_save_candidate(&first_checkpoint, 0)
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_checkpoint SET status='retained' WHERE id=?",
            [first.clone().into()],
        ))
        .await
        .unwrap();
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE compaction_operation SET status='completed' WHERE id=?",
            [first_checkpoint.operation_id.clone().into()],
        ))
        .await
        .unwrap();
    assert!(
        prepared_graph
            .resolve(&f.store, "ws", Some(&allowed), &head_source)
            .await
            .unwrap()
            .is_some(),
        "restored intermediate checkpoint did not revalidate cached metadata"
    );
    let raw =
        super::history::load_task_line_history(&f.store, "ws", "thread", None, &at_sixty.unwrap())
            .await
            .unwrap();
    assert_eq!(raw.len(), 61);
    let raw_n = raw
        .iter()
        .find(|message| message.content.contains("new work after checkpoint H"))
        .unwrap();
    let raw_n_identity = identities(std::slice::from_ref(raw_n));
    let mut selected = raw.clone();
    let selection_work = super::coverage::observe_preparation_work(&f.store, "ws");
    let mut selection_resolver =
        super::coverage::CheckpointGraphResolver::with_cache_limits(256, 64 * 1024);
    assert_eq!(
        super::checkpoint::project_compatible_checkpoint_with_resolver(
            &f.store,
            super::checkpoint::ProjectionContext {
                workspace: "ws",
                context_thread: "thread",
                source_thread: "thread",
                owner: &owner,
                allowed: &allowed,
            },
            &head,
            &mut selected,
            &mut selection_resolver,
        )
        .await
        .unwrap(),
        Some(first.clone())
    );
    assert!(
        selection_work.closure_builds() >= 4,
        "head and three older candidates were not evaluated independently"
    );
    assert_eq!(
        selection_work.body_loads(),
        1,
        "rejected ancestors loaded summary payload before compatibility was known"
    );
    assert!(selection_resolver.cached_graph_units() <= 256);
    assert!(
        selection_resolver.cached_graph_roots() < 4,
        "root closure cache retained every overlapping ancestry"
    );
    let selected_source = f
        .store
        .compaction_checkpoint_source("ws", "thread", &first)
        .await
        .unwrap()
        .unwrap();
    let selected_graph = selection_resolver
        .resolve(&f.store, "ws", Some(&allowed), &selected_source)
        .await
        .unwrap()
        .unwrap();
    let selected_metadata = selection_resolver
        .projection_metadata(&f.store, "ws", &selected_graph)
        .await
        .unwrap();
    assert_eq!(
        selected_metadata.coverage_domain,
        pioneer_compaction::CoverageDomain::OwnContribution
    );
    assert!(selected_metadata.emergency_inputs.is_empty());
    assert!(selection_resolver.cached_payload_bytes() <= 64 * 1024);
    let builds_before_miss = selection_work.closure_builds();
    assert!(
        selection_resolver
            .resolve(&f.store, "ws", Some(&allowed), &head_source)
            .await
            .unwrap()
            .is_some(),
        "evicted optimization data changed compatible graph validity"
    );
    assert!(selection_work.closure_builds() > builds_before_miss);
    let mut merged = super::coverage::CheckpointGraphResolver::with_cache_limits(256, 64 * 1024);
    merged
        .resolve(&f.store, "ws", Some(&allowed), &selected_source)
        .await
        .unwrap()
        .unwrap();
    selection_resolver.merge(merged);
    assert!(selection_resolver.cached_graph_units() <= 256);
    assert_eq!(selected.len(), 22);
    assert!(selected[0].content.contains("State through 40"));
    assert_eq!(
        selected[0].provenance.as_ref().unwrap().sources[0].id,
        first
    );
    assert_eq!(identities(&selected[1..]), identities(&raw[40..]));
    let selected_n = selected
        .iter()
        .find(|message| message.content.contains("new work after checkpoint H"))
        .unwrap();
    assert_eq!(identities(std::slice::from_ref(selected_n)), raw_n_identity);
    assert_eq!(
        selected
            .iter()
            .filter(|message| message.content.contains("new work after checkpoint H"))
            .count(),
        1,
        "new work is retained once while H is replaced by its checkpoint"
    );
    let replacement_index = raw
        .iter()
        .position(|message| message.content == "work 20")
        .unwrap();
    let replacement_source = &raw[replacement_index].provenance.as_ref().unwrap().sources[0];
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
    let expected_replaced = raw
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != replacement_index)
        .map(|(_, message)| message.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        identities(&replaced),
        identities(&expected_replaced),
        "a covered transport copy is removed through exact original projection, preserving every other source in order"
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
    assert_eq!(disjoint.len(), selected.len());
    assert!(
        disjoint[0].content.contains("State through 40"),
        "disjoint prepared summary remains intact"
    );
    assert_eq!(identities(&disjoint), identities(&selected));
    let joined =
        super::frozen::compose_frozen_basis(&f.store, "ws", "thread", &allowed, &inherited, &raw)
            .await
            .unwrap();
    assert_eq!(
        joined.len(),
        raw.len(),
        "overlapping summary is replaced by exact originals once"
    );
    assert_eq!(identities(&joined), identities(&raw));
    assert_eq!(
        joined
            .iter()
            .filter(|message| message.content.contains("new work after checkpoint H"))
            .count(),
        1
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
        crate::turn_runtime_snapshot::restore_history_json(&f.store, "ws", &allowed, &json)
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
    let checkpoint_reference = SourceRef {
        scope: selected[0].provenance.as_ref().unwrap().sources[0]
            .scope
            .clone(),
        id: selected[0].provenance.as_ref().unwrap().sources[0]
            .id
            .clone(),
        version: selected[0].provenance.as_ref().unwrap().sources[0]
            .version
            .clone(),
    };
    let covered_leaf = raw[0].provenance.as_ref().unwrap().sources[0].id.clone();
    f.store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE turn_event SET payload='changed covered H' WHERE id=?",
            [covered_leaf.into()],
        ))
        .await
        .unwrap();
    assert!(
        f.store
            .compaction_reference_fragment("ws", "thread", &checkpoint_reference, 0)
            .await
            .unwrap()
            .is_none(),
        "checkpoint text must not be readable after a covered leaf changes"
    );
    assert!(
        super::frozen::restore(&f.store, "ws", &allowed, &frozen)
            .await
            .is_err(),
        "frozen checkpoint projection must fail after a covered leaf changes"
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

#[tokio::test]
async fn compaction_reuses_completed_child_head_after_immutable_raw_output_capture() {
    use pioneer_provider::{ChatMessage, MessageProvenance, MessageSourceRef};
    use std::collections::BTreeSet;
    let f = fixture("completed A", vec![Reply::Success], true, false).await;
    let CompactionExit::Applied(id) = f.runner.run(CancellationToken::new()).await.unwrap() else {
        panic!("checkpoint must apply");
    };
    let mut checkpoint = f.store.compaction_checkpoint(&id).await.unwrap().unwrap();
    let owner = super::native::native_owner("ws", "thread");
    let mut snapshot = f.runner.snapshot.clone();
    snapshot.id = "published-child-operation".into();
    snapshot.owner = owner.clone();
    snapshot.plan.fingerprint = snapshot.id.clone();
    f.store
        .compaction_admit("ws", "thread", &snapshot)
        .await
        .unwrap();
    checkpoint.id = "published-child-summary".into();
    checkpoint.operation_id = snapshot.id.clone();
    checkpoint.owner = owner.clone();
    f.store
        .compaction_save_candidate(&checkpoint, 0)
        .await
        .unwrap();
    // Historical published checkpoint. The real grant/commit boundary is
    // covered in CRUD's frozen_own_imports regression below the Gateway.
    let db = f.store.database_connection();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='published-child-summary'",
    )
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_context SET head='published-child-summary' WHERE owner=?",
        [owner.clone().into()],
    ))
    .await
    .unwrap();
    let mut work = ChatMessage::assistant("completed A");
    work.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: Some("next-child".into()),
        unit_id: "A-work".into(),
        sources: checkpoint
            .coverage
            .iter()
            .map(|r| MessageSourceRef {
                scope: r.scope.clone(),
                id: r.id.clone(),
                version: r.version.clone(),
            })
            .collect(),
        complete: true,
        inherited: false,
        protected_input: false,
    });
    let allowed = BTreeSet::from(["thread".into(), "next-child".into()]);
    let original = vec![work.clone()];
    let mut projected = original.clone();
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "next-child",
        &allowed,
        &mut projected,
    )
    .await
    .unwrap();
    assert_eq!(projected.len(), 1);
    let origin = projected[0].provenance.as_ref().unwrap();
    assert_eq!(origin.sources[0].id, checkpoint.id);
    assert_eq!(origin.thread_id, "thread");
    assert_eq!(origin.context_thread.as_deref(), Some("next-child"));
    assert!(!origin.inherited);
    assert_ne!(projected, original);
    let once = projected.clone();
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "next-child",
        &allowed,
        &mut projected,
    )
    .await
    .unwrap();
    assert_eq!(
        projected, once,
        "repeated checks reuse the same representation"
    );
    // A later checkpoint may cover the child's whole accepted working context,
    // including H. It is reusable only when all of H + A is represented, and
    // the projected summary remains inherited rather than becoming A's output.
    db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('accepted-h','thread','turn',8,'fixture','{}',CURRENT_TIMESTAMP)").await.unwrap();
    let mut mixed_snapshot = snapshot.clone();
    mixed_snapshot.id = "published-working-operation".into();
    mixed_snapshot.expected_checkpoint = Some(checkpoint.id.clone());
    mixed_snapshot.plan.coverage_domain = pioneer_compaction::CoverageDomain::WorkingContext;
    mixed_snapshot.plan.fingerprint = mixed_snapshot.id.clone();
    f.store
        .compaction_admit("ws", "thread", &mixed_snapshot)
        .await
        .unwrap();
    let mut mixed = checkpoint.clone();
    mixed.id = "published-working-summary".into();
    mixed.operation_id = mixed_snapshot.id.clone();
    mixed.previous = Some(checkpoint.id.clone());
    mixed.coverage = vec![SourceRef {
        scope: "event:turn".into(),
        id: "accepted-h".into(),
        version: "event-revision:1".into(),
    }];
    mixed.summary = "accepted H and completed A".into();
    f.store.compaction_save_candidate(&mixed, 0).await.unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='published-working-summary'",
    )
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_context SET head='published-working-summary' WHERE owner=?",
        [owner.clone().into()],
    ))
    .await
    .unwrap();
    let mut h = ChatMessage::user("accepted H");
    h.provenance = Some(MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: Some("next-child".into()),
        unit_id: "accepted-H".into(),
        sources: vec![MessageSourceRef {
            scope: "event:turn".into(),
            id: "accepted-h".into(),
            version: "event-revision:1".into(),
        }],
        complete: true,
        inherited: true,
        protected_input: false,
    });
    let mut whole_context = vec![h, work.clone()];
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "next-child",
        &allowed,
        &mut whole_context,
    )
    .await
    .unwrap();
    assert_eq!(whole_context.len(), 1, "H and A are represented once");
    let mixed_origin = whole_context[0].provenance.as_ref().unwrap();
    assert_eq!(mixed_origin.sources[0].id, mixed.id);
    assert!(
        mixed_origin.inherited,
        "a working-context checkpoint is not A's own contribution"
    );
    // A newer head may include a later child turn outside this accepted basis.
    // Find the older compatible checkpoint; never widen the frozen boundary.
    db.execute_unprepared("INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('future-source','thread','turn',9,'fixture','{}',CURRENT_TIMESTAMP)").await.unwrap();
    let mut later = checkpoint.clone();
    later.id = "later-child-summary".into();
    later.previous = Some(checkpoint.id.clone());
    later.coverage = vec![SourceRef {
        scope: "event:turn".into(),
        id: "future-source".into(),
        version: "event-revision:1".into(),
    }];
    later.summary = "work beyond the accepted boundary".into();
    f.store.compaction_save_candidate(&later, 1).await.unwrap();
    db.execute_unprepared(
        "UPDATE compaction_checkpoint SET status='applied' WHERE id='later-child-summary'",
    )
    .await
    .unwrap();
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "UPDATE compaction_context SET head='later-child-summary' WHERE owner=?",
        [checkpoint.owner.clone().into()],
    ))
    .await
    .unwrap();
    let mut bounded = original.clone();
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "next-child",
        &allowed,
        &mut bounded,
    )
    .await
    .unwrap();
    assert_eq!(bounded, once, "a later head cannot import later work");
    work.provenance.as_mut().unwrap().inherited = true;
    let mut inherited = vec![work];
    let unchanged = inherited.clone();
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "next-child",
        &allowed,
        &mut inherited,
    )
    .await
    .unwrap();
    assert_eq!(
        inherited, unchanged,
        "H does not become an imported own contribution"
    );
    db.execute_unprepared("UPDATE turn_event SET payload='edited A' WHERE id='source'")
        .await
        .unwrap();
    let mut current = original.clone();
    super::checkpoint::project_accepted_checkpoints(
        &f.store,
        "ws",
        "next-child",
        &allowed,
        &mut current,
    )
    .await
    .unwrap();
    assert_eq!(
        current, original,
        "invalidated heads do not replace current source history"
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

#[tokio::test]
async fn legacy_history_is_prepared_before_freezing_a_new_execution_basis() {
    use pioneer_protocol::TurnItem;
    let f = fixture("unused", vec![], true, false).await;
    let db = f.store.database_connection();
    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    db.execute_unprepared("INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES('old-input','turn',0,'text','old request','{\"type\":\"text\",\"text\":\"old request\"}',CURRENT_TIMESTAMP)").await.unwrap();
    f.store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: TurnItem::AgentMessage {
                    id: "old-answer".into(),
                    text: "old answer must survive upgrade".into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();
    db.execute_unprepared("UPDATE turn SET status='completed' WHERE id='turn'")
        .await
        .unwrap();
    // Reproduce the installed legacy database: canonical data exists, but
    // pre-upgrade rows were never entered in the compaction revision journals.
    for sql in [
        "DELETE FROM compaction_input_revision WHERE turn_id='turn'",
        "DELETE FROM compaction_event_revision WHERE turn_id='turn'",
        "DELETE FROM compaction_history_preparation WHERE thread_id='thread'",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    let stale = f.store.compaction_history_read_fence().await.unwrap();
    assert!(
        f.store
            .compaction_history_turn_page("ws", "thread", "", &stale)
            .await
            .is_err()
    );
    let json =
        super::frozen::capture_execution_basis_json(&f.store, "ws", "thread", None, None, None)
            .await
            .unwrap();
    assert!(
        f.store
            .compaction_history_prepared("ws", "thread")
            .await
            .unwrap()
    );
    let restored = crate::turn_runtime_snapshot::restore_history_json(
        &f.store,
        "ws",
        &std::collections::BTreeSet::from(["thread".into()]),
        &json,
    )
    .await
    .unwrap();
    let text = serde_json::to_string(&restored).unwrap();
    assert!(text.contains("old request"));
    assert!(text.contains("old answer must survive upgrade"));
    assert!(
        f.store
            .compaction_history_turn_page("ws", "thread", "", &stale)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn history_capture_inherits_read_class_and_cancellation_releases_admission() {
    use pioneer_sqlite::{
        SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadOutcome, SqliteWriteExecutor,
    };
    use sea_orm::ConnectOptions;

    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("history-scheduling.sqlite");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let mut writer_options = ConnectOptions::new(url.clone());
    writer_options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(writer_options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO workspace(id,name,is_active,is_current) VALUES ('ws','fixture',1,1)",
        "INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status,origin_kind,access_class,created_at,updated_at) VALUES ('thread','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('background','ws','','agent','m','p','active','user','workspace',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('turn','thread','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),('background-turn','background','completed','conversation','user',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input','turn',0,'text','accepted','{\"type\":\"text\",\"text\":\"accepted\"}',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('background-input','background-turn',0,'text','background','{\"type\":\"text\",\"text\":\"background\"}',CURRENT_TIMESTAMP)",
    ] {
        writer.execute_unprepared(sql).await.unwrap();
    }
    let mut reader_options = ConnectOptions::new(url);
    reader_options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(reader_options).await.unwrap();
    let observer = Arc::new(HistoryReadObserver::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let store = CrudStore::new(database.clone());
    let maintenance_store = store.with_maintenance_access();

    // Occupy the sole maintenance-read admission. Interactive turn history
    // preparation must still reach SQLite and complete through the same store.
    let held = database.maintenance().begin_read().await.unwrap();
    let interactive_read_start = observer.reads().len();
    let interactive_write_start = observer.writes().len();
    let history_json = tokio::time::timeout(
        Duration::from_secs(5),
        super::frozen::capture_execution_basis_json(&store, "ws", "thread", None, None, None),
    )
    .await
    .expect("occupied maintenance admission must not block interactive capture")
    .unwrap();
    let mut runtime = crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
        "thread",
        "ws",
        "turn",
        pioneer_protocol::ThreadMode::Agent,
        &pioneer_agent::AgentTurnHookRuntimeContext::default(),
        "fixture-model",
        "fixture-provider",
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
    runtime.history_json = history_json;
    let runtime = store.upsert_turn_runtime_snapshot(runtime).await.unwrap();
    let (_, restored) =
        crate::turn_runtime_snapshot::restored_conversation_scope_from_snapshot(&store, &runtime)
            .await
            .unwrap();
    let (_, retried) =
        crate::turn_runtime_snapshot::restored_conversation_scope_from_snapshot(&store, &runtime)
            .await
            .unwrap();
    assert_eq!(restored, retried);
    assert!(restored.iter().any(|message| message.content == "accepted"));
    assert!(
        store
            .compaction_history_prepared("ws", "thread")
            .await
            .unwrap()
    );
    assert!(
        observer.reads()[interactive_read_start..]
            .iter()
            .any(|event| matches!(
                event,
                SqliteReadEvent::OperationFinished {
                    class: SqliteReadClass::Interactive,
                    outcome: SqliteReadOutcome::Ok,
                    ..
                }
            ))
    );
    assert!(
        observer.reads()[interactive_read_start..]
            .iter()
            .all(|event| {
                !matches!(
                    event,
                    SqliteReadEvent::OperationFinished {
                        class: SqliteReadClass::Maintenance,
                        ..
                    }
                )
            })
    );
    assert!(
        observer.writes()[interactive_write_start..]
            .iter()
            .all(|event| !matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                } | pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                } | pioneer_sqlite::SqliteWriteEvent::Released {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                } | pioneer_sqlite::SqliteWriteEvent::Cancelled {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            ))
    );
    assert!(
        observer.writes()[interactive_write_start..]
            .iter()
            .any(|event| matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: pioneer_sqlite::SqliteWriteClass::Interactive,
                    ..
                }
            ))
    );

    // The same shared capture path retains an explicitly selected background
    // scope. Cancelling its queued read must remove the waiter immediately.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            super::frozen::capture_execution_basis_json(
                &maintenance_store,
                "ws",
                "background",
                None,
                None,
                None,
            ),
        )
        .await
        .is_err()
    );
    assert!(observer.reads().iter().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionCancelled {
            class: SqliteReadClass::Maintenance,
            queue_depth: 0,
            active: 1,
            ..
        }
    )));

    drop(held);
    let maintenance_read_start = observer.reads().len();
    let maintenance_write_start = observer.writes().len();
    let background_json = tokio::time::timeout(
        Duration::from_secs(5),
        super::frozen::capture_execution_basis_json(
            &maintenance_store,
            "ws",
            "background",
            None,
            None,
            None,
        ),
    )
    .await
    .expect("cancelled waiter must not retain maintenance admission")
    .unwrap();
    let background_allowed = std::collections::BTreeSet::from(["background".to_owned()]);
    let restored_background = crate::turn_runtime_snapshot::restore_history_json(
        &maintenance_store,
        "ws",
        &background_allowed,
        &background_json,
    )
    .await
    .unwrap();
    assert!(
        restored_background
            .iter()
            .any(|message| message.content == "background")
    );
    assert!(
        maintenance_store
            .compaction_history_prepared("ws", "background")
            .await
            .unwrap()
    );
    let maintenance_events = observer.reads();
    assert!(
        maintenance_events[maintenance_read_start..]
            .iter()
            .any(|event| matches!(
                event,
                SqliteReadEvent::OperationFinished {
                    class: SqliteReadClass::Maintenance,
                    outcome: SqliteReadOutcome::Ok,
                    ..
                }
            ))
    );
    assert!(maintenance_events.iter().rev().any(|event| matches!(
        event,
        SqliteReadEvent::AdmissionReleased {
            class: SqliteReadClass::Maintenance,
            queue_depth: 0,
            active: 0,
            ..
        }
    )));
    assert!(
        maintenance_events[maintenance_read_start..]
            .iter()
            .all(|event| !matches!(
                event,
                SqliteReadEvent::OperationFinished {
                    class: SqliteReadClass::Interactive,
                    ..
                }
            ))
    );
    assert!(
        observer.writes()[maintenance_write_start..]
            .iter()
            .all(|event| !matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Enqueued {
                    class: pioneer_sqlite::SqliteWriteClass::Interactive,
                    ..
                } | pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: pioneer_sqlite::SqliteWriteClass::Interactive,
                    ..
                } | pioneer_sqlite::SqliteWriteEvent::Released {
                    class: pioneer_sqlite::SqliteWriteClass::Interactive,
                    ..
                } | pioneer_sqlite::SqliteWriteEvent::Cancelled {
                    class: pioneer_sqlite::SqliteWriteClass::Interactive,
                    ..
                }
            ))
    );
    assert!(
        observer.writes()[maintenance_write_start..]
            .iter()
            .any(|event| matches!(
                event,
                pioneer_sqlite::SqliteWriteEvent::Acquired {
                    class: pioneer_sqlite::SqliteWriteClass::Maintenance,
                    ..
                }
            ))
    );
}

struct RejectedService(Arc<dyn Summarizer>);
#[async_trait]
impl Summarizer for RejectedService {
    fn model_budget(&self) -> ModelBudget {
        self.0.model_budget()
    }
    fn input_tokens(&self, request: &SummaryRequest) -> Result<u64> {
        self.0.input_tokens(request)
    }
    async fn summarize(
        &self,
        _: SummaryRequest,
    ) -> std::result::Result<
        pioneer_compaction::summary::SummaryCompletion,
        pioneer_compaction::summary::SummaryFailure,
    > {
        Err(pioneer_compaction::summary::SummaryFailure {
            kind: FailureKind::Permanent,
            retry_after_ms: None,
            code: "cli_isolation_rejected",
            diagnostic: Some(FailureDiagnostic::new(
                "cli_config_read",
                "cli_isolation_rejected",
                "Codex did not confirm the isolated profile",
            )),
        })
    }
}
#[tokio::test]
async fn compaction_failed_attempt_retains_diagnostic_across_restart_and_lifecycle() {
    let mut f = fixture("history", vec![], true, false).await;
    let rejected = Arc::new(RejectedService(f.runner.summarizer.clone()));
    Arc::get_mut(&mut f.runner).unwrap().summarizer = rejected;
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::Permanent)
    ));
    let state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    let diagnostic = state.diagnostic.as_ref().unwrap();
    assert_eq!(diagnostic.code, "cli_isolation_rejected");
    assert_eq!(
        state.observation.as_ref().unwrap().diagnostic.as_ref(),
        Some(diagnostic)
    );
    let row = f.store.database_connection().query_one_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
        "SELECT observation FROM compaction_attempt_observation WHERE operation_id=? AND attempt=1", ["operation".into()])).await.unwrap().unwrap();
    let observation: pioneer_compaction::runner::AttemptObservation =
        serde_json::from_str(&row.try_get::<String>("", "observation").unwrap()).unwrap();
    assert_eq!(observation.diagnostic.as_ref(), Some(diagnostic));
    let restored: RunnerState =
        serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    assert_eq!(restored, state);
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::Permanent)
    ));
    assert_eq!(
        f.store
            .compaction_runner_state("operation")
            .await
            .unwrap()
            .unwrap(),
        state
    );
}

struct StaleCommitTarget(CrudStore);
#[async_trait]
impl CompactionTarget for StaleCommitTarget {
    async fn fits(&self, _: &str) -> Result<bool> {
        // Provider succeeded; invalidate only the selected revision before commit.
        self.0.database_connection().execute_unprepared(
            "UPDATE compaction_manifest SET source_version='stale' WHERE operation_id='operation'"
        ).await?;
        Ok(true)
    }
}
#[tokio::test]
async fn compaction_commit_failure_does_not_mislabel_successful_provider_attempt() {
    let mut f = fixture("completed history", vec![Reply::Success], true, false).await;
    Arc::get_mut(&mut f.runner).unwrap().target = Arc::new(StaleCommitTarget(f.store.clone()));
    assert!(matches!(
        f.runner.run(CancellationToken::new()).await.unwrap(),
        CompactionExit::Failed(FailureKind::Permanent)
    ));
    let state = f
        .store
        .compaction_runner_state("operation")
        .await
        .unwrap()
        .unwrap();
    let diagnostic = state.diagnostic.as_ref().unwrap();
    assert_eq!(diagnostic.stage, "checkpoint_commit");
    assert_eq!(diagnostic.code, "checkpoint_stale");
    let observation = state.observation.unwrap();
    assert!(observation.completion.is_some());
    assert_eq!(observation.failure, None);
    assert_eq!(observation.diagnostic, None);
    assert_eq!(f.store.compaction_head("owner").await.unwrap(), None);
}

#[tokio::test]
async fn completed_task_output_excludes_later_turns_before_decoding_and_survives_compression() {
    let f = fixture("unused", vec![], true, false).await;
    let store = f.store.with_maintenance_access();
    let db = store.database_connection();
    db.execute_unprepared("DELETE FROM turn_event WHERE id='source'")
        .await
        .unwrap();
    for sql in [
        "UPDATE turn SET status='completed',created_at='2026-01-02T00:00:00+00:00' WHERE id='turn'",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('earlier','thread','completed','conversation','user','2026-01-01T00:00:00+00:00',CURRENT_TIMESTAMP)",
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,created_at,updated_at) VALUES ('later','thread','completed','conversation','user','2026-01-03T00:00:00+00:00',CURRENT_TIMESTAMP)",
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload,created_at) VALUES ('poison','thread','later',1,'fixture','not valid JSON',CURRENT_TIMESTAMP)",
    ] {
        db.execute_unprepared(sql).await.unwrap();
    }
    for (turn, text) in [
        ("earlier", "own previous work"),
        ("turn", "own final result"),
    ] {
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: turn.into(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: format!("answer-{turn}"),
                        text: text.into(),
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
    super::history::prepare_history(&store, "ws", "thread")
        .await
        .unwrap();
    let fence = store.compaction_history_read_fence().await.unwrap();
    let messages = super::history::load_task_output_history(&store, "ws", "thread", "turn", &fence)
        .await
        .unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>(),
        ["own previous work", "own final result"]
    );
    assert!(
        super::history::load_task_output_history(&store, "ws", "thread", "missing", &fence)
            .await
            .is_err()
    );
    let allowed = std::collections::BTreeSet::from(["thread".to_owned()]);
    let frozen = super::frozen::capture(&store, "ws", "thread", &allowed, &messages)
        .await
        .unwrap();
    assert!(
        crate::database::compress_history_payloads_for_test(&store)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(
        super::frozen::restore(&store, "ws", &allowed, &frozen)
            .await
            .unwrap(),
        messages
    );
    // An unrestricted read really does encounter the poison; the output path
    // succeeds because later work is excluded, not because errors are swallowed.
    assert!(
        super::history::load_line_history(&store, "ws", "thread", None, &fence)
            .await
            .is_err()
    );
    assert!(f.provider.calls.lock().unwrap().is_empty());
}
