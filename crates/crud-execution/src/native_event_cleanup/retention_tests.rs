use super::{run, run_quantum};
use crate::{
    cli_runtime_binding::{NewCliRuntimeTurnBinding, upsert_turn_binding},
    input_mapping::InputMapping,
    native_events::{NativeEventRepository, NewCliRuntimeNativeEvent},
    test_fixture::InlineInput,
    time::unix_to_datetime,
};
use anyhow::{Context, Result};
use pioneer_sqlite::SqliteDatabase;
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass, SqliteWriteEvent,
    SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{ConnectionTrait, TransactionTrait};
use sea_orm::{DatabaseBackend, Statement};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

const NOW: i64 = 1_900_000_000;
#[derive(Default)]
struct Observer {
    reads: AtomicU64,
    writes: AtomicU64,
    queued: AtomicU64,
}
impl SqliteReadObserver for Observer {
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
impl SqliteWriteObserver for Observer {
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
    foreground: SqliteDatabase,
    db: SqliteDatabase,
    observer: Arc<Observer>,
}
impl Fixture {
    async fn open() -> Result<Self> {
        let fixture = Self::connect(pioneer_postgres_test::scoped_database().await?).await?;
        fixture.sql("INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws-cleanup','Cleanup',TRUE,FALSE)").await?;
        Ok(fixture)
    }
    async fn connect(fixture: Arc<pioneer_postgres_test::TestDatabase>) -> Result<Self> {
        let writer = fixture.connect_with_limit(1).await?;
        let reader = fixture.connect_read_only().await?;
        let observer = Arc::new(Observer::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        database.maintenance().validate_reader().await?;
        Ok(Self {
            db: database.maintenance(),
            foreground: database,
            observer,
        })
    }
    async fn sql(&self, sql: &str) -> Result<()> {
        self.foreground.execute_unprepared(sql).await?;
        Ok(())
    }
    async fn scalar(&self, sql: &str) -> Result<i64> {
        Ok(self
            .db
            .query_one_raw(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await?
            .context("missing scalar")?
            .try_get_by_index(0)?)
    }
    async fn turn(&self, id: &str, completed: bool) -> Result<()> {
        self.sql(
            "INSERT INTO thread (id, workspace_id, preview, mode, model, model_provider, status)
             VALUES ('thread-cleanup', 'ws-cleanup', '', 'agent', 'gpt-5.4', 'openai', 'active')
             ON CONFLICT (id) DO NOTHING",
        )
        .await?;
        self.foreground.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO turn (id, thread_id, status, turn_kind, origin, created_at, updated_at)
             VALUES ($1, 'thread-cleanup', $2, 'conversation', 'user', $3, $3)",
            [id.into(), (if completed { "completed" } else { "in_progress" }).into(),
             unix_to_datetime(NOW).into()],
        )).await?;
        self.bind(id, "codex", if completed { "completed" } else { "running" })
            .await
    }
    async fn bind(&self, id: &str, runtime: &str, status: &str) -> Result<()> {
        let at = chrono::DateTime::from_timestamp(NOW, 0)
            .unwrap()
            .fixed_offset();
        let binding = NewCliRuntimeTurnBinding {
            turn_id: id.into(),
            thread_id: "thread-cleanup".into(),
            continuation_thread_id: "thread-cleanup".into(),
            workspace_id: "ws-cleanup".into(),
            runtime_id: runtime.into(),
            runtime_kind: "codex".into(),
            native_thread_id: format!("native-{runtime}"),
            native_turn_id: Some(format!("native-{id}")),
            request_id: None,
            status: status.into(),
            model: None,
            cwd: None,
            sandbox_json: None,
            approval_policy: None,
            input_mapping_json: "{}".into(),
            created_at: at,
            updated_at: at,
        };
        let mapping = InputMapping::prepare(&binding.input_mapping_json, false)?;
        upsert_turn_binding(&self.foreground, binding, mapping, &InlineInput).await?;
        Ok(())
    }
    async fn event(
        &self,
        id: &str,
        turn: Option<&str>,
        runtime: &str,
        method: &str,
        bytes: usize,
    ) -> Result<()> {
        NativeEventRepository::new(&self.foreground)
            .append_cli_runtime_native_event(NewCliRuntimeNativeEvent {
                id: id.into(),
                runtime_id: runtime.into(),
                runtime_kind: "codex".into(),
                workspace_id: Some("ws-cleanup".into()),
                thread_id: Some("thread-cleanup".into()),
                turn_id: turn.map(Into::into),
                native_thread_id: Some(format!("native-{runtime}")),
                native_turn_id: turn.map(|t| format!("native-{t}")),
                native_method: method.into(),
                payload_redacted_json: "x".repeat(bytes),
                sequence: NOW * 1000,
                created_at: chrono::DateTime::from_timestamp(NOW, 0)
                    .unwrap()
                    .fixed_offset(),
            })
            .await?;
        Ok(())
    }
}

#[pioneer_postgres_test::postgres_test]
async fn preserves_retained_rows_and_coalesces_waiting_active_events() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.turn("active", false).await?;
    for (i, method) in [
        "item/started",
        "item/completed",
        "item/agentMessage/delta",
        "item/commandExecution/outputDelta",
        "turn/diff/updated",
        "thread/tokenUsage/updated",
        "account/rateLimits/updated",
    ]
    .iter()
    .enumerate()
    {
        f.event(&format!("eligible-{i}"), Some("done"), "codex", method, 8)
            .await?;
    }
    for (id, turn, runtime, method, size) in [
        ("orphan", None, "codex", "item/completed", 8),
        ("oversized", Some("done"), "codex", "item/completed", 262145),
        ("other-runtime", Some("done"), "other", "item/completed", 8),
        ("retained", Some("done"), "codex", "turn/completed", 8),
        ("error", Some("done"), "codex", "error", 8),
        ("unknown", Some("done"), "codex", "future/event", 8),
        ("active", Some("active"), "codex", "item/completed", 8),
    ] {
        f.event(id, turn, runtime, method, size).await?;
    }
    let mut deleted = 0;
    for now in 1..5 {
        deleted += run(&f.db, now).await?.events_deleted;
    }
    assert_eq!(deleted, 7);
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event")
            .await?,
        7
    );
    let revision = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='active'")
        .await?;
    use futures_util::FutureExt;
    let mut wake = f.db.work_signal("maintenance.native").subscribe();
    wake.begin_pass(&f.db).await?;
    for i in 0..10 {
        f.event(
            &format!("delta-{i}"),
            Some("active"),
            "codex",
            "item/agentMessage/delta",
            8,
        )
        .await?;
    }
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='active'")
            .await?,
        revision
    );
    assert!(
        wake.wait(None).now_or_never().is_none(),
        "active-turn deltas must not wake terminal cleanup"
    );
    assert_eq!(run(&f.db, 10).await?.jobs_examined, 0);
    f.event(
        "current-runtime-late",
        Some("done"),
        "codex",
        "item/completed",
        8,
    )
    .await?;
    assert!(
        wake.wait(None).now_or_never().is_some(),
        "late raw events for terminal Turns must wake cleanup"
    );
    assert_eq!(run(&f.db, 11).await?.events_deleted, 1);
    Ok(())
}
#[pioneer_postgres_test::postgres_test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_of_queued_cleanup_releases_writer_and_preserves_job() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.event("event", Some("done"), "codex", "item/completed", 1)
        .await?;
    let tx = f.foreground.begin().await?;
    let before = f.observer.queued.load(Ordering::SeqCst);
    let maintenance = f.db.clone();
    let task = tokio::spawn(async move { run_quantum(&maintenance).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.observer.queued.load(Ordering::SeqCst) == before {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tx.rollback().await?;
    tokio::time::timeout(
        Duration::from_secs(5),
        f.sql("UPDATE native_event_cleanup_scheduler SET new_jobs_since_served=0"),
    )
    .await??;
    assert_eq!(run(&f.db, 1).await?.events_deleted, 1);
    Ok(())
}
#[pioneer_postgres_test::postgres_test]
async fn queued_cleanup_keeps_exact_retained_ids_across_sparse_history() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.turn("active", false).await?;
    for i in 0..160 {
        f.event(
            &format!("history-{i:03}"),
            Some("done"),
            "codex",
            if i % 10 == 0 {
                "item/completed"
            } else {
                "turn/completed"
            },
            128,
        )
        .await?;
    }
    f.event("active", Some("active"), "codex", "item/completed", 128)
        .await?;
    f.event("orphan", None, "codex", "item/completed", 128)
        .await?;
    f.event("wrong", Some("done"), "wrong", "item/completed", 128)
        .await?;
    f.event("oversized", Some("done"), "codex", "item/completed", 262145)
        .await?;
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event")
            .await?,
        164
    );
    let mut deleted = 0;
    let mut finished = false;
    for _ in 0..20 {
        let metrics = run_quantum(&f.db).await?;
        deleted += metrics.events_deleted;
        if metrics.jobs_examined == 0 {
            finished = true;
            break;
        }
    }
    assert!(finished, "cleanup should settle within the bounded fixture");
    assert_eq!(deleted, 16);
    let rows =
        f.db.query_all_raw(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT id FROM cli_runtime_native_event ORDER BY id",
        ))
        .await?;
    let retained = rows
        .iter()
        .map(|row| row.try_get_by_index(0))
        .collect::<Result<Vec<String>, _>>()?;
    let mut expected: Vec<String> = ["active", "orphan", "oversized", "wrong"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    expected.extend(
        (0..160)
            .filter(|i| i % 10 != 0)
            .map(|i| format!("history-{i:03}")),
    );
    expected.sort();
    assert_eq!(retained.len(), 148);
    assert_eq!(retained, expected);
    Ok(())
}
