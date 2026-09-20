use super::*;
use crate::{CrudStore, NewCliRuntimeNativeEvent, NewCliRuntimeTurnBinding};
use migration::{Migrator, MigratorTrait};
use pioneer_protocol::{
    PersistedActorRef, SandboxMode, Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
    ThreadStatus, Turn, TurnCompletedNotification, TurnOrigin, TurnStatus,
};
use pioneer_sqlite::{
    SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteClass, SqliteWriteEvent,
    SqliteWriteExecutor, SqliteWriteObserver, sqlite_read_only_connection_url,
};
use sea_orm::sea_query::SqliteQueryBuilder;
use sea_orm::{ConnectOptions, Database, DatabaseBackend, Statement};
use std::{
    path::PathBuf,
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
struct PathGuard(PathBuf);
impl Drop for PathGuard {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}
struct Fixture {
    store: CrudStore,
    db: SqliteDatabase,
    observer: Arc<Observer>,
    path: Arc<PathGuard>,
}
impl Fixture {
    async fn open() -> Result<Self> {
        Self::open_version(true).await
    }
    async fn open_version(latest: bool) -> Result<Self> {
        let path = Arc::new(PathGuard(std::env::temp_dir().join(format!(
            "pioneer-native-cleanup-{}.sqlite",
            uuid::Uuid::new_v4()
        ))));
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.0.display()));
        options.max_connections(1);
        options.map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "ON"));
        let writer = Database::connect(options).await?;
        Migrator::up(
            &writer,
            if latest {
                None
            } else {
                Some(
                    Migrator::migrations()
                        .iter()
                        .position(|migration| {
                            migration.name() == "m20260919_000001_native_event_cleanup_queue"
                        })
                        .expect("native cleanup migration is registered")
                        as u32,
                )
            },
        )
        .await?;
        writer.execute_unprepared("PRAGMA journal_mode=WAL").await?;
        let fixture = Self::connect(path, writer).await?;
        fixture.sql("INSERT INTO workspace(id,name,is_active,is_current) VALUES('ws-cleanup','Cleanup',1,1)").await?;
        Ok(fixture)
    }
    async fn connect(path: Arc<PathGuard>, writer: sea_orm::DatabaseConnection) -> Result<Self> {
        let mut options = ConnectOptions::new(sqlite_read_only_connection_url(&path.0));
        options.max_connections(4);
        options.map_sqlx_sqlite_opts(|o| {
            o.read_only(true)
                .create_if_missing(false)
                .pragma("query_only", "ON")
        });
        let reader = Database::connect(options).await?;
        let observer = Arc::new(Observer::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::with_observer(writer, observer.clone()),
            observer.clone(),
        );
        database.maintenance().validate_reader().await?;
        let store = CrudStore::new(database);
        Ok(Self {
            db: store.with_maintenance_access().database_connection(),
            store,
            observer,
            path,
        })
    }
    async fn restart(self) -> Result<Self> {
        self.reopen(None).await
    }
    async fn migrate(self, down: bool) -> Result<Self> {
        self.reopen(Some(down)).await
    }
    async fn reopen(self, migration: Option<bool>) -> Result<Self> {
        let path = self.path.clone();
        self.db.clone().close().await?;
        drop(self);
        let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rw", path.0.display()));
        options.max_connections(1);
        options.map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "ON"));
        let writer = Database::connect(options).await?;
        match migration {
            Some(true) => Migrator::down(&writer, Some(1)).await?,
            Some(false) => {
                let applied: i64 = writer
                    .query_one_raw(Statement::from_string(
                        DatabaseBackend::Sqlite,
                        "SELECT COUNT(*) AS count FROM seaql_migrations \
                         WHERE version='m20260919_000001_native_event_cleanup_queue'"
                            .to_owned(),
                    ))
                    .await?
                    .expect("migration table returns a count")
                    .try_get("", "count")?;
                if applied == 0 {
                    // This fixture models the release boundary immediately
                    // before/after native cleanup. Do not advance into later,
                    // independently irreversible migrations on a repeated up.
                    Migrator::up(&writer, Some(1)).await?;
                }
            }
            None => {}
        }
        Self::connect(path, writer).await
    }
    async fn sql(&self, sql: &str) -> Result<()> {
        self.store
            .database_connection()
            .execute_unprepared(sql)
            .await?;
        Ok(())
    }
    async fn scalar(&self, sql: &str) -> Result<i64> {
        Ok(self
            .db
            .query_one_raw(Statement::from_string(DatabaseBackend::Sqlite, sql))
            .await?
            .context("missing scalar")?
            .try_get_by_index(0)?)
    }
    async fn turn(&self, id: &str, completed: bool) -> Result<()> {
        let thread = Thread {
            workspace_id: "ws-cleanup".into(),
            id: "thread-cleanup".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Agent,
            model: "gpt-5.4".into(),
            model_provider: "openai".into(),
            reasoning_effort: None,
            created_at: NOW,
            updated_at: NOW,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: vec![],
        };
        let mut turn = Turn {
            id: id.into(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: TurnOrigin::User,
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: vec![],
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        self.store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[],
                PersistedActorRef::System,
            )
            .await?;
        if completed {
            turn.status = TurnStatus::Completed;
            self.store
                .materialize_turn_completed(
                    TurnCompletedNotification {
                        workspace_id: thread.workspace_id,
                        thread_id: thread.id,
                        turn,
                    },
                    NOW + 1,
                )
                .await?;
        }
        self.bind(id, "codex", if completed { "completed" } else { "running" })
            .await
    }
    async fn bind(&self, id: &str, runtime: &str, status: &str) -> Result<()> {
        let at = chrono::DateTime::from_timestamp(NOW, 0)
            .unwrap()
            .fixed_offset();
        self.store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
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
            })
            .await?;
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
        self.store
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
    async fn prepared(&self, turn: &str) -> Result<Prepared> {
        let rows = candidates(&self.db, turn, None).await?;
        let runtime_id = rows.first().map(|r| r.runtime_id.clone());
        Ok(Prepared {
            job: Job {
                turn_id: turn.into(),
                regular_lane: Some("new".into()),
            },
            runtime_id,
            ids: bounded_ids(rows).0,
        })
    }
}

#[tokio::test]
async fn production_api_empty_queue_uses_one_maintenance_read_without_writer() -> Result<()> {
    let f = Fixture::open().await?;
    assert!(f.store.cleanup_native_events_quantum().await.is_err());
    let reads = f.observer.reads.load(Ordering::SeqCst);
    let writes = f.observer.writes.load(Ordering::SeqCst);
    let m = f
        .store
        .with_maintenance_access()
        .cleanup_native_events_quantum()
        .await?;
    assert_eq!(m.prepare_reads, 1);
    assert_eq!(m.jobs_examined, 0);
    assert_eq!(f.observer.reads.load(Ordering::SeqCst) - reads, 1);
    assert_eq!(f.observer.writes.load(Ordering::SeqCst), writes);
    Ok(())
}
#[tokio::test]
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
    assert_eq!(run(&f.db, 10).await?.jobs_examined, 0);
    f.event(
        "current-runtime-late",
        Some("done"),
        "codex",
        "item/completed",
        8,
    )
    .await?;
    assert_eq!(run(&f.db, 11).await?.events_deleted, 1);
    Ok(())
}
#[tokio::test]
async fn writer_revalidates_all_readiness_conditions() -> Result<()> {
    for change in [
        "UPDATE turn SET status='in_progress'",
        "UPDATE turn SET status='blocked'",
        "UPDATE turn_cli_runtime_binding SET status='running'",
        "DELETE FROM turn_cli_runtime_binding",
        "INSERT INTO turn_cli_runtime_attempt(id,turn_id,attempt_index,runtime_id,runtime_kind,native_thread_id,status,created_at,updated_at) VALUES('attempt','done',1,'codex','codex','native','starting',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        "INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,policy,provider_attempt_number,policy_snapshot,run_count,max_attempts,scheduled_at,next_run_at,created_at,updated_at,resolution_pending) VALUES('recovery','done','item','command_execution','succeeded','timeout','retry','{}',1,'{}',0,3,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)",
        "UPDATE turn_event_projection_stream_state SET status='quarantined'",
        "DELETE FROM turn_event_projection_stream_state",
        "INSERT INTO turn_event_projection_state(event_id,thread_id,turn_id,sequence,status,attempt_count,next_run_at,projection_context_json,created_at,updated_at) VALUES('receipt','thread-cleanup','done',99,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    ] {
        let f = Fixture::open().await?;
        f.turn("done", true).await?;
        f.event("selected", Some("done"), "codex", "item/completed", 8)
            .await?;
        let p = f.prepared("done").await?;
        assert_eq!(p.ids.len(), 1);
        f.sql(change).await?;
        let mut m = NativeEventCleanupMetrics::default();
        apply(&f.db, &p, 1, &mut m).await?;
        assert_eq!(m.events_deleted, 0, "{change}");
        assert_eq!(
            f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event")
                .await?,
            1
        );
    }
    Ok(())
}
#[tokio::test]
async fn byte_budget_runtime_ownership_and_empty_preparation_races() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.turn("other", true).await?;
    f.event("a", Some("done"), "codex", "item/completed", 100_000)
        .await?;
    f.event("b", Some("done"), "codex", "item/completed", 100_000)
        .await?;
    let p = f.prepared("done").await?;
    f.sql("UPDATE cli_runtime_native_event SET payload_redacted_json=printf('%.*c',204800,'x')")
        .await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 1, &mut m).await?;
    assert_eq!(m.events_deleted, 1);
    assert_eq!(m.deleted_bytes, 204800);
    let p = f.prepared("done").await?;
    f.sql("UPDATE cli_runtime_native_event SET turn_id='other'")
        .await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 2, &mut m).await?;
    assert_eq!(m.events_deleted, 0);
    let p = f.prepared("other").await?;
    f.bind("other", "changed", "completed").await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 3, &mut m).await?;
    assert_eq!(m.events_deleted, 0);
    f.event("late-seed", Some("done"), "codex", "item/completed", 8)
        .await?;
    f.sql(
        "UPDATE cli_runtime_native_event SET native_method='future/retained' WHERE id='late-seed'",
    )
    .await?;
    let p = f.prepared("done").await?;
    assert!(p.ids.is_empty());
    f.event("late", Some("done"), "codex", "item/completed", 8)
        .await?;
    let mut m = NativeEventCleanupMetrics::default();
    apply(&f.db, &p, 4, &mut m).await?;
    assert_eq!(m.events_deleted, 0);
    assert_eq!(run(&f.db, 5).await?.events_deleted, 1);
    Ok(())
}
#[tokio::test]
async fn migration_bootstrap_restart_vacuum_and_live_events_before_cursor() -> Result<()> {
    let f = Fixture::open_version(false).await?;
    f.turn("legacy", true).await?;
    for i in 0..130 {
        f.event(
            &format!("m-{i:03}"),
            Some("legacy"),
            "codex",
            "item/completed",
            1,
        )
        .await?;
    }
    let f = f.migrate(false).await?;
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job")
            .await?,
        0
    );
    let first = f
        .store
        .with_maintenance_access()
        .bootstrap_native_event_cleanup_quantum()
        .await?;
    assert_eq!(first.rows_scanned, 128);
    assert!(!first.complete);
    let f = f.restart().await?;
    f.sql("VACUUM").await?;
    f.turn("live", true).await?;
    f.event("a-live", Some("live"), "codex", "item/completed", 1)
        .await?;
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='live'")
            .await?,
        1
    );
    assert_eq!(bootstrap(&f.db).await?.rows_scanned, 2);
    assert!(bootstrap(&f.db).await?.complete);
    assert_eq!(bootstrap(&f.db).await?.rows_scanned, 0);
    let mut total = 0;
    for now in 1..6 {
        total += run(&f.db, now).await?.events_deleted;
    }
    assert_eq!(total, 131);
    let f = f.migrate(false).await?;
    assert!(bootstrap(&f.db).await?.complete);
    // Down removes only queue schema; upgrading again re-enables bootstrap.
    let f = f.migrate(true).await?;
    let f = f.migrate(false).await?;
    assert_eq!(
        f.scalar("SELECT complete FROM native_event_cleanup_bootstrap")
            .await?,
        0
    );
    Ok(())
}
#[tokio::test]
async fn rollback_unblock_and_late_registration() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.event("event", Some("done"), "codex", "item/completed", 8)
        .await?;
    f.sql("UPDATE turn_cli_runtime_binding SET status='running'")
        .await?;
    assert_eq!(run(&f.db, 1).await?.events_deleted, 0);
    let tx = f.db.begin().await?;
    tx.execute_unprepared("UPDATE turn_cli_runtime_binding SET status='completed'")
        .await?;
    tx.rollback().await?;
    assert_eq!(run(&f.db, 2).await?.jobs_examined, 0);
    f.sql("UPDATE turn_cli_runtime_binding SET status='completed'")
        .await?;
    assert_eq!(run(&f.db, 3).await?.events_deleted, 1);
    f.event("late", Some("done"), "codex", "item/completed", 8)
        .await?;
    assert_eq!(run(&f.db, 4).await?.events_deleted, 1);
    Ok(())
}
#[tokio::test]
async fn failed_delete_rolls_back_defers_only_poison_and_survives_restart() -> Result<()> {
    let f = Fixture::open().await?;
    for t in ["a-poison", "b-ready"] {
        f.turn(t, true).await?;
        f.event(t, Some(t), "codex", "item/completed", 8).await?;
    }
    f.sql("CREATE TRIGGER fail_cleanup BEFORE DELETE ON cli_runtime_native_event WHEN OLD.id='a-poison' BEGIN SELECT RAISE(ABORT,'injected'); END").await?;
    let m = run(&f.db, 100).await?;
    assert_eq!(m.errors_deferred, 1);
    assert_eq!(m.events_deleted, 0);
    assert_eq!(run(&f.db, 101).await?.events_deleted, 1);
    let f = f.restart().await?;
    assert_eq!(
        f.scalar("SELECT available_at FROM native_event_cleanup_job WHERE turn_id='a-poison'")
            .await?,
        100 + RETRY_DELAY_MICROS
    );
    assert_eq!(run(&f.db, 102).await?.jobs_examined, 0);
    f.sql("DROP TRIGGER fail_cleanup").await?;
    assert_eq!(
        run(&f.db, 100 + RETRY_DELAY_MICROS).await?.events_deleted,
        1
    );
    Ok(())
}
#[tokio::test]
async fn sustained_new_jobs_do_not_starve_served_turn_and_scheduler_noops() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("large", true).await?;
    for i in 0..512 {
        f.event(
            &format!("large-{i:03}"),
            Some("large"),
            "codex",
            "item/completed",
            1,
        )
        .await?;
    }
    assert_eq!(run(&f.db, 100).await?.events_deleted, 128);
    for i in 0..10 {
        let t = format!("new-{i:02}");
        f.turn(&t, true).await?;
        f.event(&t, Some(&t), "codex", "item/completed", 1).await?;
        run(&f.db, 101 + i).await?;
    }
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event WHERE turn_id='large'")
            .await?,
        128
    );
    for now in 200..205 {
        run(&f.db, now).await?;
    }
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event")
            .await?,
        0
    );
    f.turn("served", true).await?;
    f.event("served", Some("served"), "codex", "item/completed", 1)
        .await?;
    f.sql("UPDATE native_event_cleanup_job SET last_served_at=1; UPDATE native_event_cleanup_scheduler SET new_jobs_since_served=0").await?;
    let m = run(&f.db, 300).await?;
    assert_eq!(m.apply_writes, 3);
    assert_eq!(m.scheduler_rows_changed, 0);
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_of_queued_cleanup_releases_writer_and_preserves_job() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.event("event", Some("done"), "codex", "item/completed", 1)
        .await?;
    let tx = f.store.database_connection().begin().await?;
    let before = f.observer.queued.load(Ordering::SeqCst);
    let store = f.store.with_maintenance_access();
    let task = tokio::spawn(async move { store.cleanup_native_events_quantum().await });
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
#[test]
fn empty_candidate_binding_shape_does_not_read_events() {
    let (sql, bindings) = queries::candidates("turn", Some(&[])).build(SqliteQueryBuilder);
    let bindings = bindings.0;
    assert_eq!(sql.matches('?').count(), 1);
    assert_eq!(bindings.len(), 1);
    assert!(!sql.contains("cli_runtime_native_event"));
    let (sql, bindings) =
        queries::candidates("turn", Some(&["a".into(), "b".into()])).build(SqliteQueryBuilder);
    assert_eq!(sql.matches('?').count(), 5);
    assert_eq!(
        bindings.0,
        vec![
            "turn".into(),
            "turn".into(),
            "a".into(),
            "b".into(),
            (PAGE_ROWS as u64).into()
        ]
    );
}
#[tokio::test]
async fn actual_query_plans_use_addressed_indexes() -> Result<()> {
    let f = Fixture::open().await?;
    for (name, query, required) in [
        (
            "discovery",
            queries::discovery(1),
            vec![
                "native_event_cleanup_due_retry",
                "native_event_cleanup_new",
                "native_event_cleanup_served",
            ],
        ),
        (
            "prepare",
            queries::candidates("turn", None),
            vec!["native_event_cleanup_candidate"],
        ),
        (
            "apply_one",
            queries::candidates("turn", Some(&["a".into()])),
            vec![],
        ),
        (
            "apply_max",
            queries::candidates(
                "turn",
                Some(
                    &(0..PAGE_ROWS)
                        .map(|id| format!("event-{id}"))
                        .collect::<Vec<_>>(),
                ),
            ),
            vec![],
        ),
        (
            "remaining",
            queries::remaining("turn", Some("codex")),
            vec!["native_event_cleanup_candidate"],
        ),
    ] {
        let (sql, values) = query.build(SqliteQueryBuilder);
        let rows =
            f.db.query_all_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!("EXPLAIN QUERY PLAN {sql}"),
                values.0,
            ))
            .await?;
        let details = rows
            .iter()
            .map(|r| r.try_get::<String>("", "detail"))
            .collect::<Result<Vec<_>, _>>()?;
        for index in required {
            assert!(
                details.iter().any(|s| s.contains(index)),
                "{name}: {details:?}"
            );
        }
        if name.starts_with("apply") {
            // Revalidation is restricted to <= PAGE_ROWS prepared primary keys.
            // SQLite may prefer direct PK probes over the candidate index here;
            // both are addressed. Discovery/prepare still require their partial indexes.
            assert!(details.iter().any(|detail|
                detail.starts_with("SEARCH event USING INDEX native_event_cleanup_candidate (")
                || detail == "SEARCH event USING INDEX sqlite_autoindex_cli_runtime_native_event_1 (id=?)"
            ), "{name}: writer must use candidate-index or prepared-PK probes: {details:?}");
        }
        for detail in &details {
            if let Some(relation) = detail
                .strip_prefix("SCAN ")
                .and_then(|s| s.split_ascii_whitespace().next())
            {
                assert!(
                    ![
                        "event",
                        "a",
                        "s",
                        "r",
                        "p",
                        "receipt",
                        "cli_runtime_native_event"
                    ]
                    .contains(&relation),
                    "{name}: {details:?}"
                );
            }
        }
        let plan = rows
            .iter()
            .zip(&details)
            .map(|(row, detail)| {
                Ok((
                    row.try_get::<i64>("", "id")?,
                    row.try_get::<i64>("", "parent")?,
                    detail.as_str(),
                ))
            })
            .collect::<Result<Vec<_>, sea_orm::DbErr>>()?;
        println!("{name}: {plan:?}");
        let sorts = plan
            .iter()
            .filter(|row| row.2.contains("TEMP B-TREE"))
            .collect::<Vec<_>>();
        if name == "prepare" || name.starts_with("apply") {
            assert!(
                sql.split_ascii_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .contains("AS MATERIALIZED"),
                "{sql}"
            );
            let materialized = plan
                .iter()
                .find(|row| row.2 == "MATERIALIZE candidates")
                .expect("bounded candidates materialization")
                .0;
            assert!(sorts.len() <= 1, "{name}: {plan:?}");
            for sort in sorts {
                assert_eq!(sort.2, "USE TEMP B-TREE FOR ORDER BY");
                let mut parent = sort.1;
                for _ in 0..=plan.len() {
                    assert_ne!(parent, materialized, "source sorted before LIMIT: {plan:?}");
                    let Some(next) = plan.iter().find(|row| row.0 == parent) else {
                        break;
                    };
                    assert_ne!(next.1, parent, "cyclic plan: {plan:?}");
                    parent = next.1;
                }
                assert!(
                    plan.iter()
                        .any(|row| row.1 == sort.1 && row.2.starts_with("SCAN candidates")),
                    "sort must follow the bounded outer scan: {plan:?}"
                );
            }
        } else {
            assert!(sorts.is_empty(), "{name}: {plan:?}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn dependency_signals_wake_only_waiters_and_receipts_wait_for_last_blocker() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("sentinel", false).await?;
    f.event("sentinel", Some("sentinel"), "codex", "item/completed", 1)
        .await?;
    run(&f.db, 1).await?;
    let sentinel = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='sentinel'")
        .await?;
    f.turn("done", true).await?;
    for (block, unblock) in [
        (
            "UPDATE turn SET status='in_progress' WHERE id='done'",
            "UPDATE turn SET status='completed' WHERE id='done'",
        ),
        (
            "UPDATE turn_cli_runtime_binding SET status='running' WHERE turn_id='done'",
            "UPDATE turn_cli_runtime_binding SET status='completed' WHERE turn_id='done'",
        ),
        (
            "UPDATE turn_event_projection_stream_state SET status='quarantined' WHERE turn_id='done'",
            "UPDATE turn_event_projection_stream_state SET status='healthy' WHERE turn_id='done'",
        ),
        (
            "UPDATE turn_event_projection_stream_state SET projected_through_sequence=0 WHERE turn_id='done'",
            "UPDATE turn_event_projection_stream_state SET projected_through_sequence=2 WHERE turn_id='done'",
        ),
        (
            "INSERT INTO turn_cli_runtime_attempt(id,turn_id,attempt_index,runtime_id,runtime_kind,native_thread_id,status,created_at,updated_at) VALUES('attempt','done',1,'codex','codex','native','running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "UPDATE turn_cli_runtime_attempt SET status='completed' WHERE id='attempt'",
        ),
        (
            "INSERT INTO turn_cli_runtime_execution_segment(id,attempt_id,turn_id,segment_index,runtime_id,native_thread_id,native_turn_id,status,started_at,created_at,updated_at) VALUES('segment','attempt','done',1,'codex','native','native-segment','running',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
            "UPDATE turn_cli_runtime_execution_segment SET status='completed' WHERE id='segment'",
        ),
        (
            "INSERT INTO recovery_job(id,turn_id,item_id,item_type,status,trigger,action,policy,provider_attempt_number,policy_snapshot,run_count,max_attempts,scheduled_at,next_run_at,created_at,updated_at,resolution_pending) VALUES('recovery','done','item','command_execution','pending','timeout','retry','{}',1,'{}',0,3,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,1)",
            "UPDATE recovery_job SET status='succeeded',resolution_pending=0 WHERE id='recovery'",
        ),
    ] {
        f.sql(block).await?;
        f.event("work", Some("done"), "codex", "item/completed", 1)
            .await?;
        assert_eq!(run(&f.db, 2).await?.events_deleted, 0, "{block}");
        assert_eq!(f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='done' AND state='waiting'").await?,1);
        f.sql(unblock).await?;
        assert_eq!(run(&f.db, 3).await?.events_deleted, 1, "{unblock}");
        assert_eq!(
            f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='sentinel'")
                .await?,
            sentinel
        );
    }
    f.event("work", Some("done"), "codex", "item/completed", 1)
        .await?;
    f.sql("INSERT INTO turn_event_projection_state(event_id,thread_id,turn_id,sequence,status,attempt_count,next_run_at,projection_context_json,created_at,updated_at) VALUES
        ('low','thread-cleanup','done',1,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
        ('high1','thread-cleanup','done',100,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP),
        ('high2','thread-cleanup','done',101,'projected',0,CURRENT_TIMESTAMP,'{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)").await?;
    assert_eq!(run(&f.db, 4).await?.events_deleted, 0);
    let rev = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='done'")
        .await?;
    f.sql("DELETE FROM turn_event_projection_state WHERE event_id='low'; DELETE FROM turn_event_projection_state WHERE event_id='high1'; UPDATE turn_event_projection_state SET sequence=sequence WHERE event_id='high2'").await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='done'")
            .await?,
        rev
    );
    f.sql("UPDATE turn_event_projection_state SET sequence=2 WHERE event_id='high2'")
        .await?;
    assert_eq!(run(&f.db, 5).await?.events_deleted, 1);
    Ok(())
}

#[tokio::test]
async fn moved_event_wakes_destination_and_queued_deletes_do_not_update_job() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("source", true).await?;
    f.turn("target", true).await?;
    f.event(
        "wrong-runtime",
        Some("target"),
        "wrong",
        "item/completed",
        8,
    )
    .await?;
    run(&f.db, 1).await?;
    f.event("moving", Some("source"), "codex", "item/completed", 8)
        .await?;
    f.sql("UPDATE cli_runtime_native_event SET turn_id='target' WHERE id='moving'")
        .await?;
    assert_eq!(f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job WHERE turn_id='target' AND state='queued'").await?,1);
    let rev = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
        .await?;
    f.sql("DELETE FROM cli_runtime_native_event WHERE id='moving'")
        .await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
            .await?,
        rev
    );
    run(&f.db, 2).await?;
    run(&f.db, 3).await?;
    let rev = f
        .scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
        .await?;
    f.event("retained", Some("target"), "codex", "future/event", 8)
        .await?;
    f.sql("DELETE FROM cli_runtime_native_event WHERE id='retained'")
        .await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
            .await?,
        rev
    );
    f.sql("DELETE FROM cli_runtime_native_event WHERE id='wrong-runtime'")
        .await?;
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='target'")
            .await?,
        rev + 1
    );
    Ok(())
}

#[tokio::test]
async fn bootstrap_cursor_rolls_back_with_job_registration_failure() -> Result<()> {
    let f = Fixture::open_version(false).await?;
    f.turn("legacy", true).await?;
    f.event("legacy", Some("legacy"), "codex", "item/completed", 8)
        .await?;
    let f = f.migrate(false).await?;
    f.sql("CREATE TRIGGER fail_bootstrap BEFORE INSERT ON native_event_cleanup_job BEGIN SELECT RAISE(ABORT,'injected bootstrap failure'); END").await?;
    assert!(bootstrap(&f.db).await.is_err());
    assert_eq!(
        f.scalar("SELECT cursor_id IS NULL AND complete=0 FROM native_event_cleanup_bootstrap")
            .await?,
        1
    );
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM native_event_cleanup_job")
            .await?,
        0
    );
    f.sql("DROP TRIGGER fail_bootstrap").await?;
    assert_eq!(bootstrap(&f.db).await?.jobs_inserted, 1);
    assert_eq!(run(&f.db, 1).await?.events_deleted, 1);
    Ok(())
}

#[tokio::test]
async fn queued_cleanup_matches_baseline_retained_id_set() -> Result<()> {
    let baseline = Fixture::open().await?;
    let addressed = Fixture::open().await?;
    for f in [&baseline, &addressed] {
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
    }
    let mut cursor = 0;
    let mut finished = false;
    for _ in 0..20 {
        let m = baseline
            .store
            .with_maintenance_access()
            .cleanup_native_events_baseline_quantum(cursor)
            .await?;
        match m.last_rowid {
            Some(next) => cursor = next,
            None => {
                finished = true;
                break;
            }
        }
    }
    assert!(finished);
    finished = false;
    for now in 1..20 {
        if run(&addressed.db, now).await?.jobs_examined == 0 {
            finished = true;
            break;
        }
    }
    assert!(finished);
    async fn ids(f: &Fixture) -> Result<Vec<String>> {
        let rows =
            f.db.query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT id FROM cli_runtime_native_event ORDER BY id",
            ))
            .await?;
        Ok(rows
            .iter()
            .map(|row| row.try_get_by_index(0))
            .collect::<Result<Vec<_>, _>>()?)
    }
    assert_eq!(ids(&baseline).await?, ids(&addressed).await?);
    Ok(())
}

#[tokio::test]
async fn failure_after_delete_rolls_back_events_and_scheduler_before_deferral() -> Result<()> {
    let f = Fixture::open().await?;
    f.turn("done", true).await?;
    f.event("event", Some("done"), "codex", "item/completed", 8)
        .await?;
    f.sql("CREATE TRIGGER fail_scheduler BEFORE UPDATE ON native_event_cleanup_scheduler BEGIN SELECT RAISE(ABORT,'injected post-delete failure'); END").await?;
    let m = run(&f.db, 100).await?;
    assert_eq!(m.errors_deferred, 1);
    assert_eq!(m.events_deleted, 0);
    assert_eq!(m.deleted_bytes, 0);
    assert_eq!(m.scheduler_rows_changed, 0);
    assert_eq!(
        m.queue_rows_changed, 1,
        "only the durable deferral changes the job"
    );
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM cli_runtime_native_event WHERE id='event'")
            .await?,
        1
    );
    assert_eq!(
        f.scalar("SELECT revision FROM native_event_cleanup_job WHERE turn_id='done'")
            .await?,
        2
    );
    assert_eq!(
        f.scalar("SELECT new_jobs_since_served FROM native_event_cleanup_scheduler")
            .await?,
        0
    );
    f.sql("DROP TRIGGER fail_scheduler").await?;
    assert_eq!(
        run(&f.db, 100 + RETRY_DELAY_MICROS).await?.events_deleted,
        1
    );
    Ok(())
}

#[tokio::test]
async fn migration_schema_and_marker_roll_back_together_after_index_failure() -> Result<()> {
    let f = Fixture::open_version(false).await?;
    let path = f.path.clone();
    f.db.clone().close().await?;
    drop(f);
    // A dedicated fixture upgrade connection is opened only after both runtime
    // pools close, just as the other migration/restart fixtures do.
    let mut options = ConnectOptions::new(format!("sqlite://{}?mode=rw", path.0.display()));
    options.max_connections(1);
    options.map_sqlx_sqlite_opts(|o| o.pragma("foreign_keys", "ON"));
    let writer = Database::connect(options).await?;
    writer
        .execute_unprepared(
            "CREATE INDEX native_event_cleanup_candidate ON cli_runtime_native_event(id)",
        )
        .await?;
    assert!(Migrator::up(&writer, None).await.is_err());
    for sql in [
        "SELECT COUNT(*) FROM sqlite_master WHERE name='native_event_cleanup_job'",
        "SELECT COUNT(*) FROM seaql_migrations WHERE version='m20260919_000001_native_event_cleanup_queue'",
    ] {
        let count: i64 = writer
            .query_one_raw(Statement::from_string(DatabaseBackend::Sqlite, sql))
            .await?
            .unwrap()
            .try_get_by_index(0)?;
        assert_eq!(count, 0, "{sql}");
    }
    writer
        .execute_unprepared("DROP INDEX native_event_cleanup_candidate")
        .await?;
    Migrator::up(&writer, None).await?;
    let f = Fixture::connect(path, writer).await?;
    assert_eq!(
        f.scalar("SELECT COUNT(*) FROM native_event_cleanup_scheduler")
            .await?,
        1
    );
    assert!(bootstrap(&f.db).await?.complete);
    Ok(())
}
