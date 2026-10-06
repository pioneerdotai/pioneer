use super::*;
use migration::{Migrator, MigratorTrait};
use pioneer_sqlite::{
    SqliteDatabase, SqliteReadClass, SqliteReadEvent, SqliteReadObserver, SqliteWriteEvent,
    SqliteWriteExecutor, SqliteWriteObserver,
};
use sea_orm::{ConnectOptions, ConnectionTrait, Database};

#[derive(Default)]
pub(super) struct Observer {
    pub(super) reads: StdMutex<Vec<SqliteReadClass>>,
    pub(super) writes: StdMutex<Vec<SqliteWriteClass>>,
}
impl SqliteReadObserver for Observer {
    fn observe(&self, event: SqliteReadEvent) {
        if let SqliteReadEvent::OperationFinished { class, .. } = event {
            self.reads.lock().unwrap().push(class);
        }
    }
}
impl SqliteWriteObserver for Observer {
    fn observe(&self, event: SqliteWriteEvent) {
        if let SqliteWriteEvent::Acquired { class, .. } = event {
            self.writes.lock().unwrap().push(class);
        }
    }
}

pub(super) struct Harness {
    pub(super) directory: tempfile::TempDir,
    pub(super) processor: Arc<MessageProcessor>,
    pub(super) workspace: pioneer_entity::workspace::Model,
    pub(super) observer: Arc<Observer>,
}

pub(super) async fn harness() -> Harness {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("skills.sqlite");
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_connection_url(&path));
    options.max_connections(1).sqlx_logging(false);
    let writer = Database::connect(options).await.unwrap();
    Migrator::up(&writer, None).await.unwrap();
    writer
        .execute_unprepared("PRAGMA journal_mode=WAL")
        .await
        .unwrap();
    let mut options = ConnectOptions::new(pioneer_sqlite::sqlite_read_only_connection_url(&path));
    options
        .max_connections(2)
        .sqlx_logging(false)
        .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
    let reader = Database::connect(options).await.unwrap();
    let observer = Arc::new(Observer::default());
    let database = SqliteDatabase::from_executor_with_read_observer(
        reader,
        SqliteWriteExecutor::with_observer(writer, observer.clone()),
        observer.clone(),
    );
    let manager = Arc::new(crate::workspace::WorkspaceManager::new(database.clone()));
    manager
        .create_workspace("ws", Some("Skills test"))
        .await
        .unwrap();
    let workspace = manager
        .active_page(None)
        .await
        .unwrap()
        .into_iter()
        .find(|workspace| workspace.id == "ws")
        .unwrap();
    let mut config = crate::message::tests::test_tool_loop_config();
    config.skills.system_roots.clear();
    config.skills.user_roots = vec![
        directory
            .path()
            .join("managed/{workspaceId}/user")
            .display()
            .to_string(),
    ];
    config.skills.registry_roots = vec![
        directory
            .path()
            .join("managed/{workspaceId}/registry")
            .display()
            .to_string(),
    ];
    config.skills.user_import_roots = vec![directory.path().join("source").display().to_string()];
    config.skills.security.allow_untrusted_install = true;
    let processor = Arc::new(
        MessageProcessor::new(
            Arc::new(crate::thread::ThreadManager::new("test", "openai")),
            Arc::new(pioneer_provider::ProviderRegistry::with_provider(
                "openai",
                Arc::new(pioneer_provider::providers::EchoProvider::new()),
            )),
            Arc::new(crate::session::SessionManager::new()),
            manager,
            Arc::new(pioneer_crud::CrudStore::new(database)),
            Arc::new(crate::secrets::GatewaySecrets::new(Arc::new(
                pioneer_keystore::MemorySecretStore::new(),
            ))),
            crate::message::summary::SummaryConfig {
                summary_model: None,
                summary_model_provider: None,
                title_model: None,
                title_model_provider: None,
            },
            config,
        )
        .with_runtime_home_for_tests(directory.path().join("runtime")),
    );
    let processor = processor.with_database_class(SqliteWriteClass::Maintenance);
    observer.reads.lock().unwrap().clear();
    observer.writes.lock().unwrap().clear();
    Harness {
        directory,
        processor,
        workspace,
        observer,
    }
}

#[test]
fn late_event_and_late_rescan_cannot_be_acked_by_an_older_scan() {
    let now = Instant::now();
    let mut dirty = Dirty::new(7, now);
    dirty.first = None; // initial snapshot claimed generation 1
    dirty.mark(now, false);
    dirty.finish(7, 1, true, now);
    assert_eq!(dirty.acknowledged, 1);
    assert_eq!(dirty.generation, 2);
    assert_eq!(dirty.due(), Some(now + DEBOUNCE));
    dirty.first = None;
    dirty.mark(now, true);
    dirty.finish(7, 2, true, now);
    assert_eq!(dirty.rescan_generation, Some(3));
    assert!(dirty.due().is_some());
    dirty.finish(6, 3, true, now);
    assert_eq!(
        dirty.acknowledged, 2,
        "old incarnation cannot ACK this root"
    );
}

#[test]
fn continuous_edits_have_a_maximum_latency_and_do_not_cancel_error_delay() {
    let now = Instant::now();
    let mut dirty = Dirty::new(1, now);
    dirty.finish(1, 1, true, now);
    for milliseconds in (0..3000).step_by(100) {
        dirty.mark(now + Duration::from_millis(milliseconds), false);
    }
    assert_eq!(dirty.due(), Some(now + MAX_LATENCY));
    dirty.finish(1, dirty.generation, false, now);
    let retry = dirty.retry_at;
    for milliseconds in (0..3000).step_by(100) {
        dirty.mark(now + Duration::from_millis(milliseconds), true);
    }
    assert_eq!(dirty.retry_at, retry);
    assert!(dirty.due().unwrap() >= now + Duration::from_secs(5));
    for _ in 0..30 {
        dirty.finish(1, dirty.generation, false, now);
    }
    assert_eq!(dirty.attempts, 16);
    assert_eq!(dirty.retry_at, now + Duration::from_secs(300));
}

#[tokio::test]
async fn callback_is_targeted_bounded_and_retains_overflow_when_wake_is_consumed() {
    use notify::event::{Flag, ModifyKind, RenameMode};
    let signals = Signals::default();
    signals
        .roots
        .lock()
        .unwrap()
        .insert(PathBuf::from("/a"), Dirty::new(1, Instant::now()));
    signals
        .roots
        .lock()
        .unwrap()
        .insert(PathBuf::from("/b"), Dirty::new(2, Instant::now()));
    signals.event(Ok(Event::new(EventKind::Modify(ModifyKind::Name(
        RenameMode::Both,
    )))
    .add_path("/a/old".into())
    .add_path("/b/new".into())));
    let roots = signals.roots.lock().unwrap();
    assert_eq!(roots[Path::new("/a")].generation, 2);
    assert_eq!(roots[Path::new("/b")].generation, 2);
    // The callback never waits for the lock. The obligation survives failed
    // notification delivery/consumption; there is no event queue to overflow.
    signals.event(Ok(Event::new(EventKind::Any).add_path("/a/file".into())));
    drop(roots);
    signals.wake.notified().await;
    assert!(signals.overflow.load(Ordering::Acquire));
    signals.event(Ok(Event::new(EventKind::Other)
        .add_path("/a".into())
        .set_flag(Flag::Rescan)));
    assert!(signals.recover.load(Ordering::Acquire));
    signals.event(Err(
        notify::Error::generic("unavailable").add_path("/b".into())
    ));
    let roots = signals.roots.lock().unwrap();
    assert_eq!(roots.len(), 2);
    assert!(roots[Path::new("/b")].rescan_generation.is_some());
}

#[test]
fn nested_absence_advances_nonrecursive_sentinel_and_recreation_reinstalls_root() {
    let temp = tempfile::tempdir().unwrap();
    let base = fs_canonical(temp.path());
    let root = base.join("a/b/skills");
    assert_eq!(
        watch_plan(&root).unwrap(),
        vec![(base.clone(), RecursiveMode::NonRecursive)]
    );
    std::fs::create_dir(base.join("a")).unwrap();
    assert_eq!(
        watch_plan(&root).unwrap(),
        vec![(base.join("a"), RecursiveMode::NonRecursive)]
    );
    std::fs::create_dir_all(&root).unwrap();
    let plan = watch_plan(&root).unwrap();
    assert!(plan.contains(&(root.clone(), RecursiveMode::Recursive)));
    assert!(plan.contains(&(base.join("a/b"), RecursiveMode::NonRecursive)));
    std::fs::remove_dir_all(&root).unwrap();
    assert_eq!(
        watch_plan(&root).unwrap(),
        vec![(base.join("a/b"), RecursiveMode::NonRecursive)]
    );
}
fn fs_canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap()
}

#[tokio::test]
async fn shutdown_joins_an_owned_blocking_quantum() {
    let stop = CancellationToken::new();
    let worker_stop = stop.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let joined = Arc::new(AtomicBool::new(false));
    let completed = joined.clone();
    let work = tokio::spawn(async move {
        owned_fs(&worker_stop, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            completed.store(true, Ordering::Release);
            Ok(())
        })
        .await
    });
    started_rx.await.unwrap();
    stop.cancel();
    assert!(!work.is_finished());
    release_tx.send(()).unwrap();
    work.await.unwrap().unwrap();
    assert!(joined.load(Ordering::Acquire));
}

#[tokio::test]
async fn shared_system_roots_are_registered_once_with_all_workspace_scopes() {
    let harness = harness().await;
    harness
        .processor
        .workspace_manager
        .create_workspace("second", Some("Second"))
        .await
        .unwrap();
    let signals = Arc::new(Signals::default());
    let stop = CancellationToken::new();
    let mut native = Native::new();
    let roots = snapshot_roots(&harness.processor, &stop, &signals, &mut native)
        .await
        .unwrap();
    let system = roots.values().filter(|root| root.mappings.iter().any(|mapping| matches!(mapping, Mapping::Managed(config, _) if config.roots[0].source_kind == SkillSourceKind::System))).collect::<Vec<_>>();
    assert_eq!(system.len(), 1);
    assert_eq!(
        system[0].subscribers,
        BTreeSet::from(["ws".into(), "second".into()])
    );
    assert_eq!(system[0].mappings.len(), 1);
    // No FS snapshot occurred before registrations: all roots remain pending.
    assert!(
        signals
            .roots
            .lock()
            .unwrap()
            .values()
            .all(|dirty| dirty.acknowledged == 0)
    );
    let changed = harness.processor.workspace_manager.changes();
    let before = changed.revision();
    let scoped = harness
        .processor
        .workspace_manager
        .with_database(harness.processor.crud_store.database_connection());
    scoped
        .update_workspace("second", Some("Renamed"))
        .await
        .unwrap();
    assert!(changed.revision() > before);
    let revision = changed.revision();
    assert!(
        scoped
            .update_workspace("missing", Some("Name"))
            .await
            .is_err()
    );
    assert_eq!(
        changed.revision(),
        revision,
        "failed writes cannot invalidate a committed snapshot"
    );
    tokio::task::spawn_blocking(move || drop(native))
        .await
        .unwrap();
}

#[test]
fn native_backend_failure_has_backoff_without_a_polling_fallback() {
    let signals = Arc::new(Signals::default());
    let native = Native::new().refresh(
        vec![PathBuf::from("missing-parent")],
        BTreeSet::new(),
        signals,
    );
    assert_eq!(native.attempts, 1);
    assert!(native.retry_at > Instant::now());
}

#[tokio::test]
#[ignore = "native OS delivery integration; run explicitly after review on a supported local filesystem"]
async fn native_atomic_save_and_rename_mark_dirty_without_hash_polling() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs_canonical(temp.path());
    let signals = Arc::new(Signals::default());
    signals
        .roots
        .lock()
        .unwrap()
        .insert(root.clone(), Dirty::new(1, Instant::now()));
    let callback = signals.clone();
    let paths = vec![root.clone()];
    let native = tokio::task::spawn_blocking(move || {
        Native::new().refresh(paths, BTreeSet::new(), callback)
    })
    .await
    .unwrap();
    assert_eq!(native.attempts, 0);
    let generation = signals.roots.lock().unwrap()[&root].generation;
    std::fs::write(root.join("editor.tmp"), b"new").unwrap();
    std::fs::rename(root.join("editor.tmp"), root.join("SKILL.md")).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if signals.roots.lock().unwrap()[&root].generation > generation { break; }
            tokio::select! { _ = signals.wake.notified() => {}, _ = tokio::time::sleep(Duration::from_millis(20)) => {} }
        }
    }).await.expect("this integration fixture requires real local native events; safety resync covers unobservable loss in production");
    tokio::task::spawn_blocking(move || drop(native))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "native OS delivery and idle integration; run explicitly after review"]
async fn worker_idle_has_no_periodic_select_and_notifies_only_real_changes() {
    let harness = harness().await;
    let package = harness.directory.path().join("source/skill");
    std::fs::create_dir_all(&package).unwrap();
    let text = "---\nname: Test\nslug: test-skill\n---\ncontent";
    std::fs::write(package.join("SKILL.md"), text).unwrap();
    let before = harness.processor.current_skills_snapshot_version();
    harness.processor.start_skills_watcher().await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while harness.processor.current_skills_snapshot_version() == before {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("configured import must produce a real skills notification");
    // Let import's own native events drain, then cover two former 1.5s ticks.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let reads = harness.observer.reads.lock().unwrap().len();
    let version = harness.processor.current_skills_snapshot_version();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(harness.observer.reads.lock().unwrap().len(), reads);
    assert_eq!(harness.processor.current_skills_snapshot_version(), version);
    std::fs::write(package.join("SKILL.md"), text).unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        harness.processor.current_skills_snapshot_version(),
        version,
        "a repeated own/content-identical write cannot notify clients"
    );
    std::fs::write(package.join("save.tmp"), text.replace("content", "changed")).unwrap();
    std::fs::rename(package.join("save.tmp"), package.join("SKILL.md")).unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while harness.processor.current_skills_snapshot_version() == version {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("atomic save must produce a real skills notification");
    harness.processor.shutdown_skills_watcher().await;
    assert!(
        harness
            .processor
            .skills_watcher_worker
            .lock()
            .await
            .is_none()
    );
    let reads = harness.observer.reads.lock().unwrap().len();
    std::fs::write(package.join("SKILL.md"), text).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        harness.observer.reads.lock().unwrap().len(),
        reads,
        "shutdown must release native handles and owned jobs"
    );
}

#[test]
fn repeated_backend_events_cannot_cancel_the_recovery_delay() {
    let mut native = Native::new();
    native.request_recovery();
    let retry = native.retry_at;
    for _ in 0..1000 {
        native.request_recovery();
    }
    assert_eq!(native.attempts, 1);
    assert_eq!(native.retry_at, retry);
}
