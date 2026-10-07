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
    pub(super) writer: SqliteWriteExecutor,
}

pub(super) async fn harness() -> Harness {
    harness_with_bad_root(false).await
}
async fn harness_with_bad_root(bad_root: bool) -> Harness {
    harness_with_layout(bad_root, false).await
}
async fn harness_with_layout(bad_root: bool, overlap: bool) -> Harness {
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
    let writer = SqliteWriteExecutor::with_observer(writer, observer.clone());
    let database =
        SqliteDatabase::from_executor_with_read_observer(reader, writer.clone(), observer.clone());
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
    if overlap {
        config.skills.user_import_roots = config.skills.user_roots.clone();
        config.skills.registry_import_roots =
            vec![directory.path().join("neighbor").display().to_string()];
    }
    if bad_root {
        let path = directory.path().join("bad-root");
        std::fs::write(&path, b"not a directory").unwrap();
        config
            .skills
            .user_import_roots
            .push(path.display().to_string());
    }
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
        writer,
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
    let native = Native::new();
    let roots = snapshot_roots(&harness.processor, &stop, &signals)
        .await
        .unwrap();
    let system = roots.values().filter(|root| root.mappings.iter().any(|mapping| matches!(mapping, Mapping::Managed(config, _) if config.roots[0].source_kind == SkillSourceKind::System))).collect::<Vec<_>>();
    assert_eq!(system.len(), 1);
    assert_eq!(
        system[0].subscribers,
        BTreeSet::from(["ws".into(), "second".into()])
    );
    assert_eq!(system[0].mappings.len(), 1);
    // Workspace preparation is unpublished. Native subscription and the full
    // initial FS snapshot follow activation in the actual worker.
    assert!(signals.roots.lock().unwrap().is_empty());
    assert!(native.watched.is_empty());
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
    signals.backend_unavailable.store(true, Ordering::Release);
    let before = Instant::now();
    let native = Native::new().refresh(
        vec![PathBuf::from("missing-parent")],
        BTreeSet::new(),
        signals.clone(),
    );
    assert_eq!(signals.backend_attempts.load(Ordering::Acquire), 1);
    assert!(native.watcher.is_none());
    assert!(native.recovery_pending);
    assert_eq!(native.attempts, 1);
    assert!(native.retry_at >= before + retry_delay(1));
    let mut unavailable = Native::new();
    unavailable.request_recovery();
    assert!(
        unavailable.ready(Path::new("/degraded")),
        "native unavailability must not block initial/safety reconciliation"
    );
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
        let mut native = Native::new();
        for _ in 0..10 {
            native = native.refresh(paths.clone(), BTreeSet::new(), callback.clone());
            if native.attempts == 0 {
                break;
            }
        }
        native
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

// These exercise the actual worker, including subscriptions, deadlines, jobs and
// sleeping, rather than an isolated Duration expression.
#[tokio::test]
async fn partial_snapshot_startup_backoff_and_late_event_recovery() {
    let harness = harness().await;
    let source = harness.directory.path().join("source/pkg");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(source.join("SKILL.md"), "---\nname: Test\n---\nInitial").unwrap();
    let signals = Arc::new(Signals::default());
    *signals.snapshot_failure_page.lock().unwrap() = Some(1);
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let iterations = signals.loop_iterations.load(Ordering::Relaxed);
    assert!(iterations < 20, "snapshot backoff must sleep");
    assert!(
        signals.roots.lock().unwrap().is_empty(),
        "failed snapshot publishes no orphan roots"
    );
    std::fs::write(source.join("SKILL.md"), "---\nname: Test\n---\nLate edit").unwrap();
    *signals.snapshot_failure_page.lock().unwrap() = None;
    signals.wake.notify_one();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        signals.snapshots.load(Ordering::Relaxed),
        0,
        "wake cannot cancel poison delay"
    );
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let rows = harness
                .processor
                .crud_store
                .list_skill_installations_scope_page("user", "ws", None, 64)
                .await
                .unwrap();
            if let Some(row) = rows.iter().find(|row| !row.fingerprint.is_empty()) {
                assert!(
                    std::fs::read_to_string(Path::new(&row.install_path).join("SKILL.md"))
                        .unwrap()
                        .contains("Late edit")
                );
                assert!(signals.snapshots.load(Ordering::Relaxed) > 0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn failed_snapshot_update_keeps_live_roots_and_root_failure_is_isolated() {
    let harness = harness_with_bad_root(true).await;
    let source = harness.directory.path().join("source/pkg");
    std::fs::create_dir_all(&source).unwrap();
    for kind in ["user", "registry"] {
        std::fs::create_dir_all(harness.directory.path().join("managed/ws").join(kind)).unwrap();
    }
    let healthy_root = fs_canonical(source.parent().unwrap());
    let signals = Arc::new(Signals::default());
    // Inject the edit explicitly. Native folder-creation callbacks can cancel
    // an initial scan and schedule an unrelated five-second root backoff.
    signals
        .native_events_disabled
        .store(true, Ordering::Release);
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let settled = signals
                .roots
                .lock()
                .unwrap()
                .get(&healthy_root)
                .is_some_and(|dirty| dirty.generation == dirty.acknowledged);
            if settled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("healthy initial scan must finish before snapshot failure injection");
    let snapshots = signals.snapshots.load(Ordering::Relaxed);
    let roots = signals.roots.lock().unwrap().len();
    *signals.snapshot_failure_page.lock().unwrap() = Some(1);
    harness
        .processor
        .workspace_manager
        .create_workspace("other", Some("Changed"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), signals.snapshot_failed.notified())
        .await
        .expect("changed workspace snapshot must reach its injected failure");
    let before = signals.loop_iterations.load(Ordering::Relaxed);
    std::fs::write(
        source.join("SKILL.md"),
        "---\nname: Healthy\n---\nLate healthy root",
    )
    .unwrap();
    signals.event(Ok(Event::new(EventKind::Modify(
        notify::event::ModifyKind::Any,
    ))
    .add_path(source.join("SKILL.md"))));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(signals.roots.lock().unwrap().len(), roots);
    assert!(
        signals.loop_iterations.load(Ordering::Relaxed) - before < 200,
        "no expired provisional deadline spin"
    );
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let rows = harness
                .processor
                .crud_store
                .list_skill_installations_scope_page("user", "ws", None, 64)
                .await
                .unwrap();
            if rows.iter().any(|row| !row.fingerprint.is_empty()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("healthy previous mapping continues during snapshot backoff");
    assert_eq!(
        signals.snapshots.load(Ordering::Relaxed),
        snapshots,
        "healthy import must finish using the previous mapping while snapshots fail"
    );
    *signals.snapshot_failure_page.lock().unwrap() = None;
    // A slow import may span more than one failed snapshot retry. Preserve
    // that scheduled backoff while waiting for the replacement mappings.
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let rows = harness
                .processor
                .crud_store
                .list_skill_installations_scope_page("user", "ws", None, 64)
                .await
                .unwrap();
            if signals.snapshots.load(Ordering::Relaxed) >= 2
                && rows.iter().any(|row| !row.fingerprint.is_empty())
            {
                let other = harness
                    .processor
                    .crud_store
                    .list_skill_installations_scope_page("user", "other", None, 64)
                    .await
                    .unwrap();
                if other.iter().any(|row| !row.fingerprint.is_empty()) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    stop.cancel();
    worker.await.unwrap();
}

#[test]
fn bounded_native_directory_registration_and_shared_sentinel_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let parent = fs_canonical(directory.path());
    let root = parent.join("root");
    std::fs::create_dir(&root).unwrap();
    for i in 0..150 {
        std::fs::create_dir(root.join(format!("d{i}"))).unwrap();
    }
    let signals = Arc::new(Signals::default());
    let mut native = Native::new();
    native.recursive = false; // exercise the Linux strategy on every test host
    native = native.refresh(vec![root.clone()], BTreeSet::new(), signals.clone());
    assert!(native.watched.len() <= 64);
    assert!(!native.ready(&root));
    for _ in 0..20 {
        native = native.refresh(vec![root.clone()], BTreeSet::new(), signals.clone());
        if native.ready(&root) {
            break;
        }
    }
    assert!(native.ready(&root));
    assert_eq!(native.watched.len(), 152);
    let sentinel = parent.join("sentinel");
    std::fs::create_dir(&sentinel).unwrap();
    let a = sentinel.join("absent/a");
    let b = sentinel.join("absent/b");
    for _ in 0..4 {
        native = native.refresh(
            vec![root.clone(), a.clone(), b.clone()],
            BTreeSet::new(),
            signals.clone(),
        );
    }
    assert_eq!(native.watched[&sentinel].owners.len(), 2);
    let b_guard = {
        let mut roots = signals.roots.lock().unwrap();
        roots.insert(a.clone(), Dirty::new(1, Instant::now()));
        roots.insert(b.clone(), Dirty::new(2, Instant::now()));
        roots[&b].live.clone()
    };
    let watched_before = native.watch_calls[&sentinel];
    native.fail_watch_once.insert(sentinel.clone());
    std::fs::remove_dir(&sentinel).unwrap();
    std::fs::create_dir(&sentinel).unwrap();
    native = native.refresh(
        vec![root.clone(), a.clone(), b.clone()],
        BTreeSet::from([a.clone(), b.clone()]),
        signals.clone(),
    );
    assert!(
        !native
            .watched
            .get(&sentinel)
            .is_some_and(|watch| watch.active),
        "unwatch plus failed replacement cannot claim a live subscription"
    );
    assert!(native.plans[&a].retry_at > Instant::now());
    assert!(
        !b_guard.load(Ordering::Acquire),
        "replacing a shared subscription fences other owners' prepared jobs"
    );
    assert_eq!(native.observation(&b), Observation::Registering);
    native.plans.get_mut(&a).unwrap().retry_at = Instant::now();
    native.plans.get_mut(&b).unwrap().retry_at = Instant::now();
    for _ in 0..4 {
        native = native.refresh(
            vec![root.clone(), a.clone(), b.clone()],
            BTreeSet::new(),
            signals.clone(),
        );
    }
    assert!(native.watched.contains_key(&sentinel));
    assert!(native.watch_calls[&sentinel] > watched_before);
    std::fs::create_dir(sentinel.join("absent")).unwrap();
    for _ in 0..4 {
        native = native.refresh(
            vec![root.clone(), a.clone(), b.clone()],
            BTreeSet::from([a.clone(), b.clone()]),
            signals.clone(),
        );
    }
    assert!(
        native.watched.contains_key(&sentinel.join("absent")),
        "subscription advances without safety round"
    );
}

#[tokio::test]
async fn shutdown_preserves_protected_last_copy_discovered_after_restart() {
    let harness = harness().await;
    let config = harness.processor.managed_root_scan_config("ws").unwrap();
    let root = config
        .roots
        .iter()
        .find(|root| root.source_kind == SkillSourceKind::User)
        .unwrap()
        .managed_root
        .clone();
    let parent = root.join("container");
    std::fs::create_dir_all(&parent).unwrap();
    let wrapper = reconcile::new_attempt(&parent, root).unwrap();
    std::fs::create_dir(wrapper.join("backup")).unwrap();
    std::fs::write(wrapper.join("backup/SKILL.md"), "Last copy").unwrap();
    reconcile::set_attempt_publishing(&wrapper, true).unwrap();
    let signals = Arc::new(Signals::default());
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while signals.snapshots.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    stop.cancel();
    worker.await.unwrap();
    assert_eq!(
        std::fs::read_to_string(wrapper.join("backup/SKILL.md")).unwrap(),
        "Last copy"
    );
}

#[test]
fn prepared_guard_never_waits_for_the_callback_mutex_under_writer_capacity() {
    let signals = Arc::new(Signals::default());
    let root = PathBuf::from("/guard");
    signals
        .roots
        .lock()
        .unwrap()
        .insert(root.clone(), Dirty::new(1, Instant::now()));
    let live = signals.roots.lock().unwrap()[&root].live.clone();
    let stop = CancellationToken::new();
    let guard = JobFence {
        claims: signals.new_claim_owner(&root, live.clone()),
        signals: signals.clone(),
        live: live.clone(),
        root,
        incarnation: 1,
        stop,
    };
    let lock = signals.roots.lock().unwrap();
    assert!(
        guard.valid(),
        "a callback storm must neither block the writer nor falsely retire a live incarnation"
    );
    live.store(false, Ordering::Release);
    assert!(
        !guard.valid(),
        "retired incarnation must remain fenced even while the callback mutex is held"
    );
    drop(lock);
    assert!(!guard.valid());
}

#[tokio::test]
async fn unavailable_backend_keeps_real_worker_initial_reconciliation_and_idle_backoff() {
    let harness = harness().await;
    let source = harness.directory.path().join("source/pkg");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(
        source.join("SKILL.md"),
        "---\nname: Degraded\n---\nInitial import",
    )
    .unwrap();
    let signals = Arc::new(Signals::default());
    signals.backend_unavailable.store(true, Ordering::Release);
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let roots = signals.roots.lock().unwrap();
            let complete = !roots.is_empty()
                && roots
                    .values()
                    .all(|dirty| dirty.generation == dirty.acknowledged);
            drop(roots);
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("initial work must continue with explicit degraded native observation");
    let rows = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 64)
        .await
        .unwrap();
    assert!(rows.iter().any(|row| !row.fingerprint.is_empty()));
    tokio::time::sleep(Duration::from_millis(100)).await; // drain post-ACK notifications
    let reads = harness.observer.reads.lock().unwrap().len();
    let iterations = signals.loop_iterations.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(harness.observer.reads.lock().unwrap().len(), reads);
    assert!(signals.loop_iterations.load(Ordering::Relaxed) - iterations < 3);
    stop.cancel();
    worker.await.unwrap();
}

async fn settled(signals: &Signals) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let complete = {
                let roots = signals.roots.lock().unwrap();
                !roots.is_empty()
                    && roots
                        .values()
                        .all(|dirty| dirty.generation == dirty.acknowledged)
            };
            if complete {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real worker must settle");
}

#[tokio::test]
async fn one_need_rescan_recovers_once_and_returns_the_real_worker_to_idle() {
    let harness = harness().await;
    let signals = Arc::new(Signals::default());
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    settled(&signals).await;
    signals.event(Ok(
        Event::new(EventKind::Other).set_flag(notify::event::Flag::Rescan)
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while signals.backend_creations.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("need_rescan must recover observation");
    settled(&signals).await;
    let rounds = signals.root_rounds.load(Ordering::Acquire);
    let reads = harness.observer.reads.lock().unwrap().len();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(signals.backend_creations.load(Ordering::Acquire), 2);
    assert_eq!(signals.root_rounds.load(Ordering::Acquire), rounds);
    assert_eq!(harness.observer.reads.lock().unwrap().len(), reads);
    assert!(!signals.recover.load(Ordering::Acquire));
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn recovery_signal_is_consumed_and_a_signal_during_recreation_gets_its_own_round() {
    let harness = harness().await;
    let signals = Arc::new(Signals::default());
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    settled(&signals).await;
    assert_eq!(signals.backend_creations.load(Ordering::Acquire), 1);
    signals.event(Err(notify::Error::generic("one backend failure")));
    signals
        .signal_during_recovery
        .store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(20), async {
        while signals.backend_creations.load(Ordering::Acquire) < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("new recovery signal must survive the in-progress recreation");
    settled(&signals).await;
    assert!(!signals.recover.load(Ordering::Acquire));
    let creations = signals.backend_creations.load(Ordering::Acquire);
    let rounds = signals.root_rounds.load(Ordering::Acquire);
    let reads = harness.observer.reads.lock().unwrap().len();
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(signals.backend_creations.load(Ordering::Acquire), creations);
    assert_eq!(signals.root_rounds.load(Ordering::Acquire), rounds);
    assert_eq!(harness.observer.reads.lock().unwrap().len(), reads);
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn degraded_clean_roots_do_not_rescan_at_each_failed_constructor_retry() {
    let harness = harness().await;
    let signals = Arc::new(Signals::default());
    signals.backend_unavailable.store(true, Ordering::Release);
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    settled(&signals).await;
    let rounds = signals.root_rounds.load(Ordering::Acquire);
    let reads = harness.observer.reads.lock().unwrap().len();
    tokio::time::timeout(Duration::from_secs(20), async {
        while signals.backend_attempts.load(Ordering::Acquire) < 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("real loop must retry construction with 5/10 second backoff");
    assert_eq!(signals.backend_creations.load(Ordering::Acquire), 0);
    assert_eq!(signals.root_rounds.load(Ordering::Acquire), rounds);
    assert_eq!(harness.observer.reads.lock().unwrap().len(), reads);
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn late_registration_barrier_fences_old_job_and_observes_previously_unwatched_bytes() {
    let harness = harness().await;
    std::fs::create_dir_all(harness.directory.path().join("source")).unwrap();
    let root = fs_canonical(&harness.directory.path().join("source"));
    let signals = Arc::new(Signals::default());
    signals
        .directory_registration
        .store(true, Ordering::Release);
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    settled(&signals).await;
    let old_live = signals.roots.lock().unwrap()[&root].live.clone();
    *signals.pause_root.lock().unwrap() = Some(root.clone());
    signals.event(Ok(Event::new(EventKind::Modify(
        notify::event::ModifyKind::Any,
    ))
    .add_path(root.join("edit.txt"))));
    tokio::time::timeout(Duration::from_secs(3), signals.paused.notified())
        .await
        .unwrap();
    for i in 0..150 {
        std::fs::create_dir(root.join(format!("new-{i}"))).unwrap();
    }
    let late = root.join("new-149");
    std::fs::write(late.join("SKILL.md"), "---\nname: Late\n---\nNew bytes").unwrap();
    signals.event(Ok(Event::new(EventKind::Create(
        notify::event::CreateKind::Folder,
    ))
    .add_path(root.join("new-0"))));
    signals
        .roots
        .lock()
        .unwrap()
        .get_mut(&root)
        .unwrap()
        .watch_dirty = true;
    signals.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while old_live.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the real loop must retire the old cursor before replacement");
    settled(&signals).await;
    assert!(
        !old_live.load(Ordering::Acquire),
        "completed registration must never revive the old prepared guard"
    );
    let rows = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 64)
        .await
        .unwrap();
    assert!(rows.iter().any(|row| {
        std::fs::read_to_string(Path::new(&row.install_path).join("SKILL.md"))
            .unwrap()
            .contains("New bytes")
    }));
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn owned_linux_folder_events_do_not_restart_an_overlapping_import() {
    let harness = harness_with_layout(false, true).await;
    let root = harness.directory.path().join("managed/ws/user");
    let source = root.join("unregistered");
    std::fs::create_dir_all(&source).unwrap();
    std::fs::write(
        source.join("SKILL.md"),
        "---\nname: Owned staging\n---\nImport",
    )
    .unwrap();
    std::fs::write(source.join("large.asset"), vec![b'a'; 800 * 1024]).unwrap();
    let neighbor = harness.directory.path().join("neighbor/pkg");
    std::fs::create_dir_all(&neighbor).unwrap();
    std::fs::write(
        neighbor.join("SKILL.md"),
        "---\nname: Neighbor\n---\nIndependent",
    )
    .unwrap();
    let large_skill = format!("---\nname: Owned staging\n---\n{}", "x".repeat(600 * 1024));
    let large_sidecar = serde_json::to_vec(
        &serde_json::json!({"owner":"prepared", "extra":"y".repeat(700 * 1024)}),
    )
    .unwrap();
    std::fs::write(source.join("SKILL.md"), &large_skill).unwrap();
    std::fs::write(source.join("_meta.json"), &large_sidecar).unwrap();
    let metadata_bytes = large_skill.len() + large_sidecar.len();
    let root = fs_canonical(&root);
    let signals = Arc::new(Signals::default());
    signals
        .directory_registration
        .store(true, Ordering::Release);
    signals
        .native_events_disabled
        .store(true, Ordering::Release);
    *signals.pause_stage.lock().unwrap() = Some(root.clone());
    *signals.pause_cleanup.lock().unwrap() = Some(root.clone());
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
        .await
        .unwrap();
    let (wrapper, guard) = {
        let roots = signals.roots.lock().unwrap();
        let dirty = &roots[&root];
        (
            roots
                .owned_paths
                .iter()
                .find(|(_, claims)| {
                    claims.get(&root).is_some_and(|claim| {
                        claim.subtree && claim.lease.live.load(Ordering::Acquire)
                    })
                })
                .unwrap()
                .0
                .clone(),
            dirty.live.clone(),
        )
    };
    let stages = signals.stages_created.lock().unwrap()[&root];
    for path in [wrapper.clone(), wrapper.join("payload")] {
        signals.event(Ok(Event::new(EventKind::Create(
            notify::event::CreateKind::Folder,
        ))
        .add_path(path)));
    }
    signals.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
        .await
        .unwrap();
    assert!(
        guard.load(Ordering::Acquire),
        "owned staging/rename cannot retire its publisher"
    );
    assert_eq!(signals.stages_created.lock().unwrap()[&root], stages);
    for path in [wrapper.join("payload"), wrapper.clone()] {
        signals.event(Ok(Event::new(EventKind::Remove(
            notify::event::RemoveKind::Folder,
        ))
        .add_path(path)));
    }
    signals.resume.notify_one();
    settled(&signals).await;
    assert_eq!(
        signals.stages_created.lock().unwrap()[&root],
        stages,
        "no staging/cleanup loop"
    );
    let user = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 64)
        .await
        .unwrap();
    let registry = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("registry", "ws", None, 64)
        .await
        .unwrap();
    assert_eq!(user.len(), 1);
    assert!(!user[0].fingerprint.is_empty());
    assert_eq!(registry.len(), 1);
    assert!(!registry[0].fingerprint.is_empty());
    assert!(
        signals.verification_bytes.load(Ordering::Acquire) >= 3 * metadata_bytes as u64,
        "real worker counts source, stage and final destination reads while a neighbor progresses"
    );
    let old = signals.roots.lock().unwrap()[&root].live.clone();
    let displaced = root.with_extension("displaced");
    std::fs::rename(&root, &displaced).unwrap();
    std::fs::create_dir(&root).unwrap();
    signals.event(Ok(Event::new(EventKind::Modify(
        notify::event::ModifyKind::Name(notify::event::RenameMode::Both),
    ))
    .add_path(root.clone())
    .add_path(displaced)));
    tokio::time::timeout(Duration::from_secs(3), async {
        while old.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("external root replacement still fences");
    stop.cancel();
    worker.await.unwrap();
}

#[test]
fn local_native_registration_failures_keep_a_and_c_and_retry_only_b() {
    for point in ["metadata", "read_dir", "watch"] {
        let directory = tempfile::tempdir().unwrap();
        let root = fs_canonical(directory.path());
        for name in ["a", "b", "c"] {
            std::fs::create_dir(root.join(name)).unwrap();
        }
        std::fs::create_dir(root.join("b/old-child")).unwrap();
        let b = root.join("b");
        let signals = Arc::new(Signals::default());
        signals
            .native_events_disabled
            .store(true, Ordering::Release);
        signals
            .roots
            .lock()
            .unwrap()
            .insert(root.clone(), Dirty::new(1, Instant::now()));
        let mut native = Native::new();
        native.recursive = false;
        // Establish a prior full proof, including B's child, before a local failure.
        for _ in 0..10 {
            native = native.refresh(vec![root.clone()], BTreeSet::new(), signals.clone());
        }
        signals
            .registration_faults
            .lock()
            .unwrap()
            .insert((b.clone(), point));
        for _ in 0..10 {
            native = native.refresh(
                vec![root.clone()],
                BTreeSet::from([root.clone()]),
                signals.clone(),
            );
            if native.plans[&root].attempts != 0 {
                break;
            }
        }
        assert!(native.watched[&root.join("a")].active);
        assert!(native.watched[&root.join("c")].active);
        assert!(
            native.watched.contains_key(&root.join("b/old-child")),
            "partial walk cannot prune old proof"
        );
        assert_eq!(native.observation(&root), Observation::Degraded);
        assert!(native.plans[&root].failed_paths.contains_key(&b));
        let calls = native.watch_calls.clone();
        for _ in 0..3 {
            native = native.refresh(vec![root.clone()], BTreeSet::new(), signals.clone());
        }
        assert_eq!(
            native.watch_calls, calls,
            "backoff prevents immediate retries"
        );
        native.plans.get_mut(&root).unwrap().retry_at = Instant::now();
        for _ in 0..10 {
            native = native.refresh(vec![root.clone()], BTreeSet::new(), signals.clone());
            if native.plans[&root].walk.is_none() {
                break;
            }
        }
        assert_eq!(
            native.watch_calls.get(&root.join("a")),
            calls.get(&root.join("a"))
        );
        assert_eq!(
            native.watch_calls.get(&root.join("c")),
            calls.get(&root.join("c"))
        );
        assert!(native.plans[&root].attempts >= 2);
        signals.registration_faults.lock().unwrap().clear();
        native.plans.get_mut(&root).unwrap().retry_at = Instant::now();
        for _ in 0..10 {
            native = native.refresh(vec![root.clone()], BTreeSet::new(), signals.clone());
        }
        assert_eq!(native.observation(&root), Observation::Subscribed);
        assert!(native.watched[&b].active);
        assert!(
            signals.roots.lock().unwrap()[&root]
                .rescan_generation
                .is_some()
        );
    }
}

#[tokio::test]
async fn registration_unwind_keeps_the_worker_alive_with_backoff_and_shutdown() {
    let harness = harness().await;
    let signals = Arc::new(Signals::default());
    signals
        .native_events_disabled
        .store(true, Ordering::Release);
    signals
        .directory_registration
        .store(true, Ordering::Release);
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    settled(&signals).await;
    let before = signals.backend_attempts.load(Ordering::Acquire);
    signals
        .registration_panic_once
        .store(true, Ordering::Release);
    let root = signals.roots.lock().unwrap().keys().next().unwrap().clone();
    let old = signals.roots.lock().unwrap()[&root].live.clone();
    signals.event(Ok(Event::new(EventKind::Create(
        notify::event::CreateKind::Folder,
    ))
    .add_path(root.clone())));
    tokio::time::timeout(Duration::from_secs(3), async {
        while old.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!worker.is_finished());
    assert_eq!(
        signals.backend_attempts.load(Ordering::Acquire),
        before,
        "unwind does not retry immediately"
    );
    tokio::time::timeout(Duration::from_secs(12), async {
        while signals.backend_creations.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    settled(&signals).await;
    let attempts = signals.backend_attempts.load(Ordering::Acquire);
    let rounds = signals.root_rounds.load(Ordering::Acquire);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(signals.backend_attempts.load(Ordering::Acquire), attempts);
    assert_eq!(signals.root_rounds.load(Ordering::Acquire), rounds);
    stop.cancel();
    worker.await.unwrap();
}

#[tokio::test]
async fn local_registration_recovery_fences_a_degraded_job_before_its_old_ack() {
    let harness = harness().await;
    let source = harness.directory.path().join("source");
    for name in ["a", "b", "c"] {
        std::fs::create_dir_all(source.join(name)).unwrap();
    }
    for name in ["a", "c"] {
        std::fs::write(
            source.join(name).join("SKILL.md"),
            format!("---\nname: {name}\n---\nNeighbor"),
        )
        .unwrap();
    }
    let root = fs_canonical(&source);
    let signals = Arc::new(Signals::default());
    signals
        .native_events_disabled
        .store(true, Ordering::Release);
    signals
        .directory_registration
        .store(true, Ordering::Release);
    signals
        .registration_faults
        .lock()
        .unwrap()
        .insert((root.join("b"), "metadata"));
    *signals.pause_root.lock().unwrap() = Some(root.clone());
    let stop = CancellationToken::new();
    let worker = tokio::spawn(run_with_signals(
        harness.processor.clone(),
        stop.clone(),
        signals.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
        .await
        .unwrap();
    let old = signals.roots.lock().unwrap()[&root].live.clone();
    signals.registration_faults.lock().unwrap().clear();
    // The blocked test barrier lets the registration retry deadline elapse.
    tokio::time::sleep(Duration::from_secs(6)).await;
    signals.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while old.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("restored B cannot allow an old degraded cursor to ACK");
    settled(&signals).await;
    let rows = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 64)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| !row.fingerprint.is_empty()));
    stop.cancel();
    worker.await.unwrap();
}

#[test]
fn callback_reservation_lookups_follow_path_depth_instead_of_completed_packages() {
    for reservations in [16, 20_000] {
        let root = PathBuf::from("/reserved");
        let signals = Signals::default();
        signals
            .directory_registration
            .store(true, Ordering::Release);
        let mut dirty = Dirty::new(1, Instant::now());
        dirty.watch_dirty = false;
        dirty.watch_fence = false;
        signals.roots.lock().unwrap().insert(root.clone(), dirty);
        let live = signals.roots.lock().unwrap()[&root].live.clone();
        let lease = signals.new_claim_owner(&root, live);
        for index in 0..reservations {
            signals.claim_path(&lease, &root.join(format!("completed-{index}")), true);
        }
        let exact = root.join("destination");
        let subtree = root.join("owned-wrapper");
        signals.claim_path(&lease, &exact, false);
        signals.claim_path(&lease, &subtree, true);
        for (path, fenced) in [
            (exact.clone(), false),
            (subtree.join("payload/assets"), false),
            (exact.join("external-child"), true),
            (root.join("external-folder"), true),
            (root.clone(), true),
            (PathBuf::from("/"), true),
        ] {
            signals.owned_lookups.store(0, Ordering::Relaxed);
            let before = {
                let mut roots = signals.roots.lock().unwrap();
                let dirty = roots.get_mut(&root).unwrap();
                dirty.watch_fence = false;
                dirty.watch_dirty = false;
                dirty.generation
            };
            signals.event(Ok(Event::new(EventKind::Create(
                notify::event::CreateKind::Folder,
            ))
            .add_path(path.clone())));
            let roots = signals.roots.lock().unwrap();
            assert_eq!(roots[&root].watch_fence, fenced, "{path:?}");
            assert!(roots[&root].watch_dirty);
            assert_eq!(
                roots[&root].generation,
                before + 1,
                "own proof never suppresses dirty events"
            );
            let lookups = signals.owned_lookups.load(Ordering::Relaxed);
            assert!(lookups <= path.ancestors().count() as u64);
            if path == exact {
                assert_eq!(lookups, 1);
            }
            if path == subtree.join("payload/assets") {
                assert_eq!(lookups, 3);
            }
            if path == root.join("external-folder") {
                assert_eq!(lookups, path.ancestors().count() as u64);
            }
        }
    }
}

fn reservation_job(signals: &Arc<Signals>, root: &Path) -> Job {
    let live = signals
        .roots
        .lock()
        .unwrap()
        .get(root)
        .map(|dirty| dirty.live.clone())
        .unwrap_or_else(|| Arc::new(AtomicBool::new(true)));
    Job {
        signals: signals.clone(),
        claims: signals.new_claim_owner(root, live),
        incarnation: 1,
        generation: 1,
        stream: Box::pin(futures_util::stream::empty()),
        failed: false,
        resume_at: Instant::now(),
    }
}
fn cleanup_work(signals: &Signals) -> u64 {
    signals.claim_cleanup_records.load(Ordering::Acquire)
        + signals.claim_cleanup_owners.load(Ordering::Acquire)
}
fn reservation_event(signals: &Signals, root: &Path, path: &Path, fenced: bool) {
    let before = {
        let mut state = signals.roots.lock().unwrap();
        let dirty = state.get_mut(root).unwrap();
        dirty.watch_fence = false;
        dirty.watch_dirty = false;
        dirty.generation
    };
    signals.event(Ok(Event::new(EventKind::Create(
        notify::event::CreateKind::Folder,
    ))
    .add_path(path.to_path_buf())));
    let state = signals.roots.lock().unwrap();
    assert_eq!(state[root].watch_fence, fenced, "{path:?}");
    assert!(state[root].watch_dirty);
    assert_eq!(state[root].generation, before + 1);
}

#[test]
fn active_reservations_are_not_rescanned_and_retired_records_are_removed_in_quanta() {
    let signals = Arc::new(Signals::default());
    let root = PathBuf::from("/large-claims");
    let job = reservation_job(&signals, &root);
    for index in 0..20_000 {
        signals.claim_path(&job.claims, &root.join(format!("path-{index:05}")), true);
    }
    for _ in 0..128 {
        assert!(!signals.cleanup_claims());
    }
    assert_eq!(cleanup_work(&signals), 0, "no job ended: no index walk");
    let lease = job.claims.clone();
    // Job destruction can happen inside the roots lock in watch_fence/snapshot.
    let lock = signals.roots.lock().unwrap();
    drop(job);
    assert!(!lease.live.load(Ordering::Acquire));
    drop(lock);
    let mut quanta = 0;
    loop {
        let before = cleanup_work(&signals);
        let pending = signals.cleanup_claims();
        assert!(cleanup_work(&signals) - before <= CLAIM_CLEANUP_QUANTUM as u64);
        quanta += 1;
        if !pending {
            break;
        }
    }
    assert!(quanta > 300);
    assert_eq!(
        signals.claim_cleanup_records.load(Ordering::Acquire),
        20_000
    );
    let state = signals.roots.lock().unwrap();
    assert!(state.owners.is_empty());
    assert!(state.owned_paths.is_empty());
}

#[test]
fn retirement_during_cleanup_keeps_the_cursor_and_revisits_earlier_owners() {
    let signals = Arc::new(Signals::default());
    let mut prefix = (0..100)
        .map(|i| reservation_job(&signals, &PathBuf::from(format!("/active-{i}"))))
        .collect::<Vec<_>>();
    let retired = reservation_job(&signals, Path::new("/retired"));
    for i in 0..256 {
        signals.claim_path(
            &retired.claims,
            &PathBuf::from(format!("/retired/{i}")),
            true,
        );
    }
    drop(retired);
    assert!(signals.cleanup_claims());
    assert_eq!(signals.claim_cleanup_records.load(Ordering::Acquire), 0);
    let early = prefix.remove(0);
    let early_id = early.claims.id;
    drop(early);
    let before = cleanup_work(&signals);
    assert!(signals.cleanup_claims());
    assert!(cleanup_work(&signals) - before <= CLAIM_CLEANUP_QUANTUM as u64);
    assert!(
        signals.claim_cleanup_records.load(Ordering::Acquire) > 0,
        "new retirement must not reset the cursor to the active prefix"
    );
    while signals.cleanup_claims() {}
    assert!(
        !signals.roots.lock().unwrap().owners.contains_key(&early_id),
        "retirement behind the cursor is visited on the next pass"
    );
    assert_eq!(signals.claim_cleanup_records.load(Ordering::Acquire), 256);
    drop(prefix);
    while signals.cleanup_claims() {}
    assert!(signals.roots.lock().unwrap().owners.is_empty());
}

#[test]
fn same_root_replacement_and_overlapping_owners_survive_old_claim_cleanup() {
    let signals = Arc::new(Signals::default());
    signals
        .directory_registration
        .store(true, Ordering::Release);
    let root = PathBuf::from("/overlap");
    let nested = root.join("nested");
    {
        let mut state = signals.roots.lock().unwrap();
        state.insert(root.clone(), Dirty::new(1, Instant::now()));
        state.insert(nested.clone(), Dirty::new(2, Instant::now()));
    }
    let old = reservation_job(&signals, &root);
    let other = reservation_job(&signals, &nested);
    let shared = nested.join("shared");
    let exact = root.join("new-exact");
    let abandoned = root.join("abandoned");
    signals.claim_path(&old.claims, &shared, true);
    signals.claim_path(&other.claims, &shared, true);
    signals.claim_path(&old.claims, &exact, true);
    signals.claim_path(&old.claims, &abandoned, true);
    for i in 0..512 {
        signals.claim_path(&old.claims, &root.join(format!("a-{i:04}")), true);
    }
    let old_lease = old.claims.clone();
    drop(old);
    let replacement = reservation_job(&signals, &root);
    assert_ne!(old_lease.id, replacement.claims.id);
    signals.claim_path(&replacement.claims, &exact, false);
    // An outliving closure cannot reactivate ownership of the retired job.
    signals.claim_path(&old_lease, &abandoned, true);
    assert!(signals.cleanup_claims());
    assert!(
        signals
            .roots
            .lock()
            .unwrap()
            .owners
            .contains_key(&old_lease.id)
    );
    reservation_event(&signals, &root, &abandoned.join("external"), true);
    reservation_event(&signals, &root, &exact, false);
    reservation_event(&signals, &root, &exact.join("external"), true);
    reservation_event(&signals, &root, &shared.join("payload"), false);
    reservation_event(&signals, &nested, &shared.join("payload"), false);
    while signals.cleanup_claims() {}
    reservation_event(&signals, &root, &exact, false);
    reservation_event(&signals, &nested, &shared.join("payload"), false);
    let state = signals.roots.lock().unwrap();
    assert!(!state.owners.contains_key(&old_lease.id));
    assert!(Arc::ptr_eq(
        &state.owned_paths[&exact][&root].lease,
        &replacement.claims
    ));
    assert!(Arc::ptr_eq(
        &state.owned_paths[&shared][&nested].lease,
        &other.claims
    ));
    drop(state);
    // Registration recovery can fence a suspended job before it is dropped.
    signals.roots.lock().unwrap()[&root]
        .live
        .store(false, Ordering::Release);
    assert!(replacement.claims.live.load(Ordering::Acquire));
    assert!(!replacement.claims.valid());
    reservation_event(&signals, &root, &exact, true);
    reservation_event(&signals, &nested, &shared.join("payload"), false);
    // Even a reservation cannot suppress replacement of a root/sentinel.
    reservation_event(&signals, &root, &root, true);
    reservation_event(&signals, &nested, &root, true);
}

#[test]
fn every_job_map_removal_revokes_ownership_before_physical_cleanup() {
    for removal in ["remove", "retain", "clear", "shutdown"] {
        let signals = Arc::new(Signals::default());
        let root = PathBuf::from("/job-lifecycle");
        let job = reservation_job(&signals, &root);
        for i in 0..256 {
            signals.claim_path(&job.claims, &root.join(format!("claim-{i}")), true);
        }
        let lease = job.claims.clone();
        let mut jobs = BTreeMap::from([(root.clone(), job)]);
        let lock = signals.roots.lock().unwrap();
        match removal {
            "retain" => jobs.retain(|_, _| false),
            "clear" => jobs.clear(),
            "shutdown" => {
                signals.retire_claims(&jobs[&root].claims);
                assert!(!lease.live.load(Ordering::Acquire));
                jobs.clear();
            }
            _ => {
                jobs.remove(&root);
            }
        }
        assert!(!lease.live.load(Ordering::Acquire), "{removal}");
        assert_eq!(cleanup_work(&signals), 0, "{removal}: no Drop walk");
        drop(lock);
        let before = cleanup_work(&signals);
        assert!(signals.cleanup_claims());
        assert!(cleanup_work(&signals) - before <= CLAIM_CLEANUP_QUANTUM as u64);
        assert!(signals.roots.lock().unwrap().owners.contains_key(&lease.id));
    }
}

#[tokio::test]
async fn real_worker_keeps_active_claims_and_cleans_retired_owners_while_neighbor_advances() {
    for cancel in [false, true] {
        let harness = harness_with_layout(false, true).await;
        let root = harness.directory.path().join("managed/ws/user");
        let source = root.join("unregistered");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(
            source.join("SKILL.md"),
            "---\nname: Lease lifecycle\n---\nImport",
        )
        .unwrap();
        std::fs::write(source.join("large.asset"), vec![b'a'; 800 * 1024]).unwrap();
        let root = fs_canonical(&root);
        let signals = Arc::new(Signals::default());
        signals
            .directory_registration
            .store(true, Ordering::Release);
        signals
            .native_events_disabled
            .store(true, Ordering::Release);
        *signals.pause_stage.lock().unwrap() = Some(root.clone());
        let stop = CancellationToken::new();
        let worker = tokio::spawn(run_with_signals(
            harness.processor.clone(),
            stop.clone(),
            signals.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
            .await
            .unwrap();
        let lease = signals
            .roots
            .lock()
            .unwrap()
            .owners
            .values()
            .find(|owner| owner.lease.root == root && owner.lease.live.load(Ordering::Acquire))
            .unwrap()
            .lease
            .clone();
        let prepared_guard = signals.roots.lock().unwrap()[&root].live.clone();
        for i in 0..20_000 {
            signals.claim_path(&lease, &root.join(format!("reserved-{i:05}")), true);
        }
        let records = signals.claim_cleanup_records.load(Ordering::Acquire);
        for _ in 0..3 {
            *signals.pause_root.lock().unwrap() = Some(root.clone());
            signals.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
                .await
                .unwrap();
            assert!(lease.live.load(Ordering::Acquire));
            assert_eq!(
                signals.claim_cleanup_records.load(Ordering::Acquire),
                records,
                "real useful worker quanta never rescan an active owner's paths"
            );
        }
        signals.pause_claim_cleanup.store(true, Ordering::Release);
        if cancel {
            signals.event(Ok(Event::new(EventKind::Modify(
                notify::event::ModifyKind::Name(notify::event::RenameMode::Both),
            ))
            .add_path(root.clone())
            .add_path(root.with_extension("external-replacement"))));
        }
        signals.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
            .await
            .unwrap();
        assert!(!lease.live.load(Ordering::Acquire));
        if cancel {
            assert!(
                !prepared_guard.load(Ordering::Acquire),
                "external root replacement fences prepared publication as well as ownership"
            );
        }
        assert!(
            signals.claim_cleanup_records.load(Ordering::Acquire) - records
                <= CLAIM_CLEANUP_QUANTUM as u64
        );
        assert!(signals.roots.lock().unwrap().owners.contains_key(&lease.id));
        reservation_event(&signals, &root, &root.join("reserved-19999/external"), true);
        let neighbor = harness.directory.path().join("neighbor");
        let package = neighbor.join("pkg");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(
            package.join("SKILL.md"),
            "---\nname: Cleanup neighbor\n---\nAvailable",
        )
        .unwrap();
        let neighbor = fs_canonical(&neighbor);
        signals.event(Ok(Event::new(EventKind::Create(
            notify::event::CreateKind::Folder,
        ))
        .add_path(neighbor.clone())));
        let mut progressed = false;
        for _ in 0..200 {
            // The worker is held at the cleanup barrier. Let its actual dirty
            // or installer-lock wait expire without draining more reservations.
            // Counting quanta alone cannot advance debounce/retry deadlines.
            let job_deadline = signals
                .paused_job_deadlines
                .lock()
                .unwrap()
                .get(&neighbor)
                .copied();
            let deadline = job_deadline.or_else(|| signals.roots.lock().unwrap()[&neighbor].due());
            if let Some(deadline) = deadline {
                tokio::time::sleep_until(deadline.into()).await;
            }
            let before = cleanup_work(&signals);
            signals.pause_claim_cleanup.store(true, Ordering::Release);
            signals.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(10), signals.paused.notified())
                .await
                .unwrap();
            assert!(cleanup_work(&signals) - before <= CLAIM_CLEANUP_QUANTUM as u64);
            let rows = harness
                .processor
                .crud_store
                .list_skill_installations_scope_page("registry", "ws", None, 64)
                .await
                .unwrap();
            if rows.iter().any(|row| !row.fingerprint.is_empty()) {
                assert!(
                    signals.roots.lock().unwrap().owners.contains_key(&lease.id),
                    "neighbor publishes before the large retired owner is drained"
                );
                progressed = true;
                break;
            }
        }
        assert!(progressed);
        stop.cancel();
        worker.await.unwrap();
        let state = signals.roots.lock().unwrap();
        assert!(state.owners.is_empty());
        assert!(state.owned_paths.is_empty());
        assert!(!lease.live.load(Ordering::Acquire));
    }
}
