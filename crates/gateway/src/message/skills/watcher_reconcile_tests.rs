use super::super::tests::{Harness, harness};
use super::super::{Dirty, Signals, next_root};
use super::*;
use sea_orm::{ActiveModelTrait, EntityTrait, Set};

fn package(path: &Path, text: &str) {
    fs::create_dir_all(path.join("assets")).unwrap();
    fs::write(
        path.join("SKILL.md"),
        format!("---\nname: Test\nslug: test-skill\n---\n{text}"),
    )
    .unwrap();
    fs::write(path.join("assets/value.txt"), text).unwrap();
}
fn config(harness: &Harness, source: &Path) -> ConfiguredRootImportConfig {
    let mut config = harness
        .processor
        .configured_root_import_config(&harness.workspace.id, false)
        .unwrap();
    config.roots[0].source_root = source.to_path_buf();
    config
}
fn fence(path: &Path) -> JobFence {
    let signals = Arc::new(Signals::default());
    signals
        .roots
        .lock()
        .unwrap()
        .insert(path.to_path_buf(), Dirty::new(1, std::time::Instant::now()));
    JobFence {
        signals,
        root: path.to_path_buf(),
        incarnation: 1,
        stop: tokio_util::sync::CancellationToken::new(),
    }
}
async fn consume(mut work: Work) -> (usize, usize) {
    let mut changed = 0;
    let mut failed = 0;
    while let Some(result) = work.next().await {
        match result.unwrap() {
            Progress::Changed(_) => changed += 1,
            Progress::Failed => failed += 1,
            Progress::Quantum => {}
        }
    }
    (changed, failed)
}
fn job(
    harness: &Harness,
    source: &Path,
    baseline: Arc<StdMutex<Baseline>>,
    guard: JobFence,
) -> Work {
    root_job(
        harness.processor.clone(),
        source.to_path_buf(),
        vec![Mapping::Import(
            config(harness, source),
            Some(harness.workspace.clone()),
        )],
        baseline,
        Arc::default(),
        guard,
    )
}

#[test]
fn enumeration_and_copy_resume_without_dropping_late_valid_packages() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    fs::create_dir(&source).unwrap();
    for index in 0..300 {
        fs::write(source.join(format!("irrelevant-{index}")), b"x").unwrap();
    }
    package(&source.join("z-package"), "late valid package");
    let mut discovery = Discovery::new(&source, 256, Arc::default()).unwrap();
    assert!(!discovery.step().unwrap());
    while !discovery.step().unwrap() {}
    assert_eq!(
        discovery.packages,
        BTreeSet::from([source.join("z-package")])
    );
    fs::write(
        source.join("z-package/assets/large.bin"),
        vec![9; BYTES * 3],
    )
    .unwrap();
    let target = temp.path().join("stage");
    fs::create_dir(&target).unwrap();
    let mut copy = Tree::new(
        source.join("z-package"),
        Some(target.clone()),
        BYTES * 4,
        false,
    )
    .unwrap();
    assert!(!copy.step().unwrap());
    let copied: u64 = fs::read_dir(target.join("assets"))
        .unwrap()
        .map(|entry| entry.unwrap().metadata().unwrap().len())
        .sum();
    assert!(copied <= BYTES as u64, "a copy quantum has a byte budget");
    while !copy.step().unwrap() {}
    let mut scan = Tree::new(target, None, BYTES * 4, false).unwrap();
    while !scan.step().unwrap() {}
    assert_eq!(copy.fingerprint(), scan.fingerprint());
}

#[test]
fn atomic_same_size_save_changes_content_digest_and_cleanup_is_bounded() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("skill");
    package(&source, "aaaa");
    let mut before = Tree::new(source.clone(), None, 4096, false).unwrap();
    while !before.step().unwrap() {}
    fs::write(source.join("assets/editor.tmp"), b"bbbb").unwrap();
    fs::rename(
        source.join("assets/editor.tmp"),
        source.join("assets/value.txt"),
    )
    .unwrap();
    let mut after = Tree::new(source.clone(), None, 4096, false).unwrap();
    while !after.step().unwrap() {}
    assert_ne!(before.fingerprint(), after.fingerprint());
    for index in 0..300 {
        fs::write(source.join(format!("file-{index}")), b"x").unwrap();
    }
    let mut removal = Removal::new(source.clone()).unwrap();
    assert!(!removal.step().unwrap());
    assert!(source.exists());
    while !removal.step().unwrap() {}
    assert!(!source.exists());
}

#[cfg(unix)]
#[test]
fn directory_and_file_symlinks_are_not_traversed_during_discovery_or_copy() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    fs::create_dir(&source).unwrap();
    let outside = temp.path().join("outside");
    package(&outside, "outside");
    symlink(&outside, source.join("escaped-package")).unwrap();
    let mut discovery = Discovery::new(&source, 256, Arc::default()).unwrap();
    while !discovery.step().unwrap() {}
    assert!(discovery.packages.is_empty());
    let inside = source.join("inside");
    package(&inside, "inside");
    symlink(outside.join("SKILL.md"), inside.join("assets/escape")).unwrap();
    let mut tree = Tree::new(inside, None, 4096, false).unwrap();
    let mut blocked = false;
    loop {
        match tree.step() {
            Ok(false) => {}
            Ok(true) => break,
            Err(_) => {
                blocked = true;
                break;
            }
        }
    }
    assert!(blocked);
    assert!(outside.join("SKILL.md").exists());
}

#[tokio::test]
async fn own_writes_and_failed_summary_retry_do_not_rewrite_successful_packages() {
    let harness = harness().await;
    let source = harness.directory.path().join("source");
    package(&source.join("good"), "good");
    package(&source.join("bad"), "bad");
    // Discovery succeeds, but domain validation fails for only this package.
    fs::write(
        source.join("bad/assets/oversize.bin"),
        vec![0; 1024 * 1024 + 1],
    )
    .unwrap();
    let baseline: Arc<StdMutex<Baseline>> = Arc::default();
    let (changed, failed) = consume(job(&harness, &source, baseline.clone(), fence(&source))).await;
    assert_eq!(
        (changed, failed),
        (1, 1),
        "Ok completion with failed package work must not count as success ACK"
    );
    let rows = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 64)
        .await
        .unwrap();
    let good = rows
        .iter()
        .find(|row| row.source_ref.ends_with("/good"))
        .unwrap()
        .clone();
    harness.observer.writes.lock().unwrap().clear();
    let (changed, failed) = consume(job(&harness, &source, baseline.clone(), fence(&source))).await;
    assert_eq!((changed, failed), (0, 1));
    assert!(
        harness.observer.writes.lock().unwrap().is_empty(),
        "own writes/no-op/retry must not amplify WAL writes"
    );
    assert_eq!(
        harness
            .processor
            .crud_store
            .find_skill_installation(&good.skill_id)
            .await
            .unwrap()
            .unwrap(),
        good
    );
    fs::remove_file(source.join("bad/assets/oversize.bin")).unwrap();
    assert_eq!(
        consume(job(&harness, &source, baseline, fence(&source))).await,
        (1, 0)
    );
    assert!(
        harness
            .observer
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|class| *class == pioneer_sqlite::SqliteReadClass::Maintenance)
    );
    assert!(
        harness
            .observer
            .writes
            .lock()
            .unwrap()
            .iter()
            .all(|class| *class == SqliteWriteClass::Maintenance)
    );
    assert!(
        harness
            .processor
            .crud_store
            .database_connection()
            .reader_query_only_enabled()
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn events_during_snapshot_and_scan_leave_another_generation_pending() {
    let harness = harness().await;
    let source = harness.directory.path().join("source");
    package(&source.join("skill"), "before");
    let guard = fence(&source);
    let signals = guard.signals.clone();
    let mut work = job(&harness, &source, Arc::default(), guard);
    assert!(matches!(
        work.next().await.unwrap().unwrap(),
        Progress::Quantum
    ));
    package(&source.join("skill"), "after");
    signals.event(Ok(
        notify::Event::new(notify::EventKind::Any).add_path(source.join("skill/SKILL.md"))
    ));
    let _ = consume(work).await;
    let mut roots = signals.roots.lock().unwrap();
    let dirty = roots.get_mut(&source).unwrap();
    dirty.finish(1, 1, true, std::time::Instant::now());
    assert!(dirty.generation > dirty.acknowledged);
}

#[tokio::test]
async fn a_workspace_commit_during_preparation_fences_the_old_job() {
    let harness = harness().await;
    let source = harness.directory.path().join("source");
    package(&source.join("skill"), "body");
    let mut work = job(&harness, &source, Arc::default(), fence(&source));
    let pending = loop {
        work.next().await.unwrap().unwrap();
        let page = harness
            .processor
            .crud_store
            .list_skill_reconciliation_page("user", "ws", None, 16)
            .await
            .unwrap();
        if let Some(row) = page.into_iter().next() {
            break row;
        }
    };
    let mut model: pioneer_entity::workspace::ActiveModel = harness.workspace.clone().into();
    model.is_active = Set(false);
    model
        .update(&harness.processor.crud_store.database_connection())
        .await
        .unwrap();
    let (_, failed) = consume(work).await;
    assert!(failed > 0);
    let row = harness
        .processor
        .crud_store
        .find_skill_installation(&pending.record.skill_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row, pending.record,
        "the inactive Workspace cannot receive a stale prepared projection"
    );
    assert!(
        !Path::new(&row.install_path)
            .join("skills-lock.toml")
            .exists()
    );
}

#[tokio::test]
async fn root_quanta_are_fair_under_a_hot_slow_root() {
    let harness = harness().await;
    let slow = harness.directory.path().join("slow");
    let fast = harness.directory.path().join("fast");
    package(&slow.join("skill"), "slow");
    package(&fast.join("skill"), "fast");
    for index in 0..1024 {
        fs::write(slow.join(format!("ignored-{index}")), b"x").unwrap();
    }
    let mut jobs = BTreeMap::from([
        (
            slow.clone(),
            job(&harness, &slow, Arc::default(), fence(&slow)),
        ),
        (
            fast.clone(),
            job(&harness, &fast, Arc::default(), fence(&fast)),
        ),
    ]);
    let mut last = None;
    let mut slow_changed = false;
    let mut fast_changed = false;
    while !jobs.is_empty() {
        let ready = jobs.keys().cloned().collect::<Vec<_>>();
        let path = next_root(&ready, last.as_ref()).unwrap().clone();
        last = Some(path.clone());
        match jobs.get_mut(&path).unwrap().next().await {
            Some(Ok(Progress::Changed(_))) if path == fast => {
                assert!(!slow_changed);
                fast_changed = true;
            }
            Some(Ok(Progress::Changed(_))) => slow_changed = true,
            Some(Err(error)) => panic!("root quantum failed: {error:#}"),
            None => {
                jobs.remove(&path);
            }
            _ => {}
        }
    }
    assert!(fast_changed && slow_changed);
}

#[tokio::test]
async fn scoped_pages_and_exact_guards_preserve_row_workspace_and_pack_facts() {
    let harness = harness().await;
    let store = &harness.processor.crud_store;
    harness
        .processor
        .workspace_manager
        .create_workspace("other", Some("Other"))
        .await
        .unwrap();
    let other = harness
        .processor
        .workspace_manager
        .active_page(None)
        .await
        .unwrap()
        .into_iter()
        .find(|workspace| workspace.id == "other")
        .unwrap();
    let mut row = SkillInstallationRecord {
        skill_id: SkillId::new("A".repeat(21)).unwrap(),
        owner: None,
        slug: "skill".into(),
        version: None,
        source_kind: "user".into(),
        scope_key: "ws".into(),
        source_ref: "import-path:/source".into(),
        install_path: "/source".into(),
        trust_level: "community".into(),
        fingerprint: "old".into(),
        updated_at_unix: 1700000000,
        pack_id: None,
        pack_member_key: None,
    };
    let mut illegal_import = row.clone();
    illegal_import.pack_member_key = Some("member".into());
    assert!(
        store
            .register_skill_import_pending(
                &illegal_import,
                Some(&harness.workspace),
                row.updated_at_unix
            )
            .await
            .is_err()
    );
    assert!(
        store
            .find_skill_installation(&row.skill_id)
            .await
            .unwrap()
            .is_none()
    );
    let expected = store
        .register_skill_import_pending(&row, Some(&harness.workspace), row.updated_at_unix)
        .await
        .unwrap()
        .unwrap();
    row.skill_id = SkillId::new("B".repeat(21)).unwrap();
    assert!(
        store
            .register_skill_import_pending(&row, Some(&harness.workspace), row.updated_at_unix)
            .await
            .unwrap()
            .is_none(),
        "duplicate provenance must be checked under the writer"
    );
    row.scope_key = "other".into();
    store
        .register_skill_import_pending(&row, Some(&other), row.updated_at_unix)
        .await
        .unwrap()
        .unwrap();
    let page = store
        .list_skill_reconciliation_page("user", "ws", None, 1024)
        .await
        .unwrap();
    assert_eq!(page, vec![expected.clone()]);
    assert!(
        store
            .list_skill_reconciliation_page(
                "user",
                "ws",
                Some(expected.record.skill_id.as_str()),
                64
            )
            .await
            .unwrap()
            .is_empty()
    );
    let patch = SkillInstallationPatch {
        fingerprint: Some("new".into()),
        ..Default::default()
    };
    let mut substituted = expected.clone();
    substituted.record.scope_key = "other".into();
    assert!(
        store
            .reconcile_skill_installation(&substituted, &patch, 1700000001, Some(&other), &|| true)
            .await
            .is_err()
    );
    assert!(
        !store
            .reconcile_skill_installation(
                &expected,
                &patch,
                1700000001,
                Some(&harness.workspace),
                &|| false
            )
            .await
            .unwrap()
    );
    let model = pioneer_entity::skill_installation::Entity::find_by_id(
        expected.record.skill_id.to_string(),
    )
    .one(&store.database_connection())
    .await
    .unwrap()
    .unwrap();
    let mut active: pioneer_entity::skill_installation::ActiveModel = model.clone().into();
    active.updated_at = Set(model.updated_at + chrono::Duration::nanoseconds(1));
    active.update(&store.database_connection()).await.unwrap();
    assert_eq!(
        store
            .find_skill_installation(&expected.record.skill_id)
            .await
            .unwrap()
            .unwrap()
            .updated_at_unix,
        expected.record.updated_at_unix
    );
    assert!(
        !store
            .reconcile_skill_installation(
                &expected,
                &patch,
                1700000001,
                Some(&harness.workspace),
                &|| true
            )
            .await
            .unwrap(),
        "second-resolution foreground records must not weaken the prepared CAS"
    );
    let fresh = store
        .list_skill_reconciliation_page("user", "ws", None, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let illegal = SkillInstallationPatch {
        fingerprint: Some("new".into()),
        pack_id: Some(None),
        ..Default::default()
    };
    assert!(
        store
            .reconcile_skill_installation(
                &fresh,
                &illegal,
                1700000001,
                Some(&harness.workspace),
                &|| true
            )
            .await
            .is_err()
    );
    assert_eq!(
        store
            .list_skill_reconciliation_page("user", "ws", None, 1)
            .await
            .unwrap(),
        vec![fresh],
        "failed domain writes roll back atomically"
    );
}

#[tokio::test]
async fn scope_reader_pages_are_capped_and_resume_after_the_last_id() {
    let harness = harness().await;
    let store = &harness.processor.crud_store;
    for index in 0..70 {
        let row = SkillInstallationRecord {
            skill_id: SkillId::new(format!("{index:021}")).unwrap(),
            owner: None,
            slug: "skill".into(),
            version: None,
            source_kind: "user".into(),
            scope_key: "ws".into(),
            source_ref: format!("import-path:/source/{index}"),
            install_path: format!("/source/{index}"),
            trust_level: "community".into(),
            fingerprint: "old".into(),
            updated_at_unix: 1700000000,
            pack_id: None,
            pack_member_key: None,
        };
        store
            .register_skill_import_pending(&row, Some(&harness.workspace), 1700000000)
            .await
            .unwrap()
            .unwrap();
    }
    harness.observer.reads.lock().unwrap().clear();
    let first = store
        .list_skill_reconciliation_page("user", "ws", None, 10000)
        .await
        .unwrap();
    assert_eq!(first.len(), 64);
    let last = first.last().unwrap().record.skill_id.as_str();
    let second = store
        .list_skill_reconciliation_page("user", "ws", Some(last), 10000)
        .await
        .unwrap();
    assert_eq!(second.len(), 6);
    assert!(second.iter().all(|row| row.record.skill_id.as_str() > last));
    assert!(
        store
            .list_skill_reconciliation_page("registry", "ws", None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_skill_reconciliation_page("user", "other", None, 64)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        harness
            .observer
            .reads
            .lock()
            .unwrap()
            .iter()
            .all(|class| *class == pioneer_sqlite::SqliteReadClass::Maintenance)
    );
}

#[cfg(unix)]
#[test]
fn explicit_root_aliases_do_not_import_owned_stages_or_hide_valid_dot_packages() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    let alias = temp.path().join("alias");
    fs::create_dir(&root).unwrap();
    symlink(&root, &alias).unwrap();
    let staged = alias.join("container/.pioneer-relocation-stage");
    package(&staged, "owned stage");
    package(&root.join("container/.valid-package"), "valid package");
    let physical = super::super::physical_root(&alias).unwrap();
    let stage = super::super::physical_root(&staged).unwrap();
    let attempts = Arc::new(StdMutex::new(BTreeMap::from([(stage, physical.clone())])));
    let mut discovery = Discovery::new(&physical, 256, attempts).unwrap();
    while !discovery.step().unwrap() {}
    assert_eq!(
        discovery.packages,
        BTreeSet::from([physical.join("container/.valid-package")])
    );
}

#[tokio::test]
async fn managed_leaf_normalization_replaces_destination_and_cleans_the_owned_backup() {
    let harness = harness().await;
    let mut config = harness.processor.managed_root_scan_config("ws").unwrap();
    config
        .roots
        .retain(|root| root.source_kind == SkillSourceKind::User);
    let root = config.roots[0].managed_root.clone();
    let id = SkillId::new("D".repeat(21)).unwrap();
    let source = root.join(id.as_str()).join("New Name");
    let destination = root.join(id.as_str()).join("new-name");
    package(&source, "new source");
    package(&destination, "old destination");
    let row = SkillInstallationRecord {
        skill_id: id.clone(),
        owner: None,
        slug: "new-name".into(),
        version: None,
        source_kind: "user".into(),
        scope_key: "ws".into(),
        source_ref: "managed-test".into(),
        install_path: source.display().to_string(),
        trust_level: "community".into(),
        fingerprint: "old".into(),
        updated_at_unix: 1700000000,
        pack_id: None,
        pack_member_key: None,
    };
    harness
        .processor
        .crud_store
        .register_skill_import_pending(&row, Some(&harness.workspace), 1700000000)
        .await
        .unwrap()
        .unwrap();
    let physical = super::super::physical_root(&root).unwrap();
    let attempts: Attempts = Arc::default();
    let work = root_job(
        harness.processor.clone(),
        physical.clone(),
        vec![Mapping::Managed(config, Some(harness.workspace.clone()))],
        Arc::default(),
        attempts.clone(),
        fence(&physical),
    );
    assert_eq!(consume(work).await, (1, 0));
    assert!(!source.exists());
    assert_eq!(
        fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
        "new source"
    );
    assert!(attempts.lock().unwrap().is_empty());
    assert_eq!(
        fs::read_dir(destination.parent().unwrap()).unwrap().count(),
        1
    );
    assert_eq!(
        harness
            .processor
            .crud_store
            .find_skill_installation(&id)
            .await
            .unwrap()
            .unwrap()
            .install_path,
        destination.display().to_string()
    );
}
