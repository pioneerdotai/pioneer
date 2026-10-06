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
    let attempts = Arc::new(StdMutex::new(BTreeMap::from([(
        stage,
        Attempt::new(physical.clone()),
    )])));
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

#[tokio::test]
async fn shared_source_bytes_and_preparation_are_read_once_for_matching_scope_policies() {
    let harness = harness().await;
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
        .find(|w| w.id == "other")
        .unwrap();
    let source = harness.directory.path().join("source");
    package(&source.join("pkg"), "shared bytes");
    let first = config(&harness, &source);
    let mut second = harness
        .processor
        .configured_root_import_config("other", false)
        .unwrap();
    second.roots[0].source_root = source.clone();
    let mut registry = first.clone();
    registry.roots[0].source_kind = SkillSourceKind::Registry;
    registry.roots[0].managed_root = harness
        .processor
        .managed_root_scan_config("ws")
        .unwrap()
        .roots
        .into_iter()
        .find(|root| root.source_kind == SkillSourceKind::Registry)
        .unwrap()
        .managed_root;
    let guard = fence(&source);
    let counters = guard.signals.clone();
    assert_eq!(
        consume(root_job(
            harness.processor.clone(),
            source.clone(),
            vec![
                Mapping::Import(first.clone(), Some(harness.workspace.clone())),
                Mapping::Import(second.clone(), Some(other.clone())),
                Mapping::Import(registry, Some(harness.workspace.clone()))
            ],
            Arc::default(),
            Arc::default(),
            guard
        ))
        .await,
        (3, 0)
    );
    assert_eq!(
        counters
            .preparations
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        counters.hashes.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        counters
            .source_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        2,
        "only source hashing reads, destination copies remain separate"
    );
    let user = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 64)
        .await
        .unwrap();
    let other_rows = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "other", None, 64)
        .await
        .unwrap();
    assert_ne!(user[0].skill_id, other_rows[0].skill_id);
    assert_ne!(user[0].install_path, other_rows[0].install_path);
    // An incompatible policy cannot borrow a successful security decision.
    second.installer_policy.security.max_install_file_bytes = 8;
    assert_eq!(
        consume(root_job(
            harness.processor.clone(),
            source.clone(),
            vec![
                Mapping::Import(first, Some(harness.workspace.clone())),
                Mapping::Import(second, Some(other))
            ],
            Arc::default(),
            Arc::default(),
            fence(&source)
        ))
        .await
        .1,
        1
    );
}

#[tokio::test]
async fn fresh_worker_recognizes_owned_staging_and_retains_uncertain_backup_and_dot_packages() {
    let harness = harness().await;
    let root = harness.directory.path().join("overlap");
    let container = root.join("container");
    fs::create_dir_all(&container).unwrap();
    let staging = new_attempt(&container, root.clone()).unwrap();
    package(&staging.join("payload"), "unpublished stage");
    let backup = new_attempt(&container, root.clone()).unwrap();
    package(&backup.join("backup"), "last necessary copy");
    set_attempt_publishing(&backup, true).unwrap();
    package(
        &container.join(".valid-dot-package"),
        "legitimate hidden skill",
    );
    let tracked: Attempts = Arc::default(); // no inherited in-memory ownership
    let mut discovery = Discovery::new(&root, 50, tracked.clone()).unwrap();
    let mut steps = 0;
    while !discovery.step().unwrap() {
        steps += 1;
    }
    assert!(steps < 10);
    assert_eq!(
        discovery.packages,
        BTreeSet::from([container.join(".valid-dot-package")])
    );
    assert_eq!(tracked.lock().unwrap().len(), 2);
    assert_eq!(
        consume(cleanup(tracked.clone(), fence(&root))).await,
        (0, 0)
    );
    assert!(!staging.exists());
    assert_eq!(
        fs::read_to_string(backup.join("backup/assets/value.txt")).unwrap(),
        "last necessary copy"
    );
    assert!(container.join(".valid-dot-package/SKILL.md").exists());
    // A fresh worker with an overlapping configured System source has exactly
    // the same marker exclusion, even though source_is_pioneer_managed is false.
    let again: Attempts = Arc::default();
    let mut discovery = Discovery::new(&root, 50, again.clone()).unwrap();
    while !discovery.step().unwrap() {}
    assert!(!discovery.packages.contains(&backup));
    assert_eq!(again.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn baseline_prunes_current_scope_after_success_and_removed_mapping_but_not_failed_round() {
    let harness = harness().await;
    let mut config = harness.processor.managed_root_scan_config("ws").unwrap();
    config
        .roots
        .retain(|root| root.source_kind == SkillSourceKind::User);
    let root = config.roots[0].managed_root.clone();
    fs::create_dir_all(&root).unwrap();
    let baseline: Arc<StdMutex<Baseline>> = Arc::default();
    let row = SkillInstallationRecord {
        skill_id: SkillId::new("Q".repeat(21)).unwrap(),
        owner: None,
        slug: "missing".into(),
        version: None,
        source_kind: "user".into(),
        scope_key: "ws".into(),
        source_ref: "test-prune".into(),
        install_path: root
            .join("Q".repeat(21))
            .join("missing")
            .display()
            .to_string(),
        trust_level: "community".into(),
        fingerprint: "old".into(),
        updated_at_unix: 1700000000,
        pack_id: None,
        pack_member_key: None,
    };
    changed_availability(&baseline, &row, None);
    let bad = harness.directory.path().join("bad-baseline-root");
    fs::write(&bad, b"not a directory").unwrap();
    let failed = root_job(
        harness.processor.clone(),
        bad.clone(),
        vec![Mapping::Managed(
            config.clone(),
            Some(harness.workspace.clone()),
        )],
        baseline.clone(),
        Arc::default(),
        fence(&bad),
    );
    let mut failed = failed;
    let mut error = false;
    while let Some(progress) = failed.next().await {
        if progress.is_err() {
            error = true;
            break;
        }
    }
    assert!(error);
    assert_eq!(baseline.lock().unwrap().managed.len(), 1);
    for _ in 0..5 {
        harness
            .processor
            .crud_store
            .insert_skill_installation(&row, row.updated_at_unix)
            .await
            .unwrap();
        consume(root_job(
            harness.processor.clone(),
            root.clone(),
            vec![Mapping::Managed(
                config.clone(),
                Some(harness.workspace.clone()),
            )],
            baseline.clone(),
            Arc::default(),
            fence(&root),
        ))
        .await;
        assert_eq!(baseline.lock().unwrap().managed.len(), 1);
        assert!(
            harness
                .processor
                .crud_store
                .delete_skill_installation(&row.skill_id)
                .await
                .unwrap()
        );
        assert_eq!(
            consume(root_job(
                harness.processor.clone(),
                root.clone(),
                vec![Mapping::Managed(
                    config.clone(),
                    Some(harness.workspace.clone())
                )],
                baseline.clone(),
                Arc::default(),
                fence(&root)
            ))
            .await
            .1,
            0
        );
        assert!(baseline.lock().unwrap().managed.is_empty());
        changed_availability(&baseline, &row, None);
    }
    consume(root_job(
        harness.processor.clone(),
        root.clone(),
        Vec::new(),
        baseline.clone(),
        Arc::default(),
        fence(&root),
    ))
    .await;
    assert!(
        baseline.lock().unwrap().managed.is_empty(),
        "removed mapping cannot retain historical records"
    );
}

async fn publication_fixture(
    harness: &Harness,
) -> (
    SkillStorageRelocationCandidate,
    pioneer_crud::SkillReconciliationSnapshot,
    PathBuf,
    PathBuf,
) {
    let root = config(harness, &harness.directory.path().join("source")).roots[0]
        .managed_root
        .clone();
    let id = SkillId::new("Z".repeat(21)).unwrap();
    let source = harness.directory.path().join("source/pkg");
    package(&source, "new publication");
    let destination = root.join(id.as_str()).join("test-skill");
    package(&destination, "old destination");
    let prepared = pioneer_skills::prepare_materialized_skill(PrepareMaterializedSkillRequest {
        skill_id: id.clone(),
        source_kind: SkillSourceKind::User,
        source_ref: storage::import_source_ref(&source).unwrap(),
        materialized_source_path: source.clone(),
        policy: config(harness, &source).installer_policy,
    })
    .unwrap();
    let row = SkillInstallationRecord {
        skill_id: id,
        owner: None,
        slug: "test-skill".into(),
        version: None,
        source_kind: "user".into(),
        scope_key: "ws".into(),
        source_ref: prepared.source_ref.clone(),
        install_path: destination.display().to_string(),
        trust_level: "community".into(),
        fingerprint: "old".into(),
        updated_at_unix: 1700000000,
        pack_id: None,
        pack_member_key: None,
    };
    let snapshot = harness
        .processor
        .crud_store
        .register_skill_import_pending(&row, Some(&harness.workspace), 1700000000)
        .await
        .unwrap()
        .unwrap();
    let wrapper = new_attempt(
        destination.parent().unwrap(),
        source.parent().unwrap().to_path_buf(),
    )
    .unwrap();
    let stage = wrapper.join("payload");
    fs::create_dir(&stage).unwrap();
    let mut copy = Tree::new(source.clone(), Some(stage.clone()), 1024 * 1024, false).unwrap();
    while !copy.step().unwrap() {}
    let candidate = SkillStorageRelocationCandidate {
        expected_row: row,
        source_path: source,
        install_root: root.clone(),
        destination,
        prepared_metadata: metadata(&prepared, "test-skill".into()),
        remove_managed_source_after_switch: false,
        managed_path_to_remove_after_switch: None,
        managed_lock_path: Some(root.join("skills-lock.toml")),
        max_skill_file_bytes: 1024 * 1024,
    };
    (candidate, snapshot, stage, wrapper)
}
struct PublicationFaultReset;
impl Drop for PublicationFaultReset {
    fn drop(&mut self) {
        storage::WATCH_PUBLICATION_FAULT.with(|fault| fault.set(None));
    }
}

#[tokio::test]
async fn cancellation_before_first_db_poll_restores_files_and_lock_and_has_no_commit() {
    let harness = harness().await;
    let (candidate, snapshot, stage, wrapper) = publication_fixture(&harness).await;
    let destination = candidate.destination.clone();
    let lock = candidate.managed_lock_path.clone().unwrap();
    let stop = tokio_util::sync::CancellationToken::new();
    let cancel = stop.clone();
    let error = storage::publish_watched_candidate(
        &harness.processor.crud_store,
        &harness.processor.skills_write_lock,
        candidate,
        snapshot.clone(),
        Some(harness.workspace.clone()),
        Some(stage.clone()),
        None,
        stop,
        || true,
        move |_, _| {
            cancel.cancel();
            Ok(())
        },
    )
    .await
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<pioneer_crud::SkillReconciliationError>()
            .unwrap()
            .outcome,
        pioneer_crud::SkillReconciliationFailure::NotCommitted
    );
    assert_eq!(
        fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
        "old destination"
    );
    assert!(stage.join("SKILL.md").exists());
    assert!(
        pioneer_skills::read_skills_lock(&lock)
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(!attempt_marker(&wrapper).unwrap().unwrap().publishing);
    assert_eq!(
        harness
            .processor
            .crud_store
            .list_skill_reconciliation_page("user", "ws", None, 1)
            .await
            .unwrap(),
        vec![snapshot]
    );
}

#[tokio::test]
async fn confirmed_mutation_rollback_restores_publication_while_lost_commit_ack_retains_backup() {
    use sea_orm::ConnectionTrait;
    let harness = harness().await;
    let (candidate, snapshot, stage, wrapper) = publication_fixture(&harness).await;
    let destination = candidate.destination.clone();
    harness.processor.crud_store.database_connection().execute_unprepared("CREATE TRIGGER reject_watched_update BEFORE UPDATE ON skill_installation BEGIN SELECT RAISE(ABORT,'injected mutation rejection'); END").await.unwrap();
    let error = storage::publish_watched_candidate(
        &harness.processor.crud_store,
        &harness.processor.skills_write_lock,
        candidate.clone(),
        snapshot.clone(),
        Some(harness.workspace.clone()),
        Some(stage.clone()),
        None,
        Default::default(),
        || true,
        |_, _| Ok(()),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<pioneer_crud::SkillReconciliationError>()
            .unwrap()
            .outcome,
        pioneer_crud::SkillReconciliationFailure::NotCommitted
    );
    assert_eq!(
        fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
        "old destination"
    );
    assert!(!attempt_marker(&wrapper).unwrap().unwrap().publishing);
    harness
        .processor
        .crud_store
        .database_connection()
        .execute_unprepared("DROP TRIGGER reject_watched_update")
        .await
        .unwrap();
    let _reset = PublicationFaultReset;
    storage::WATCH_PUBLICATION_FAULT.with(|fault| fault.set(Some("commit_ack")));
    let error = storage::publish_watched_candidate(
        &harness.processor.crud_store,
        &harness.processor.skills_write_lock,
        candidate,
        snapshot,
        Some(harness.workspace.clone()),
        Some(stage),
        None,
        Default::default(),
        || true,
        |_, _| Ok(()),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<pioneer_crud::SkillReconciliationError>()
            .unwrap()
            .outcome,
        pioneer_crud::SkillReconciliationFailure::Unknown
    );
    assert_eq!(
        fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
        "new publication"
    );
    assert_eq!(
        fs::read_to_string(wrapper.join("backup/assets/value.txt")).unwrap(),
        "old destination"
    );
    assert!(attempt_marker(&wrapper).unwrap().unwrap().publishing);
    let tracked: Attempts = Arc::default();
    tracked.lock().unwrap().insert(
        wrapper.clone(),
        Attempt::new(destination.parent().unwrap().to_path_buf()),
    );
    consume(cleanup(tracked, fence(destination.parent().unwrap()))).await;
    assert!(wrapper.join("backup/SKILL.md").exists());
}

#[tokio::test]
async fn preparation_rejection_and_failed_file_or_lock_restoration_keep_last_backup() {
    for fault in [None, Some("restore_files"), Some("restore_lock")] {
        let harness = harness().await;
        let (candidate, mut snapshot, stage, wrapper) = publication_fixture(&harness).await;
        let destination = candidate.destination.clone();
        snapshot.record.scope_key = "substituted".into(); // rejection before begin
        let _reset = PublicationFaultReset;
        storage::WATCH_PUBLICATION_FAULT.with(|slot| slot.set(fault));
        assert!(
            storage::publish_watched_candidate(
                &harness.processor.crud_store,
                &harness.processor.skills_write_lock,
                candidate,
                snapshot,
                Some(harness.workspace.clone()),
                Some(stage),
                None,
                Default::default(),
                || true,
                |_, _| Ok(())
            )
            .await
            .is_err()
        );
        if fault.is_none() {
            assert_eq!(
                fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
                "old destination"
            );
            assert!(!attempt_marker(&wrapper).unwrap().unwrap().publishing);
        } else {
            assert!(attempt_marker(&wrapper).unwrap().unwrap().publishing);
            assert_eq!(
                fs::read_to_string(wrapper.join("previous-lock-entry.json")).unwrap(),
                "null"
            );
            assert_eq!(
                fs::read_to_string(wrapper.join("backup/assets/value.txt")).unwrap(),
                "old destination"
            );
            let tracked: Attempts = Arc::default();
            tracked.lock().unwrap().insert(
                wrapper.clone(),
                Attempt::new(destination.parent().unwrap().to_path_buf()),
            );
            consume(cleanup(tracked, fence(destination.parent().unwrap()))).await;
            assert!(wrapper.join("backup/SKILL.md").exists());
        }
    }
}

#[tokio::test]
async fn indexed_provenance_and_one_fallback_catalog_pass_preserve_ambiguity() {
    let harness = harness().await;
    let source = harness.directory.path().join("source");
    for i in 0..8 {
        package(&source.join(format!("pkg{i}")), "provenance test");
    }
    for i in 0..80 {
        let path = if i < 8 {
            source.join(format!("pkg{i}"))
        } else {
            harness.directory.path().join(format!("unrelated-{i}"))
        };
        let row = SkillInstallationRecord {
            skill_id: SkillId::new(format!("{i:021}")).unwrap(),
            owner: None,
            slug: "test-skill".into(),
            version: None,
            source_kind: "user".into(),
            scope_key: "ws".into(),
            source_ref: if i % 2 == 0 {
                storage::import_source_ref(&path).unwrap()
            } else {
                format!("legacy:{i}")
            },
            install_path: path.display().to_string(),
            trust_level: "community".into(),
            fingerprint: String::new(),
            updated_at_unix: 1700000000,
            pack_id: None,
            pack_member_key: None,
        };
        harness
            .processor
            .crud_store
            .insert_skill_installation(&row, 1700000000)
            .await
            .unwrap();
    }
    let guard = fence(&source);
    let counters = guard.signals.clone();
    assert_eq!(
        consume(job(&harness, &source, Arc::default(), guard)).await,
        (8, 0)
    );
    assert_eq!(
        counters
            .provenance_lookups
            .load(std::sync::atomic::Ordering::Relaxed),
        8
    );
    assert_eq!(
        counters
            .fallback_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        80,
        "eight packages must not select 8 * 80 fallback rows"
    );
    let mut duplicate = harness
        .processor
        .crud_store
        .list_skill_installations_scope_page("user", "ws", None, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    duplicate.skill_id = SkillId::new("Y".repeat(21)).unwrap();
    harness
        .processor
        .crud_store
        .insert_skill_installation(&duplicate, 1700000000)
        .await
        .unwrap();
    assert!(
        harness
            .processor
            .crud_store
            .find_skill_import_provenance("user", "ws", &duplicate.source_ref)
            .await
            .is_err()
    );
    let ambiguous_path = source.join("pkg8");
    package(&ambiguous_path, "ambiguous fallback");
    for (id, reference) in [("U", "legacy-path-a"), ("V", "legacy-path-b")] {
        let mut row = duplicate.clone();
        row.skill_id = SkillId::new(id.repeat(21)).unwrap();
        row.source_ref = reference.into();
        row.install_path = ambiguous_path.display().to_string();
        harness
            .processor
            .crud_store
            .insert_skill_installation(&row, 1700000000)
            .await
            .unwrap();
    }
    let guard = fence(&source);
    let counters = guard.signals.clone();
    let (_, failed) = consume(job(&harness, &source, Arc::default(), guard)).await;
    // pkg0 has ambiguous exact provenance; pkg8 has ambiguous normalized paths.
    assert_eq!(failed, 2);
    assert_eq!(
        counters
            .fallback_rows
            .load(std::sync::atomic::Ordering::Relaxed),
        83
    );
    assert!(
        harness
            .processor
            .crud_store
            .find_skill_import_provenance(
                "user",
                "ws",
                &storage::import_source_ref(&ambiguous_path).unwrap()
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn fresh_worker_forwards_unknown_commit_with_current_guards_then_reclaims_backup() {
    let harness = harness().await;
    let (candidate, snapshot, stage, wrapper) = publication_fixture(&harness).await;
    let destination = candidate.destination.clone();
    let _reset = PublicationFaultReset;
    storage::WATCH_PUBLICATION_FAULT.with(|fault| fault.set(Some("commit_ack")));
    assert!(
        storage::publish_watched_candidate(
            &harness.processor.crud_store,
            &harness.processor.skills_write_lock,
            candidate,
            snapshot,
            Some(harness.workspace.clone()),
            Some(stage),
            None,
            Default::default(),
            || true,
            |_, _| Ok(())
        )
        .await
        .is_err()
    );
    storage::WATCH_PUBLICATION_FAULT.with(|fault| fault.set(None));
    let mut config = harness.processor.managed_root_scan_config("ws").unwrap();
    config
        .roots
        .retain(|root| root.source_kind == SkillSourceKind::User);
    let root = super::super::physical_root(&config.roots[0].managed_root).unwrap();
    // Fresh ownership state must discover the wrapper before any package import.
    let attempts: Attempts = Arc::default();
    assert_eq!(
        consume(root_job(
            harness.processor.clone(),
            root.clone(),
            vec![Mapping::Managed(config, Some(harness.workspace.clone()))],
            Arc::default(),
            attempts.clone(),
            fence(&root)
        ))
        .await,
        (1, 0)
    );
    assert!(destination.join("SKILL.md").exists());
    assert!(!wrapper.exists());
    assert!(attempts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_begin_after_successful_reader_preparation_proves_no_commit() {
    let harness = harness().await;
    let (candidate, snapshot, stage, wrapper) = publication_fixture(&harness).await;
    let destination = candidate.destination.clone();
    // The fixture owns the executor and closes only its writer. Reader preparation
    // remains real and succeeds; begin then returns a genuine closed-pool error.
    harness.writer.clone().close().await.unwrap();
    let error = storage::publish_watched_candidate(
        &harness.processor.crud_store,
        &harness.processor.skills_write_lock,
        candidate,
        snapshot,
        Some(harness.workspace.clone()),
        Some(stage),
        None,
        Default::default(),
        || true,
        |_, _| Ok(()),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<pioneer_crud::SkillReconciliationError>()
            .unwrap()
            .outcome,
        pioneer_crud::SkillReconciliationFailure::NotCommitted
    );
    assert_eq!(
        fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
        "old destination"
    );
    assert!(!attempt_marker(&wrapper).unwrap().unwrap().publishing);
}

#[tokio::test]
async fn missing_scoped_pack_parent_and_incompatible_parent_reject_before_begin() {
    for parent_scope in ["other", "ws"] {
        let harness = harness().await;
        harness
            .processor
            .workspace_manager
            .create_workspace("other", Some("Other"))
            .await
            .unwrap();
        let (candidate, snapshot, stage, wrapper) = publication_fixture(&harness).await;
        let destination = candidate.destination.clone();
        let pack_id = pioneer_protocol::SkillPackId::new("L".repeat(21)).unwrap();
        harness
            .processor
            .crud_store
            .insert_skill_pack_installation(&pioneer_crud::SkillPackInstallationRecord {
                pack_id: pack_id.clone(),
                name: "Invalid parent".into(),
                scope_key: parent_scope.into(),
                source_kind: "registry".into(),
                created_at_unix: 1700000000,
                updated_at_unix: 1700000000,
            })
            .await
            .unwrap();
        let model = pioneer_entity::skill_installation::Entity::find_by_id(
            snapshot.record.skill_id.to_string(),
        )
        .one(&harness.processor.crud_store.database_connection())
        .await
        .unwrap()
        .unwrap();
        let mut corrupt: pioneer_entity::skill_installation::ActiveModel = model.into();
        corrupt.pack_id = Set(Some(pack_id.to_string()));
        corrupt.pack_member_key = Set(Some("member".into()));
        // Deliberately invalid fixture, preserving the real FK. Production APIs
        // forbid this bypass; it models corrupted/pre-existing pack metadata.
        corrupt
            .update(&harness.processor.crud_store.database_connection())
            .await
            .unwrap();
        let writes = harness.observer.writes.lock().unwrap().len();
        let error = storage::publish_watched_candidate(
            &harness.processor.crud_store,
            &harness.processor.skills_write_lock,
            candidate,
            snapshot,
            Some(harness.workspace.clone()),
            Some(stage),
            None,
            Default::default(),
            || true,
            |_, _| Ok(()),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<pioneer_crud::SkillReconciliationError>()
                .unwrap()
                .outcome,
            pioneer_crud::SkillReconciliationFailure::NotCommitted
        );
        assert_eq!(
            harness.observer.writes.lock().unwrap().len(),
            writes,
            "preparation rejection must not begin a writer transaction"
        );
        assert_eq!(
            fs::read_to_string(destination.join("assets/value.txt")).unwrap(),
            "old destination"
        );
        assert!(!attempt_marker(&wrapper).unwrap().unwrap().publishing);
    }
}

#[test]
fn interrupted_garbage_quantum_keeps_restart_ownership_until_payload_is_gone() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("root");
    fs::create_dir(&root).unwrap();
    let wrapper = new_attempt(&root, root.clone()).unwrap();
    package(&wrapper.join("payload"), "staging");
    for index in 0..200 {
        fs::write(
            wrapper.join("payload").join(format!("file-{index}")),
            "data",
        )
        .unwrap();
    }
    let mut removal = Removal::new(wrapper.clone()).unwrap();
    assert!(!removal.step().unwrap());
    drop(removal); // interrupted worker, no in-memory cursor survives
    assert!(attempt_marker(&wrapper).unwrap().is_some());
    let tracked: Attempts = Arc::default();
    let mut discovery = Discovery::new(&root, 50, tracked.clone()).unwrap();
    while !discovery.step().unwrap() {}
    assert!(discovery.packages.is_empty());
    assert!(tracked.lock().unwrap().contains_key(&wrapper));
    let mut removal = Removal::new(wrapper.clone()).unwrap();
    while !removal.step().unwrap() {}
    assert!(!wrapper.exists());
}

// Preservation/concurrency coverage only: this deliberately does not claim
// bounded TOML parsing/serialization (review criterion 7 remains open).
#[tokio::test]
async fn large_lock_preserves_foreign_entries_and_serializes_a_foreground_writer() {
    let harness = harness().await;
    let (candidate, snapshot, stage, wrapper) = publication_fixture(&harness).await;
    let lock_path = candidate.managed_lock_path.clone().unwrap();
    let old = pioneer_skills::SkillLockEntry {
        skill_id: snapshot.record.skill_id.clone(),
        owner: None,
        slug: "old".into(),
        source_kind: "user".into(),
        source_ref: "old-provenance".into(),
        install_path: candidate.destination.display().to_string(),
        version: None,
        trust_level: pioneer_skills::SkillTrustLevel::Community,
        fingerprint: "old".into(),
        installed_at: 123,
    };
    let mut lock = pioneer_skills::SkillsLock::default();
    lock.entries.push(old.clone());
    for index in 0..1000 {
        let mut entry = old.clone();
        entry.skill_id = SkillId::new(format!("{index:021}")).unwrap();
        entry.source_ref = format!("foreign-{index}");
        lock.entries.push(entry);
    }
    pioneer_skills::write_skills_lock_atomic(&lock_path, &lock).unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = StdMutex::new(Some(started_tx));
    let foreground_lock = harness.processor.skills_write_lock.clone();
    let foreground_path = lock_path.clone();
    let mut foreground_entry = old.clone();
    foreground_entry.skill_id = SkillId::new("Y".repeat(21)).unwrap();
    foreground_entry.source_ref = "foreground-provenance".into();
    let foreground = tokio::spawn(async move {
        started_rx.await.unwrap();
        let _guard = foreground_lock.lock().await;
        owned_fs(&Default::default(), move || {
            let mut current = pioneer_skills::read_skills_lock(&foreground_path)?;
            pioneer_skills::upsert_lock_entry(&mut current, foreground_entry);
            pioneer_skills::write_skills_lock_atomic(&foreground_path, &current)
        })
        .await
        .unwrap();
    });
    assert_eq!(
        storage::publish_watched_candidate(
            &harness.processor.crud_store,
            &harness.processor.skills_write_lock,
            candidate,
            snapshot,
            Some(harness.workspace.clone()),
            Some(stage),
            None,
            Default::default(),
            || true,
            |_, committed| {
                if !committed {
                    if let Some(sender) = started_tx.lock().unwrap().take() {
                        sender.send(()).unwrap();
                    }
                }
                Ok(())
            },
        )
        .await
        .unwrap(),
        SkillStorageRelocationOutcome::Switched
    );
    foreground.await.unwrap();
    let actual = pioneer_skills::read_skills_lock(&lock_path).unwrap();
    assert_eq!(actual.entries.len(), 1002);
    for index in 0..1000 {
        let id = SkillId::new(format!("{index:021}")).unwrap();
        assert_eq!(
            actual
                .entries
                .iter()
                .find(|entry| entry.skill_id == id)
                .unwrap()
                .source_ref,
            format!("foreign-{index}")
        );
    }
    assert!(
        actual
            .entries
            .iter()
            .any(|entry| entry.source_ref == "foreground-provenance")
    );
    let previous: Option<pioneer_skills::SkillLockEntry> =
        serde_json::from_slice(&fs::read(wrapper.join("previous-lock-entry.json")).unwrap())
            .unwrap();
    assert_eq!(previous, Some(old));
}
