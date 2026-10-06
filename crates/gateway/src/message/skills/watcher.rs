use super::storage_relocation::{
    ConfiguredRootImportConfig, ManagedRootScanConfig, normalize_absolute_path,
};
use super::*;
use futures_util::{FutureExt, Stream, StreamExt};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::{
    Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[path = "watcher_reconcile.rs"]
pub(super) mod reconcile;

const DEBOUNCE: Duration = Duration::from_millis(300);
const MAX_LATENCY: Duration = Duration::from_secs(2);
const SAFETY_ROUND: Duration = Duration::from_secs(30 * 60);

pub(crate) struct SkillsWatcherWorker {
    stop: CancellationToken,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
enum Mapping {
    Import(
        ConfiguredRootImportConfig,
        Option<pioneer_entity::workspace::Model>,
    ),
    Managed(
        ManagedRootScanConfig,
        Option<pioneer_entity::workspace::Model>,
    ),
}

impl Mapping {
    fn identity(
        &self,
    ) -> (
        bool,
        String,
        String,
        PathBuf,
        Option<pioneer_entity::workspace::Model>,
    ) {
        match self {
            Self::Import(config, workspace) => {
                let root = &config.roots[0];
                (
                    true,
                    root.source_kind.as_db_value().into(),
                    root.scope_key.clone(),
                    root.managed_root.clone(),
                    workspace.clone(),
                )
            }
            Self::Managed(config, workspace) => {
                let root = &config.roots[0];
                (
                    false,
                    root.source_kind.as_db_value().into(),
                    root.scope_key.clone(),
                    root.managed_root.clone(),
                    workspace.clone(),
                )
            }
        }
    }
}

struct Registration {
    mappings: Vec<Mapping>,
    subscribers: BTreeSet<String>,
    baseline: Arc<StdMutex<reconcile::Baseline>>,
    changed: BTreeSet<String>,
}

impl Default for Registration {
    fn default() -> Self {
        Self {
            mappings: Vec::new(),
            subscribers: BTreeSet::new(),
            baseline: Arc::default(),
            changed: BTreeSet::new(),
        }
    }
}

/// Callback state is bounded by registered physical roots, never by events.
struct Dirty {
    incarnation: u64,
    generation: u64,
    acknowledged: u64,
    first: Option<Instant>,
    last: Instant,
    retry_at: Instant,
    attempts: u32,
    rescan_generation: Option<u64>,
    watch_dirty: bool,
}

impl Dirty {
    fn new(incarnation: u64, now: Instant) -> Self {
        Self {
            incarnation,
            generation: 1,
            acknowledged: 0,
            first: Some(now - DEBOUNCE),
            last: now - DEBOUNCE,
            retry_at: now,
            attempts: 0,
            rescan_generation: Some(1),
            watch_dirty: true,
        }
    }
    fn mark(&mut self, now: Instant, rescan: bool) {
        // Saturation is deliberately never ACKed, rather than permitting ABA.
        self.generation = self.generation.saturating_add(1);
        self.first.get_or_insert(now);
        self.last = now;
        if rescan {
            self.rescan_generation = Some(self.generation);
        }
    }
    fn due(&self) -> Option<Instant> {
        if self.generation == self.acknowledged {
            return None;
        }
        let first = self.first?;
        let ready = if self.rescan_generation.is_some() {
            first
        } else {
            (self.last + DEBOUNCE).min(first + MAX_LATENCY)
        };
        Some(ready.max(self.retry_at))
    }
    fn finish(&mut self, incarnation: u64, generation: u64, success: bool, now: Instant) {
        if self.incarnation != incarnation {
            return;
        }
        if success {
            if generation == u64::MAX {
                return;
            }
            self.acknowledged = generation;
            if self
                .rescan_generation
                .is_some_and(|rescan| rescan <= generation)
            {
                self.rescan_generation = None;
            }
            self.attempts = 0;
            self.retry_at = now;
            if self.generation == generation {
                self.first = None;
            }
        } else {
            self.attempts = self.attempts.saturating_add(1).min(16);
            self.retry_at = now + retry_delay(self.attempts);
            self.first.get_or_insert(now);
        }
    }
}

fn retry_delay(attempts: u32) -> Duration {
    Duration::from_secs((5_u64 << attempts.saturating_sub(1).min(6)).min(300))
}

#[derive(Default)]
struct Signals {
    roots: StdMutex<BTreeMap<PathBuf, Dirty>>,
    // A failed try_lock/wake never drops a rescan obligation.
    overflow: AtomicBool,
    recover: AtomicBool,
    wake: Notify,
    #[cfg(test)]
    snapshot_failure_page: StdMutex<Option<usize>>,
    #[cfg(test)]
    loop_iterations: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    snapshots: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    source_reads: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    preparations: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    hashes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    provenance_lookups: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fallback_rows: std::sync::atomic::AtomicU64,
}

impl Signals {
    fn event(&self, event: notify::Result<Event>) {
        if matches!(&event, Ok(event) if matches!(event.kind, EventKind::Access(_)) && !event.need_rescan())
        {
            return;
        }
        let rescan = event.as_ref().map_or(true, |event| event.need_rescan());
        if rescan {
            self.recover.store(true, Ordering::Release);
        }
        let Ok(mut roots) = self.roots.try_lock() else {
            self.overflow.store(true, Ordering::Release);
            self.wake.notify_one();
            return;
        };
        let paths = match &event {
            Ok(event) => &event.paths,
            Err(error) => &error.paths,
        };
        let all = paths.is_empty() || paths.len() > 64;
        let now = Instant::now();
        // Both paths of a rename are visited. Unknown kinds with a path reconcile
        // the containing root; pathless/oversized events conservatively resync all.
        let mut changed = false;
        for (root, dirty) in roots.iter_mut() {
            if all
                || paths
                    .iter()
                    .any(|path| path.starts_with(root) || root.starts_with(path))
            {
                changed = true;
                dirty.mark(now, rescan || all);
                dirty.watch_dirty |= all
                    || paths.iter().any(|path| root.starts_with(path))
                    || (!cfg!(target_os = "macos")
                        && matches!(&event, Ok(event) if matches!(event.kind, EventKind::Create(notify::event::CreateKind::Folder | notify::event::CreateKind::Any) | EventKind::Remove(notify::event::RemoveKind::Folder | notify::event::RemoveKind::Any) | EventKind::Modify(notify::event::ModifyKind::Name(_)))));
            }
        }
        drop(roots);
        if changed || rescan {
            self.wake.notify_one();
        }
    }
    fn invalidate_all(&self) {
        let now = Instant::now();
        for dirty in self.roots.lock().expect("skills roots lock").values_mut() {
            dirty.mark(now, true);
        }
    }
}

#[derive(Clone)]
struct JobFence {
    signals: Arc<Signals>,
    root: PathBuf,
    incarnation: u64,
    stop: CancellationToken,
}

impl JobFence {
    fn check(&self) -> Result<()> {
        if !self.valid() {
            bail!("stale skills root incarnation");
        }
        Ok(())
    }
    fn valid(&self) -> bool {
        !self.stop.is_cancelled()
            && self.signals.roots.try_lock().is_ok_and(|roots| {
                roots
                    .get(&self.root)
                    .is_some_and(|dirty| dirty.incarnation == self.incarnation)
            })
    }
}

enum Progress {
    Quantum,
    Changed(String),
    Failed,
}
struct Job {
    incarnation: u64,
    generation: u64,
    stream: Pin<Box<dyn Stream<Item = Result<Progress>> + Send>>,
    failed: bool,
}

struct WatchWalk {
    initial: std::collections::VecDeque<(PathBuf, RecursiveMode)>,
    directories: Vec<std::fs::ReadDir>,
    prune_after: Option<PathBuf>,
}
struct NativePlan {
    paths: BTreeMap<PathBuf, u64>,
    generation: u64,
    walk: Option<WatchWalk>,
    pending: bool,
    replacement: bool,
    initialized: bool,
    rescan_after_registration: bool,
    retry_at: Instant,
    attempts: u32,
}
impl NativePlan {
    fn new() -> Self {
        Self {
            paths: BTreeMap::new(),
            generation: 0,
            walk: None,
            pending: true,
            replacement: false,
            initialized: false,
            rescan_after_registration: false,
            retry_at: Instant::now(),
            attempts: 0,
        }
    }
}
struct NativeWatch {
    active: bool,
    mode: RecursiveMode,
    owners: BTreeSet<PathBuf>,
}
struct Native {
    watcher: Option<RecommendedWatcher>,
    watched: BTreeMap<PathBuf, NativeWatch>,
    plans: BTreeMap<PathBuf, NativePlan>,
    retry_at: Instant,
    attempts: u32,
    recovery_pending: bool,
    last_root: Option<PathBuf>,
    sync_after: Option<PathBuf>,
    // Linux notify Recursive performs an internal unbounded WalkDir. Instead
    // subscribe each directory before enumerating it, in this worker's cursor.
    recursive: bool,
    #[cfg(test)]
    fail_watch_once: BTreeSet<PathBuf>,
    #[cfg(test)]
    watch_calls: BTreeMap<PathBuf, usize>,
}
impl Native {
    fn new() -> Self {
        Self {
            watcher: None,
            watched: BTreeMap::new(),
            plans: BTreeMap::new(),
            retry_at: Instant::now(),
            attempts: 0,
            recovery_pending: false,
            last_root: None,
            sync_after: None,
            recursive: !cfg!(target_os = "linux"),
            #[cfg(test)]
            fail_watch_once: BTreeSet::new(),
            #[cfg(test)]
            watch_calls: BTreeMap::new(),
        }
    }
    fn request_recovery(&mut self) {
        if !self.recovery_pending {
            self.recovery_pending = true;
            self.attempts = self.attempts.saturating_add(1).min(16);
            self.retry_at = Instant::now() + retry_delay(self.attempts);
        }
    }
    fn ready(&self, root: &Path) -> bool {
        (self.watcher.is_none() && self.recovery_pending)
            || self
                .plans
                .get(root)
                .is_some_and(|p| p.initialized || p.attempts != 0)
    }
    fn refresh(
        mut self,
        paths: Vec<PathBuf>,
        changed: BTreeSet<PathBuf>,
        signals: Arc<Signals>,
    ) -> Self {
        let now = Instant::now();
        if self.recovery_pending || signals.recover.swap(false, Ordering::AcqRel) {
            self.recovery_pending = false;
            self.watcher.take();
            self.watched.clear();
            self.plans.clear();
            self.sync_after = None;
            signals.invalidate_all();
        }
        if self.watcher.is_none() {
            let callback = signals.clone();
            match RecommendedWatcher::new(
                move |event| callback.event(event),
                notify::Config::default().with_follow_symlinks(false),
            ) {
                Ok(watcher) => self.watcher = Some(watcher),
                Err(_) => {
                    self.request_recovery();
                    return self;
                }
            }
        }
        // Root synchronization and subscription work each consume an explicit
        // quantum; repeated calls advance the same owned directory iterators.
        let active: BTreeSet<_> = paths.into_iter().collect();
        let page = active
            .iter()
            .filter(|root| self.sync_after.as_ref().is_none_or(|after| *root > after))
            .take(64)
            .cloned()
            .collect::<Vec<_>>();
        self.sync_after = page.last().cloned();
        for root in page {
            self.plans.entry(root).or_insert_with(NativePlan::new);
        }
        for root in changed {
            if let Some(plan) = self.plans.get_mut(&root) {
                plan.pending = true;
                plan.replacement = true;
                plan.rescan_after_registration = true;
            }
        }
        // Removed roots are pruned in the same bounded path cursor below.
        let candidates = self
            .plans
            .iter()
            .filter(|(root, plan)| {
                !active.contains(*root)
                    || ((plan.pending || plan.walk.is_some()) && plan.retry_at <= now)
            })
            .map(|(root, _)| root.clone())
            .collect::<Vec<_>>();
        if let Some(root) = next_root(&candidates, self.last_root.as_ref()).cloned() {
            self.last_root = Some(root.clone());
            let mut plan = self.plans.remove(&root).expect("native plan");
            let mut failed_subscription = None;
            let result = (|| -> Result<()> {
                if plan.walk.is_none() {
                    plan.generation = plan
                        .generation
                        .checked_add(1)
                        .context("native generation exhausted")?;
                    let initial = if active.contains(&root) {
                        watch_plan(&root)?
                    } else {
                        Vec::new()
                    };
                    plan.walk = Some(WatchWalk {
                        initial: initial.into(),
                        directories: Vec::new(),
                        prune_after: None,
                    });
                    plan.pending = false;
                }
                for _ in 0..64 {
                    let walk = plan.walk.as_mut().expect("registration cursor");
                    let next = if let Some(item) = walk.initial.pop_front() {
                        Some(item)
                    } else if let Some(entries) = walk.directories.last_mut() {
                        match entries.next() {
                            None => {
                                walk.directories.pop();
                                continue;
                            }
                            Some(entry) => {
                                let entry = entry?;
                                let metadata = std::fs::symlink_metadata(entry.path())?;
                                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                                    continue;
                                }
                                Some((entry.path(), RecursiveMode::Recursive))
                            }
                        }
                    } else {
                        None
                    };
                    if let Some((path, requested)) = next {
                        let mode = if !self.recursive {
                            RecursiveMode::NonRecursive
                        } else if active.contains(&path) {
                            RecursiveMode::Recursive
                        } else {
                            requested
                        };
                        let owner_roots = self
                            .watched
                            .get(&path)
                            .map(|w| w.owners.clone())
                            .unwrap_or_default();
                        let replace =
                            plan.replacement && plan.paths.get(&path) != Some(&plan.generation);
                        let existing = self.watched.get(&path);
                        let watch = existing.is_none_or(|watch| !watch.active)
                            || replace
                            || existing.is_some_and(|w| w.mode != mode);
                        plan.paths.insert(path.clone(), plan.generation);
                        if watch {
                            failed_subscription = Some(path.clone());
                            // Ownership survives a failed replacement, while the
                            // active flag loses proof before unwatch. Shared roots
                            // must not lose their sentinel when another root retries.
                            if self.watched.get(&path).is_some_and(|watch| watch.active) {
                                self.watched.get_mut(&path).unwrap().active = false;
                                let _ = self.watcher.as_mut().unwrap().unwatch(&path);
                            }
                            let mut owners = owner_roots;
                            owners.insert(root.clone());
                            self.watched.insert(
                                path.clone(),
                                NativeWatch {
                                    active: false,
                                    mode,
                                    owners,
                                },
                            );
                            #[cfg(test)]
                            {
                                *self.watch_calls.entry(path.clone()).or_default() += 1;
                                if self.fail_watch_once.remove(&path) {
                                    bail!("injected replacement watch failure");
                                }
                            }
                            self.watcher.as_mut().unwrap().watch(&path, mode)?;
                            self.watched.get_mut(&path).unwrap().active = true;
                            failed_subscription = None;
                        }
                        self.watched
                            .get_mut(&path)
                            .unwrap()
                            .owners
                            .insert(root.clone());
                        if !self.recursive && requested == RecursiveMode::Recursive {
                            // No snapshot/enumeration before this directory's native
                            // subscription. New/moved folders request another round.
                            walk.directories.push(std::fs::read_dir(&path)?);
                        }
                        continue;
                    }
                    let next = plan
                        .paths
                        .range::<PathBuf, _>((
                            walk.prune_after
                                .as_ref()
                                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                            std::ops::Bound::Unbounded,
                        ))
                        .next()
                        .map(|(path, generation)| (path.clone(), *generation));
                    let Some((path, generation)) = next else {
                        plan.walk = None;
                        plan.replacement = false;
                        break;
                    };
                    walk.prune_after = Some(path.clone());
                    if generation != plan.generation {
                        plan.paths.remove(&path);
                        if let Some(watch) = self.watched.get_mut(&path) {
                            watch.owners.remove(&root);
                            if watch.owners.is_empty() {
                                self.watched.remove(&path);
                                let _ = self.watcher.as_mut().unwrap().unwatch(&path);
                            }
                        }
                    }
                }
                Ok(())
            })();
            if result.is_err() {
                plan.attempts = plan.attempts.saturating_add(1).min(16);
                plan.retry_at = now + retry_delay(plan.attempts);
                plan.walk = None;
                plan.pending = true;
                plan.rescan_after_registration = true;
                if let Some(watch) = failed_subscription
                    .as_ref()
                    .and_then(|path| self.watched.get(path))
                {
                    for owner in &watch.owners {
                        if let Some(other) = self.plans.get_mut(owner) {
                            other.pending = true;
                            other.retry_at = other.retry_at.max(plan.retry_at);
                            other.rescan_after_registration = true;
                        }
                    }
                }
                warn!(
                    "skills native registration failed; useful subscriptions retained and root retry delayed"
                );
            } else {
                if plan.walk.is_none() {
                    plan.attempts = 0;
                    plan.initialized = true;
                }
                if plan.walk.is_none() && plan.rescan_after_registration {
                    if let Some(dirty) = signals
                        .roots
                        .lock()
                        .expect("skills roots lock")
                        .get_mut(&root)
                    {
                        dirty.mark(Instant::now(), true);
                    }
                    plan.rescan_after_registration = false;
                }
            }
            if active.contains(&root) || !plan.paths.is_empty() {
                self.plans.insert(root, plan);
            }
        }
        self.attempts = u32::from(
            self.sync_after.is_some()
                || self.plans.iter().any(|(root, plan)| {
                    !active.contains(root) || plan.pending || plan.walk.is_some()
                }),
        );
        self.retry_at = self
            .plans
            .values()
            .filter(|p| p.pending || p.walk.is_some())
            .map(|p| p.retry_at)
            .min()
            .unwrap_or(now);
        self
    }
}

fn watch_plan(root: &Path) -> Result<Vec<(PathBuf, RecursiveMode)>> {
    let mut plan = Vec::new();
    match std::fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            plan.push((root.to_path_buf(), RecursiveMode::Recursive))
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    // Even an existing root has a nonrecursive parent sentinel for replacement.
    let mut parent = root.parent().context("skills root has no parent")?;
    loop {
        match std::fs::symlink_metadata(parent) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                plan.push((parent.to_path_buf(), RecursiveMode::NonRecursive));
                break;
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        parent = parent
            .parent()
            .context("skills root has no existing parent")?;
    }
    Ok(plan)
}

// Resolve an existing prefix once; aliases such as /tmp and /private/tmp share
// native registrations while scope/provenance remain explicit in each mapping.
fn physical_root(path: &Path) -> Result<PathBuf> {
    let path = normalize_absolute_path(path)?;
    let mut parent = path.as_path();
    let mut suffix = Vec::new();
    loop {
        match std::fs::canonicalize(parent) {
            Ok(mut canonical) => {
                for part in suffix.into_iter().rev() {
                    canonical.push(part);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                suffix.push(
                    parent
                        .file_name()
                        .context("invalid skills root")?
                        .to_os_string(),
                );
                parent = parent.parent().context("invalid skills root parent")?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn owned_fs<T: Send + 'static>(
    stop: &CancellationToken,
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    if stop.is_cancelled() {
        bail!("skills watcher stopped");
    }
    // Always join. Selecting cancellation against this join would detach FS work.
    let result = tokio::task::spawn_blocking(work)
        .await
        .context("skills FS job panicked")?;
    result
}

async fn cancellable_db<T>(
    stop: &CancellationToken,
    work: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! { biased; _ = stop.cancelled() => bail!("skills watcher stopped"), result = work => result }
}

impl MessageProcessor {
    pub(crate) async fn start_skills_watcher(self: &Arc<Self>) {
        let mut guard = self.skills_watcher_worker.lock().await;
        if guard.is_some() || !self.tool_loop_config.skills.enabled {
            return;
        }
        let this = self.with_database_class(SqliteWriteClass::Maintenance);
        let stop = CancellationToken::new();
        let worker_stop = stop.clone();
        let handle = tokio::spawn(async move {
            crate::database::attribution::scope_database_workload_result(
                pioneer_observability::DatabaseWorkload::SkillsWatch,
                run(this, worker_stop).map(Ok::<_, anyhow::Error>),
            )
            .await
            .ok();
        });
        *guard = Some(SkillsWatcherWorker { stop, handle });
    }
    pub(crate) async fn shutdown_skills_watcher(&self) {
        if let Some(worker) = self.skills_watcher_worker.lock().await.take() {
            worker.stop.cancel();
            let _ = worker.handle.await;
        }
    }
}

fn next_root<'a>(ready: &'a [PathBuf], last: Option<&PathBuf>) -> Option<&'a PathBuf> {
    ready
        .iter()
        .find(|path| last.is_none_or(|last| *path > last))
        .or_else(|| ready.first())
}

async fn run(this: Arc<MessageProcessor>, stop: CancellationToken) {
    run_with_signals(this, stop, Arc::new(Signals::default())).await;
}
async fn run_with_signals(
    this: Arc<MessageProcessor>,
    stop: CancellationToken,
    signals: Arc<Signals>,
) {
    let changes = this.workspace_manager.changes();
    let mut native = Native::new();
    let mut registrations: BTreeMap<PathBuf, Registration> = BTreeMap::new();
    let mut jobs: BTreeMap<PathBuf, Job> = BTreeMap::new();
    let attempts: reconcile::Attempts = Arc::default();
    let mut incarnation = 0_u64;
    let mut applied_revision = None;
    let mut next_safety = Instant::now() + SAFETY_ROUND;
    let mut last_root: Option<PathBuf> = None;
    let mut refresh_watches = true;
    let mut rewatch = BTreeSet::new();
    let mut snapshot_retry = Instant::now();
    let mut snapshot_attempts: u32 = 0;

    warn!(
        "skills native events may be unavailable on network filesystems; observation there is degraded to the bounded 30-minute safety reconciliation"
    );
    while !stop.is_cancelled() {
        #[cfg(test)]
        signals.loop_iterations.fetch_add(1, Ordering::Relaxed);
        {
            let mut roots = signals.roots.lock().expect("skills roots lock");
            for (path, dirty) in roots.iter_mut() {
                if dirty.generation == u64::MAX {
                    warn!("skills root generation exhausted; watcher must restart");
                    stop.cancel();
                }
                if dirty.watch_dirty {
                    dirty.watch_dirty = false;
                    rewatch.insert(path.clone());
                    refresh_watches = true;
                }
            }
        }
        if signals.overflow.swap(false, Ordering::AcqRel) {
            signals.invalidate_all();
            rewatch.extend(registrations.keys().cloned());
            refresh_watches = true;
        }
        if signals.recover.load(Ordering::Acquire) {
            native.request_recovery();
            refresh_watches = true;
        }
        let safety = Instant::now() >= next_safety;
        if safety {
            next_safety = Instant::now() + SAFETY_ROUND;
            signals.invalidate_all();
            rewatch.extend(registrations.keys().cloned());
            refresh_watches = true;
        }

        // Startup and revision changes are logical snapshots with bounded reader
        // pages. Each root is subscribed before its first FS/catalog snapshot.
        if (applied_revision != Some(changes.revision()) || safety)
            && Instant::now() >= snapshot_retry
        {
            let revision = changes.revision();
            let snapshot = snapshot_roots(&this, &stop, &signals).await;
            match snapshot {
                Ok(mut next) => {
                    #[cfg(test)]
                    signals.snapshots.fetch_add(1, Ordering::Relaxed);
                    let mut dirty = signals.roots.lock().expect("skills roots lock");
                    dirty.retain(|path, _| next.contains_key(path));
                    for (path, registration) in &mut next {
                        let unchanged = registrations.get(path).is_some_and(|old| {
                            old.mappings
                                .iter()
                                .map(Mapping::identity)
                                .collect::<Vec<_>>()
                                == registration
                                    .mappings
                                    .iter()
                                    .map(Mapping::identity)
                                    .collect::<Vec<_>>()
                        });
                        if let Some(old) = registrations.get(path) {
                            registration.baseline = old.baseline.clone();
                            registration.changed = old.changed.clone();
                        }
                        if !unchanged {
                            let Some(next_incarnation) = incarnation.checked_add(1) else {
                                stop.cancel();
                                break;
                            };
                            incarnation = next_incarnation;
                            let mut fresh = Dirty::new(incarnation, Instant::now());
                            if let Some(old) = dirty.get(path) {
                                fresh.retry_at = old.retry_at;
                                fresh.attempts = old.attempts;
                            }
                            dirty.insert(path.clone(), fresh);
                            jobs.remove(path);
                        }
                    }
                    jobs.retain(|path, _| next.contains_key(path));
                    drop(dirty);
                    registrations = next;
                    applied_revision = Some(revision);
                    snapshot_attempts = 0;
                    refresh_watches = true;
                }
                Err(_) => {
                    applied_revision = None;
                    snapshot_attempts = snapshot_attempts.saturating_add(1).min(16);
                    snapshot_retry = Instant::now() + retry_delay(snapshot_attempts);
                    warn!("skills Workspace snapshot failed; retry is retained");
                }
            }
        }
        if stop.is_cancelled() {
            break;
        }
        if refresh_watches && Instant::now() >= native.retry_at {
            let paths = registrations.keys().cloned().collect();
            let callback = signals.clone();
            let moved = std::mem::replace(&mut native, Native::new());
            let changed = std::mem::take(&mut rewatch);
            match owned_fs(&stop, move || Ok(moved.refresh(paths, changed, callback))).await {
                Ok(updated) => {
                    native = updated;
                    refresh_watches = native.attempts != 0;
                }
                Err(_) => break,
            }
        }

        let now = Instant::now();
        let ready = {
            let roots = signals.roots.lock().expect("skills roots lock");
            registrations
                .keys()
                .filter(|root| {
                    native.ready(root)
                        && (jobs.contains_key(*root)
                            || roots
                                .get(*root)
                                .and_then(Dirty::due)
                                .is_some_and(|due| due <= now))
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        if !ready.is_empty() {
            let path = next_root(&ready, last_root.as_ref())
                .expect("ready roots")
                .clone();
            last_root = Some(path.clone());
            if !jobs.contains_key(&path) {
                let (job_incarnation, generation) = {
                    let mut roots = signals.roots.lock().expect("skills roots lock");
                    let dirty = roots.get_mut(&path).expect("registered root");
                    dirty.first = None;
                    (dirty.incarnation, dirty.generation)
                };
                let registration = &registrations[&path];
                let fence = JobFence {
                    signals: signals.clone(),
                    root: path.clone(),
                    incarnation: job_incarnation,
                    stop: stop.clone(),
                };
                let stream = reconcile::root_job(
                    this.clone(),
                    path.clone(),
                    registration.mappings.clone(),
                    registration.baseline.clone(),
                    attempts.clone(),
                    fence,
                );
                jobs.insert(
                    path.clone(),
                    Job {
                        incarnation: job_incarnation,
                        generation,
                        stream,
                        failed: false,
                    },
                );
            }
            let job = jobs.get_mut(&path).expect("job exists");
            let progress = std::panic::AssertUnwindSafe(job.stream.next())
                .catch_unwind()
                .await;
            match progress {
                Ok(Some(Ok(Progress::Quantum))) => {}
                Ok(Some(Ok(Progress::Changed(scope)))) => {
                    registrations
                        .get_mut(&path)
                        .expect("registered root")
                        .changed
                        .insert(scope);
                }
                Ok(Some(Ok(Progress::Failed))) => {
                    job.failed = true;
                }
                result => {
                    let success = matches!(result, Ok(None)) && !job.failed;
                    if !success {
                        warn!(
                            "skills root reconciliation failed; successful packages coalesce and failed work retains backoff"
                        );
                    }
                    let job = jobs.remove(&path).expect("finished job");
                    if let Some(dirty) = signals
                        .roots
                        .lock()
                        .expect("skills roots lock")
                        .get_mut(&path)
                    {
                        dirty.finish(job.incarnation, job.generation, success, Instant::now());
                    }
                    let changed = std::mem::take(
                        &mut registrations
                            .get_mut(&path)
                            .expect("registered root")
                            .changed,
                    );
                    for scope in &changed {
                        let targets = if scope == "system" {
                            registrations[&path].subscribers.clone()
                        } else {
                            BTreeSet::from([scope.clone()])
                        };
                        for workspace in targets {
                            let active = tokio::select! { biased; _ = stop.cancelled() => false, result = this.workspace_manager.validate_workspace_id(&workspace) => result.is_ok() };
                            if !active {
                                continue;
                            }
                            tokio::select! { biased; _ = stop.cancelled() => {}, _ = this.notify_skills_changed(&workspace, "catalog_changed", Vec::new(), now_timestamp_secs()) => {} }
                        }
                    }
                    refresh_watches = true;
                }
            }
            // One owned quantum per root, including ignored directory entries.
            tokio::task::yield_now().await;
            continue;
        }
        let next_due = signals
            .roots
            .lock()
            .expect("skills roots lock")
            .iter()
            .filter(|(path, _)| registrations.contains_key(*path) && native.ready(path))
            .filter_map(|(_, dirty)| dirty.due())
            .min()
            .unwrap_or(next_safety);
        let mut deadline = next_due.min(next_safety);
        if refresh_watches {
            deadline = deadline.min(native.retry_at);
        }
        if applied_revision != Some(changes.revision()) {
            deadline = deadline.min(snapshot_retry);
        }
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = changes.wake.notified() => {},
            _ = signals.wake.notified() => {},
            _ = tokio::time::sleep_until(deadline.into()) => {},
        }
    }
    // No job is being polled here, hence every blocking quantum has been joined.
    jobs.clear();
    let _ = tokio::task::spawn_blocking(move || drop(native)).await;
    // Cancellation stops after the owned quantum. Exact durable markers let the
    // next worker reclaim staging; shutdown must neither erase an uncertain
    // backup nor recursively drain arbitrarily large garbage before returning.
}

async fn snapshot_roots(
    this: &MessageProcessor,
    stop: &CancellationToken,
    _signals: &Arc<Signals>,
) -> Result<BTreeMap<PathBuf, Registration>> {
    let mut roots = BTreeMap::<PathBuf, Registration>::new();
    let mut after = None;
    #[cfg(test)]
    let mut page_number = 0;
    loop {
        #[cfg(test)]
        {
            if *_signals
                .snapshot_failure_page
                .lock()
                .expect("snapshot failpoint")
                == Some(page_number)
            {
                bail!("injected Workspace page failure");
            }
            page_number += 1;
        }
        let page = tokio::select! { biased; _ = stop.cancelled() => bail!("skills watcher stopped"), page = this.workspace_manager.active_page(after.as_deref()) => page? };
        if page.is_empty() {
            break;
        }
        after = page.last().map(|workspace| workspace.id.clone());
        for workspace in page {
            let import = this.configured_root_import_config(&workspace.id, false)?;
            let managed = this.managed_root_scan_config(&workspace.id)?;
            let workspace_id = workspace.id.clone();
            let registrations = owned_fs(stop, move || {
                let mut found = Vec::new();
                for root in &import.roots {
                    let mut config = import.clone();
                    config.roots = vec![root.clone()];
                    let owner =
                        (root.source_kind != SkillSourceKind::System).then_some(workspace.clone());
                    found.push((
                        physical_root(&root.source_root)
                            .unwrap_or(normalize_absolute_path(&root.source_root)?),
                        Mapping::Import(config, owner),
                    ));
                }
                for root in &managed.roots {
                    let mut config = managed.clone();
                    config.roots = vec![root.clone()];
                    let owner =
                        (root.source_kind != SkillSourceKind::System).then_some(workspace.clone());
                    found.push((
                        physical_root(&root.managed_root)
                            .unwrap_or(normalize_absolute_path(&root.managed_root)?),
                        Mapping::Managed(config, owner),
                    ));
                }
                Ok(found)
            })
            .await?;
            for (path, mapping) in registrations {
                let registration = roots.entry(path).or_default();
                if !registration
                    .mappings
                    .iter()
                    .any(|old| old.identity() == mapping.identity())
                {
                    registration.mappings.push(mapping);
                }
                registration.subscribers.insert(workspace_id.clone());
            }
        }
    }
    Ok(roots)
}

#[cfg(test)]
#[path = "watcher_tests.rs"]
mod tests;
