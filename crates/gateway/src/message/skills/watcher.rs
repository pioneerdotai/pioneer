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
const CLAIM_CLEANUP_QUANTUM: usize = 64;
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

// The lease identifies one job, independently of a reusable root incarnation.
struct ClaimLease {
    id: u64,
    root: PathBuf,
    live: AtomicBool,
    root_live: Arc<AtomicBool>,
}
impl ClaimLease {
    fn valid(&self) -> bool {
        self.live.load(Ordering::Acquire) && self.root_live.load(Ordering::Acquire)
    }
}
struct PathClaim {
    lease: Arc<ClaimLease>,
    subtree: bool,
}
struct OwnerClaims {
    lease: Arc<ClaimLease>,
    paths: BTreeSet<PathBuf>,
}

#[derive(Default)]
struct RootSignals {
    roots: BTreeMap<PathBuf, Dirty>,
    // Forward lookup for the callback; inverse index for direct bounded removal.
    // Shared/overlapping roots can independently reserve the same path. The
    // forward index keeps one lease per owner root, never historical job IDs.
    owned_paths: BTreeMap<PathBuf, BTreeMap<PathBuf, PathClaim>>,
    owners: BTreeMap<u64, OwnerClaims>,
    next_owner: u64,
    cleanup_after: Option<u64>,
    cleanup_running: bool,
    cleanup_again: bool,
}
impl std::ops::Deref for RootSignals {
    type Target = BTreeMap<PathBuf, Dirty>;
    fn deref(&self) -> &Self::Target {
        &self.roots
    }
}
impl std::ops::DerefMut for RootSignals {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.roots
    }
}

fn owns_path(
    owned: &BTreeMap<PathBuf, BTreeMap<PathBuf, PathClaim>>,
    path: &Path,
    mut lookup: impl FnMut(),
) -> bool {
    for (depth, ancestor) in path.ancestors().enumerate() {
        lookup();
        if owned.get(ancestor).is_some_and(|claims| {
            claims
                .values()
                .any(|claim| claim.lease.valid() && (depth == 0 || claim.subtree))
        }) {
            return true;
        }
    }
    false
}

/// Dirty state is bounded by registered physical roots, never by events.
struct Dirty {
    incarnation: u64,
    live: Arc<AtomicBool>,
    generation: u64,
    acknowledged: u64,
    first: Option<Instant>,
    last: Instant,
    retry_at: Instant,
    attempts: u32,
    rescan_generation: Option<u64>,
    watch_dirty: bool,
    watch_fence: bool,
}

impl Dirty {
    fn new(incarnation: u64, now: Instant) -> Self {
        Self {
            incarnation,
            live: Arc::new(AtomicBool::new(true)),
            generation: 1,
            acknowledged: 0,
            first: Some(now - DEBOUNCE),
            last: now - DEBOUNCE,
            retry_at: now,
            attempts: 0,
            rescan_generation: Some(1),
            watch_dirty: true,
            watch_fence: true,
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
    roots: StdMutex<RootSignals>,
    claims_retired: AtomicBool,
    // A failed try_lock/wake never drops a rescan obligation.
    overflow: AtomicBool,
    recover: AtomicBool,
    wake: Notify,
    #[cfg(test)]
    snapshot_failure_page: StdMutex<Option<usize>>,
    #[cfg(test)]
    snapshot_failed: Notify,
    #[cfg(test)]
    backend_unavailable: AtomicBool,
    #[cfg(test)]
    native_events_disabled: AtomicBool,
    #[cfg(test)]
    backend_creations: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    backend_attempts: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    signal_during_recovery: AtomicBool,
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
    prepared_inputs: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    preparation_input_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    root_rounds: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pause_root: StdMutex<Option<PathBuf>>,
    #[cfg(test)]
    paused: Notify,
    #[cfg(test)]
    resume: Notify,
    #[cfg(test)]
    directory_registration: AtomicBool,
    #[cfg(test)]
    provenance_lookups: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    fallback_rows: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    registration_panic_once: AtomicBool,
    #[cfg(test)]
    registration_faults: StdMutex<BTreeSet<(PathBuf, &'static str)>>,
    #[cfg(test)]
    verification_bytes: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    stages_created: StdMutex<BTreeMap<PathBuf, usize>>,
    #[cfg(test)]
    pause_stage: StdMutex<Option<PathBuf>>,
    #[cfg(test)]
    pause_cleanup: StdMutex<Option<PathBuf>>,
    #[cfg(test)]
    owned_lookups: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    claim_cleanup_records: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    claim_cleanup_owners: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pause_claim_cleanup: AtomicBool,
    #[cfg(test)]
    paused_job_deadlines: StdMutex<BTreeMap<PathBuf, Instant>>,
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
        let RootSignals {
            roots: dirty_roots,
            owned_paths,
            ..
        } = &mut *roots;
        for (root, dirty) in dirty_roots.iter_mut() {
            if all
                || paths
                    .iter()
                    .any(|path| path.starts_with(root) || root.starts_with(path))
            {
                changed = true;
                dirty.mark(now, rescan || all);
                let folder = self.directory_events()
                    && matches!(&event, Ok(event) if matches!(event.kind,
                        EventKind::Create(notify::event::CreateKind::Folder | notify::event::CreateKind::Any)
                        | EventKind::Remove(notify::event::RemoveKind::Folder | notify::event::RemoveKind::Any)
                        | EventKind::Modify(notify::event::ModifyKind::Name(_))));
                let replacement = all || paths.iter().any(|path| root.starts_with(path));
                dirty.watch_dirty |= replacement || folder;
                dirty.watch_fence |= replacement
                    || (folder
                        && paths
                            .iter()
                            .filter(|path| path.starts_with(root) || root.starts_with(path))
                            .any(|path| {
                                !owns_path(owned_paths, path, || {
                                    #[cfg(test)]
                                    self.owned_lookups.fetch_add(1, Ordering::Relaxed);
                                })
                            }));
            }
        }
        drop(roots);
        if changed || rescan {
            self.wake.notify_one();
        }
    }
    fn directory_events(&self) -> bool {
        #[cfg(test)]
        if self.directory_registration.load(Ordering::Acquire) {
            return true;
        }
        !cfg!(target_os = "macos")
    }
    fn new_claim_owner(&self, root: &Path, root_live: Arc<AtomicBool>) -> Arc<ClaimLease> {
        let mut state = self.roots.lock().expect("skills roots lock");
        state.next_owner = state
            .next_owner
            .checked_add(1)
            .expect("skills claim identity exhausted");
        let lease = Arc::new(ClaimLease {
            id: state.next_owner,
            root: root.to_path_buf(),
            live: AtomicBool::new(true),
            root_live,
        });
        state.owners.insert(
            lease.id,
            OwnerClaims {
                lease: lease.clone(),
                paths: BTreeSet::new(),
            },
        );
        lease
    }
    fn claim_path(&self, lease: &Arc<ClaimLease>, path: &Path, subtree: bool) {
        let mut state = self.roots.lock().expect("skills roots lock");
        // Shutdown compensation can still write files after ownership is revoked.
        // Such writes must never revive the retired lease.
        if !lease.valid() {
            return;
        }
        let Some(owner) = state.owners.get_mut(&lease.id) else {
            return;
        };
        owner.paths.insert(path.to_path_buf());
        let claim = state
            .owned_paths
            .entry(path.to_path_buf())
            .or_default()
            .entry(lease.root.clone())
            .or_insert_with(|| PathClaim {
                lease: lease.clone(),
                subtree,
            });
        if Arc::ptr_eq(&claim.lease, lease) {
            claim.subtree |= subtree;
        } else {
            *claim = PathClaim {
                lease: lease.clone(),
                subtree,
            };
        }
    }
    fn retire_claims(&self, lease: &ClaimLease) {
        // No mutex or path walk in Job::drop, including drops inside roots lock.
        if lease.live.swap(false, Ordering::AcqRel) {
            self.claims_retired.store(true, Ordering::Release);
        }
    }
    fn cleanup_claims(&self) -> bool {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut state = self.roots.lock().expect("skills roots lock");
        if self.claims_retired.swap(false, Ordering::AcqRel) {
            if state.cleanup_running {
                // Do not restart an in-progress pass on every job completion:
                // that could starve retired owners after long-lived jobs.
                state.cleanup_again = true;
            } else {
                state.cleanup_running = true;
                state.cleanup_after = None;
            }
        }
        if !state.cleanup_running {
            return false;
        }
        let mut allowance = CLAIM_CLEANUP_QUANTUM;
        while allowance > 0 {
            let next = match state.cleanup_after {
                Some(after) => state.owners.range((Excluded(after), Unbounded)).next(),
                None => state.owners.iter().next(),
            }
            .map(|(id, owner)| (*id, owner.lease.clone()));
            let Some((id, lease)) = next else {
                if state.cleanup_again {
                    state.cleanup_again = false;
                    state.cleanup_after = None;
                    continue;
                }
                state.cleanup_running = false;
                break;
            };
            if lease.valid() {
                allowance -= 1;
                #[cfg(test)]
                self.claim_cleanup_owners.fetch_add(1, Ordering::Relaxed);
                state.cleanup_after = Some(id);
                continue;
            }
            // Stay on this owner until its inverse index is exhausted. Each
            // removal is a point lookup, never a retain over unrelated claims.
            let path = state
                .owners
                .get_mut(&id)
                .expect("claim owner")
                .paths
                .pop_first();
            allowance -= 1;
            if let Some(path) = path {
                #[cfg(test)]
                self.claim_cleanup_records.fetch_add(1, Ordering::Relaxed);
                if let Some(claims) = state.owned_paths.get_mut(&path) {
                    if claims
                        .get(&lease.root)
                        .is_some_and(|claim| Arc::ptr_eq(&claim.lease, &lease))
                    {
                        claims.remove(&lease.root);
                    }
                    if claims.is_empty() {
                        state.owned_paths.remove(&path);
                    }
                }
            } else {
                #[cfg(test)]
                self.claim_cleanup_owners.fetch_add(1, Ordering::Relaxed);
                state.owners.remove(&id);
                state.cleanup_after = Some(id);
            }
        }
        state.cleanup_running
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
    live: Arc<AtomicBool>,
    claims: Arc<ClaimLease>,
    root: PathBuf,
    incarnation: u64,
    stop: CancellationToken,
}

impl JobFence {
    fn claim_path(&self, path: &Path, subtree: bool) {
        self.signals.claim_path(&self.claims, path, subtree);
    }
    fn check(&self) -> Result<()> {
        if !self.valid() {
            bail!("stale skills root incarnation {}", self.incarnation);
        }
        Ok(())
    }
    fn valid(&self) -> bool {
        !self.stop.is_cancelled() && self.live.load(Ordering::Acquire)
    }
}

enum Progress {
    Quantum,
    Changed(String),
    Failed,
    Waiting(Instant),
}
struct Job {
    signals: Arc<Signals>,
    claims: Arc<ClaimLease>,
    incarnation: u64,
    generation: u64,
    stream: Pin<Box<dyn Stream<Item = Result<Progress>> + Send>>,
    failed: bool,
    resume_at: Instant,
}

impl Drop for Job {
    fn drop(&mut self) {
        self.signals.retire_claims(&self.claims);
    }
}

struct WatchWalk {
    initial: std::collections::VecDeque<(PathBuf, RecursiveMode)>,
    directories: Vec<(PathBuf, std::fs::ReadDir)>,
    failed: bool,
    full: bool,
    prune_after: Option<PathBuf>,
}
struct NativePlan {
    paths: BTreeMap<PathBuf, u64>,
    failed_paths: BTreeMap<PathBuf, RecursiveMode>,
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
            failed_paths: BTreeMap::new(),
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Observation {
    Subscribed,
    Registering,
    Degraded,
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
    fn observation(&self, root: &Path) -> Observation {
        if self.watcher.is_none() && self.recovery_pending {
            return Observation::Degraded;
        }
        if self.recovery_pending {
            return Observation::Registering;
        }
        match self.plans.get(root) {
            Some(plan)
                if !plan.pending
                    && plan.walk.is_none()
                    && plan.failed_paths.is_empty()
                    && plan.initialized =>
            {
                Observation::Subscribed
            }
            Some(plan) if plan.attempts != 0 && plan.walk.is_none() => Observation::Degraded,
            _ => Observation::Registering,
        }
    }
    fn ready(&self, root: &Path) -> bool {
        self.observation(root) != Observation::Registering
    }
    fn refresh(
        mut self,
        paths: Vec<PathBuf>,
        changed: BTreeSet<PathBuf>,
        signals: Arc<Signals>,
    ) -> Self {
        #[cfg(test)]
        if signals
            .registration_panic_once
            .swap(false, Ordering::AcqRel)
        {
            panic!("injected native registration unwind");
        }
        let now = Instant::now();
        // Consume independently: short-circuiting on recovery_pending strands
        // the signal. A signal arriving after this swap stays pending for the
        // next worker turn, including one emitted by the new backend itself.
        let signalled = signals.recover.swap(false, Ordering::AcqRel);
        if signalled && !self.recovery_pending {
            // Callback may race the worker's swap and this blocking quantum.
            // Begin a delayed recovery instead of bypassing retry policy.
            for dirty in signals
                .roots
                .lock()
                .expect("skills roots lock")
                .values_mut()
            {
                dirty.live.store(false, Ordering::Release);
                dirty.live = Arc::new(AtomicBool::new(true));
                dirty.mark(now, true);
            }
            self.request_recovery();
            return self;
        }
        let recovering = self.recovery_pending || signalled;
        if recovering {
            self.recovery_pending = false;
            self.watcher.take();
            self.watched.clear();
            self.plans.clear();
            self.sync_after = None;
        }
        if self.watcher.is_none() {
            #[cfg(test)]
            signals.backend_attempts.fetch_add(1, Ordering::Relaxed);
            #[cfg(test)]
            if signals.backend_unavailable.load(Ordering::Acquire) {
                self.request_recovery();
                return self;
            }
            let callback = signals.clone();
            match RecommendedWatcher::new(
                move |event| {
                    #[cfg(test)]
                    if callback.native_events_disabled.load(Ordering::Acquire) {
                        return;
                    }
                    callback.event(event);
                },
                notify::Config::default().with_follow_symlinks(false),
            ) {
                Ok(watcher) => {
                    self.watcher = Some(watcher);
                    // Failed constructor retries do not rescan clean degraded
                    // roots. Recovered observation creates one mandatory rescan.
                    if recovering {
                        signals.invalidate_all();
                    }
                    #[cfg(test)]
                    signals.backend_creations.fetch_add(1, Ordering::Relaxed);
                    #[cfg(test)]
                    if signals.signal_during_recovery.swap(false, Ordering::AcqRel) {
                        signals.recover.store(true, Ordering::Release);
                    }
                }
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
                plan.walk = None;
                plan.failed_paths.clear();
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
                    || ((plan.pending || plan.walk.is_some() || !plan.failed_paths.is_empty())
                        && plan.retry_at <= now)
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
                    if !active.contains(&root) {
                        plan.failed_paths.clear();
                    }
                    let full = plan.pending || plan.failed_paths.is_empty();
                    if full {
                        plan.failed_paths.clear();
                    }
                    let initial = if !full {
                        std::mem::take(&mut plan.failed_paths).into_iter().collect()
                    } else if active.contains(&root) {
                        watch_plan(&root)?
                    } else {
                        Vec::new()
                    };
                    plan.walk = Some(WatchWalk {
                        initial: initial.into(),
                        directories: Vec::new(),
                        failed: false,
                        full,
                        prune_after: None,
                    });
                    plan.pending = false;
                }
                for _ in 0..64 {
                    let walk = plan.walk.as_mut().expect("registration cursor");
                    let next = if let Some(item) = walk.initial.pop_front() {
                        Some(item)
                    } else if let Some((parent, entries)) = walk.directories.last_mut() {
                        match entries.next() {
                            None => {
                                walk.directories.pop();
                                continue;
                            }
                            Some(entry) => {
                                let entry = match entry {
                                    Ok(entry) => entry,
                                    Err(_) => {
                                        walk.failed = true;
                                        plan.failed_paths
                                            .insert(parent.clone(), RecursiveMode::Recursive);
                                        continue;
                                    }
                                };
                                let path = entry.path();
                                let metadata = match registration_metadata(&signals, &path) {
                                    Ok(metadata) => metadata,
                                    Err(_) => {
                                        walk.failed = true;
                                        plan.failed_paths.insert(path, RecursiveMode::Recursive);
                                        continue;
                                    }
                                };
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
                        if !walk.full {
                            match registration_metadata(&signals, &path) {
                                Ok(metadata)
                                    if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
                                Ok(_) => continue,
                                Err(_) => {
                                    walk.failed = true;
                                    plan.failed_paths.insert(path, requested);
                                    continue;
                                }
                            }
                        }
                        // Our staging trees have no catalog snapshot to subscribe.
                        let owned = signals
                            .roots
                            .lock()
                            .expect("skills roots lock")
                            .owned_paths
                            .get(&path)
                            .is_some_and(|claims| {
                                claims
                                    .values()
                                    .any(|claim| claim.subtree && claim.lease.valid())
                            });
                        if owned {
                            continue;
                        }
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
                                // Replacing a shared sentinel also suspends the
                                // other owners' old snapshots. Rechecking their
                                // plans must not revive an already prepared job.
                                for owner in &owner_roots {
                                    if owner == &root {
                                        continue;
                                    }
                                    if let Some(other) = self.plans.get_mut(owner) {
                                        other.pending = true;
                                        other.rescan_after_registration = true;
                                    }
                                    if let Some(dirty) = signals
                                        .roots
                                        .lock()
                                        .expect("skills roots lock")
                                        .get_mut(owner)
                                    {
                                        dirty.live.store(false, Ordering::Release);
                                        dirty.live = Arc::new(AtomicBool::new(true));
                                        dirty.mark(now, true);
                                    }
                                }
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
                                if self.fail_watch_once.remove(&path)
                                    || registration_fault(&signals, &path, "watch").is_err()
                                {
                                    walk.failed = true;
                                    plan.failed_paths.insert(path.clone(), requested);
                                    continue;
                                }
                            }
                            if self.watcher.as_mut().unwrap().watch(&path, mode).is_err() {
                                walk.failed = true;
                                plan.failed_paths.insert(path.clone(), requested);
                                continue;
                            }
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
                            match registration_read_dir(&signals, &path) {
                                Ok(entries) => walk.directories.push((path.clone(), entries)),
                                Err(_) => {
                                    walk.failed = true;
                                    plan.failed_paths.insert(path.clone(), requested);
                                }
                            }
                        }
                        continue;
                    }
                    if walk.failed {
                        // Keep all proven subscriptions: an unreadable directory
                        // is not evidence that its old descendants disappeared.
                        bail!("incomplete native directory registration");
                    }
                    if !walk.full {
                        plan.walk = None;
                        plan.replacement = false;
                        break;
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
                plan.pending = plan.failed_paths.is_empty();
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
                    failed_directories = plan.failed_paths.len(),
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
                    !active.contains(root)
                        || plan.pending
                        || plan.walk.is_some()
                        || !plan.failed_paths.is_empty()
                }),
        );
        self.retry_at = self
            .plans
            .values()
            .filter(|p| p.pending || p.walk.is_some() || !p.failed_paths.is_empty())
            .map(|p| p.retry_at)
            .min()
            .unwrap_or(now);
        self
    }
}

fn registration_fault(_signals: &Signals, _path: &Path, _point: &'static str) -> Result<()> {
    #[cfg(test)]
    if _signals
        .registration_faults
        .lock()
        .unwrap()
        .contains(&(_path.to_path_buf(), _point))
    {
        bail!("injected native directory registration failure");
    }
    Ok(())
}
fn registration_metadata(signals: &Signals, path: &Path) -> Result<std::fs::Metadata> {
    registration_fault(signals, path, "metadata")?;
    Ok(std::fs::symlink_metadata(path)?)
}
fn registration_read_dir(signals: &Signals, path: &Path) -> Result<std::fs::ReadDir> {
    registration_fault(signals, path, "read_dir")?;
    Ok(std::fs::read_dir(path)?)
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

pub(super) async fn owned_fs<T: Send + 'static>(
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
    #[cfg(test)]
    if signals.directory_registration.load(Ordering::Acquire) {
        native.recursive = false;
    }
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
                    // Suspend the old snapshot before changing subscriptions.
                    // Its prepared guards remain false even after registration.
                    if dirty.watch_fence {
                        dirty.watch_fence = false;
                        dirty.live.store(false, Ordering::Release);
                        dirty.live = Arc::new(AtomicBool::new(true));
                        if let Some(job) = jobs.remove(path) {
                            // Cancellation of attempted work must not bypass
                            // poison backoff and create a staging/cleanup loop.
                            dirty.finish(job.incarnation, job.generation, false, Instant::now());
                        }
                    } else {
                        rewatch.remove(path);
                        if let Some(plan) = native.plans.get_mut(path) {
                            plan.pending = true;
                            plan.walk = None;
                            plan.rescan_after_registration = true;
                        }
                    }
                    refresh_watches = true;
                }
            }
        }
        if signals.overflow.swap(false, Ordering::AcqRel) {
            signals.invalidate_all();
            rewatch.extend(registrations.keys().cloned());
            refresh_watches = true;
        }
        if signals.recover.swap(false, Ordering::AcqRel) {
            if !native.recovery_pending {
                for dirty in signals
                    .roots
                    .lock()
                    .expect("skills roots lock")
                    .values_mut()
                {
                    dirty.live.store(false, Ordering::Release);
                    dirty.live = Arc::new(AtomicBool::new(true));
                    dirty.mark(Instant::now(), true);
                }
                jobs.clear();
            }
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
                    dirty.retain(|path, dirty| {
                        let keep = next.contains_key(path);
                        if !keep {
                            dirty.live.store(false, Ordering::Release);
                        }
                        keep
                    });
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
                                old.live.store(false, Ordering::Release);
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
                    #[cfg(test)]
                    signals.snapshot_failed.notify_one();
                    warn!("skills Workspace snapshot failed; retry is retained");
                }
            }
        }
        if stop.is_cancelled() {
            break;
        }
        if refresh_watches && Instant::now() >= native.retry_at {
            for path in &rewatch {
                let cancelled = jobs.remove(path);
                if let Some(dirty) = signals
                    .roots
                    .lock()
                    .expect("skills roots lock")
                    .get_mut(path)
                {
                    dirty.live.store(false, Ordering::Release);
                    dirty.live = Arc::new(AtomicBool::new(true));
                    if let Some(job) = cancelled {
                        dirty.finish(job.incarnation, job.generation, false, Instant::now());
                    }
                }
            }
            let was_recovering = native.recovery_pending;
            let recovery_attempts = native.attempts;
            let was_degraded = jobs
                .keys()
                .filter(|root| native.observation(root) == Observation::Degraded)
                .cloned()
                .collect::<Vec<_>>();
            let paths = registrations.keys().cloned().collect();
            let callback = signals.clone();
            let moved = std::mem::replace(&mut native, Native::new());
            let changed = std::mem::take(&mut rewatch);
            let registration_stop = stop.clone();
            // The closure owns Native: even cancellation before its first poll
            // must not drop a native handle (and possibly join its backend) on
            // the reactor. Transfer it through a joined blocking job regardless.
            match owned_fs(&CancellationToken::new(), move || {
                if registration_stop.is_cancelled() {
                    Ok(moved)
                } else {
                    Ok(moved.refresh(paths, changed, callback))
                }
            })
            .await
            {
                Ok(updated) => {
                    native = updated;
                    if !was_recovering && native.recovery_pending {
                        jobs.clear();
                    }
                    for path in was_degraded {
                        if native.observation(&path) != Observation::Degraded {
                            if let Some(dirty) = signals
                                .roots
                                .lock()
                                .expect("skills roots lock")
                                .get_mut(&path)
                            {
                                dirty.live.store(false, Ordering::Release);
                                dirty.live = Arc::new(AtomicBool::new(true));
                                dirty.mark(Instant::now(), true);
                            }
                            jobs.remove(&path);
                        }
                    }
                    refresh_watches = native.attempts != 0;
                }
                Err(_) if stop.is_cancelled() => break,
                Err(_) => {
                    // The moved Native was dropped by the joined unwinding job.
                    // Stay in this worker; clean degraded roots need no repeated
                    // rescan on each constructor retry.
                    warn!("skills native registration job failed; observation recovery retained");
                    for dirty in signals
                        .roots
                        .lock()
                        .expect("skills roots lock")
                        .values_mut()
                    {
                        dirty.live.store(false, Ordering::Release);
                        dirty.live = Arc::new(AtomicBool::new(true));
                        dirty.mark(Instant::now(), true);
                    }
                    jobs.clear();
                    native.attempts = recovery_attempts;
                    native.request_recovery();
                    refresh_watches = true;
                }
            }
        }

        if signals.recover.load(Ordering::Acquire)
            || signals
                .roots
                .lock()
                .expect("skills roots lock")
                .values()
                .any(|dirty| dirty.watch_dirty)
        {
            continue;
        }
        let claims_pending = signals.cleanup_claims();
        #[cfg(test)]
        if claims_pending && signals.pause_claim_cleanup.swap(false, Ordering::AcqRel) {
            *signals.paused_job_deadlines.lock().unwrap() = jobs
                .iter()
                .map(|(path, job)| (path.clone(), job.resume_at))
                .collect();
            signals.paused.notify_one();
            tokio::select! { biased; _ = stop.cancelled() => {}, _ = signals.resume.notified() => {} }
        }
        let now = Instant::now();
        let ready = {
            let roots = signals.roots.lock().expect("skills roots lock");
            registrations
                .keys()
                .filter(|root| {
                    (native.ready(root)
                        || (jobs.contains_key(*root)
                            && !native.recovery_pending
                            && native
                                .plans
                                .get(*root)
                                .is_some_and(|plan| !plan.replacement)))
                        && (jobs.get(*root).is_some_and(|job| job.resume_at <= now)
                            || (!jobs.contains_key(*root)
                                && roots
                                    .get(*root)
                                    .and_then(Dirty::due)
                                    .is_some_and(|due| due <= now)))
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
                let (job_incarnation, generation, live) = {
                    let mut roots = signals.roots.lock().expect("skills roots lock");
                    let dirty = roots.get_mut(&path).expect("registered root");
                    dirty.first = None;
                    (dirty.incarnation, dirty.generation, dirty.live.clone())
                };
                #[cfg(test)]
                signals.root_rounds.fetch_add(1, Ordering::Relaxed);
                let registration = &registrations[&path];
                let claims = signals.new_claim_owner(&path, live.clone());
                let fence = JobFence {
                    claims: claims.clone(),
                    signals: signals.clone(),
                    live,
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
                        signals: signals.clone(),
                        claims,
                        incarnation: job_incarnation,
                        generation,
                        stream,
                        failed: false,
                        resume_at: now,
                    },
                );
            }
            let job = jobs.get_mut(&path).expect("job exists");
            let progress = std::panic::AssertUnwindSafe(job.stream.next())
                .catch_unwind()
                .await;
            match progress {
                Ok(Some(Ok(Progress::Quantum))) => {
                    #[cfg(test)]
                    {
                        let pause = signals
                            .pause_root
                            .lock()
                            .unwrap()
                            .take_if(|root| root == &path)
                            .is_some();
                        if pause {
                            signals.paused.notify_one();
                            tokio::select! { biased; _ = stop.cancelled() => {}, _ = signals.resume.notified() => {} }
                        }
                    }
                }
                Ok(Some(Ok(Progress::Changed(scope)))) => {
                    registrations
                        .get_mut(&path)
                        .expect("registered root")
                        .changed
                        .insert(scope);
                }
                Ok(Some(Ok(Progress::Waiting(until)))) => {
                    job.resume_at = until;
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
                    signals.retire_claims(&job.claims);
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
        if claims_pending {
            tokio::task::yield_now().await;
            continue;
        }
        let next_due = signals
            .roots
            .lock()
            .expect("skills roots lock")
            .iter()
            .filter(|(path, _)| {
                registrations.contains_key(*path)
                    && (native.ready(path)
                        || (jobs.contains_key(*path)
                            && !native.recovery_pending
                            && native
                                .plans
                                .get(*path)
                                .is_some_and(|plan| !plan.replacement)))
            })
            .filter_map(|(path, dirty)| {
                jobs.get(path)
                    .map(|job| job.resume_at)
                    .or_else(|| dirty.due())
            })
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
    // Resume a suspended publisher so pre-DB cancellation can compensate a
    // known refusal. Unpublished cursors fail their stop check immediately.
    for job in jobs.values() {
        signals.retire_claims(&job.claims);
    }
    for (_, mut job) in jobs {
        loop {
            match std::panic::AssertUnwindSafe(job.stream.next())
                .catch_unwind()
                .await
            {
                Ok(Some(Ok(_))) => {}
                _ => break,
            }
        }
    }
    while signals.cleanup_claims() {
        tokio::task::yield_now().await;
    }
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
            let import = this.configured_root_import_config(&workspace.id)?;
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
