use crate::apply_patch::file_mutation::{CanonicalTarget, TargetManifest};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct TargetLockRegistry {
    inner: Arc<RegistryInner>,
}

#[derive(Debug)]
struct RegistryInner {
    state: Mutex<RegistryState>,
    wake: Condvar,
}

#[derive(Debug, Default)]
struct RegistryState {
    next_ticket: u64,
    entries: HashMap<String, LockEntry>,
}

#[derive(Debug, Default)]
struct LockEntry {
    held: bool,
    queue: VecDeque<u64>,
}

impl Default for TargetLockRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                state: Mutex::new(RegistryState::default()),
                wake: Condvar::new(),
            }),
        }
    }
}

impl TargetLockRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquires every target in sorted identity order. Queue heads make
    /// conflicting waiters FIFO while the single state mutex prevents
    /// reverse-order deadlocks.
    pub fn acquire(
        &self,
        manifest: &TargetManifest,
        timeout: Duration,
    ) -> Result<TargetLockGuard, LockError> {
        let mut keys = manifest.identities().map(str::to_owned).collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        self.acquire_keys(keys, timeout)
    }

    pub fn acquire_targets(
        &self,
        targets: &[CanonicalTarget],
        timeout: Duration,
    ) -> Result<TargetLockGuard, LockError> {
        let mut keys = targets
            .iter()
            .map(|target| target.identity().to_owned())
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        self.acquire_keys(keys, timeout)
    }

    fn acquire_keys(
        &self,
        keys: Vec<String>,
        timeout: Duration,
    ) -> Result<TargetLockGuard, LockError> {
        if keys.is_empty() {
            return Err(LockError::new(LockErrorCode::EmptyTargetSet));
        }
        let deadline = Instant::now() + timeout;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| LockError::new(LockErrorCode::Poisoned))?;
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        for key in &keys {
            state
                .entries
                .entry(key.clone())
                .or_default()
                .queue
                .push_back(ticket);
        }

        loop {
            let can_acquire = keys.iter().all(|key| {
                state
                    .entries
                    .get(key)
                    .is_some_and(|entry| !entry.held && entry.queue.front() == Some(&ticket))
            });
            if can_acquire {
                for key in &keys {
                    let entry = state.entries.get_mut(key).expect("queued lock entry");
                    entry.queue.pop_front();
                    entry.held = true;
                }
                return Ok(TargetLockGuard {
                    inner: Arc::clone(&self.inner),
                    keys,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                remove_ticket(&mut state, &keys, ticket);
                self.inner.wake.notify_all();
                return Err(LockError::new(LockErrorCode::Timeout));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, result) = self
                .inner
                .wake
                .wait_timeout(state, remaining)
                .map_err(|_| LockError::new(LockErrorCode::Poisoned))?;
            state = next_state;
            if result.timed_out() {
                remove_ticket(&mut state, &keys, ticket);
                self.inner.wake.notify_all();
                return Err(LockError::new(LockErrorCode::Timeout));
            }
        }
    }

    pub fn entry_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .map(|state| state.entries.len())
            .unwrap_or(0)
    }
}

#[derive(Debug)]
pub struct TargetLockGuard {
    inner: Arc<RegistryInner>,
    keys: Vec<String>,
}

impl TargetLockGuard {
    pub fn keys(&self) -> &[String] {
        &self.keys
    }
}

impl Drop for TargetLockGuard {
    fn drop(&mut self) {
        let Ok(mut state) = self.inner.state.lock() else {
            return;
        };
        for key in &self.keys {
            if let Some(entry) = state.entries.get_mut(key) {
                entry.held = false;
            }
        }
        state
            .entries
            .retain(|_, entry| entry.held || !entry.queue.is_empty());
        self.inner.wake.notify_all();
    }
}

fn remove_ticket(state: &mut RegistryState, keys: &[String], ticket: u64) {
    for key in keys {
        if let Some(entry) = state.entries.get_mut(key) {
            entry.queue.retain(|queued| *queued != ticket);
        }
    }
    state
        .entries
        .retain(|_, entry| entry.held || !entry.queue.is_empty());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockErrorCode {
    EmptyTargetSet,
    Timeout,
    Poisoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LockError {
    pub code: LockErrorCode,
}

impl LockError {
    pub const fn new(code: LockErrorCode) -> Self {
        Self { code }
    }
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "target lock failed: {:?}", self.code)
    }
}

impl std::error::Error for LockError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{TargetExpectation, TargetResolver, TargetRole};
    use std::sync::mpsc;
    use std::thread;

    fn manifest(root: &std::path::Path, names: &[&str]) -> TargetManifest {
        let resolver = TargetResolver::new(root).unwrap();
        TargetManifest::new(
            names
                .iter()
                .map(|name| {
                    resolver
                        .resolve(name, TargetRole::Source, TargetExpectation::Missing)
                        .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn reverse_order_requests_do_not_deadlock() {
        let root = tempfile::tempdir().unwrap();
        let registry = TargetLockRegistry::new();
        let first = manifest(root.path(), &["a", "b"]);
        let second = manifest(root.path(), &["b", "a"]);
        let held = registry.acquire(&first, Duration::from_millis(10)).unwrap();
        let (sender, receiver) = mpsc::channel();
        let clone = registry.clone();
        let thread = thread::spawn(move || {
            let result = clone.acquire(&second, Duration::from_millis(20));
            sender.send(result.is_err()).unwrap();
        });
        assert!(receiver.recv_timeout(Duration::from_millis(200)).unwrap());
        drop(held);
        thread.join().unwrap();
        assert_eq!(registry.entry_count(), 0);
    }

    #[test]
    fn disjoint_targets_progress_while_one_is_held() {
        let root = tempfile::tempdir().unwrap();
        let registry = TargetLockRegistry::new();
        let first = manifest(root.path(), &["a"]);
        let second = manifest(root.path(), &["b"]);
        let held = registry.acquire(&first, Duration::from_millis(10)).unwrap();
        let available = registry.acquire(&second, Duration::from_millis(20));
        assert!(available.is_ok());
        drop(available);
        drop(held);
        assert_eq!(registry.entry_count(), 0);
    }

    #[test]
    fn empty_and_timeout_are_structured() {
        let registry = TargetLockRegistry::new();
        let empty = TargetManifest::new(Vec::new()).unwrap();
        assert_eq!(
            registry.acquire(&empty, Duration::ZERO).unwrap_err().code,
            LockErrorCode::EmptyTargetSet
        );
        let root = tempfile::tempdir().unwrap();
        let one = manifest(root.path(), &["a"]);
        let _held = registry.acquire(&one, Duration::ZERO).unwrap();
        assert_eq!(
            registry.acquire(&one, Duration::ZERO).unwrap_err().code,
            LockErrorCode::Timeout
        );
    }
}
