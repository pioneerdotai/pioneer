//! Process-local executable digests, independent of provider readiness.
//! Synchronous: callers on an async runtime must use a blocking worker.
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, Metadata},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, OnceLock},
    time::UNIX_EPOCH,
};

const READY_LIMIT: usize = 64;
const FLIGHT_LIMIT: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    path: PathBuf,
    size: u64,
    #[cfg(unix)]
    stamp: (u64, u64, i64, i64, i64, i64, u32),
    #[cfg(not(unix))]
    modified: std::time::SystemTime,
}
impl Key {
    fn new(path: PathBuf, metadata: &Metadata) -> Result<Self> {
        ensure!(metadata.is_file(), "executable is not a regular file");
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            path,
            size: metadata.len(),
            #[cfg(unix)]
            stamp: (
                metadata.dev(),
                metadata.ino(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.ctime(),
                metadata.ctime_nsec(),
                metadata.mode(),
            ),
            #[cfg(not(unix))]
            modified: metadata.modified()?,
        })
    }
}

struct Opened {
    file: File,
    configured: PathBuf,
    key: Key,
}
impl Opened {
    fn new(path: &Path, size: u64, modified_nanos: u64) -> Result<Self> {
        let canonical = fs::canonicalize(path).context("resolve executable for digest")?;
        // Open the configured path too: a symlink retarget between resolve/open
        // must fail validation, rather than hash the previous target.
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = options.open(path).context("open executable for digest")?;
        let metadata = file.metadata()?;
        let key = Key::new(canonical, &metadata)?;
        let modified = metadata.modified()?.duration_since(UNIX_EPOCH)?;
        ensure!(
            key.size == size && modified.as_nanos().min(u64::MAX as u128) as u64 == modified_nanos,
            "executable changed since expected identity"
        );
        let opened = Self {
            file,
            configured: path.to_owned(),
            key,
        };
        opened.validate()?;
        Ok(opened)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            fs::canonicalize(&self.configured)? == self.key.path,
            "executable target changed"
        );
        ensure!(
            Key::new(self.key.path.clone(), &self.file.metadata()?)? == self.key,
            "opened executable changed"
        );
        ensure!(
            Key::new(self.key.path.clone(), &fs::metadata(&self.configured)?)? == self.key,
            "executable path changed"
        );
        Ok(())
    }
    fn hash(&mut self) -> Result<String> {
        let mut digest = Sha256::new();
        let mut remaining = self.key.size;
        let mut buffer = [0u8; 64 * 1024];
        while remaining != 0 {
            let limit = remaining.min(buffer.len() as u64) as usize;
            let count = self.file.read(&mut buffer[..limit])?;
            ensure!(count != 0, "executable shortened during digest");
            digest.update(&buffer[..count]);
            remaining -= count as u64;
        }
        ensure!(
            self.file.read(&mut buffer[..1])? == 0,
            "executable grew during digest"
        );
        self.validate()?;
        Ok(hex::encode(digest.finalize()))
    }
}

type Outcome = std::result::Result<String, String>;
#[derive(Default)]
struct Flight {
    result: Mutex<Option<Outcome>>,
    done: Condvar,
}
#[derive(Default)]
struct State {
    ready: std::collections::VecDeque<(Key, String)>,
    flights: std::collections::HashMap<Key, Arc<Flight>>,
}
#[derive(Default)]
struct Cache {
    #[cfg(any(test, feature = "test-support"))]
    hashes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    joined: (Mutex<usize>, Condvar),
    state: Mutex<State>,
}
impl Cache {
    fn digest(&self, path: &Path, size: u64, modified: u64) -> Result<String> {
        self.digest_with(path, size, modified, |opened| {
            #[cfg(any(test, feature = "test-support"))]
            self.hashes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            opened.hash()
        })
    }
    fn digest_with(
        &self,
        path: &Path,
        size: u64,
        modified: u64,
        hash: impl FnOnce(&mut Opened) -> Result<String>,
    ) -> Result<String> {
        {
            let mut opened = Opened::new(path, size, modified)?;
            // Platforms without a reliable stamp always hash; never reuse.
            if !cfg!(unix) {
                return hash(&mut opened);
            }
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((_, digest)) = state.ready.iter().find(|(key, _)| key == &opened.key) {
                let digest = digest.clone();
                drop(state);
                opened.validate()?;
                return Ok(digest);
            }
            if let Some(flight) = state.flights.get(&opened.key).cloned() {
                drop(state);
                #[cfg(test)]
                {
                    *self.joined.0.lock().unwrap() += 1;
                    self.joined.1.notify_all();
                }
                let mut result = flight.result.lock().unwrap_or_else(|e| e.into_inner());
                while result.is_none() {
                    result = flight.done.wait(result).unwrap_or_else(|e| e.into_inner());
                }
                let result = result.as_ref().unwrap().clone();
                opened.validate()?;
                return result.map_err(anyhow::Error::msg);
            }
            if state.flights.len() == FLIGHT_LIMIT {
                // Fail transiently rather than queue non-cancellable blocking
                // jobs before hash admission. Existing flights still coalesce.
                anyhow::bail!("executable digest capacity exhausted; retry later");
            }
            let key = opened.key.clone();
            let flight = Arc::new(Flight::default());
            state.flights.insert(key.clone(), flight.clone());
            drop(state);
            // Catch only to complete waiters/unregister on panic, then propagate.
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hash(&mut opened)));
            let result = match &outcome {
                Ok(Ok(digest)) => Ok(digest.clone()),
                Ok(Err(error)) => Err(format!("{error:#}")),
                Err(_) => Err("executable digest worker panicked".to_owned()),
            };
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Ok(digest) = &result {
                if state.ready.len() == READY_LIMIT {
                    state.ready.pop_front();
                }
                state.ready.push_back((key.clone(), digest.clone()));
            }
            *flight.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result.clone());
            state.flights.remove(&key);
            drop(state);
            flight.done.notify_all();
            if let Err(panic) = outcome {
                std::panic::resume_unwind(panic);
            }
            return result.map_err(anyhow::Error::msg);
        }
    }
}

/// Hash a provider executable, fencing the caller's existing public identity.
/// No caller can supply or publish a digest. Only verified Unix results persist.
pub fn executable_sha256(
    path: &Path,
    expected_size: u64,
    expected_modified_unix_nanos: u64,
) -> Result<String> {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(cache) = TEST_CACHE.with(|cache| cache.borrow().clone()) {
        return cache.digest(path, expected_size, expected_modified_unix_nanos);
    }
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE
        .get_or_init(Cache::default)
        .digest(path, expected_size, expected_modified_unix_nanos)
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    static TEST_CACHE: std::cell::RefCell<Option<Arc<Cache>>> = const { std::cell::RefCell::new(None) };
}

/// Isolated instrumentation for downstream crate regressions. Absent from
/// normal builds; cannot inject a digest or mark an entry verified.
#[cfg(feature = "test-support")]
#[derive(Clone, Default)]
pub struct DigestTestScope {
    cache: Arc<Cache>,
}
#[cfg(feature = "test-support")]
impl DigestTestScope {
    pub fn run_sync<T>(&self, work: impl FnOnce() -> T) -> T {
        struct Reset(Option<Arc<Cache>>);
        impl Drop for Reset {
            fn drop(&mut self) {
                TEST_CACHE.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _reset = Reset(TEST_CACHE.with(|slot| slot.replace(Some(self.cache.clone()))));
        work()
    }
    pub fn hash_invocations(&self) -> usize {
        self.cache.hashes.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    fn identity(path: &Path) -> (u64, u64) {
        let metadata = fs::metadata(path).unwrap();
        (
            metadata.len(),
            metadata
                .modified()
                .unwrap()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        )
    }
    fn get(cache: &Cache, path: &Path, reads: &AtomicUsize) -> Result<String> {
        let (size, modified) = identity(path);
        cache.digest_with(path, size, modified, |file| {
            reads.fetch_add(1, Ordering::SeqCst);
            file.hash()
        })
    }

    fn wait_for_followers(cache: &Cache, count: usize) {
        let mut joined = cache.joined.0.lock().unwrap();
        while *joined < count {
            joined = cache.joined.1.wait(joined).unwrap();
        }
    }

    #[test]
    fn claude_codex_contracts_share_verified_content_digest() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cli");
        fs::write(&path, b"shared executable").unwrap();
        let cache = Arc::new(Cache::default());
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                TEST_CACHE.with(|slot| *slot.borrow_mut() = None);
            }
        }
        TEST_CACHE.with(|slot| *slot.borrow_mut() = Some(cache.clone()));
        let _reset = Reset;
        let binary = path.to_str().unwrap();
        let first =
            crate::codex_attestation::attest_codex_executable(binary, "v1", &"a".repeat(64))
                .unwrap();
        let next_identity = crate::codex_attestation::codex_executable_cache_identity(
            binary,
            "v2",
            &"b".repeat(64),
        )
        .unwrap();
        let second =
            crate::codex_attestation::attest_codex_executable_identity(next_identity).unwrap();
        let claude_identity =
            crate::claude::claude_executable_cache_identity(binary, "v3", &"c".repeat(64)).unwrap();
        let claude = crate::claude::attest_claude_executable_identity(claude_identity).unwrap();
        assert_eq!(first.binary_sha256, second.binary_sha256);
        assert_eq!(first.binary_sha256, claude.binary_sha256);
        assert_ne!(
            first.local_executable_fingerprint,
            second.local_executable_fingerprint
        );
        assert_ne!(
            first.local_executable_fingerprint,
            claude.local_executable_fingerprint
        );
        assert_eq!(cache.hashes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn reuse_changes_replacement_symlink_and_cold_cache() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cli");
        fs::write(&path, b"first").unwrap();
        let cache = Cache::default();
        let reads = AtomicUsize::new(0);
        let first = get(&cache, &path, &reads).unwrap();
        assert_eq!(first, hex::encode(Sha256::digest(b"first")));
        assert_eq!(get(&cache, &path, &reads).unwrap(), first);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        let old_key = Opened::new(&path, identity(&path).0, identity(&path).1)
            .unwrap()
            .key;
        let replacement = root.path().join("replacement");
        fs::write(&replacement, b"other").unwrap();
        File::options()
            .write(true)
            .open(&replacement)
            .unwrap()
            .set_modified(fs::metadata(&path).unwrap().modified().unwrap())
            .unwrap();
        fs::rename(&replacement, &path).unwrap();
        let new_key = Opened::new(&path, identity(&path).0, identity(&path).1)
            .unwrap()
            .key;
        assert_ne!(old_key, new_key);
        let replaced_digest = get(&cache, &path, &reads).unwrap();
        assert_ne!(replaced_digest, first);
        let removed_identity = identity(&path);
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert_eq!(
            get(&cache, &link, &reads).unwrap(),
            get(&cache, &path, &reads).unwrap()
        );
        let target = root.path().join("target");
        fs::write(&target, b"third").unwrap();
        fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let retargeted = get(&cache, &link, &reads).unwrap();
        assert_eq!(retargeted, hex::encode(Sha256::digest(b"third")));
        assert_ne!(retargeted, replaced_digest);
        fs::write(&target, b"fourth").unwrap();
        assert_eq!(
            get(&cache, &link, &reads).unwrap(),
            hex::encode(Sha256::digest(b"fourth"))
        );
        let before = reads.load(Ordering::SeqCst);
        get(&Cache::default(), &path, &reads).unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), before + 1);
        let removed_target_identity = identity(&link);
        fs::remove_file(&path).unwrap();
        assert!(
            cache
                .digest(&path, removed_identity.0, removed_identity.1)
                .is_err()
        );
        fs::remove_file(&target).unwrap();
        assert!(
            cache
                .digest(&link, removed_target_identity.0, removed_target_identity.1)
                .is_err()
        );
    }

    #[test]
    fn changes_during_read_io_error_and_panic_are_retryable() {
        let root = tempfile::tempdir().unwrap();
        let cache = Cache::default();
        for fault in 0..5 {
            let path = root.path().join(format!("cli-{fault}"));
            fs::write(&path, b"original").unwrap();
            let (size, modified) = identity(&path);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cache.digest_with(&path, size, modified, |opened| {
                    match fault {
                        0 => fs::write(&path, b"short").unwrap(),
                        1 => fs::write(&path, b"longer than original").unwrap(),
                        2 => {
                            let before = fs::metadata(&path).unwrap().modified().unwrap();
                            fs::write(&path, b"modified").unwrap();
                            File::options()
                                .write(true)
                                .open(&path)
                                .unwrap()
                                .set_modified(before + std::time::Duration::from_secs(2))
                                .unwrap();
                        }
                        3 => opened.file = File::open(root.path()).unwrap(),
                        _ => panic!("injected worker panic"),
                    }
                    opened.hash()
                })
            }));
            assert!(result.is_err() || result.unwrap().is_err());
            let reads = AtomicUsize::new(0);
            let digest = get(&cache, &path, &reads).unwrap();
            assert_eq!(
                reads.load(Ordering::SeqCst),
                1,
                "failure cannot publish a digest"
            );
            assert_eq!(
                digest,
                crate::codex_attestation::sha256_file_contents(&path).unwrap()
            );
        }
    }

    #[test]
    fn coalescing_and_eviction_count_actual_hashes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cli");
        fs::write(&path, b"unchanged").unwrap();
        let cache = Arc::new(Cache::default());
        let reads = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let cache = &cache;
            let reads = &reads;
            let path = &path;
            let leader = scope.spawn(move || {
                let (size, modified) = identity(&path);
                cache
                    .digest_with(&path, size, modified, |file| {
                        reads.fetch_add(1, Ordering::SeqCst);
                        started_tx.send(()).unwrap();
                        finish_rx.recv().unwrap();
                        file.hash()
                    })
                    .unwrap()
            });
            started_rx.recv().unwrap();
            let followers: Vec<_> = (0..8)
                .map(|_| scope.spawn(move || get(cache, path, reads).unwrap()))
                .collect();
            wait_for_followers(cache, 8);
            finish_tx.send(()).unwrap();
            let digest = leader.join().unwrap();
            for follower in followers {
                assert_eq!(follower.join().unwrap(), digest);
            }
        });
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        for index in 0..READY_LIMIT {
            let other = root.path().join(format!("other-{index}"));
            fs::write(&other, b"other").unwrap();
            get(&cache, &other, &reads).unwrap();
        }
        let before = reads.load(Ordering::SeqCst);
        get(&cache, &path, &reads).unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), before + 1);
    }

    #[test]
    fn saturation_keeps_live_flights_and_recovers_after_failure() {
        let root = tempfile::tempdir().unwrap();
        let cache = Arc::new(Cache::default());
        let barrier = Arc::new(std::sync::Barrier::new(FLIGHT_LIMIT + 1));
        let (started_tx, started_rx) = mpsc::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for index in 0..FLIGHT_LIMIT {
            let path = root.path().join(format!("cli-{index}"));
            fs::write(&path, b"fixture").unwrap();
            let cache = cache.clone();
            let barrier = barrier.clone();
            let started = started_tx.clone();
            let reads = reads.clone();
            workers.push(std::thread::spawn(move || {
                let (size, modified) = identity(&path);
                cache.digest_with(&path, size, modified, |_| {
                    reads.fetch_add(1, Ordering::SeqCst);
                    started.send(()).unwrap();
                    barrier.wait();
                    anyhow::bail!("injected failure")
                })
            }));
        }
        for _ in 0..FLIGHT_LIMIT {
            started_rx.recv().unwrap();
        }
        let extra = root.path().join("extra");
        fs::write(&extra, b"extra").unwrap();
        let extra_cache = cache.clone();
        let extra_reads = reads.clone();
        let (enter_tx, enter_rx) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            enter_tx.send(()).unwrap();
            get(&extra_cache, &extra, &extra_reads)
        });
        enter_rx.recv().unwrap();
        assert!(waiter.join().unwrap().is_err());
        assert_eq!(reads.load(Ordering::SeqCst), FLIGHT_LIMIT);
        let follower_cache = cache.clone();
        let follower_path = root.path().join("cli-0");
        let follower_reads = reads.clone();
        let follower =
            std::thread::spawn(move || get(&follower_cache, &follower_path, &follower_reads));
        wait_for_followers(&cache, 1);
        barrier.wait();
        assert!(follower.join().unwrap().is_err());
        for worker in workers {
            assert!(worker.join().unwrap().is_err());
        }
        let first = root.path().join("cli-0");
        assert_eq!(
            get(&cache, &first, &reads).unwrap(),
            hex::encode(Sha256::digest(b"fixture"))
        );
        assert_eq!(reads.load(Ordering::SeqCst), FLIGHT_LIMIT + 1);
    }

    #[test]
    fn failed_and_panicking_leaders_complete_existing_waiters() {
        for panic_worker in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("cli");
            fs::write(&path, b"fixture").unwrap();
            let cache = Arc::new(Cache::default());
            let (started_tx, started_rx) = mpsc::channel();
            let (finish_tx, finish_rx) = mpsc::channel();
            let worker_cache = cache.clone();
            let worker_path = path.clone();
            let leader = std::thread::spawn(move || {
                let (size, modified) = identity(&worker_path);
                worker_cache.digest_with(&worker_path, size, modified, |_| {
                    started_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                    if panic_worker {
                        panic!("injected panic");
                    }
                    anyhow::bail!("injected failure")
                })
            });
            started_rx.recv().unwrap();
            let follower_cache = cache.clone();
            let follower_path = path.clone();
            let unexpected_hashes = Arc::new(AtomicUsize::new(0));
            let follower_reads = unexpected_hashes.clone();
            let follower =
                std::thread::spawn(move || get(&follower_cache, &follower_path, &follower_reads));
            wait_for_followers(&cache, 1);
            finish_tx.send(()).unwrap();
            assert!(follower.join().unwrap().is_err());
            assert_eq!(unexpected_hashes.load(Ordering::SeqCst), 0);
            let result = leader.join();
            if panic_worker {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert_eq!(
                get(&cache, &path, &unexpected_hashes).unwrap(),
                hex::encode(Sha256::digest(b"fixture"))
            );
            assert_eq!(unexpected_hashes.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn cancelled_waiter_keeps_worker_capacity_and_digest_flight() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cli");
        fs::write(&path, b"unchanged").unwrap();
        let cache = Arc::new(Cache::default());
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let reads = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let worker_cache = cache.clone();
        let worker_path = path.clone();
        let worker_reads = reads.clone();
        let worker_slots = slots.clone();
        let leader = tokio::spawn(async move {
            let slot = worker_slots.acquire_owned().await.unwrap();
            pioneer_observability::work::spawn_blocking(move || {
                let _slot = slot;
                let (size, modified) = identity(&worker_path);
                worker_cache.digest_with(&worker_path, size, modified, |file| {
                    worker_reads.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    finish_rx.recv().unwrap();
                    file.hash()
                })
            })
            .await
            .unwrap()
        });
        started_rx.await.unwrap();
        leader.abort();
        let _ = leader.await;
        assert_eq!(slots.available_permits(), 0);
        let follower_cache = cache.clone();
        let follower_path = path.clone();
        let follower_reads = reads.clone();
        let follower = pioneer_observability::work::spawn_blocking(move || {
            get(&follower_cache, &follower_path, &follower_reads)
        });
        let observer_cache = cache.clone();
        pioneer_observability::work::spawn_blocking(move || wait_for_followers(&observer_cache, 1))
            .await
            .unwrap();
        finish_tx.send(()).unwrap();
        assert_eq!(
            follower.await.unwrap().unwrap(),
            hex::encode(Sha256::digest(b"unchanged"))
        );
        let permit = slots.acquire().await.unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        drop(permit);
    }
}

#[cfg(all(test, not(unix)))]
mod fallback_tests {
    use super::*;
    #[test]
    fn fallback_hashes_each_call_with_identical_sha() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("cli");
        fs::write(&path, b"fallback fixture").unwrap();
        let metadata = fs::metadata(&path).unwrap();
        let modified = metadata
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let cache = Cache::default();
        let first = cache.digest(&path, metadata.len(), modified).unwrap();
        let second = cache.digest(&path, metadata.len(), modified).unwrap();
        assert_eq!(first, hex::encode(Sha256::digest(b"fallback fixture")));
        assert_eq!(first, second);
        assert_eq!(cache.hashes.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}
