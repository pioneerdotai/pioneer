use std::fs::{File, OpenOptions};
use std::path::Path;
use std::thread;
use std::time::Duration;

use fs2::FileExt;
use same_file::Handle;

use crate::error::{MemvidError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockMode {
    None,
    Shared,
    Exclusive,
}

/// File lock guard that can hold either a shared or exclusive OS lock.
pub struct FileLock {
    file: File,
    mode: LockMode,
}

impl FileLock {
    /// Opens a file at `path` with read/write permissions and acquires an exclusive lock.
    pub fn open_and_lock(path: &Path) -> Result<(File, Self)> {
        Self::open_path_and_lock(path, false, LockMode::Exclusive)
    }

    /// Opens or creates a file at `path` and acquires an exclusive lock.
    ///
    /// The file is deliberately not truncated until the caller holds the returned lock.
    pub(crate) fn open_or_create_and_lock(path: &Path) -> Result<(File, Self)> {
        Self::open_path_and_lock(path, true, LockMode::Exclusive)
    }

    /// Opens a file at `path` with read/write permissions and acquires a shared lock.
    pub fn open_read_only(path: &Path) -> Result<(File, Self)> {
        Self::open_path_and_lock(path, false, LockMode::Shared)
    }

    /// Returns a non-locking guard for callers that only require a stable clone handle.
    pub fn unlocked(file: &File) -> Result<Self> {
        Ok(Self {
            file: file.try_clone()?,
            mode: LockMode::None,
        })
    }

    /// Clones the provided file handle and locks it exclusively.
    pub fn acquire(file: &File, _path: &Path) -> Result<Self> {
        Self::acquire_with_mode(file, LockMode::Exclusive)
    }

    /// Attempts a non-blocking exclusive lock, returning None if already locked.
    pub fn try_acquire(_file: &File, path: &Path) -> Result<Option<Self>> {
        loop {
            let clone = OpenOptions::new().read(true).write(true).open(path)?;
            match clone.try_lock_exclusive() {
                Ok(()) => {
                    if !Self::is_current_path_file(&clone, path)? {
                        clone
                            .unlock()
                            .map_err(|err| MemvidError::Lock(err.to_string()))?;
                        continue;
                    }
                    return Ok(Some(Self {
                        file: clone,
                        mode: LockMode::Exclusive,
                    }));
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(err) => return Err(MemvidError::Lock(err.to_string())),
            }
        }
    }

    /// Releases the underlying OS file lock.
    pub fn unlock(&mut self) -> Result<()> {
        if self.mode == LockMode::None {
            return Ok(());
        }
        let result = self.file.unlock();
        // After an unlock error the physical state is not trustworthy. Report None rather than
        // allowing callers to rely on a shared/exclusive mode that may no longer exist.
        self.mode = LockMode::None;
        result.map_err(|err| MemvidError::Lock(err.to_string()))
    }

    /// Exposes a clone of the locked handle for buffered operations.
    pub fn clone_handle(&self) -> Result<File> {
        Ok(self.file.try_clone()?)
    }

    #[must_use]
    pub fn mode(&self) -> LockMode {
        self.mode
    }

    pub fn downgrade_to_shared(&mut self) -> Result<()> {
        self.transition_to(LockMode::Exclusive, LockMode::Shared)
    }

    pub(crate) fn downgrade_to_shared_for_path(&mut self, path: &Path) -> Result<()> {
        self.downgrade_to_shared()?;
        self.validate_current_path(path, "downgrade")
    }

    pub fn upgrade_to_exclusive(&mut self) -> Result<()> {
        self.upgrade_to_exclusive_inner()?;
        Ok(())
    }

    pub(crate) fn upgrade_to_exclusive_for_path(&mut self, path: &Path) -> Result<()> {
        self.upgrade_to_exclusive_inner()?;
        self.validate_current_path(path, "upgrade")
    }

    fn upgrade_to_exclusive_inner(&mut self) -> Result<()> {
        self.transition_to(LockMode::Shared, LockMode::Exclusive)
    }

    fn transition_to(&mut self, from: LockMode, to: LockMode) -> Result<()> {
        if self.mode == to {
            return Ok(());
        }
        if self.mode != from {
            return Err(MemvidError::Lock(format!(
                "cannot convert {:?} file lock to {:?}",
                self.mode, to
            )));
        }

        // flock conversion is not guaranteed to be atomic, and LockFileEx does not provide a
        // conversion operation. Make the unlock window explicit on every platform so the guard's
        // reported mode always describes the physical lock that is actually held.
        self.unlock()?;
        #[cfg(test)]
        run_transition_hook();

        Self::lock_with_retry(&self.file, to)?;
        self.mode = to;
        Ok(())
    }

    fn validate_current_path(&mut self, path: &Path, operation: &str) -> Result<()> {
        match Self::is_current_path_file(&self.file, path) {
            Ok(true) => Ok(()),
            Ok(false) => {
                let _ = self.unlock();
                Err(MemvidError::Lock(format!(
                    "cannot {operation} a stale snapshot after the file was replaced"
                )))
            }
            Err(err) => {
                let _ = self.unlock();
                Err(err)
            }
        }
    }

    pub(crate) fn acquire_with_mode(file: &File, mode: LockMode) -> Result<Self> {
        let clone = file.try_clone()?;
        Self::lock_with_retry(&clone, mode)?;
        Ok(Self { file: clone, mode })
    }

    fn open_path_and_lock(path: &Path, create: bool, mode: LockMode) -> Result<(File, Self)> {
        Self::open_path_and_lock_after_open(path, create, mode, |_| {})
    }

    fn open_path_and_lock_after_open<F>(
        path: &Path,
        create: bool,
        mode: LockMode,
        mut after_open: F,
    ) -> Result<(File, Self)>
    where
        F: FnMut(&File),
    {
        loop {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(create)
                .open(path)?;
            after_open(&file);
            let guard = Self::acquire_with_mode(&file, mode)?;

            // An atomic publisher may have replaced `path` after this file was opened but
            // before its old inode became lockable. Never return that stale inode to a writer.
            if Self::is_current_path_file(&file, path)? {
                return Ok((file, guard));
            }
            drop(guard);
        }
    }

    pub(crate) fn is_current_path_file(file: &File, path: &Path) -> Result<bool> {
        let opened = Handle::from_file(file.try_clone()?)?;
        match Handle::from_path(path) {
            Ok(current) => Ok(opened == current),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    fn lock_with_retry(file: &File, mode: LockMode) -> Result<()> {
        const MAX_ATTEMPTS: u32 = 200; // ~10 seconds with 50ms backoff
        const BACKOFF: Duration = Duration::from_millis(50);
        #[cfg(test)]
        let max_attempts = LOCK_MAX_ATTEMPTS.with(|value| value.get().unwrap_or(MAX_ATTEMPTS));
        #[cfg(not(test))]
        let max_attempts = MAX_ATTEMPTS;
        let mut attempts = 0;
        loop {
            let result = match mode {
                LockMode::None => return Ok(()),
                LockMode::Exclusive => file.try_lock_exclusive(),
                LockMode::Shared => FileExt::try_lock_shared(file),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    if attempts >= max_attempts {
                        return Err(MemvidError::Lock(
                            "exclusive access unavailable; file is in use by another process"
                                .to_string(),
                        ));
                    }
                    attempts += 1;
                    thread::sleep(BACKOFF);
                    continue;
                }
                Err(err) => return Err(MemvidError::Lock(err.to_string())),
            }
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.mode != LockMode::None {
            let _ = self.file.unlock();
        }
    }
}

#[cfg(test)]
thread_local! {
    static LOCK_MAX_ATTEMPTS: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
    static TRANSITION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn run_transition_hook() {
    TRANSITION_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
}

#[cfg(test)]
pub(crate) fn set_transition_hook(hook: impl FnOnce() + 'static) {
    TRANSITION_HOOK.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
}

#[cfg(test)]
pub(crate) fn set_lock_max_attempts(attempts: Option<u32>) {
    LOCK_MAX_ATTEMPTS.with(|value| value.set(attempts));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    #[test]
    #[cfg(not(target_os = "windows"))] // Windows has different file locking semantics
    fn acquiring_lock_blocks_second_writer() {
        let temp = NamedTempFile::new().expect("temp file");
        let path = temp.path();
        writeln!(&mut temp.as_file().try_clone().unwrap(), "seed").unwrap();

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("open file");
        let guard = FileLock::acquire(&file, path).expect("first lock succeeds");

        let second = FileLock::try_acquire(&file, path).expect("second lock attempt");
        assert!(second.is_none(), "lock should already be held");

        drop(guard);
        let third = FileLock::try_acquire(&file, path).expect("third lock attempt");
        assert!(third.is_some(), "lock released after drop");
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn waiter_opened_before_replacement_retries_current_inode() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("capsule.mv2");
        std::fs::write(&path, b"old").expect("write old file");
        let (_old_file, old_lock) = FileLock::open_and_lock(&path).expect("lock old file");

        let (opened_tx, opened_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let waiter_path = path.clone();
        let waiter = std::thread::spawn(move || {
            let mut first_open = true;
            let result = FileLock::open_path_and_lock_after_open(
                &waiter_path,
                false,
                LockMode::Exclusive,
                |_| {
                    if first_open {
                        first_open = false;
                        opened_tx.send(()).expect("signal opened old inode");
                        continue_rx.recv().expect("continue waiter");
                    }
                },
            );
            result_tx.send(result).expect("send waiter result");
        });

        opened_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter opened old inode");
        let staged_path = dir.path().join("staged.mv2");
        std::fs::write(&staged_path, b"new").expect("write staged file");
        let staged_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&staged_path)
            .expect("open staged file");
        let staged_lock = FileLock::acquire(&staged_file, &staged_path).expect("lock staged file");
        std::fs::rename(&staged_path, &path).expect("publish replacement");

        continue_tx.send(()).expect("release waiter hook");
        drop(old_lock);
        assert!(
            result_rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "waiter must retry the current inode and block on its lock"
        );
        drop(staged_lock);

        let (mut current_file, current_lock) = result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("waiter completed after current lock release")
            .expect("waiter locked current inode");
        let mut contents = String::new();
        current_file
            .read_to_string(&mut contents)
            .expect("read current file");
        assert_eq!(contents, "new");
        drop(current_lock);
        waiter.join().expect("waiter thread");
    }

    #[test]
    fn failed_upgrade_is_honestly_unlocked_and_cannot_be_reused() {
        let temp = NamedTempFile::new().expect("temp file");
        let path = temp.path();
        let (_first_file, mut first) = FileLock::open_read_only(path).expect("first reader");
        let (_second_file, second) = FileLock::open_read_only(path).expect("second reader");

        set_lock_max_attempts(Some(0));
        let error = first
            .upgrade_to_exclusive_for_path(path)
            .expect_err("second shared reader must exclude upgrade");
        set_lock_max_attempts(None);
        assert!(error.to_string().contains("access unavailable"));
        assert_eq!(first.mode(), LockMode::None);
        assert!(
            first.upgrade_to_exclusive().is_err(),
            "an unlocked guard must not be reusable as a stale writer"
        );
        assert!(first.downgrade_to_shared().is_err());

        let third_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("third open");
        assert!(
            FileLock::try_acquire(&third_file, path)
                .expect("third exclusive probe")
                .is_none(),
            "the second reader must still physically hold its shared lock"
        );
        drop(second);
        assert!(
            FileLock::try_acquire(&third_file, path)
                .expect("exclusive after reader release")
                .is_some(),
            "failed conversion must not leave a hidden physical lock"
        );
    }

    #[test]
    fn downgrade_really_allows_shared_and_still_excludes_exclusive() {
        let temp = NamedTempFile::new().expect("temp file");
        let path = temp.path();
        let (file, mut first) = FileLock::open_and_lock(path).expect("exclusive lock");

        first.downgrade_to_shared().expect("physical downgrade");
        assert_eq!(first.mode(), LockMode::Shared);
        let (_reader_file, reader) = FileLock::open_read_only(path).expect("second shared lock");
        assert!(
            FileLock::try_acquire(&file, path)
                .expect("exclusive probe")
                .is_none(),
            "exclusive must remain excluded while either reader lives"
        );

        drop(reader);
        first
            .upgrade_to_exclusive()
            .expect("upgrade after reader release");
        assert_eq!(first.mode(), LockMode::Exclusive);
        assert!(
            FileLock::try_acquire(&file, path)
                .expect("exclusive probe after upgrade")
                .is_none()
        );
    }

    #[test]
    fn replacement_during_conversion_invalidates_guard() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("capsule.mv2");
        std::fs::write(&path, b"old").expect("old file");
        let (_file, mut reader) = FileLock::open_read_only(&path).expect("shared old file");

        let hook_path = path.clone();
        let staged = dir.path().join("staged.mv2");
        set_transition_hook(move || {
            std::fs::write(&staged, b"new").expect("staged file");
            std::fs::rename(&staged, &hook_path).expect("replace path");
        });
        let error = reader
            .upgrade_to_exclusive_for_path(&path)
            .expect_err("old inode cannot become writer");
        assert!(error.to_string().contains("stale snapshot"));
        assert_eq!(reader.mode(), LockMode::None);

        let current = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("current file");
        assert!(
            FileLock::try_acquire(&current, &path)
                .expect("current inode probe")
                .is_some()
        );
    }
}
