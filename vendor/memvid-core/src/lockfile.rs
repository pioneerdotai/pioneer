use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use fs_err::{self as fs, File, OpenOptions};

use crate::error::{LockOwnerHint, LockedError, Result};
use crate::registry::{self, FileId, LockRecord};

const DEFAULT_TIMEOUT_MS: u64 = 250;
const DEFAULT_HEARTBEAT_MS: u64 = 2_000;
const DEFAULT_STALE_GRACE_MS: u64 = 10_000;
const SPIN_SLEEP_MS: u64 = 10;

fn default_command() -> String {
    std::env::args().collect::<Vec<_>>().join(" ")
}

fn lockfile_path(path: &Path) -> PathBuf {
    let mut lock_path = path.to_path_buf();
    let suffix = match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if !ext.is_empty() => format!("{ext}.lock"),
        _ => "lock".to_string(),
    };
    lock_path.set_extension(suffix);
    lock_path
}

#[derive(Debug, Clone)]
pub struct LockOptions<'a> {
    pub timeout: Duration,
    pub heartbeat: Duration,
    pub stale_grace: Duration,
    pub command: Option<&'a str>,
    pub force_stale: bool,
}

impl Default for LockOptions<'_> {
    fn default() -> Self {
        Self {
            timeout: Duration::from_millis(DEFAULT_TIMEOUT_MS),
            heartbeat: Duration::from_millis(DEFAULT_HEARTBEAT_MS),
            stale_grace: Duration::from_millis(DEFAULT_STALE_GRACE_MS),
            command: None,
            force_stale: false,
        }
    }
}

impl<'a> LockOptions<'a> {
    #[must_use]
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout = Duration::from_millis(timeout_ms);
        self
    }

    #[must_use]
    pub fn heartbeat_ms(mut self, heartbeat_ms: u64) -> Self {
        self.heartbeat = Duration::from_millis(heartbeat_ms);
        self
    }

    #[must_use]
    pub fn stale_grace_ms(mut self, stale_grace_ms: u64) -> Self {
        self.stale_grace = Duration::from_millis(stale_grace_ms);
        self
    }

    #[must_use]
    pub fn command(mut self, command: &'a str) -> Self {
        self.command = Some(command);
        self
    }

    #[must_use]
    pub fn force_stale(mut self, force: bool) -> Self {
        self.force_stale = force;
        self
    }
}

#[allow(dead_code)]
pub struct LockfileGuard {
    lock_path: PathBuf,
    #[allow(dead_code)]
    file: File,
    file_id: FileId,
    record: LockRecord,
    heartbeat_interval: Duration,
}

#[allow(dead_code)]
impl LockfileGuard {
    pub fn heartbeat(&mut self) -> Result<()> {
        if self.heartbeat_interval.is_zero() {
            return Ok(());
        }
        self.record.touch()?;
        registry::write_record(&self.record)?;
        Ok(())
    }

    #[must_use]
    pub fn file_id(&self) -> &FileId {
        &self.file_id
    }

    #[must_use]
    pub fn owner_hint(&self) -> LockOwnerHint {
        self.record.to_owner_hint()
    }
}

impl Drop for LockfileGuard {
    fn drop(&mut self) {
        let _ = registry::remove_record(&self.file_id);
        let _ = fs::remove_file(&self.lock_path);
    }
}

pub fn acquire(path: &Path, options: LockOptions<'_>) -> Result<LockfileGuard> {
    let lock_path = lockfile_path(path);
    let file_id = registry::compute_file_id(path)?;
    let command = options
        .command
        .map_or_else(default_command, std::borrow::ToOwned::to_owned);
    let heartbeat_ms = options
        .heartbeat
        .as_millis()
        .try_into()
        .unwrap_or(DEFAULT_HEARTBEAT_MS);
    let record = LockRecord::new(&file_id, path, command, heartbeat_ms)?;
    let start = Instant::now();

    loop {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(file) => {
                if let Err(err) = registry::write_record(&record) {
                    let _ = fs::remove_file(&lock_path);
                    return Err(err);
                }
                return Ok(LockfileGuard {
                    lock_path,
                    file,
                    file_id,
                    record,
                    heartbeat_interval: options.heartbeat,
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = registry::read_record(&file_id)?;
                let stale = existing
                    .as_ref()
                    .is_none_or(|rec| registry::is_stale(rec, options.stale_grace));

                if options.force_stale && stale {
                    let _ = registry::remove_record(&file_id);
                    match fs::remove_file(&lock_path) {
                        Ok(()) => continue,
                        Err(inner) if inner.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(inner) => return Err(inner.into()),
                    }
                }

                if start.elapsed() >= options.timeout {
                    let hint = registry::to_owner_hint(existing.clone());
                    let message = existing
                        .as_ref()
                        .map(|rec| {
                            format!(
                                "memory locked by pid {} (cmd: {}) since {}",
                                rec.pid, rec.cmd, rec.started_at
                            )
                        })
                        .unwrap_or_else(|| "memory locked by another process".to_string());
                    return Err(Box::new(LockedError::new(
                        path.to_path_buf(),
                        message,
                        hint,
                        stale,
                    ))
                    .into());
                }

                let remaining = options
                    .timeout
                    .checked_sub(start.elapsed())
                    .unwrap_or_else(|| Duration::from_millis(SPIN_SLEEP_MS));
                let sleep = Duration::from_millis(SPIN_SLEEP_MS).min(remaining);
                thread::sleep(sleep);
            }
            Err(err) => return Err(err.into()),
        }
    }
}

pub fn current_owner(path: &Path) -> Result<Option<LockOwnerHint>> {
    let file_id = match registry::compute_file_id(path) {
        Ok(id) => id,
        Err(crate::error::MemvidError::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound =>
        {
            return Ok(None);
        }
        Err(err) => return Err(err),
    };
    let record = registry::read_record(&file_id)?;
    Ok(registry::to_owner_hint(record))
}
