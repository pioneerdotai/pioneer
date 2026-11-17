//! Cooperative ownership for supported processes opening a writable database.
use anyhow::{Context, Result};
use fs4::FileExt;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug)]
pub struct WriterBusy;
impl std::fmt::Display for WriterBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("database writer is owned by another operation")
    }
}
impl std::error::Error for WriterBusy {}

pub struct WriterOwnership {
    _file: File,
}

impl WriterOwnership {
    /// Resolve the physical database's canonical pathname, including before
    /// first creation. Scoped runtime handles do not define process ownership.
    pub fn database_identity(path: &Path) -> Result<PathBuf> {
        let mut candidate = path.to_path_buf();
        for _ in 0..32 {
            match std::fs::canonicalize(&candidate) {
                Ok(path) => return Ok(path),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("canonicalize database identity"),
            }
            let parent = candidate
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            // A final symlink can precede its target's first creation. Resolve
            // it now so first startup and the later existing DB use one lock.
            match std::fs::symlink_metadata(&candidate) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    let target = std::fs::read_link(&candidate)?;
                    candidate = if target.is_absolute() {
                        target
                    } else {
                        parent.join(target)
                    };
                    continue;
                }
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    return Err(error.into());
                }
                _ => {}
            }
            let parent =
                std::fs::canonicalize(parent).context("canonicalize database directory")?;
            return Ok(parent.join(candidate.file_name().context("database filename missing")?));
        }
        anyhow::bail!("database identity has too many symbolic links")
    }

    pub fn acquire(path: &Path) -> Result<Arc<Self>> {
        let database_path = Self::database_identity(path)?;
        let mut lock_path = database_path.into_os_string();
        lock_path.push(".writer.lock");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(PathBuf::from(lock_path))
            .context("open database ownership lock")?;
        // Keep this sidecar inode; unlinking it could admit a second owner.
        match FileExt::try_lock(&file) {
            Ok(()) => {}
            Err(fs4::TryLockError::WouldBlock) => return Err(WriterBusy.into()),
            Err(error) => return Err(error).context("lock database writer ownership"),
        }
        Ok(Arc::new(Self { _file: file }))
    }

    /// Attach ownership to the native handle. SQLx can finish dispatched work
    /// after the Rust request/executor has been dropped; SQLite's client-data
    /// destructor is the physical lifetime boundary required here.
    pub async fn retain_on_connection(
        self: &Arc<Self>,
        connection: &mut sea_orm::sqlx::SqliteConnection,
    ) -> Result<(), sea_orm::sqlx::Error> {
        let mut locked = connection.lock_handle().await?;
        unsafe extern "C" fn release(pointer: *mut std::ffi::c_void) {
            // SAFETY: exactly one Arc was transferred to SQLite below. Its
            // destructor runs exactly once, including allocation failure.
            unsafe {
                drop(Arc::from_raw(pointer.cast::<WriterOwnership>()));
            }
        }
        let pointer = Arc::into_raw(self.clone()).cast_mut().cast();
        // SAFETY: SQLx exclusively locks the live native handle; SQLite owns
        // the transferred Arc and the static NUL-terminated client-data key.
        let result = unsafe {
            rusqlite::ffi::sqlite3_set_clientdata(
                locked.as_raw_handle().as_ptr(),
                c"pioneer.writer.ownership".as_ptr(),
                pointer,
                Some(release),
            )
        };
        if result != rusqlite::ffi::SQLITE_OK {
            return Err(sea_orm::sqlx::Error::Protocol(
                "retain database writer ownership failed".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn symlink_before_first_creation_uses_the_target_ownership_lock() {
        let directory =
            std::env::temp_dir().join(format!("pioneer-owner-alias-{}", rand::random::<u64>()));
        std::fs::create_dir(&directory).unwrap();
        let target = directory.join("data.sqlite");
        let alias = directory.join("alias.sqlite");
        std::os::unix::fs::symlink("data.sqlite", &alias).unwrap();
        let first = WriterOwnership::acquire(&alias).unwrap();
        assert!(
            WriterOwnership::acquire(&target)
                .err()
                .unwrap()
                .downcast_ref::<WriterBusy>()
                .is_some()
        );
        drop(first);
        std::fs::remove_file(alias).unwrap();
        std::fs::remove_file(directory.join("data.sqlite.writer.lock")).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

    // Executor admission tests cannot prove process ownership survives the
    // wrapper that opened the physical SQLx connection.
    #[tokio::test]
    async fn native_connection_retains_owner_until_physical_close() {
        let path = std::env::temp_dir().join(format!(
            "pioneer-writer-ownership-{}-{}.sqlite",
            std::process::id(),
            rand::random::<u64>()
        ));
        let lease = WriterOwnership::acquire(&path).unwrap();
        let weak = Arc::downgrade(&lease);
        let mut options = sea_orm::ConnectOptions::new(crate::sqlite_connection_url(&path));
        options.max_connections(1);
        options.map_sqlx_sqlite_pool_opts(move |options| {
            let weak = weak.clone();
            options.after_connect(move |connection, _| {
                let lease = weak.upgrade().unwrap();
                Box::pin(async move { lease.retain_on_connection(connection).await })
            })
        });
        let database = sea_orm::Database::connect(options).await.unwrap();
        drop(lease);
        assert!(
            WriterOwnership::acquire(&path)
                .err()
                .unwrap()
                .downcast_ref::<WriterBusy>()
                .is_some()
        );
        database.close().await.unwrap();
        drop(WriterOwnership::acquire(&path).unwrap());
        std::fs::remove_file(&path).unwrap();
        let mut lock_path = path.into_os_string();
        lock_path.push(".writer.lock");
        std::fs::remove_file(PathBuf::from(lock_path)).unwrap();
    }
}
