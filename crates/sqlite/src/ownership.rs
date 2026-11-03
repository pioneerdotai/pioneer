//! Cooperative ownership of a configured file database, acquired before SQLite
//! opens it. This is not protection against an OS user bypassing the protocol.
use anyhow::{Context, Result, ensure};
use fs4::FileExt;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct Lease {
    // Never unlink the lockfile: another process may already have opened it.
    _file: File,
    path: PathBuf,
}

/// Retained by connect options and, before any application operation, by a
/// native collation destructor. Startup must not execute durable PRAGMAs before
/// SQLx attaches collations: use the canonical URL and configure those through
/// the serialized writer after connect (as Gateway already does).
pub struct Opening {
    lease: Arc<Lease>,
}
impl Opening {
    pub fn acquire(path: &Path) -> Result<Self> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let path = absolute.as_path();
        let parent = path.parent().context("database path has no parent")?;
        std::fs::create_dir_all(parent).context("cannot create database parent")?;
        let path = match std::fs::canonicalize(path) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                ensure!(
                    std::fs::symlink_metadata(path).is_err(),
                    "dangling database symlink is not supported"
                );
                std::fs::canonicalize(parent)?
                    .join(path.file_name().context("database name missing")?)
            }
            Err(error) => return Err(error).context("cannot resolve database identity"),
        };
        check_alias(&path)?;
        let mut lock_name = path
            .file_name()
            .context("database name missing")?
            .to_os_string();
        lock_name.push(".pioneer-writer.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_file_name(lock_name))
            .context("cannot open database ownership lock")?;
        FileExt::try_lock(&file)
            .context("database is already owned; use the running Gateway for online mutations")?;
        // Resolve aliases again after obtaining ownership. A missing configured
        // name replaced by a symlink must not silently select a different DB.
        if path.exists() {
            ensure!(
                std::fs::canonicalize(&path)? == path,
                "database identity changed during ownership acquisition"
            );
        }
        check_alias(&path)?;
        Ok(Self {
            lease: Arc::new(Lease { _file: file, path }),
        })
    }
    pub fn path(&self) -> &Path {
        &self.lease.path
    }

    pub fn configure(&self, options: &mut sea_orm::ConnectOptions) {
        self.configure_with(options, |options| options);
    }

    /// Compose ownership with the caller's existing SQLite option settings.
    /// SeaORM's map setter replaces its previous callback rather than composing.
    pub fn configure_with<F>(&self, options: &mut sea_orm::ConnectOptions, configure: F)
    where
        F: Fn(
                sea_orm::sqlx::sqlite::SqliteConnectOptions,
            ) -> sea_orm::sqlx::sqlite::SqliteConnectOptions
            + Send
            + Sync
            + 'static,
    {
        let lease = self.lease.clone();
        options.map_sqlx_sqlite_opts(move |options| {
            let connection_lease = lease.clone();
            configure(options).filename(&lease.path).collation(
                "pioneer_writer_lifetime",
                move |left, right| {
                    // SQLx owns this closure both in connect options and in native
                    // sqlite3_create_collation_v2 user data. SQLite destroys the
                    // latter AFTER rollback and B-tree close, including close_v2.
                    // No production SQL uses this collation; it conveys no rights.
                    let _keep_alive = &connection_lease;
                    left.cmp(right)
                },
            )
        });
    }
    /// Call synchronously after connect completes and before any further await.
    pub fn connected(self) -> Ownership {
        Ownership(self.lease.clone())
    }
}
// No failed-open lease cache: SQLx's default pre-collation statement is only
// connection-local foreign_keys=ON. Gateway uses a filename-only URL; SQLx's
// standalone CLI URL parser accepts only mode/cache/immutable/vfs flags, never
// journal/schema PRAGMAs. The configure callbacks add only connection settings. If cancelled before collation attachment,
// no durable write/transaction has been dispatched; options own the lease until
// then. After attachment, native destruction retains it through rollback/close.

/// Retained by the writer runtime and every native connection. The final Arc
/// releases the lock; completion of a pool close is not a native-close fence
/// after cancellation (SQLx may already have removed that connection).
#[derive(Clone)]
pub struct Ownership(Arc<Lease>);
impl Ownership {
    pub(crate) fn pool_closed(self) {
        drop(self.0);
    }
}

fn begin_owned_pool_close(connection: &sea_orm::DatabaseConnection) {
    let pool = connection.get_sqlite_connection_pool();
    // SQLx marks closed synchronously, even if no cleanup future can currently
    // be polled. Native references still retain ownership until actual close.
    drop(pool.close());
    pool.set_connect_options(sea_orm::sqlx::sqlite::SqliteConnectOptions::new().read_only(true));
}

pub(crate) async fn close_owned_pool(connection: sea_orm::DatabaseConnection) {
    let pool = connection.get_sqlite_connection_pool();
    // close() synchronously marks the pool closed, before any await. Retire
    // only its options' lifetime reference: native collations and already
    // connecting operations retain their OWN references through final close.
    // A metrics handle can then retain the closed pool without retaining the
    // OS lock forever. Never forcibly unlock the shared lease here.
    let closing = pool.close();
    pool.set_connect_options(sea_orm::sqlx::sqlite::SqliteConnectOptions::new().read_only(true));
    closing.await;
}
#[derive(Default)]
pub(crate) struct RuntimeOwnership(
    pub(crate)  Mutex<
        Option<(
            Ownership,
            sea_orm::DatabaseConnection,
            Option<tokio::runtime::Handle>,
        )>,
    >,
);
impl Drop for RuntimeOwnership {
    fn drop(&mut self) {
        if let Some((ownership, connection, runtime)) = self.0.get_mut().unwrap().take() {
            begin_owned_pool_close(&connection);
            // Last application executor/transaction/stream is gone. Metrics
            // pool observers do not own the runtime and must not keep it open.
            // Cleanup performs only close/rollback, never new application work.
            if let Some(runtime) = runtime.or_else(|| tokio::runtime::Handle::try_current().ok()) {
                runtime.spawn(async move {
                    close_owned_pool(connection).await;
                    ownership.pool_closed();
                });
            }
            // Without a live runtime the native closure still prevents early
            // unlock. Explicit close is required before runtime teardown.
        }
    }
}

fn check_alias(path: &Path) -> Result<()> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("cannot inspect database identity"),
    };
    ensure!(metadata.is_file(), "database must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            metadata.nlink() == 1,
            "hard-linked database aliases are not supported"
        );
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        #[repr(C)]
        struct Info {
            attributes: u32,
            creation: [u32; 2],
            access: [u32; 2],
            write: [u32; 2],
            volume: u32,
            size_high: u32,
            size_low: u32,
            links: u32,
            index_high: u32,
            index_low: u32,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetFileInformationByHandle(handle: *mut std::ffi::c_void, info: *mut Info) -> i32;
        }
        let file = File::open(path)?;
        let mut info = std::mem::MaybeUninit::<Info>::uninit();
        // SAFETY: file is live, Info has the documented BY_HANDLE_FILE_INFORMATION
        // layout, and it is read only after the API reports success.
        let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle(), info.as_mut_ptr()) };
        if ok == 0 {
            return Err(std::io::Error::last_os_error())
                .context("cannot inspect database link identity");
        }
        ensure!(
            unsafe { info.assume_init() }.links == 1,
            "hard-linked database aliases are not supported"
        );
    }
    #[cfg(not(any(unix, windows)))]
    {
        anyhow::bail!("database ownership is unsupported on this platform");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SqliteDatabase, SqliteWriteExecutor};
    use futures_util::FutureExt;
    use sea_orm::{ConnectionTrait, Database, TransactionTrait};

    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "pioneer-ownership-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn ownership_survives_clones_transactions_and_pool_close() {
        let directory = Directory::new();
        let path = directory.0.join("db.sqlite");
        let opening = Opening::acquire(&path).unwrap();
        assert!(Opening::acquire(&path).is_err());
        let mut options =
            sea_orm::ConnectOptions::new(crate::sqlite_connection_url(opening.path()));
        options.max_connections(1);
        opening.configure(&mut options);
        let writer = Database::connect(options).await.unwrap();
        let owner = opening.connected();
        let executor = SqliteWriteExecutor::new(writer).with_ownership(owner);
        // No reader operations in this test: borrowing the pool as a constructor
        // fixture does not change the writer/transaction ownership being tested.
        let mut read = sea_orm::ConnectOptions::new(crate::sqlite_read_only_connection_url(&path));
        read.map_sqlx_sqlite_opts(|options| options.read_only(true).pragma("query_only", "ON"));
        let reader = Database::connect(read).await.unwrap();
        let db = SqliteDatabase::from_executor(reader, executor);
        let clone = db.maintenance();
        let transaction = clone.begin().await.unwrap();
        transaction
            .execute_unprepared("CREATE TABLE ownership_probe(id INTEGER)")
            .await
            .unwrap();
        drop(clone);
        assert!(Opening::acquire(&path).is_err());
        let closing = db.close();
        tokio::pin!(closing);
        assert!(closing.as_mut().now_or_never().is_none());
        assert!(Opening::acquire(&path).is_err());
        transaction.rollback().await.unwrap();
        closing.await.unwrap();
        let reopened = Opening::acquire(&path).unwrap();
        // This acquisition was only a filesystem test; no SQLite open was sent.
        reopened.connected().pool_closed();
    }

    #[tokio::test]
    async fn cancelled_pool_close_finishes_after_rollback_despite_observer_handle() {
        let directory = Directory::new();
        let path = directory.0.join("cancel.sqlite");
        let opening = Opening::acquire(&path).unwrap();
        let mut options =
            sea_orm::ConnectOptions::new(crate::sqlite_connection_url(opening.path()));
        options.max_connections(1);
        opening.configure(&mut options);
        let writer = Database::connect(options).await.unwrap();
        let observed_pool = writer.get_sqlite_connection_pool().clone();
        let executor = SqliteWriteExecutor::new(writer).with_ownership(opening.connected());
        let mut options =
            sea_orm::ConnectOptions::new(crate::sqlite_read_only_connection_url(&path));
        options.map_sqlx_sqlite_opts(|options| options.read_only(true).pragma("query_only", "ON"));
        let reader = Database::connect(options).await.unwrap();
        let db = SqliteDatabase::from_executor(reader, executor);
        let transaction = db.begin().await.unwrap();
        transaction
            .execute_unprepared("CREATE TABLE cancelled_owner_probe(id INTEGER)")
            .await
            .unwrap();
        let mut closing = Box::pin(db.close());
        assert!(closing.as_mut().now_or_never().is_none());
        drop(closing);
        assert!(Opening::acquire(&path).is_err());
        transaction.rollback().await.unwrap();
        let reopened = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Ok(owner) = Opening::acquire(&path) {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("last application owner must finish cancelled close");
        assert!(observed_pool.is_closed());
        assert_eq!(observed_pool.size(), 0);
        reopened.connected().pool_closed();
    }

    #[tokio::test]
    async fn final_application_handle_dropped_outside_runtime_still_closes_observed_pool() {
        let directory = Directory::new();
        let path = directory.0.join("outside-runtime.sqlite");
        let opening = Opening::acquire(&path).unwrap();
        let mut options =
            sea_orm::ConnectOptions::new(crate::sqlite_connection_url(opening.path()));
        options.max_connections(1);
        opening.configure(&mut options);
        let writer = Database::connect(options).await.unwrap();
        let observed_pool = writer.get_sqlite_connection_pool().clone();
        let executor = SqliteWriteExecutor::new(writer).with_ownership(opening.connected());
        // This ordinary OS thread has no entered Tokio context. Only shutdown
        // is handed back to the runtime captured by the writable owner.
        std::thread::spawn(move || drop(executor)).join().unwrap();
        assert!(
            observed_pool.is_closed(),
            "admission closes synchronously in Drop"
        );
        let reopened = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Ok(owner) = Opening::acquire(&path) {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained metrics pool cannot keep the native writer open indefinitely");
        assert_eq!(observed_pool.size(), 0);
        drop(reopened);
    }

    #[tokio::test]
    async fn retrying_cancelled_close_cannot_unlock_a_still_closing_native_connection() {
        struct NativeClose {
            reached: tokio::sync::Notify,
            release: (Mutex<bool>, std::sync::Condvar),
        }
        struct NativeReference {
            gate: Arc<NativeClose>,
            _owner: Ownership,
        }
        impl Drop for NativeReference {
            fn drop(&mut self) {
                self.gate.reached.notify_one();
                let (lock, condition) = &self.gate.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = condition.wait(released).unwrap();
                }
            }
        }
        // Like the real lifetime collation, this fixture is owned by options
        // and the native connection, and destroyed on the SQLite worker.
        let directory = Directory::new();
        let path = directory.0.join("native-close.sqlite");
        let opening = Opening::acquire(&path).unwrap();
        let gate = Arc::new(NativeClose {
            reached: tokio::sync::Notify::new(),
            release: (Mutex::new(false), std::sync::Condvar::new()),
        });
        let reference = Arc::new(NativeReference {
            gate: gate.clone(),
            _owner: Ownership(opening.lease.clone()),
        });
        let mut options = sea_orm::ConnectOptions::new(crate::sqlite_connection_url(&path));
        options.max_connections(1);
        opening.configure_with(&mut options, move |options| {
            let reference = reference.clone();
            options.collation("native_close_test_barrier", move |left, right| {
                let _retained = &reference;
                left.cmp(right)
            })
        });
        let writer = Database::connect(options).await.unwrap();
        let executor = SqliteWriteExecutor::new(writer).with_ownership(opening.connected());
        let closing = tokio::spawn(executor.clone().close());
        gate.reached.notified().await;
        closing.abort();
        let _ = closing.await;
        // SQLx's first close already removed the idle connection. The retry
        // can finish while its native shutdown is still paused.
        executor.clone().close().await.unwrap();
        let still_owned = Opening::acquire(&path).is_err();
        *gate.release.0.lock().unwrap() = true;
        gate.release.1.notify_one();
        assert!(still_owned);
        let reopened = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Ok(owner) = Opening::acquire(&path) {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(reopened);
    }

    #[cfg(unix)]
    #[test]
    fn aliases_and_hardlink_refusal_share_file_identity() {
        let directory = Directory::new();
        let parent_alias = directory.0.join("alias");
        std::os::unix::fs::symlink(&directory.0, &parent_alias).unwrap();
        let path = directory.0.join("db.sqlite");
        let owner = Opening::acquire(&path).unwrap();
        assert!(Opening::acquire(&parent_alias.join("db.sqlite")).is_err());
        std::fs::write(&path, b"").unwrap();
        let file_alias = directory.0.join("symlink.sqlite");
        std::os::unix::fs::symlink(&path, &file_alias).unwrap();
        assert!(Opening::acquire(&file_alias).is_err());
        let hardlink = directory.0.join("hardlink.sqlite");
        std::fs::hard_link(&path, &hardlink).unwrap();
        assert!(Opening::acquire(&hardlink).is_err());
        owner.connected().pool_closed();
        assert!(Opening::acquire(&path).is_err());
    }
}
