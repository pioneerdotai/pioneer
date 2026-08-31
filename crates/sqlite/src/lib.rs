use anyhow::{Context, Result, bail};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use std::collections::VecDeque;
use std::future::Future;
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

mod database;

pub use database::{SqliteDatabase, SqliteReadPool, SqliteWriter};

pub mod zstd;

pub const DEFAULT_LOCK_RETRY_ATTEMPTS: usize = 5;
pub const DEFAULT_LOCK_RETRY_BASE_DELAY_MS: u64 = 20;
pub const DEFAULT_SQLITE_BUSY_TIMEOUT_MS: u64 = 5000;
pub const DEFAULT_MAX_FOREGROUND_BURST: usize = 16;
pub const DEFAULT_MAX_BACKGROUND_WAIT_MS: u64 = 2_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteAdmissionClass {
    Foreground,
    Background,
}

impl SqliteAdmissionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Background => "background",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteAdmissionGrantReason {
    Uncontended,
    ForegroundOnly,
    BackgroundOnly,
    ForegroundPriority,
    BackgroundFairness,
}

impl SqliteAdmissionGrantReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uncontended => "uncontended",
            Self::ForegroundOnly => "foreground_only",
            Self::BackgroundOnly => "background_only",
            Self::ForegroundPriority => "foreground_priority",
            Self::BackgroundFairness => "background_fairness",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SqliteAdmissionQueueSnapshot {
    pub foreground: usize,
    pub background: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum SqliteAdmissionEvent {
    Enqueued {
        class: SqliteAdmissionClass,
        queue: SqliteAdmissionQueueSnapshot,
    },
    Acquired {
        class: SqliteAdmissionClass,
        reason: SqliteAdmissionGrantReason,
        waited: Duration,
        queue: SqliteAdmissionQueueSnapshot,
    },
    Released {
        class: SqliteAdmissionClass,
        held: Duration,
        queue: SqliteAdmissionQueueSnapshot,
    },
    Cancelled {
        class: SqliteAdmissionClass,
        waited: Duration,
        queue: SqliteAdmissionQueueSnapshot,
    },
}

pub trait SqliteAdmissionObserver: Send + Sync + 'static {
    fn observe(&self, event: SqliteAdmissionEvent);
}

#[derive(Default)]
struct NoopSqliteAdmissionObserver;

impl SqliteAdmissionObserver for NoopSqliteAdmissionObserver {
    fn observe(&self, _event: SqliteAdmissionEvent) {}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqliteAdmissionPolicy {
    pub max_foreground_burst: usize,
    pub max_background_wait: Duration,
}

impl Default for SqliteAdmissionPolicy {
    fn default() -> Self {
        Self {
            max_foreground_burst: DEFAULT_MAX_FOREGROUND_BURST,
            max_background_wait: Duration::from_millis(DEFAULT_MAX_BACKGROUND_WAIT_MS),
        }
    }
}

struct SqliteAdmissionWaiter {
    id: u64,
    enqueued_at: Instant,
    sender: oneshot::Sender<SqliteAdmissionReservation>,
}

#[derive(Default)]
struct SqliteAdmissionState {
    active: bool,
    foreground: VecDeque<SqliteAdmissionWaiter>,
    background: VecDeque<SqliteAdmissionWaiter>,
    consecutive_foreground_grants: usize,
}

impl SqliteAdmissionState {
    fn snapshot(&self) -> SqliteAdmissionQueueSnapshot {
        SqliteAdmissionQueueSnapshot {
            foreground: self.foreground.len(),
            background: self.background.len(),
        }
    }
}

struct SqliteAdmissionInner {
    state: Mutex<SqliteAdmissionState>,
    next_waiter_id: AtomicU64,
    policy: SqliteAdmissionPolicy,
    observer: Arc<dyn SqliteAdmissionObserver>,
}

struct SqliteAdmissionDispatch {
    sender: oneshot::Sender<SqliteAdmissionReservation>,
    reservation: SqliteAdmissionReservation,
}

struct SqliteAdmissionReservation {
    inner: Arc<SqliteAdmissionInner>,
    class: SqliteAdmissionClass,
    reason: SqliteAdmissionGrantReason,
    enqueued_at: Instant,
    acquired_at: Instant,
    queue: SqliteAdmissionQueueSnapshot,
    started: bool,
}

impl SqliteAdmissionReservation {
    fn start(&mut self) {
        self.started = true;
        self.inner.observer.observe(SqliteAdmissionEvent::Acquired {
            class: self.class,
            reason: self.reason,
            waited: self.acquired_at.saturating_duration_since(self.enqueued_at),
            queue: self.queue,
        });
    }
}

impl Drop for SqliteAdmissionReservation {
    fn drop(&mut self) {
        let held = self.acquired_at.elapsed();
        let (queue, dispatch) = self.inner.release();
        if self.started {
            self.inner.observer.observe(SqliteAdmissionEvent::Released {
                class: self.class,
                held,
                queue,
            });
        }
        SqliteAdmissionInner::dispatch(dispatch);
    }
}

struct SqliteAdmissionWaitRegistration {
    inner: Arc<SqliteAdmissionInner>,
    id: u64,
    class: SqliteAdmissionClass,
    enqueued_at: Instant,
    armed: bool,
}

impl SqliteAdmissionWaitRegistration {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SqliteAdmissionWaitRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some((queue, dispatch)) = self.inner.cancel(self.id, self.class) else {
            return;
        };
        self.inner
            .observer
            .observe(SqliteAdmissionEvent::Cancelled {
                class: self.class,
                waited: self.enqueued_at.elapsed(),
                queue,
            });
        SqliteAdmissionInner::dispatch(dispatch);
    }
}

pub struct SqliteAdmissionPermit {
    reservation: Option<SqliteAdmissionReservation>,
}

impl SqliteAdmissionPermit {
    fn new(mut reservation: SqliteAdmissionReservation) -> Self {
        reservation.start();
        Self {
            reservation: Some(reservation),
        }
    }
}

impl Drop for SqliteAdmissionPermit {
    fn drop(&mut self) {
        drop(self.reservation.take());
    }
}

impl SqliteAdmissionInner {
    fn enqueue(
        self: &Arc<Self>,
        class: SqliteAdmissionClass,
    ) -> (
        oneshot::Receiver<SqliteAdmissionReservation>,
        SqliteAdmissionWaitRegistration,
    ) {
        let id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let enqueued_at = Instant::now();
        let (sender, receiver) = oneshot::channel();
        let waiter = SqliteAdmissionWaiter {
            id,
            enqueued_at,
            sender,
        };
        let (queue, dispatch) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match class {
                SqliteAdmissionClass::Foreground => state.foreground.push_back(waiter),
                SqliteAdmissionClass::Background => state.background.push_back(waiter),
            }
            let queue = state.snapshot();
            let dispatch = (!state.active)
                .then(|| self.grant_next_locked(&mut state))
                .flatten();
            (queue, dispatch)
        };
        self.observer
            .observe(SqliteAdmissionEvent::Enqueued { class, queue });
        Self::dispatch(dispatch);
        (
            receiver,
            SqliteAdmissionWaitRegistration {
                inner: self.clone(),
                id,
                class,
                enqueued_at,
                armed: true,
            },
        )
    }

    fn try_reserve(
        self: &Arc<Self>,
        class: SqliteAdmissionClass,
    ) -> Option<SqliteAdmissionReservation> {
        let now = Instant::now();
        let queue = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.active || !state.foreground.is_empty() || !state.background.is_empty() {
                return None;
            }
            state.active = true;
            state.snapshot()
        };
        Some(SqliteAdmissionReservation {
            inner: self.clone(),
            class,
            reason: SqliteAdmissionGrantReason::Uncontended,
            enqueued_at: now,
            acquired_at: now,
            queue,
            started: false,
        })
    }

    fn cancel(
        self: &Arc<Self>,
        id: u64,
        class: SqliteAdmissionClass,
    ) -> Option<(
        SqliteAdmissionQueueSnapshot,
        Option<SqliteAdmissionDispatch>,
    )> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queue = match class {
            SqliteAdmissionClass::Foreground => &mut state.foreground,
            SqliteAdmissionClass::Background => &mut state.background,
        };
        let position = queue.iter().position(|waiter| waiter.id == id)?;
        queue.remove(position);
        let snapshot = state.snapshot();
        let dispatch = (!state.active)
            .then(|| self.grant_next_locked(&mut state))
            .flatten();
        Some((snapshot, dispatch))
    }

    fn release(
        self: &Arc<Self>,
    ) -> (
        SqliteAdmissionQueueSnapshot,
        Option<SqliteAdmissionDispatch>,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.active, "SQLite admission released without an owner");
        state.active = false;
        let queue = state.snapshot();
        let dispatch = self.grant_next_locked(&mut state);
        (queue, dispatch)
    }

    fn grant_next_locked(
        self: &Arc<Self>,
        state: &mut SqliteAdmissionState,
    ) -> Option<SqliteAdmissionDispatch> {
        debug_assert!(!state.active);
        let now = Instant::now();
        let has_foreground = !state.foreground.is_empty();
        let has_background = !state.background.is_empty();
        let (class, reason) = match (has_foreground, has_background) {
            (false, false) => return None,
            (true, false) => {
                state.consecutive_foreground_grants = 0;
                (
                    SqliteAdmissionClass::Foreground,
                    SqliteAdmissionGrantReason::ForegroundOnly,
                )
            }
            (false, true) => {
                state.consecutive_foreground_grants = 0;
                (
                    SqliteAdmissionClass::Background,
                    SqliteAdmissionGrantReason::BackgroundOnly,
                )
            }
            (true, true) => {
                let background_aged = state.background.front().is_some_and(|waiter| {
                    waiter.enqueued_at.elapsed() >= self.policy.max_background_wait
                });
                if background_aged
                    || state.consecutive_foreground_grants
                        >= self.policy.max_foreground_burst.max(1)
                {
                    state.consecutive_foreground_grants = 0;
                    (
                        SqliteAdmissionClass::Background,
                        SqliteAdmissionGrantReason::BackgroundFairness,
                    )
                } else {
                    state.consecutive_foreground_grants =
                        state.consecutive_foreground_grants.saturating_add(1);
                    (
                        SqliteAdmissionClass::Foreground,
                        SqliteAdmissionGrantReason::ForegroundPriority,
                    )
                }
            }
        };
        let waiter = match class {
            SqliteAdmissionClass::Foreground => state.foreground.pop_front(),
            SqliteAdmissionClass::Background => state.background.pop_front(),
        }
        .expect("selected SQLite admission queue must contain a waiter");
        state.active = true;
        let queue = state.snapshot();
        Some(SqliteAdmissionDispatch {
            sender: waiter.sender,
            reservation: SqliteAdmissionReservation {
                inner: self.clone(),
                class,
                reason,
                enqueued_at: waiter.enqueued_at,
                acquired_at: now,
                queue,
                started: false,
            },
        })
    }

    fn dispatch(dispatch: Option<SqliteAdmissionDispatch>) {
        let Some(dispatch) = dispatch else {
            return;
        };
        if let Err(reservation) = dispatch.sender.send(dispatch.reservation) {
            drop(reservation);
        }
    }
}

#[derive(Clone)]
pub struct SqliteWriteCoordinator {
    admission: Arc<SqliteAdmissionInner>,
    retry_attempts: usize,
    retry_base_delay: Duration,
}

impl Default for SqliteWriteCoordinator {
    fn default() -> Self {
        Self::new(
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
    }
}

impl SqliteWriteCoordinator {
    pub fn new(retry_attempts: usize, retry_base_delay: Duration) -> Self {
        Self::with_policy_and_observer(
            retry_attempts,
            retry_base_delay,
            SqliteAdmissionPolicy::default(),
            Arc::new(NoopSqliteAdmissionObserver),
        )
    }

    pub fn with_observer(observer: Arc<dyn SqliteAdmissionObserver>) -> Self {
        Self::with_policy_and_observer(
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
            SqliteAdmissionPolicy::default(),
            observer,
        )
    }

    pub fn with_policy_and_observer(
        retry_attempts: usize,
        retry_base_delay: Duration,
        policy: SqliteAdmissionPolicy,
        observer: Arc<dyn SqliteAdmissionObserver>,
    ) -> Self {
        Self {
            admission: Arc::new(SqliteAdmissionInner {
                state: Mutex::new(SqliteAdmissionState::default()),
                next_waiter_id: AtomicU64::new(1),
                policy,
                observer,
            }),
            retry_attempts,
            retry_base_delay,
        }
    }

    pub async fn acquire_foreground(&self) -> SqliteAdmissionPermit {
        self.acquire(SqliteAdmissionClass::Foreground).await
    }

    pub async fn acquire_background(&self) -> SqliteAdmissionPermit {
        self.acquire(SqliteAdmissionClass::Background).await
    }

    async fn acquire(&self, class: SqliteAdmissionClass) -> SqliteAdmissionPermit {
        let (receiver, mut registration) = self.admission.enqueue(class);
        let reservation = receiver
            .await
            .expect("SQLite admission sender must live as long as its coordinator");
        registration.disarm();
        SqliteAdmissionPermit::new(reservation)
    }

    pub async fn run_serialized_with_retry<T, E, F, Fut, P>(
        &self,
        operation: F,
        is_retryable: P,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        self.run_serialized_with_retry_class(
            SqliteAdmissionClass::Foreground,
            operation,
            is_retryable,
        )
        .await
    }

    pub async fn run_background_serialized_with_retry<T, E, F, Fut, P>(
        &self,
        operation: F,
        is_retryable: P,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        self.run_serialized_with_retry_class(
            SqliteAdmissionClass::Background,
            operation,
            is_retryable,
        )
        .await
    }

    async fn run_serialized_with_retry_class<T, E, F, Fut, P>(
        &self,
        class: SqliteAdmissionClass,
        mut operation: F,
        is_retryable: P,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        for attempt in 0..=self.retry_attempts {
            let permit = self.acquire(class).await;
            let result = operation().await;
            drop(permit);
            match result {
                Ok(value) => return Ok(value),
                Err(error) if attempt < self.retry_attempts && is_retryable(&error) => {
                    let multiplier = 1u64 << attempt;
                    let delay_ms = self
                        .retry_base_delay
                        .as_millis()
                        .saturating_mul(multiplier as u128)
                        .min(u64::MAX as u128) as u64;
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("SQLite admission retry loop must return in all branches")
    }

    pub async fn try_run_serialized_with_retry<T, E, F, Fut, P>(
        &self,
        operation: F,
        is_retryable: P,
    ) -> Option<Result<T, E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        self.try_run_serialized_with_retry_class(
            SqliteAdmissionClass::Foreground,
            operation,
            is_retryable,
        )
        .await
    }

    pub async fn try_run_background_serialized_with_retry<T, E, F, Fut, P>(
        &self,
        operation: F,
        is_retryable: P,
    ) -> Option<Result<T, E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        self.try_run_serialized_with_retry_class(
            SqliteAdmissionClass::Background,
            operation,
            is_retryable,
        )
        .await
    }

    async fn try_run_serialized_with_retry_class<T, E, F, Fut, P>(
        &self,
        class: SqliteAdmissionClass,
        operation: F,
        is_retryable: P,
    ) -> Option<Result<T, E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        P: Fn(&E) -> bool,
    {
        let Some(reservation) = self.admission.try_reserve(class) else {
            return None;
        };
        let permit = SqliteAdmissionPermit::new(reservation);
        let result = retry_with_backoff(
            operation,
            is_retryable,
            self.retry_attempts,
            self.retry_base_delay,
        )
        .await;
        drop(permit);
        Some(result)
    }
}

pub async fn retry_with_backoff<T, E, F, Fut, P>(
    mut operation: F,
    is_retryable: P,
    retry_attempts: usize,
    retry_base_delay: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: Fn(&E) -> bool,
{
    for attempt in 0..=retry_attempts {
        let operation: Pin<Box<Fut>> = Box::pin(operation());
        match operation.await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < retry_attempts && is_retryable(&error) => {
                let multiplier = 1u64 << attempt;
                let delay_ms = retry_base_delay
                    .as_millis()
                    .saturating_mul(multiplier as u128)
                    .min(u64::MAX as u128) as u64;
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("sqlite retry loop must return in all branches")
}

pub fn is_sqlite_lock_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    (normalized.contains("database") && normalized.contains("locked"))
        || (normalized.contains("database") && normalized.contains("busy"))
        || normalized.contains("sqlite_busy")
}

pub fn is_anyhow_sqlite_lock(error: &anyhow::Error) -> bool {
    is_sqlite_lock_message(format!("{error:#}").as_str())
}

pub fn is_sqlite_transient_open_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("sqlite_cantopen") || normalized.contains("unable to open database file")
}

pub fn is_anyhow_sqlite_transient_open(error: &anyhow::Error) -> bool {
    is_sqlite_transient_open_message(format!("{error:#}").as_str())
}

pub fn is_sqlite_pool_timeout_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("connection pool timed out")
        || normalized.contains("failed to acquire connection from pool")
}

pub fn is_anyhow_sqlite_pool_timeout(error: &anyhow::Error) -> bool {
    is_sqlite_pool_timeout_message(format!("{error:#}").as_str())
}

pub fn is_anyhow_sqlite_transient_access(error: &anyhow::Error) -> bool {
    is_anyhow_sqlite_transient_open(error) || is_anyhow_sqlite_pool_timeout(error)
}

pub async fn apply_sqlite_pragmas(connection: &DatabaseConnection) -> Result<()> {
    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA journal_mode = WAL",
        ))
        .await
        .context("failed to set sqlite PRAGMA journal_mode = WAL")?;

    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            format!("PRAGMA busy_timeout = {DEFAULT_SQLITE_BUSY_TIMEOUT_MS}"),
        ))
        .await
        .context("failed to set sqlite PRAGMA busy_timeout")?;

    connection
        .query_one_raw(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys = OFF",
        ))
        .await
        .context("failed to set sqlite PRAGMA foreign_keys = OFF")?;

    Ok(())
}

pub fn sqlite_connection_url(path: &Path) -> String {
    sqlite_connection_url_with_mode(path, "rwc")
}

pub fn sqlite_read_only_connection_url(path: &Path) -> String {
    sqlite_connection_url_with_mode(path, "ro")
}

fn sqlite_connection_url_with_mode(path: &Path, mode: &str) -> String {
    let normalized_path = path.to_string_lossy().replace('\\', "/");
    let has_windows_drive_prefix = normalized_path
        .as_bytes()
        .get(1)
        .is_some_and(|value| *value == b':');
    let needs_leading_slash =
        (path.is_absolute() || has_windows_drive_prefix) && !normalized_path.starts_with('/');

    let path_part = if needs_leading_slash {
        format!("/{normalized_path}")
    } else {
        normalized_path
    };

    format!("sqlite://{path_part}?mode={mode}")
}

pub fn normalize_relative_database_file_name(value: &str, field_name: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field_name} must not be empty");
    }

    let path = Path::new(trimmed);

    if path.is_absolute() {
        bail!("{field_name} must be relative");
    }

    if path.components().any(is_disallowed_component) {
        bail!("{field_name} must not contain parent or root components");
    }

    Ok(trimmed.to_owned())
}

fn is_disallowed_component(component: Component<'_>) -> bool {
    matches!(
        component,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
    )
}

#[cfg(test)]
mod tests {
    use super::{
        NoopSqliteAdmissionObserver, SqliteAdmissionClass, SqliteAdmissionPolicy,
        SqliteAdmissionQueueSnapshot, SqliteWriteCoordinator, is_sqlite_lock_message,
        is_sqlite_pool_timeout_message, is_sqlite_transient_open_message,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{Notify, mpsc};

    fn coordinator_with_foreground_burst(max_foreground_burst: usize) -> SqliteWriteCoordinator {
        SqliteWriteCoordinator::with_policy_and_observer(
            0,
            Duration::ZERO,
            SqliteAdmissionPolicy {
                max_foreground_burst,
                max_background_wait: Duration::from_secs(60),
            },
            Arc::new(NoopSqliteAdmissionObserver),
        )
    }

    fn queue_snapshot(coordinator: &SqliteWriteCoordinator) -> SqliteAdmissionQueueSnapshot {
        coordinator
            .admission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
    }

    async fn wait_for_queue(
        coordinator: &SqliteWriteCoordinator,
        foreground: usize,
        background: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if queue_snapshot(coordinator)
                    == (SqliteAdmissionQueueSnapshot {
                        foreground,
                        background,
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("SQLite admission queue should reach the expected state");
    }

    fn spawn_admission_waiter(
        coordinator: SqliteWriteCoordinator,
        class: SqliteAdmissionClass,
        label: &'static str,
        acquired: mpsc::UnboundedSender<&'static str>,
        release: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let permit = match class {
                SqliteAdmissionClass::Foreground => coordinator.acquire_foreground().await,
                SqliteAdmissionClass::Background => coordinator.acquire_background().await,
            };
            acquired
                .send(label)
                .expect("admission order receiver should remain open");
            release.notified().await;
            drop(permit);
        })
    }

    #[test]
    fn detects_sqlite_transient_open_errors() {
        assert!(is_sqlite_transient_open_message(
            "Query Error: error returned from database: (code: 14) unable to open database file"
        ));
        assert!(is_sqlite_transient_open_message("SQLITE_CANTOPEN"));
    }

    #[test]
    fn transient_open_errors_do_not_match_lock_only_predicate() {
        assert!(!is_sqlite_lock_message(
            "Query Error: error returned from database: (code: 14) unable to open database file"
        ));
    }

    #[test]
    fn detects_sqlite_pool_acquire_timeouts() {
        assert!(is_sqlite_pool_timeout_message(
            "failed to query expired running attempts: Failed to acquire connection from pool: Connection pool timed out: Connection pool timed out",
        ));
    }

    #[test]
    fn pool_acquire_timeouts_do_not_match_lock_only_predicate() {
        assert!(!is_sqlite_lock_message(
            "Failed to acquire connection from pool: Connection pool timed out"
        ));
    }

    #[tokio::test]
    async fn low_priority_write_skips_while_foreground_write_is_active() {
        let coordinator = SqliteWriteCoordinator::default();
        let foreground_coordinator = coordinator.clone();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let foreground = tokio::spawn({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                foreground_coordinator
                    .run_serialized_with_retry(
                        || async {
                            entered.notify_one();
                            release.notified().await;
                            Ok::<_, ()>(())
                        },
                        |_| false,
                    )
                    .await
            }
        });

        entered.notified().await;
        let skipped = coordinator
            .try_run_serialized_with_retry(|| async { Ok::<_, ()>(()) }, |_| false)
            .await;
        assert!(skipped.is_none());

        release.notify_one();
        foreground
            .await
            .expect("foreground task should join")
            .expect("foreground write should succeed");

        let completed = coordinator
            .try_run_serialized_with_retry(|| async { Ok::<_, ()>(42) }, |_| false)
            .await;
        assert_eq!(completed, Some(Ok(42)));
    }

    #[tokio::test]
    async fn foreground_waiter_jumps_ahead_of_queued_background_work() {
        let coordinator = coordinator_with_foreground_burst(4);
        let blocker = coordinator.acquire_foreground().await;
        let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
        let background_release = Arc::new(Notify::new());
        let foreground_release = Arc::new(Notify::new());

        let background = spawn_admission_waiter(
            coordinator.clone(),
            SqliteAdmissionClass::Background,
            "background",
            acquired_tx.clone(),
            background_release.clone(),
        );
        wait_for_queue(&coordinator, 0, 1).await;
        let foreground = spawn_admission_waiter(
            coordinator.clone(),
            SqliteAdmissionClass::Foreground,
            "foreground",
            acquired_tx,
            foreground_release.clone(),
        );
        wait_for_queue(&coordinator, 1, 1).await;

        drop(blocker);
        assert_eq!(acquired_rx.recv().await, Some("foreground"));
        foreground_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("background"));
        background_release.notify_one();

        foreground.await.expect("foreground waiter should join");
        background.await.expect("background waiter should join");
    }

    #[tokio::test]
    async fn background_progress_is_guaranteed_during_continuous_foreground_load() {
        let coordinator = coordinator_with_foreground_burst(2);
        let blocker = coordinator.acquire_foreground().await;
        let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
        let background_release = Arc::new(Notify::new());
        let foreground_one_release = Arc::new(Notify::new());
        let foreground_two_release = Arc::new(Notify::new());
        let foreground_three_release = Arc::new(Notify::new());

        let background = spawn_admission_waiter(
            coordinator.clone(),
            SqliteAdmissionClass::Background,
            "background",
            acquired_tx.clone(),
            background_release.clone(),
        );
        wait_for_queue(&coordinator, 0, 1).await;
        let foreground_one = spawn_admission_waiter(
            coordinator.clone(),
            SqliteAdmissionClass::Foreground,
            "foreground-1",
            acquired_tx.clone(),
            foreground_one_release.clone(),
        );
        let foreground_two = spawn_admission_waiter(
            coordinator.clone(),
            SqliteAdmissionClass::Foreground,
            "foreground-2",
            acquired_tx.clone(),
            foreground_two_release.clone(),
        );
        let foreground_three = spawn_admission_waiter(
            coordinator.clone(),
            SqliteAdmissionClass::Foreground,
            "foreground-3",
            acquired_tx,
            foreground_three_release.clone(),
        );
        wait_for_queue(&coordinator, 3, 1).await;

        drop(blocker);
        assert_eq!(acquired_rx.recv().await, Some("foreground-1"));
        foreground_one_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("foreground-2"));
        foreground_two_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("background"));
        background_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("foreground-3"));
        foreground_three_release.notify_one();

        for waiter in [foreground_one, foreground_two, foreground_three, background] {
            waiter.await.expect("admission waiter should join");
        }
    }

    #[tokio::test]
    async fn cancelling_queued_waiter_does_not_leak_admission_capacity() {
        let coordinator = coordinator_with_foreground_burst(2);
        let blocker = coordinator.acquire_foreground().await;
        let queued = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                let _permit = coordinator.acquire_background().await;
            }
        });
        wait_for_queue(&coordinator, 0, 1).await;
        queued.abort();
        let _ = queued.await;
        wait_for_queue(&coordinator, 0, 0).await;
        drop(blocker);

        tokio::time::timeout(Duration::from_secs(1), coordinator.acquire_foreground())
            .await
            .expect("foreground acquisition should not be blocked by cancelled waiter");
    }

    #[tokio::test]
    async fn foreground_is_admitted_within_one_bounded_background_quantum() {
        let coordinator = coordinator_with_foreground_burst(4);
        let first_quantum_entered = Arc::new(Notify::new());
        let background_coordinator = coordinator.clone();
        let background = tokio::spawn({
            let first_quantum_entered = first_quantum_entered.clone();
            async move {
                for quantum in 0..8 {
                    background_coordinator
                        .run_background_serialized_with_retry(
                            || {
                                let first_quantum_entered = first_quantum_entered.clone();
                                async move {
                                    if quantum == 0 {
                                        first_quantum_entered.notify_one();
                                    }
                                    tokio::time::sleep(Duration::from_millis(25)).await;
                                    Ok::<_, ()>(())
                                }
                            },
                            |_| false,
                        )
                        .await
                        .expect("background quantum should succeed");
                }
            }
        });

        first_quantum_entered.notified().await;
        let permit =
            tokio::time::timeout(Duration::from_millis(500), coordinator.acquire_foreground())
                .await
                .expect("foreground must not wait behind the complete background job");
        drop(permit);

        background.await.expect("background task should join");
    }

    #[tokio::test]
    async fn cancelling_active_quantum_releases_admission_capacity() {
        let coordinator = coordinator_with_foreground_burst(2);
        let entered = Arc::new(Notify::new());
        let never_release = Arc::new(Notify::new());
        let active = tokio::spawn({
            let coordinator = coordinator.clone();
            let entered = entered.clone();
            let never_release = never_release.clone();
            async move {
                coordinator
                    .run_background_serialized_with_retry(
                        || {
                            let entered = entered.clone();
                            let never_release = never_release.clone();
                            async move {
                                entered.notify_one();
                                never_release.notified().await;
                                Ok::<_, ()>(())
                            }
                        },
                        |_| false,
                    )
                    .await
            }
        });

        entered.notified().await;
        active.abort();
        let _ = active.await;
        tokio::time::timeout(Duration::from_secs(1), coordinator.acquire_foreground())
            .await
            .expect("cancelled active quantum must release its permit");
    }
}
