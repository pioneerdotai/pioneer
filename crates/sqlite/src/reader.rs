use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sea_orm::DbErr;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Span;

pub const DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS: usize = 1;

/// Scheduling class for SQLite reads. Interactive reads use the pool
/// directly; maintenance reads share one process-local permit so background
/// work cannot consume every physical reader connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqliteReadClass {
    #[default]
    Interactive,
    Maintenance,
}

impl SqliteReadClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Maintenance => "maintenance",
        }
    }
}

/// Terminal outcome for one typed SQLite reader operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteReadOutcome {
    Ok,
    Error,
    Cancelled,
}

impl SqliteReadOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Low-cardinality lifecycle emitted by the typed SQLite reader runtime.
/// Events deliberately contain no SQL, identifiers, paths, or error text.
#[derive(Clone, Copy, Debug)]
pub enum SqliteReadEvent {
    OperationFinished {
        class: SqliteReadClass,
        outcome: SqliteReadOutcome,
        elapsed: Duration,
    },
    AdmissionEnqueued {
        class: SqliteReadClass,
        queue_depth: usize,
        active: usize,
    },
    AdmissionAcquired {
        class: SqliteReadClass,
        waited: Duration,
        queue_depth: usize,
        active: usize,
    },
    AdmissionReleased {
        class: SqliteReadClass,
        held: Duration,
        queue_depth: usize,
        active: usize,
    },
    AdmissionCancelled {
        class: SqliteReadClass,
        waited: Duration,
        queue_depth: usize,
        active: usize,
    },
}

/// Receives low-cardinality reader lifecycle observations.
///
/// Implementations must return promptly: callbacks run inline after state has
/// been snapshotted, although never while the limiter state lock is held.
pub trait SqliteReadObserver: Send + Sync + 'static {
    fn observe(&self, event: SqliteReadEvent);
}

#[derive(Default)]
struct NoopSqliteReadObserver;

impl SqliteReadObserver for NoopSqliteReadObserver {
    fn observe(&self, _event: SqliteReadEvent) {}
}

pub(crate) fn noop_read_observer() -> Arc<dyn SqliteReadObserver> {
    Arc::new(NoopSqliteReadObserver)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ReadAdmissionSnapshot {
    queue_depth: usize,
    active: usize,
}

#[derive(Default)]
struct ReadAdmissionState {
    queue_depth: usize,
    active: usize,
}

pub(crate) struct MaintenanceReadLimiter {
    semaphore: Arc<Semaphore>,
    state: Mutex<ReadAdmissionState>,
    observer: Arc<dyn SqliteReadObserver>,
}

impl std::fmt::Debug for MaintenanceReadLimiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MaintenanceReadLimiter")
    }
}

impl MaintenanceReadLimiter {
    pub(crate) fn new(observer: Arc<dyn SqliteReadObserver>) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS)),
            state: Mutex::new(ReadAdmissionState::default()),
            observer,
        })
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<SqliteMaintenanceReadPermit, DbErr> {
        let enqueued_at = Instant::now();
        let snapshot = {
            let mut state = self.lock();
            state.queue_depth = state.queue_depth.saturating_add(1);
            state.snapshot()
        };
        self.observer.observe(SqliteReadEvent::AdmissionEnqueued {
            class: SqliteReadClass::Maintenance,
            queue_depth: snapshot.queue_depth,
            active: snapshot.active,
        });

        let mut registration = ReadWaitRegistration {
            limiter: self.clone(),
            enqueued_at,
            armed: true,
        };
        let raw = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| DbErr::Custom("SQLite maintenance read limiter closed".to_owned()))?;
        let snapshot = {
            let mut state = self.lock();
            debug_assert!(state.queue_depth > 0);
            debug_assert!(state.active < DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS);
            state.queue_depth = state.queue_depth.saturating_sub(1);
            state.active = state.active.saturating_add(1);
            state.snapshot()
        };
        registration.armed = false;
        let acquired_at = Instant::now();
        self.observer.observe(SqliteReadEvent::AdmissionAcquired {
            class: SqliteReadClass::Maintenance,
            waited: acquired_at.saturating_duration_since(enqueued_at),
            queue_depth: snapshot.queue_depth,
            active: snapshot.active,
        });
        Ok(SqliteMaintenanceReadPermit {
            limiter: self.clone(),
            raw: Some(raw),
            acquired_at,
        })
    }

    fn cancel_wait(&self, enqueued_at: Instant) {
        let snapshot = {
            let mut state = self.lock();
            debug_assert!(state.queue_depth > 0);
            state.queue_depth = state.queue_depth.saturating_sub(1);
            state.snapshot()
        };
        self.observer.observe(SqliteReadEvent::AdmissionCancelled {
            class: SqliteReadClass::Maintenance,
            waited: enqueued_at.elapsed(),
            queue_depth: snapshot.queue_depth,
            active: snapshot.active,
        });
    }

    fn release(&self, raw: &mut Option<OwnedSemaphorePermit>, acquired_at: Instant) {
        let snapshot = {
            let mut state = self.lock();
            // Wake the next waiter while holding the state lock. It cannot
            // increment `active` until this permit has decremented it, so the
            // exported snapshot can never exceed the physical limit.
            debug_assert!(raw.is_some());
            debug_assert!(state.active > 0);
            raw.take();
            state.active = state.active.saturating_sub(1);
            state.snapshot()
        };
        self.observer.observe(SqliteReadEvent::AdmissionReleased {
            class: SqliteReadClass::Maintenance,
            held: acquired_at.elapsed(),
            queue_depth: snapshot.queue_depth,
            active: snapshot.active,
        });
    }

    fn lock(&self) -> MutexGuard<'_, ReadAdmissionState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ReadAdmissionState {
    fn snapshot(&self) -> ReadAdmissionSnapshot {
        ReadAdmissionSnapshot {
            queue_depth: self.queue_depth,
            active: self.active,
        }
    }
}

struct ReadWaitRegistration {
    limiter: Arc<MaintenanceReadLimiter>,
    enqueued_at: Instant,
    armed: bool,
}

impl Drop for ReadWaitRegistration {
    fn drop(&mut self) {
        if self.armed {
            self.limiter.cancel_wait(self.enqueued_at);
        }
    }
}

pub(crate) struct SqliteMaintenanceReadPermit {
    limiter: Arc<MaintenanceReadLimiter>,
    raw: Option<OwnedSemaphorePermit>,
    acquired_at: Instant,
}

impl Drop for SqliteMaintenanceReadPermit {
    fn drop(&mut self) {
        self.limiter.release(&mut self.raw, self.acquired_at);
    }
}

pub(crate) struct SqliteReadOperation {
    observer: Arc<dyn SqliteReadObserver>,
    class: SqliteReadClass,
    started_at: Instant,
    finished: bool,
}

impl SqliteReadOperation {
    pub(crate) fn start(observer: Arc<dyn SqliteReadObserver>, class: SqliteReadClass) -> Self {
        Self {
            observer,
            class,
            started_at: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn finish(&mut self, outcome: SqliteReadOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.observer.observe(SqliteReadEvent::OperationFinished {
            class: self.class,
            outcome,
            elapsed: self.started_at.elapsed(),
        });
    }
}

impl Drop for SqliteReadOperation {
    fn drop(&mut self) {
        self.finish(SqliteReadOutcome::Cancelled);
    }
}

/// Parent span used only to attribute SQLx pool-acquisition events. The five
/// possible names are fixed and consumed by pioneer-observability; SQL and
/// request data never enter this span.
pub(crate) fn reader_pool_span(class: SqliteReadClass) -> Span {
    match class {
        SqliteReadClass::Interactive => tracing::trace_span!(
            target: "pioneer_sqlite::pool",
            "pioneer.sqlite.reader.interactive"
        ),
        SqliteReadClass::Maintenance => tracing::trace_span!(
            target: "pioneer_sqlite::pool",
            "pioneer.sqlite.reader.maintenance"
        ),
    }
}
