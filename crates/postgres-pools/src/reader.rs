use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use sea_orm::DbErr;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS: usize = 1;

/// Scheduling class for PostgreSQL reads. Interactive reads use the pool
/// directly; maintenance reads share one process-local permit so background
/// work cannot consume every physical reader connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PostgresReadClass {
    #[default]
    Interactive,
    Maintenance,
}

impl PostgresReadClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Maintenance => "maintenance",
        }
    }
}

/// Terminal outcome for one typed PostgreSQL reader operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostgresReadOutcome {
    Ok,
    Error,
    Cancelled,
}

impl PostgresReadOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Low-cardinality lifecycle emitted by the typed PostgreSQL reader runtime.
/// Events deliberately contain no SQL, identifiers, paths, or error text.
#[derive(Clone, Copy, Debug)]
pub enum PostgresReadEvent {
    OperationFinished {
        class: PostgresReadClass,
        outcome: PostgresReadOutcome,
        elapsed: Duration,
    },
    AdmissionEnqueued {
        class: PostgresReadClass,
        queue_depth: usize,
        active: usize,
    },
    AdmissionAcquired {
        class: PostgresReadClass,
        waited: Duration,
        queue_depth: usize,
        active: usize,
    },
    AdmissionReleased {
        class: PostgresReadClass,
        held: Duration,
        queue_depth: usize,
        active: usize,
    },
    AdmissionCancelled {
        class: PostgresReadClass,
        waited: Duration,
        queue_depth: usize,
        active: usize,
    },
}

/// Receives low-cardinality reader lifecycle observations.
///
/// Implementations must return promptly: callbacks run inline after state has
/// been snapshotted, although never while the limiter state lock is held.
pub trait PostgresReadObserver: Send + Sync + 'static {
    fn observe(&self, event: PostgresReadEvent);

    /// Atomic observation adapter. Called after the native state lock is released.
    fn observe_admission(
        &self,
        event: PostgresReadEvent,
        snapshot: crate::PostgresAdmissionSnapshot,
    ) {
        self.observe(event);
        self.observe_queue_snapshot(snapshot);
    }
    /// Source-ordered current state; milestone depths above remain historical.
    fn observe_queue_snapshot(&self, _snapshot: crate::PostgresAdmissionSnapshot) {}
}

#[derive(Default)]
struct NoopPostgresReadObserver;

impl PostgresReadObserver for NoopPostgresReadObserver {
    fn observe(&self, _event: PostgresReadEvent) {}
}

pub(crate) fn noop_read_observer() -> Arc<dyn PostgresReadObserver> {
    Arc::new(NoopPostgresReadObserver)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadAdmissionSnapshot {
    current: crate::PostgresAdmissionSnapshot,
    queue_depth: usize,
    active: usize,
}

#[derive(Default)]
struct ReadAdmissionState {
    observation_revision: u64,
    queue_depth: usize,
    active: usize,
}

pub(crate) struct MaintenanceReadLimiter {
    semaphore: Arc<Semaphore>,
    state: Mutex<ReadAdmissionState>,
    observer: Arc<dyn PostgresReadObserver>,
}

impl std::fmt::Debug for MaintenanceReadLimiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MaintenanceReadLimiter")
    }
}

impl MaintenanceReadLimiter {
    pub(crate) fn new(observer: Arc<dyn PostgresReadObserver>) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_MAINTENANCE_READS)),
            state: Mutex::new(ReadAdmissionState::default()),
            observer,
        })
    }

    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<PostgresMaintenanceReadPermit, DbErr> {
        let enqueued_at = Instant::now();
        let snapshot = {
            let mut state = self.lock();
            state.queue_depth = state.queue_depth.saturating_add(1);
            state.snapshot()
        };
        self.observer.observe_admission(
            PostgresReadEvent::AdmissionEnqueued {
                class: PostgresReadClass::Maintenance,
                queue_depth: snapshot.queue_depth,
                active: snapshot.active,
            },
            snapshot.current,
        );

        let mut registration = ReadWaitRegistration {
            limiter: self.clone(),
            enqueued_at,
            armed: true,
        };
        let raw =
            self.semaphore.clone().acquire_owned().await.map_err(|_| {
                DbErr::Custom("PostgreSQL maintenance read limiter closed".to_owned())
            })?;
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
        self.observer.observe_admission(
            PostgresReadEvent::AdmissionAcquired {
                class: PostgresReadClass::Maintenance,
                waited: acquired_at.saturating_duration_since(enqueued_at),
                queue_depth: snapshot.queue_depth,
                active: snapshot.active,
            },
            snapshot.current,
        );
        Ok(PostgresMaintenanceReadPermit {
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
        self.observer.observe_admission(
            PostgresReadEvent::AdmissionCancelled {
                class: PostgresReadClass::Maintenance,
                waited: enqueued_at.elapsed(),
                queue_depth: snapshot.queue_depth,
                active: snapshot.active,
            },
            snapshot.current,
        );
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
        self.observer.observe_admission(
            PostgresReadEvent::AdmissionReleased {
                class: PostgresReadClass::Maintenance,
                held: acquired_at.elapsed(),
                queue_depth: snapshot.queue_depth,
                active: snapshot.active,
            },
            snapshot.current,
        );
    }

    fn lock(&self) -> MutexGuard<'_, ReadAdmissionState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ReadAdmissionState {
    fn snapshot(&mut self) -> ReadAdmissionSnapshot {
        self.observation_revision = self.observation_revision.saturating_add(1);
        ReadAdmissionSnapshot {
            current: crate::PostgresAdmissionSnapshot::capture(
                self.observation_revision,
                self.queue_depth,
                None,
            ),
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

pub(crate) struct PostgresMaintenanceReadPermit {
    limiter: Arc<MaintenanceReadLimiter>,
    raw: Option<OwnedSemaphorePermit>,
    acquired_at: Instant,
}

impl Drop for PostgresMaintenanceReadPermit {
    fn drop(&mut self) {
        self.limiter.release(&mut self.raw, self.acquired_at);
    }
}

pub(crate) struct PostgresReadOperation {
    observer: Arc<dyn PostgresReadObserver>,
    class: PostgresReadClass,
    started_at: Instant,
    finished: bool,
}

impl PostgresReadOperation {
    pub(crate) fn start(observer: Arc<dyn PostgresReadObserver>, class: PostgresReadClass) -> Self {
        Self {
            observer,
            class,
            started_at: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn finish(&mut self, outcome: PostgresReadOutcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        self.observer.observe(PostgresReadEvent::OperationFinished {
            class: self.class,
            outcome,
            elapsed: self.started_at.elapsed(),
        });
    }
}

impl Drop for PostgresReadOperation {
    fn drop(&mut self) {
        self.finish(PostgresReadOutcome::Cancelled);
    }
}

pub(crate) fn reader_pool_context(
    class: PostgresReadClass,
) -> pioneer_observability::pool_context::PoolContext {
    use pioneer_observability::pool_context::PoolContext;
    match class {
        PostgresReadClass::Interactive => PoolContext::ReaderInteractive,
        PostgresReadClass::Maintenance => PoolContext::ReaderMaintenance,
    }
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    #[test]
    fn native_reader_snapshot_has_source_revision_and_no_invented_head() {
        let mut state = ReadAdmissionState {
            queue_depth: 1,
            ..Default::default()
        };
        let old = state.snapshot();
        state.queue_depth = 0;
        let new = state.snapshot();
        assert_eq!((old.current.depth, new.current.depth), (1, 0));
        assert!(new.current.revision > old.current.revision);
        assert!(new.current.sampled_monotonic_ms >= old.current.sampled_monotonic_ms);
        assert_eq!(old.current.oldest_waited_ms, None);
        assert_eq!(new.current.oldest_waited_ms, None);
        NoopPostgresReadObserver.observe_queue_snapshot(new.current);
        NoopPostgresReadObserver.observe_queue_snapshot(old.current);
    }
}
