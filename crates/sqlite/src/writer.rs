use async_trait::async_trait;
use futures_util::Stream;
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr,
    ExecResult, IsolationLevel, QueryResult, QueryStream, Statement, StreamTrait, TransactionError,
    TransactionOptions, TransactionSession, TransactionStream, TransactionTrait,
};
use sea_orm_migration::MigratorTrait;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, oneshot};

static NEXT_EXECUTOR_ID: AtomicU64 = AtomicU64::new(1);

tokio::task_local! {
    static ACTIVE_WRITE_SCOPES: Vec<ActiveWriteScope>;
}

pub const DEFAULT_MAX_CRITICAL_BURST: usize = 8;
pub const DEFAULT_MAX_NON_MAINTENANCE_BURST: usize = 16;
pub const DEFAULT_MAX_MAINTENANCE_WAIT_MS: u64 = 2_000;

/// Scheduling class for every operation which can mutate the Gateway SQLite
/// database. The class is carried by a scoped database handle; callers never
/// acquire scheduler permits themselves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqliteWriteClass {
    Critical,
    #[default]
    Interactive,
    Maintenance,
}

impl SqliteWriteClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Interactive => "interactive",
            Self::Maintenance => "maintenance",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteWriteGrantReason {
    Uncontended,
    OnlyClass,
    Priority,
    InteractiveFairness,
    MaintenanceFairness,
}

impl SqliteWriteGrantReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uncontended => "uncontended",
            Self::OnlyClass => "only_class",
            Self::Priority => "priority",
            Self::InteractiveFairness => "interactive_fairness",
            Self::MaintenanceFairness => "maintenance_fairness",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SqliteWriteQueueSnapshot {
    pub critical: usize,
    pub interactive: usize,
    pub maintenance: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum SqliteWriteEvent {
    Enqueued {
        class: SqliteWriteClass,
        queue: SqliteWriteQueueSnapshot,
    },
    Acquired {
        class: SqliteWriteClass,
        reason: SqliteWriteGrantReason,
        waited: Duration,
        queue: SqliteWriteQueueSnapshot,
    },
    Released {
        class: SqliteWriteClass,
        held: Duration,
        queue: SqliteWriteQueueSnapshot,
    },
    Cancelled {
        class: SqliteWriteClass,
        waited: Duration,
        queue: SqliteWriteQueueSnapshot,
    },
}

pub trait SqliteWriteObserver: Send + Sync + 'static {
    fn observe(&self, event: SqliteWriteEvent);
}

#[derive(Default)]
struct NoopSqliteWriteObserver;

impl SqliteWriteObserver for NoopSqliteWriteObserver {
    fn observe(&self, _event: SqliteWriteEvent) {}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqliteWritePolicy {
    pub max_critical_burst: usize,
    pub max_non_maintenance_burst: usize,
    pub max_maintenance_wait: Duration,
}

impl Default for SqliteWritePolicy {
    fn default() -> Self {
        Self {
            max_critical_burst: DEFAULT_MAX_CRITICAL_BURST,
            max_non_maintenance_burst: DEFAULT_MAX_NON_MAINTENANCE_BURST,
            max_maintenance_wait: Duration::from_millis(DEFAULT_MAX_MAINTENANCE_WAIT_MS),
        }
    }
}

struct Waiter {
    id: u64,
    enqueued_at: Instant,
    sender: oneshot::Sender<Reservation>,
}

#[derive(Default)]
struct AdmissionState {
    active: bool,
    critical: VecDeque<Waiter>,
    interactive: VecDeque<Waiter>,
    maintenance: VecDeque<Waiter>,
    consecutive_critical: usize,
    consecutive_non_maintenance: usize,
}

impl AdmissionState {
    fn snapshot(&self) -> SqliteWriteQueueSnapshot {
        SqliteWriteQueueSnapshot {
            critical: self.critical.len(),
            interactive: self.interactive.len(),
            maintenance: self.maintenance.len(),
        }
    }

    fn queue_mut(&mut self, class: SqliteWriteClass) -> &mut VecDeque<Waiter> {
        match class {
            SqliteWriteClass::Critical => &mut self.critical,
            SqliteWriteClass::Interactive => &mut self.interactive,
            SqliteWriteClass::Maintenance => &mut self.maintenance,
        }
    }
}

struct AdmissionInner {
    state: Mutex<AdmissionState>,
    next_waiter_id: AtomicU64,
    policy: SqliteWritePolicy,
    observer: Arc<dyn SqliteWriteObserver>,
}

struct Dispatch {
    sender: oneshot::Sender<Reservation>,
    reservation: Reservation,
}

struct Reservation {
    inner: Arc<AdmissionInner>,
    class: SqliteWriteClass,
    reason: SqliteWriteGrantReason,
    enqueued_at: Instant,
    acquired_at: Instant,
    queue: SqliteWriteQueueSnapshot,
    started: bool,
}

impl Reservation {
    fn start(&mut self) {
        self.started = true;
        self.inner.observer.observe(SqliteWriteEvent::Acquired {
            class: self.class,
            reason: self.reason,
            waited: self.acquired_at.saturating_duration_since(self.enqueued_at),
            queue: self.queue,
        });
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let held = self.acquired_at.elapsed();
        let (queue, dispatch) = self.inner.release();
        if self.started {
            self.inner.observer.observe(SqliteWriteEvent::Released {
                class: self.class,
                held,
                queue,
            });
        } else {
            // The waiter can be cancelled after the scheduler has selected it
            // but before the receiving future constructs its permit. That
            // race still has to close the observer lifecycle; otherwise an
            // `Enqueued` event would have neither `Acquired` nor `Cancelled`.
            self.inner.observer.observe(SqliteWriteEvent::Cancelled {
                class: self.class,
                waited: self.acquired_at.saturating_duration_since(self.enqueued_at),
                queue,
            });
        }
        AdmissionInner::dispatch(dispatch);
    }
}

struct WaitRegistration {
    inner: Arc<AdmissionInner>,
    id: u64,
    class: SqliteWriteClass,
    enqueued_at: Instant,
    armed: bool,
}

impl WaitRegistration {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some((queue, dispatch)) = self.inner.cancel(self.id, self.class) else {
            return;
        };
        self.inner.observer.observe(SqliteWriteEvent::Cancelled {
            class: self.class,
            waited: self.enqueued_at.elapsed(),
            queue,
        });
        AdmissionInner::dispatch(dispatch);
    }
}

#[derive(Clone)]
struct SqliteWritePermit {
    // The reservation is released only after the final owner disappears.
    // Nested transactions clone this lease so dropping the root wrapper
    // cannot expose the physical writer while a savepoint is still alive.
    _reservation: Arc<Reservation>,
}

#[derive(Clone)]
struct ActiveWriteScope {
    executor_id: u64,
    permit: SqliteWritePermit,
}

impl SqliteWritePermit {
    fn new(mut reservation: Reservation) -> Self {
        reservation.start();
        Self {
            _reservation: Arc::new(reservation),
        }
    }
}

impl AdmissionInner {
    fn enqueue(
        self: &Arc<Self>,
        class: SqliteWriteClass,
    ) -> (oneshot::Receiver<Reservation>, WaitRegistration) {
        let id = self.next_waiter_id.fetch_add(1, Ordering::Relaxed);
        let enqueued_at = Instant::now();
        let (sender, receiver) = oneshot::channel();
        let waiter = Waiter {
            id,
            enqueued_at,
            sender,
        };
        let (queue, dispatch) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.queue_mut(class).push_back(waiter);
            let queue = state.snapshot();
            let dispatch = (!state.active)
                .then(|| self.grant_next_locked(&mut state, true))
                .flatten();
            (queue, dispatch)
        };
        self.observer
            .observe(SqliteWriteEvent::Enqueued { class, queue });
        Self::dispatch(dispatch);
        (
            receiver,
            WaitRegistration {
                inner: self.clone(),
                id,
                class,
                enqueued_at,
                armed: true,
            },
        )
    }

    fn cancel(
        self: &Arc<Self>,
        id: u64,
        class: SqliteWriteClass,
    ) -> Option<(SqliteWriteQueueSnapshot, Option<Dispatch>)> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let position = state
            .queue_mut(class)
            .iter()
            .position(|waiter| waiter.id == id)?;
        state.queue_mut(class).remove(position);
        let snapshot = state.snapshot();
        let dispatch = (!state.active)
            .then(|| self.grant_next_locked(&mut state, false))
            .flatten();
        Some((snapshot, dispatch))
    }

    fn release(self: &Arc<Self>) -> (SqliteWriteQueueSnapshot, Option<Dispatch>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.active, "SQLite writer released without an owner");
        state.active = false;
        let queue = state.snapshot();
        let dispatch = self.grant_next_locked(&mut state, false);
        (queue, dispatch)
    }

    fn grant_next_locked(
        self: &Arc<Self>,
        state: &mut AdmissionState,
        newly_uncontended: bool,
    ) -> Option<Dispatch> {
        debug_assert!(!state.active);
        let now = Instant::now();
        let critical = !state.critical.is_empty();
        let interactive = !state.interactive.is_empty();
        let maintenance = !state.maintenance.is_empty();
        let class_count =
            usize::from(critical) + usize::from(interactive) + usize::from(maintenance);

        let maintenance_aged = state
            .maintenance
            .front()
            .is_some_and(|waiter| waiter.enqueued_at.elapsed() >= self.policy.max_maintenance_wait);
        let maintenance_due = maintenance
            && (maintenance_aged
                || state.consecutive_non_maintenance
                    >= self.policy.max_non_maintenance_burst.max(1));
        let interactive_due = interactive
            && critical
            && state.consecutive_critical >= self.policy.max_critical_burst.max(1);

        let (class, reason) = if class_count == 0 {
            return None;
        } else if newly_uncontended {
            debug_assert_eq!(class_count, 1);
            let class = if critical {
                SqliteWriteClass::Critical
            } else if interactive {
                SqliteWriteClass::Interactive
            } else {
                SqliteWriteClass::Maintenance
            };
            (class, SqliteWriteGrantReason::Uncontended)
        } else if class_count == 1 {
            let class = if critical {
                SqliteWriteClass::Critical
            } else if interactive {
                SqliteWriteClass::Interactive
            } else {
                SqliteWriteClass::Maintenance
            };
            (class, SqliteWriteGrantReason::OnlyClass)
        } else if maintenance_due {
            (
                SqliteWriteClass::Maintenance,
                SqliteWriteGrantReason::MaintenanceFairness,
            )
        } else if interactive_due {
            (
                SqliteWriteClass::Interactive,
                SqliteWriteGrantReason::InteractiveFairness,
            )
        } else if critical {
            (SqliteWriteClass::Critical, SqliteWriteGrantReason::Priority)
        } else if interactive {
            (
                SqliteWriteClass::Interactive,
                SqliteWriteGrantReason::Priority,
            )
        } else {
            (
                SqliteWriteClass::Maintenance,
                SqliteWriteGrantReason::Priority,
            )
        };

        match reason {
            SqliteWriteGrantReason::Uncontended | SqliteWriteGrantReason::OnlyClass => {
                // Fairness debt is meaningful only while classes contend.
                // Work which ran alone must not make a future waiter jump the
                // queue based on activity which predated that waiter.
                state.consecutive_critical = 0;
                state.consecutive_non_maintenance = 0;
            }
            SqliteWriteGrantReason::Priority
            | SqliteWriteGrantReason::InteractiveFairness
            | SqliteWriteGrantReason::MaintenanceFairness => match class {
                SqliteWriteClass::Critical => {
                    state.consecutive_critical = state.consecutive_critical.saturating_add(1);
                    state.consecutive_non_maintenance =
                        state.consecutive_non_maintenance.saturating_add(1);
                }
                SqliteWriteClass::Interactive => {
                    state.consecutive_critical = 0;
                    state.consecutive_non_maintenance =
                        state.consecutive_non_maintenance.saturating_add(1);
                }
                SqliteWriteClass::Maintenance => {
                    state.consecutive_critical = 0;
                    state.consecutive_non_maintenance = 0;
                }
            },
        }

        let waiter = state
            .queue_mut(class)
            .pop_front()
            .expect("selected SQLite writer queue must contain a waiter");
        state.active = true;
        let queue = state.snapshot();
        Some(Dispatch {
            sender: waiter.sender,
            reservation: Reservation {
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

    fn dispatch(dispatch: Option<Dispatch>) {
        let Some(dispatch) = dispatch else {
            return;
        };
        if let Err(reservation) = dispatch.sender.send(dispatch.reservation) {
            drop(reservation);
        }
    }
}

/// Sole owner of the physical SQLite writer connection and its scheduling
/// queue. A write cannot reach the connection without a classed reservation.
#[derive(Clone)]
pub struct SqliteWriteExecutor {
    id: u64,
    connection: DatabaseConnection,
    admission: Arc<AdmissionInner>,
}

impl std::fmt::Debug for SqliteWriteExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteWriteExecutor")
    }
}

impl SqliteWriteExecutor {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self::with_policy_and_observer(
            connection,
            SqliteWritePolicy::default(),
            Arc::new(NoopSqliteWriteObserver),
        )
    }

    pub fn with_observer(
        connection: DatabaseConnection,
        observer: Arc<dyn SqliteWriteObserver>,
    ) -> Self {
        Self::with_policy_and_observer(connection, SqliteWritePolicy::default(), observer)
    }

    pub fn with_policy_and_observer(
        connection: DatabaseConnection,
        policy: SqliteWritePolicy,
        observer: Arc<dyn SqliteWriteObserver>,
    ) -> Self {
        Self {
            id: NEXT_EXECUTOR_ID.fetch_add(1, Ordering::Relaxed),
            connection,
            admission: Arc::new(AdmissionInner {
                state: Mutex::new(AdmissionState::default()),
                next_waiter_id: AtomicU64::new(1),
                policy,
                observer,
            }),
        }
    }

    pub(crate) fn connection(&self, class: SqliteWriteClass) -> SqliteWriteConnection {
        SqliteWriteConnection {
            executor: self.clone(),
            class,
        }
    }

    pub fn max_connections(&self) -> u32 {
        self.connection
            .get_sqlite_connection_pool()
            .options()
            .get_max_connections()
    }

    pub async fn close(self) -> Result<(), DbErr> {
        self.connection.close().await
    }

    /// Applies the Gateway's connection pragmas while the physical writer is
    /// exclusively reserved. The raw connection never crosses this crate's
    /// API boundary.
    pub async fn apply_pragmas(&self, class: SqliteWriteClass) -> anyhow::Result<()> {
        let _permit = self.acquire_or_reuse(class).await;
        crate::apply_sqlite_pragmas(&self.connection).await
    }

    /// Applies one SeaORM migrator while the physical writer is exclusively
    /// reserved. Keeping the generic migration entry point here prevents a
    /// bootstrap callback from cloning and leaking the raw connection.
    pub async fn run_migrations<M>(
        &self,
        class: SqliteWriteClass,
        steps: Option<u32>,
    ) -> Result<(), DbErr>
    where
        M: MigratorTrait,
    {
        let _permit = self.acquire_or_reuse(class).await;
        M::up(&self.connection, steps).await
    }

    /// Runs one logical write operation under this executor's classed
    /// reservation. Nested statements and transactions reuse the same
    /// reservation, preserving operation-level serialization without a
    /// second admission controller. The scope is task-local and deliberately
    /// is not inherited by spawned tasks.
    pub(crate) async fn run_scoped<T, Fut>(&self, class: SqliteWriteClass, operation: Fut) -> T
    where
        Fut: Future<Output = T>,
    {
        if self.active_permit().is_some() {
            return operation.await;
        }

        let permit = self.acquire(class).await;
        let mut scopes = ACTIVE_WRITE_SCOPES
            .try_with(Clone::clone)
            .unwrap_or_default();
        scopes.push(ActiveWriteScope {
            executor_id: self.id,
            permit,
        });
        ACTIVE_WRITE_SCOPES.scope(scopes, operation).await
    }

    fn active_permit(&self) -> Option<SqliteWritePermit> {
        ACTIVE_WRITE_SCOPES
            .try_with(|scopes| {
                scopes
                    .iter()
                    .rev()
                    .find(|scope| scope.executor_id == self.id)
                    .map(|scope| scope.permit.clone())
            })
            .ok()
            .flatten()
    }

    async fn acquire_or_reuse(&self, class: SqliteWriteClass) -> SqliteWritePermit {
        match self.active_permit() {
            Some(permit) => permit,
            None => self.acquire(class).await,
        }
    }

    async fn acquire(&self, class: SqliteWriteClass) -> SqliteWritePermit {
        let (receiver, mut registration) = self.admission.enqueue(class);
        let reservation = receiver
            .await
            .expect("SQLite writer executor must outlive its queued reservations");
        registration.disarm();
        SqliteWritePermit::new(reservation)
    }
}

/// A class-scoped view of the writer executor. It exposes SeaORM traits but
/// never the underlying physical connection.
#[derive(Clone, Debug)]
pub(crate) struct SqliteWriteConnection {
    executor: SqliteWriteExecutor,
    class: SqliteWriteClass,
}

#[async_trait]
impl ConnectionTrait for SqliteWriteConnection {
    fn get_database_backend(&self) -> DbBackend {
        self.executor.connection.get_database_backend()
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        let _permit = self.executor.acquire_or_reuse(self.class).await;
        self.executor.connection.execute_raw(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        let _permit = self.executor.acquire_or_reuse(self.class).await;
        self.executor.connection.execute_unprepared(sql).await
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        let _permit = self.executor.acquire_or_reuse(self.class).await;
        self.executor.connection.query_one_raw(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        let _permit = self.executor.acquire_or_reuse(self.class).await;
        self.executor.connection.query_all_raw(statement).await
    }

    fn support_returning(&self) -> bool {
        self.executor.connection.support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.executor.connection.is_mock_connection()
    }
}

/// Stream wrapper which retains either the writer reservation or the
/// maintenance-read permit until the returned rows have been consumed or the
/// stream is dropped.
pub struct SqliteQueryStream {
    inner: Pin<Box<QueryStream>>,
    writer_permit: Option<SqliteWritePermit>,
    maintenance_read_permit: Option<OwnedSemaphorePermit>,
}

impl SqliteQueryStream {
    pub(crate) fn read(inner: QueryStream, permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            inner: Box::pin(inner),
            writer_permit: None,
            maintenance_read_permit: permit,
        }
    }

    fn write(inner: QueryStream, permit: SqliteWritePermit) -> Self {
        Self {
            inner: Box::pin(inner),
            writer_permit: Some(permit),
            maintenance_read_permit: None,
        }
    }
}

impl Stream for SqliteQueryStream {
    type Item = Result<QueryResult, DbErr>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.inner.as_mut().poll_next(context);
        if matches!(result, Poll::Ready(None)) {
            self.writer_permit.take();
            self.maintenance_read_permit.take();
        }
        result
    }
}

impl StreamTrait for SqliteWriteConnection {
    type Stream<'a> = SqliteQueryStream;

    fn get_database_backend(&self) -> DbBackend {
        self.executor.connection.get_database_backend()
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            let permit = self.executor.acquire_or_reuse(self.class).await;
            let stream = self.executor.connection.stream_raw(statement).await?;
            Ok(SqliteQueryStream::write(stream, permit))
        })
    }
}

/// Transaction returned by the writer executor. The reservation is released
/// only after the SeaORM transaction commits, rolls back, or is dropped.
pub struct SqliteTransaction {
    inner: Option<DatabaseTransaction>,
    _permit: Option<SqliteWritePermit>,
}

impl std::fmt::Debug for SqliteTransaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteTransaction")
    }
}

impl SqliteTransaction {
    fn root(inner: DatabaseTransaction, permit: SqliteWritePermit) -> Self {
        Self {
            inner: Some(inner),
            _permit: Some(permit),
        }
    }

    fn nested(inner: DatabaseTransaction, permit: SqliteWritePermit) -> Self {
        Self {
            inner: Some(inner),
            _permit: Some(permit),
        }
    }

    fn inner(&self) -> &DatabaseTransaction {
        self.inner
            .as_ref()
            .expect("SQLite transaction was already completed")
    }

    pub async fn commit(mut self) -> Result<(), DbErr> {
        self.inner
            .take()
            .expect("SQLite transaction was already completed")
            .commit()
            .await
    }

    pub async fn rollback(mut self) -> Result<(), DbErr> {
        self.inner
            .take()
            .expect("SQLite transaction was already completed")
            .rollback()
            .await
    }
}

impl Deref for SqliteTransaction {
    type Target = DatabaseTransaction;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

#[async_trait]
impl TransactionSession for SqliteTransaction {
    async fn commit(self) -> Result<(), DbErr> {
        SqliteTransaction::commit(self).await
    }

    async fn rollback(self) -> Result<(), DbErr> {
        SqliteTransaction::rollback(self).await
    }
}

#[async_trait]
impl ConnectionTrait for SqliteTransaction {
    fn get_database_backend(&self) -> DbBackend {
        ConnectionTrait::get_database_backend(self.inner())
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.inner().execute_raw(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.inner().execute_unprepared(sql).await
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.inner().query_one_raw(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.inner().query_all_raw(statement).await
    }

    fn support_returning(&self) -> bool {
        self.inner().support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.inner().is_mock_connection()
    }
}

impl StreamTrait for SqliteTransaction {
    type Stream<'a> = TransactionStream<'a>;

    fn get_database_backend(&self) -> DbBackend {
        StreamTrait::get_database_backend(self.inner())
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        self.inner().stream_raw(statement)
    }
}

async fn finish_callback<T, E>(
    transaction: SqliteTransaction,
    result: Result<T, E>,
) -> Result<T, TransactionError<E>>
where
    E: std::fmt::Display + std::fmt::Debug + Send,
{
    match result {
        Ok(value) => {
            transaction
                .commit()
                .await
                .map_err(TransactionError::Connection)?;
            Ok(value)
        }
        Err(error) => {
            transaction
                .rollback()
                .await
                .map_err(TransactionError::Connection)?;
            Err(TransactionError::Transaction(error))
        }
    }
}

#[async_trait]
impl TransactionTrait for SqliteWriteConnection {
    type Transaction = SqliteTransaction;

    async fn begin(&self) -> Result<Self::Transaction, DbErr> {
        let permit = self.executor.acquire_or_reuse(self.class).await;
        let transaction = self.executor.connection.begin().await?;
        Ok(SqliteTransaction::root(transaction, permit))
    }

    async fn begin_with_config(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<Self::Transaction, DbErr> {
        let permit = self.executor.acquire_or_reuse(self.class).await;
        let transaction = self
            .executor
            .connection
            .begin_with_config(isolation_level, access_mode)
            .await?;
        Ok(SqliteTransaction::root(transaction, permit))
    }

    async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction, DbErr> {
        let permit = self.executor.acquire_or_reuse(self.class).await;
        let transaction = self.executor.connection.begin_with_options(options).await?;
        Ok(SqliteTransaction::root(transaction, permit))
    }

    async fn transaction<F, T, E>(&self, callback: F) -> Result<T, TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        let transaction = self.begin().await.map_err(TransactionError::Connection)?;
        let result = callback(&transaction).await;
        finish_callback(transaction, result).await
    }

    async fn transaction_with_config<F, T, E>(
        &self,
        callback: F,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<T, TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        let transaction = self
            .begin_with_config(isolation_level, access_mode)
            .await
            .map_err(TransactionError::Connection)?;
        let result = callback(&transaction).await;
        finish_callback(transaction, result).await
    }
}

#[async_trait]
impl TransactionTrait for SqliteTransaction {
    type Transaction = SqliteTransaction;

    async fn begin(&self) -> Result<Self::Transaction, DbErr> {
        Ok(SqliteTransaction::nested(
            self.inner().begin().await?,
            self._permit
                .as_ref()
                .expect("SQLite transaction must retain a writer permit")
                .clone(),
        ))
    }

    async fn begin_with_config(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<Self::Transaction, DbErr> {
        Ok(SqliteTransaction::nested(
            self.inner()
                .begin_with_config(isolation_level, access_mode)
                .await?,
            self._permit
                .as_ref()
                .expect("SQLite transaction must retain a writer permit")
                .clone(),
        ))
    }

    async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction, DbErr> {
        Ok(SqliteTransaction::nested(
            self.inner().begin_with_options(options).await?,
            self._permit
                .as_ref()
                .expect("SQLite transaction must retain a writer permit")
                .clone(),
        ))
    }

    async fn transaction<F, T, E>(&self, callback: F) -> Result<T, TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        let transaction = self.begin().await.map_err(TransactionError::Connection)?;
        let result = callback(&transaction).await;
        finish_callback(transaction, result).await
    }

    async fn transaction_with_config<F, T, E>(
        &self,
        callback: F,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<T, TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c Self::Transaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        let transaction = self
            .begin_with_config(isolation_level, access_mode)
            .await
            .map_err(TransactionError::Connection)?;
        let result = callback(&transaction).await;
        finish_callback(transaction, result).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use sea_orm::{ConnectOptions, Database};
    use tokio::sync::{Notify, mpsc};

    fn admission(policy: SqliteWritePolicy) -> Arc<AdmissionInner> {
        Arc::new(AdmissionInner {
            state: Mutex::new(AdmissionState::default()),
            next_waiter_id: AtomicU64::new(1),
            policy,
            observer: Arc::new(NoopSqliteWriteObserver),
        })
    }

    async fn acquire(inner: &Arc<AdmissionInner>, class: SqliteWriteClass) -> SqliteWritePermit {
        let (receiver, mut registration) = inner.enqueue(class);
        let reservation = receiver.await.expect("reservation");
        registration.disarm();
        SqliteWritePermit::new(reservation)
    }

    fn spawn_waiter(
        inner: Arc<AdmissionInner>,
        class: SqliteWriteClass,
        name: &'static str,
        acquired: mpsc::UnboundedSender<&'static str>,
        release: Arc<Notify>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let _permit = acquire(&inner, class).await;
            acquired.send(name).expect("receiver lives");
            release.notified().await;
        })
    }

    async fn wait_for_queue(inner: &Arc<AdmissionInner>, expected: SqliteWriteQueueSnapshot) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let snapshot = inner
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .snapshot();
                if snapshot == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queue should reach expected state");
    }

    #[tokio::test]
    async fn critical_precedes_interactive_and_maintenance() {
        let inner = admission(SqliteWritePolicy::default());
        let blocker = acquire(&inner, SqliteWriteClass::Interactive).await;
        let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
        let critical_release = Arc::new(Notify::new());
        let interactive_release = Arc::new(Notify::new());
        let maintenance_release = Arc::new(Notify::new());
        let maintenance = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Maintenance,
            "maintenance",
            acquired_tx.clone(),
            maintenance_release.clone(),
        );
        let interactive = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Interactive,
            "interactive",
            acquired_tx.clone(),
            interactive_release.clone(),
        );
        let critical = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical",
            acquired_tx,
            critical_release.clone(),
        );
        wait_for_queue(
            &inner,
            SqliteWriteQueueSnapshot {
                critical: 1,
                interactive: 1,
                maintenance: 1,
            },
        )
        .await;
        drop(blocker);

        assert_eq!(acquired_rx.recv().await, Some("critical"));
        critical_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("interactive"));
        interactive_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("maintenance"));
        maintenance_release.notify_one();
        critical.await.unwrap();
        interactive.await.unwrap();
        maintenance.await.unwrap();
    }

    #[tokio::test]
    async fn interactive_progress_is_bounded_under_continuous_critical_load() {
        let inner = admission(SqliteWritePolicy {
            max_critical_burst: 2,
            max_non_maintenance_burst: 32,
            max_maintenance_wait: Duration::from_secs(60),
        });
        let blocker = acquire(&inner, SqliteWriteClass::Interactive).await;
        let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
        let releases = (0..4).map(|_| Arc::new(Notify::new())).collect::<Vec<_>>();
        let interactive = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Interactive,
            "interactive",
            acquired_tx.clone(),
            releases[0].clone(),
        );
        let critical_one = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical-1",
            acquired_tx.clone(),
            releases[1].clone(),
        );
        let critical_two = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical-2",
            acquired_tx.clone(),
            releases[2].clone(),
        );
        let critical_three = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical-3",
            acquired_tx,
            releases[3].clone(),
        );
        wait_for_queue(
            &inner,
            SqliteWriteQueueSnapshot {
                critical: 3,
                interactive: 1,
                maintenance: 0,
            },
        )
        .await;

        drop(blocker);
        assert_eq!(acquired_rx.recv().await, Some("critical-1"));
        releases[1].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("critical-2"));
        releases[2].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("interactive"));
        releases[0].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("critical-3"));
        releases[3].notify_one();

        for waiter in [interactive, critical_one, critical_two, critical_three] {
            waiter.await.expect("writer waiter should join");
        }
    }

    #[tokio::test]
    async fn maintenance_progress_is_bounded_under_mixed_non_maintenance_load() {
        let inner = admission(SqliteWritePolicy {
            max_critical_burst: 2,
            max_non_maintenance_burst: 3,
            max_maintenance_wait: Duration::from_secs(60),
        });
        let blocker = acquire(&inner, SqliteWriteClass::Interactive).await;
        let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
        let releases = (0..5).map(|_| Arc::new(Notify::new())).collect::<Vec<_>>();
        let maintenance = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Maintenance,
            "maintenance",
            acquired_tx.clone(),
            releases[0].clone(),
        );
        let interactive = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Interactive,
            "interactive",
            acquired_tx.clone(),
            releases[1].clone(),
        );
        let critical_one = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical-1",
            acquired_tx.clone(),
            releases[2].clone(),
        );
        let critical_two = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical-2",
            acquired_tx.clone(),
            releases[3].clone(),
        );
        let critical_three = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical-3",
            acquired_tx,
            releases[4].clone(),
        );
        wait_for_queue(
            &inner,
            SqliteWriteQueueSnapshot {
                critical: 3,
                interactive: 1,
                maintenance: 1,
            },
        )
        .await;

        drop(blocker);
        assert_eq!(acquired_rx.recv().await, Some("critical-1"));
        releases[2].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("critical-2"));
        releases[3].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("interactive"));
        releases[1].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("maintenance"));
        releases[0].notify_one();
        assert_eq!(acquired_rx.recv().await, Some("critical-3"));
        releases[4].notify_one();

        for waiter in [
            maintenance,
            interactive,
            critical_one,
            critical_two,
            critical_three,
        ] {
            waiter.await.expect("writer waiter should join");
        }
    }

    #[tokio::test]
    async fn cancelling_a_queued_waiter_does_not_leak_writer_capacity() {
        let inner = admission(SqliteWritePolicy::default());
        let blocker = acquire(&inner, SqliteWriteClass::Interactive).await;
        let cancelled = tokio::spawn({
            let inner = inner.clone();
            async move {
                let _permit = acquire(&inner, SqliteWriteClass::Maintenance).await;
            }
        });
        wait_for_queue(
            &inner,
            SqliteWriteQueueSnapshot {
                critical: 0,
                interactive: 0,
                maintenance: 1,
            },
        )
        .await;
        cancelled.abort();
        let _ = cancelled.await;
        wait_for_queue(&inner, SqliteWriteQueueSnapshot::default()).await;
        drop(blocker);

        tokio::time::timeout(
            Duration::from_secs(1),
            acquire(&inner, SqliteWriteClass::Critical),
        )
        .await
        .expect("cancelled waiter must release queue capacity");
    }

    #[tokio::test]
    async fn maintenance_wait_deadline_overrides_priority_at_the_next_boundary() {
        let inner = admission(SqliteWritePolicy {
            max_critical_burst: 32,
            max_non_maintenance_burst: 32,
            max_maintenance_wait: Duration::from_millis(10),
        });
        let blocker = acquire(&inner, SqliteWriteClass::Interactive).await;
        let (acquired_tx, mut acquired_rx) = mpsc::unbounded_channel();
        let maintenance_release = Arc::new(Notify::new());
        let critical_release = Arc::new(Notify::new());
        let maintenance = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Maintenance,
            "maintenance",
            acquired_tx.clone(),
            maintenance_release.clone(),
        );
        let critical = spawn_waiter(
            inner.clone(),
            SqliteWriteClass::Critical,
            "critical",
            acquired_tx,
            critical_release.clone(),
        );
        wait_for_queue(
            &inner,
            SqliteWriteQueueSnapshot {
                critical: 1,
                interactive: 0,
                maintenance: 1,
            },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(25)).await;

        drop(blocker);
        assert_eq!(acquired_rx.recv().await, Some("maintenance"));
        maintenance_release.notify_one();
        assert_eq!(acquired_rx.recv().await, Some("critical"));
        critical_release.notify_one();
        maintenance.await.expect("maintenance waiter should join");
        critical.await.expect("critical waiter should join");
    }

    #[derive(Default)]
    struct RecordingObserver {
        acquired: Mutex<Vec<SqliteWriteClass>>,
        cancelled: Mutex<Vec<SqliteWriteClass>>,
    }

    impl SqliteWriteObserver for RecordingObserver {
        fn observe(&self, event: SqliteWriteEvent) {
            match event {
                SqliteWriteEvent::Acquired { class, .. } => self
                    .acquired
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(class),
                SqliteWriteEvent::Cancelled { class, .. } => self
                    .cancelled
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(class),
                SqliteWriteEvent::Enqueued { .. } | SqliteWriteEvent::Released { .. } => {}
            }
        }
    }

    #[tokio::test]
    async fn cancelling_after_dispatch_closes_observer_lifecycle_and_releases_capacity() {
        let observer = Arc::new(RecordingObserver::default());
        let inner = Arc::new(AdmissionInner {
            state: Mutex::new(AdmissionState::default()),
            next_waiter_id: AtomicU64::new(1),
            policy: SqliteWritePolicy::default(),
            observer: observer.clone(),
        });

        // With an idle executor enqueue dispatches synchronously. Dropping the
        // receiver exercises cancellation after selection but before the
        // future can construct a SqliteWritePermit.
        let (receiver, registration) = inner.enqueue(SqliteWriteClass::Critical);
        drop(receiver);
        drop(registration);

        assert_eq!(
            *observer
                .cancelled
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![SqliteWriteClass::Critical]
        );
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire(&inner, SqliteWriteClass::Interactive),
        )
        .await
        .expect("dispatched cancellation must release writer capacity");
    }

    #[tokio::test]
    async fn class_scoped_connections_reach_the_executor_with_their_declared_class() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let observer = Arc::new(RecordingObserver::default());
        let executor = SqliteWriteExecutor::with_observer(connection, observer.clone());

        executor
            .connection(SqliteWriteClass::Critical)
            .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .await
            .expect("critical write");
        executor
            .connection(SqliteWriteClass::Interactive)
            .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
            .await
            .expect("interactive write");
        executor
            .connection(SqliteWriteClass::Maintenance)
            .execute_unprepared("DELETE FROM probe WHERE id = 1")
            .await
            .expect("maintenance write");

        assert_eq!(
            *observer
                .acquired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![
                SqliteWriteClass::Critical,
                SqliteWriteClass::Interactive,
                SqliteWriteClass::Maintenance,
            ]
        );
    }

    #[tokio::test]
    async fn logical_write_scope_reuses_one_classed_reservation() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let observer = Arc::new(RecordingObserver::default());
        let executor = SqliteWriteExecutor::with_observer(connection, observer.clone());
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);

        executor
            .run_scoped(SqliteWriteClass::Maintenance, async {
                maintenance
                    .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
                    .await
                    .expect("create table in scope");
                maintenance
                    .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
                    .await
                    .expect("insert in scope");
                executor
                    .run_scoped(SqliteWriteClass::Critical, async {
                        maintenance
                            .execute_unprepared("INSERT INTO probe (id) VALUES (2)")
                            .await
                            .expect("nested scope insert");
                    })
                    .await;
            })
            .await;

        assert_eq!(
            *observer
                .acquired
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![SqliteWriteClass::Maintenance]
        );
    }

    #[tokio::test]
    async fn spawned_tasks_do_not_inherit_a_logical_write_scope() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);
        let interactive = executor.connection(SqliteWriteClass::Interactive);

        let child = executor
            .run_scoped(SqliteWriteClass::Maintenance, async {
                maintenance
                    .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
                    .await
                    .expect("create table in scope");
                let child = tokio::spawn(async move {
                    interactive
                        .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
                        .await
                });
                wait_for_queue(
                    &executor.admission,
                    SqliteWriteQueueSnapshot {
                        critical: 0,
                        interactive: 1,
                        maintenance: 0,
                    },
                )
                .await;
                assert!(!child.is_finished());
                Some(child)
            })
            .await
            .expect("scope should return the child handle");

        tokio::time::timeout(Duration::from_secs(1), child)
            .await
            .expect("child must run after the parent scope releases the writer")
            .expect("child should join")
            .expect("child write should succeed");
    }

    #[tokio::test]
    async fn cancelling_an_active_logical_scope_releases_writer_capacity() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let entered = Arc::new(Notify::new());
        let never_release = Arc::new(Notify::new());
        let active = tokio::spawn({
            let executor = executor.clone();
            let entered = entered.clone();
            let never_release = never_release.clone();
            async move {
                executor
                    .run_scoped(SqliteWriteClass::Maintenance, async move {
                        entered.notify_one();
                        never_release.notified().await;
                    })
                    .await;
            }
        });

        entered.notified().await;
        active.abort();
        let _ = active.await;
        tokio::time::timeout(
            Duration::from_secs(1),
            acquire(&executor.admission, SqliteWriteClass::Critical),
        )
        .await
        .expect("cancelled logical scope must release writer capacity");
    }

    #[tokio::test]
    async fn transaction_escaping_a_logical_scope_retains_its_reservation() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);
        let interactive = executor.connection(SqliteWriteClass::Interactive);
        maintenance
            .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let transaction = executor
            .run_scoped(SqliteWriteClass::Maintenance, maintenance.begin())
            .await
            .expect("begin transaction in scope");
        let interactive_task = tokio::spawn(async move {
            interactive
                .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!interactive_task.is_finished());

        transaction.rollback().await.expect("rollback transaction");
        tokio::time::timeout(Duration::from_secs(1), interactive_task)
            .await
            .expect("transaction completion must release writer capacity")
            .expect("interactive task should join")
            .expect("interactive write should succeed");
    }

    #[tokio::test]
    async fn transaction_holds_executor_until_commit() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);
        let interactive = executor.connection(SqliteWriteClass::Interactive);
        maintenance
            .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let transaction = maintenance.begin().await.expect("begin maintenance");
        transaction
            .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
            .await
            .expect("insert in transaction");
        let interactive_task = tokio::spawn(async move {
            interactive
                .execute_unprepared("INSERT INTO probe (id) VALUES (2)")
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!interactive_task.is_finished());
        transaction.commit().await.expect("commit maintenance");
        interactive_task
            .await
            .expect("join interactive")
            .expect("interactive write");
    }

    #[tokio::test]
    async fn dropped_transaction_releases_executor_capacity() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);
        let interactive = executor.connection(SqliteWriteClass::Interactive);
        maintenance
            .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let transaction = maintenance.begin().await.expect("begin maintenance");
        let interactive_task = tokio::spawn(async move {
            interactive
                .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!interactive_task.is_finished());
        drop(transaction);
        tokio::time::timeout(Duration::from_secs(1), interactive_task)
            .await
            .expect("dropped transaction must release writer executor")
            .expect("interactive task should join")
            .expect("interactive write should succeed");
    }

    #[tokio::test]
    async fn nested_transaction_retains_executor_after_root_wrapper_is_dropped() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);
        let interactive = executor.connection(SqliteWriteClass::Interactive);
        maintenance
            .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let root = maintenance.begin().await.expect("begin root transaction");
        let nested = root.begin().await.expect("begin nested transaction");
        drop(root);
        let interactive_task = tokio::spawn(async move {
            interactive
                .execute_unprepared("INSERT INTO probe (id) VALUES (1)")
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!interactive_task.is_finished());

        drop(nested);
        tokio::time::timeout(Duration::from_secs(1), interactive_task)
            .await
            .expect("final nested transaction owner must release writer executor")
            .expect("interactive task should join")
            .expect("interactive write should succeed");
    }

    #[tokio::test]
    async fn write_stream_holds_executor_until_stream_drop() {
        let mut options = ConnectOptions::new("sqlite::memory:");
        options.max_connections(1);
        let connection = Database::connect(options).await.expect("connect");
        let executor = SqliteWriteExecutor::new(connection);
        let maintenance = executor.connection(SqliteWriteClass::Maintenance);
        let interactive = executor.connection(SqliteWriteClass::Interactive);
        maintenance
            .execute_unprepared("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .await
            .expect("create table");

        let mut stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Sqlite,
                "INSERT INTO probe (id) VALUES (1) RETURNING id",
            ))
            .await
            .expect("create returning stream");
        let interactive_task = tokio::spawn(async move {
            interactive
                .execute_unprepared("INSERT INTO probe (id) VALUES (2)")
                .await
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(!interactive_task.is_finished());
        assert!(
            stream
                .next()
                .await
                .transpose()
                .expect("stream row")
                .is_some()
        );
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), interactive_task)
            .await
            .expect("dropped stream must release writer executor")
            .expect("interactive task should join")
            .expect("interactive write should succeed");
    }
}
