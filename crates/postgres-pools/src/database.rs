use crate::query_scope::{
    DiagnosticStream, PoolFutureExt, ScopedFuture, observe_control, open_stream,
};
use async_trait::async_trait;
use pioneer_observability::query_cpu::{self, QueryScope};
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr,
    ExecResult, IsolationLevel, QueryResult, Statement, StreamTrait, TransactionError,
    TransactionOptions, TransactionSession, TransactionStream, TransactionTrait,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::reader::{
    MaintenanceReadLimiter, PostgresMaintenanceReadPermit, PostgresReadClass, PostgresReadObserver,
    PostgresReadOperation, PostgresReadOutcome, noop_read_observer, reader_pool_context,
};
use crate::statement_route::{StatementRoute, statement_route};
use crate::writer::{
    PostgresQueryStream, PostgresTransaction, PostgresWriteClass, PostgresWriteConnection,
    PostgresWriteExecutor, PostgresWriteObserver,
};

/// The private owner of the Gateway reader pool.
///
/// The Gateway opens read-only PostgreSQL sessions. Standalone queries reach
/// this pool after routing, with the scheduling class from [`PostgresDatabase`].
#[derive(Clone)]
struct PostgresReadPool {
    connection: DatabaseConnection,
    maintenance: Arc<MaintenanceReadLimiter>,
    observer: Arc<dyn PostgresReadObserver>,
}

impl std::fmt::Debug for PostgresReadPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PostgresReadPool")
    }
}

impl PostgresReadPool {
    fn with_observer(
        connection: DatabaseConnection,
        observer: Arc<dyn PostgresReadObserver>,
    ) -> Self {
        Self {
            connection,
            maintenance: MaintenanceReadLimiter::new(observer.clone()),
            observer,
        }
    }

    fn reject_write() -> DbErr {
        DbErr::Custom("write attempted through the database read pool".to_owned())
    }

    fn ensure_read_statement(statement: &Statement) -> Result<(), DbErr> {
        if statement_route(statement.sql.as_str()) == StatementRoute::Read {
            Ok(())
        } else {
            Err(Self::reject_write())
        }
    }

    fn max_connections(&self) -> u32 {
        self.connection
            .get_postgres_connection_pool()
            .options()
            .get_max_connections()
    }

    async fn is_read_only(&self, class: PostgresReadClass) -> Result<bool, DbErr> {
        let mut operation = PostgresReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            let _startup_execute = pioneer_observability::turn_startup::current_stage(
                pioneer_observability::turn_startup::Stage::DbExecute,
            );
            let row = observe_control(
                self.connection.query_one_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "SELECT current_setting('transaction_read_only') AS read_only",
                )),
                "bootstrap",
            )
            .in_pool(reader_pool_context(class))
            .await?
            .ok_or_else(|| DbErr::Custom("read-only pool check returned no row".to_owned()))?;
            Ok(row.try_get::<String>("", "read_only")? == "on")
        }
        .await;
        operation.finish(if result.is_ok() {
            PostgresReadOutcome::Ok
        } else {
            PostgresReadOutcome::Error
        });
        result
    }

    async fn ping(&self, class: PostgresReadClass) -> Result<(), DbErr> {
        let mut operation = PostgresReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            let _startup_execute = pioneer_observability::turn_startup::current_stage(
                pioneer_observability::turn_startup::Stage::DbExecute,
            );
            observe_control(self.connection.ping(), "ping")
                .in_pool(reader_pool_context(class))
                .await
        }
        .await;
        operation.finish(if result.is_ok() {
            PostgresReadOutcome::Ok
        } else {
            PostgresReadOutcome::Error
        });
        result
    }

    async fn acquire(
        &self,
        class: PostgresReadClass,
    ) -> Result<Option<PostgresMaintenanceReadPermit>, DbErr> {
        let _startup_wait = pioneer_observability::turn_startup::current_stage(
            pioneer_observability::turn_startup::Stage::DbAdmission,
        );
        match class {
            PostgresReadClass::Interactive => Ok(None),
            PostgresReadClass::Maintenance => self.maintenance.acquire().await.map(Some),
        }
    }

    async fn query_one_confirmed_read(
        &self,
        class: PostgresReadClass,
        statement: Statement,
    ) -> Result<Option<QueryResult>, DbErr> {
        let mut operation = PostgresReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            let _startup_execute = pioneer_observability::turn_startup::current_stage(
                pioneer_observability::turn_startup::Stage::DbExecute,
            );
            query_cpu::observe(self.connection.query_one_raw(statement))
                .in_pool(reader_pool_context(class))
                .await
        }
        .await;
        operation.finish(if result.is_ok() {
            PostgresReadOutcome::Ok
        } else {
            PostgresReadOutcome::Error
        });
        result
    }

    async fn query_all_confirmed_read(
        &self,
        class: PostgresReadClass,
        statement: Statement,
    ) -> Result<Vec<QueryResult>, DbErr> {
        let mut operation = PostgresReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            let _startup_execute = pioneer_observability::turn_startup::current_stage(
                pioneer_observability::turn_startup::Stage::DbExecute,
            );
            query_cpu::observe(self.connection.query_all_raw(statement))
                .in_pool(reader_pool_context(class))
                .await
        }
        .await;
        operation.finish(if result.is_ok() {
            PostgresReadOutcome::Ok
        } else {
            PostgresReadOutcome::Error
        });
        result
    }

    async fn stream_confirmed_read(
        &self,
        class: PostgresReadClass,
        statement: Statement,
    ) -> Result<PostgresQueryStream, DbErr> {
        let mut operation = PostgresReadOperation::start(self.observer.clone(), class);
        let result = async {
            let permit = self.acquire(class).await?;
            let stream = open_stream(self.connection.stream_raw(statement))
                .in_pool(reader_pool_context(class))
                .await?;
            Ok((stream, permit))
        }
        .await;
        match result {
            Ok((stream, permit)) => Ok(PostgresQueryStream::read(stream, permit, operation)),
            Err(error) => {
                operation.finish(PostgresReadOutcome::Error);
                Err(error)
            }
        }
    }

    async fn begin(&self, class: PostgresReadClass) -> Result<PostgresReadTransaction, DbErr> {
        let mut operation = PostgresReadOperation::start(self.observer.clone(), class);
        let result = async {
            let permit = self.acquire(class).await?;
            let transaction = observe_control(
                self.connection.begin_with_config(
                    Some(IsolationLevel::RepeatableRead),
                    Some(AccessMode::ReadOnly),
                ),
                "begin",
            )
            .in_pool(reader_pool_context(class))
            .await?;
            Ok((transaction, permit))
        }
        .await;
        match result {
            Ok((transaction, permit)) => Ok(PostgresReadTransaction::new(
                transaction,
                permit,
                operation,
                class,
            )),
            Err(error) => {
                operation.finish(PostgresReadOutcome::Error);
                Err(error)
            }
        }
    }
}

/// Read-only snapshot transaction acquired from the physical reader pool.
///
/// Unlike [`PostgresTransaction`], this type never owns writer admission. Its
/// mutation APIs reject all calls, and maintenance admission is retained until
/// commit, rollback, or drop.
pub struct PostgresReadTransaction {
    inner: Option<DatabaseTransaction>,
    _maintenance_permit: Option<PostgresMaintenanceReadPermit>,
    operation: Option<PostgresReadOperation>,
    class: PostgresReadClass,
}

impl std::fmt::Debug for PostgresReadTransaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PostgresReadTransaction")
    }
}

impl PostgresReadTransaction {
    fn new(
        inner: DatabaseTransaction,
        maintenance_permit: Option<PostgresMaintenanceReadPermit>,
        operation: PostgresReadOperation,
        class: PostgresReadClass,
    ) -> Self {
        Self {
            inner: Some(inner),
            _maintenance_permit: maintenance_permit,
            operation: Some(operation),
            class,
        }
    }

    fn inner(&self) -> &DatabaseTransaction {
        self.inner
            .as_ref()
            .expect("PostgreSQL read transaction was already completed")
    }

    pub async fn commit(mut self) -> Result<(), DbErr> {
        let result = observe_control(
            self.inner
                .take()
                .expect("PostgreSQL read transaction was already completed")
                .commit(),
            "commit",
        )
        .in_pool(reader_pool_context(self.class))
        .await;
        self._maintenance_permit.take();
        if let Some(operation) = self.operation.as_mut() {
            operation.finish(if result.is_ok() {
                PostgresReadOutcome::Ok
            } else {
                PostgresReadOutcome::Error
            });
        }
        self.operation.take();
        result
    }

    pub async fn rollback(mut self) -> Result<(), DbErr> {
        let result = observe_control(
            self.inner
                .take()
                .expect("PostgreSQL read transaction was already completed")
                .rollback(),
            "rollback",
        )
        .in_pool(reader_pool_context(self.class))
        .await;
        self._maintenance_permit.take();
        if let Some(operation) = self.operation.as_mut() {
            operation.finish(if result.is_ok() {
                PostgresReadOutcome::Cancelled
            } else {
                PostgresReadOutcome::Error
            });
        }
        self.operation.take();
        result
    }
}

// Drop-triggered rollback is its own control command, never a successful SQL query.
impl Drop for PostgresReadTransaction {
    fn drop(&mut self) {
        if self.inner.is_some() {
            let _pool = reader_pool_context(self.class).enter();
            let scope = QueryScope::control("rollback_drop");
            let _entered = scope.enter();
            drop(self.inner.take());
        }
    }
}

#[async_trait]
impl TransactionSession for PostgresReadTransaction {
    async fn commit(self) -> Result<(), DbErr> {
        PostgresReadTransaction::commit(self).await
    }

    async fn rollback(self) -> Result<(), DbErr> {
        PostgresReadTransaction::rollback(self).await
    }
}

#[async_trait]
impl ConnectionTrait for PostgresReadTransaction {
    fn get_database_backend(&self) -> DbBackend {
        ConnectionTrait::get_database_backend(self.inner())
    }

    async fn execute_raw(&self, _statement: Statement) -> Result<ExecResult, DbErr> {
        Err(PostgresReadPool::reject_write())
    }

    async fn execute_unprepared(&self, _sql: &str) -> Result<ExecResult, DbErr> {
        Err(PostgresReadPool::reject_write())
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        PostgresReadPool::ensure_read_statement(&statement)?;
        query_cpu::observe(self.inner().query_one_raw(statement))
            .in_pool(reader_pool_context(self.class))
            .await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        PostgresReadPool::ensure_read_statement(&statement)?;
        query_cpu::observe(self.inner().query_all_raw(statement))
            .in_pool(reader_pool_context(self.class))
            .await
    }

    fn support_returning(&self) -> bool {
        false
    }

    fn is_mock_connection(&self) -> bool {
        self.inner().is_mock_connection()
    }
}

impl StreamTrait for PostgresReadTransaction {
    type Stream<'a> = DiagnosticStream<TransactionStream<'a>>;

    fn get_database_backend(&self) -> DbBackend {
        StreamTrait::get_database_backend(self.inner())
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            PostgresReadPool::ensure_read_statement(&statement)?;
            let scope = QueryScope::new();
            let result = ScopedFuture::new(self.inner().stream_raw(statement), &scope)
                .in_pool(reader_pool_context(self.class))
                .await;
            match result {
                Ok(stream) => Ok(DiagnosticStream::new(
                    stream,
                    scope,
                    Some(reader_pool_context(self.class)),
                )),
                Err(error) => {
                    scope.finish(false);
                    Err(error)
                }
            }
        })
    }
}

/// Scoped database access with independent read and write pools.
///
/// Calls which return rows are classified from the actual SQL so that
/// row locks and mutations returning rows use the writer. Use a writer
/// transaction when a lock must protect later statements. `begin_read` opens
/// a read-only snapshot on the reader pool. Supply reader sessions configured
/// for [`crate::MANAGED_SEARCH_PATH`] and the Pioneer schema.
#[derive(Clone, Debug)]
pub struct PostgresDatabase {
    reader: PostgresReadPool,
    writer: PostgresWriteExecutor,
    read_class: PostgresReadClass,
    write_class: PostgresWriteClass,
}

impl PostgresDatabase {
    /// Runtime-local, coalesced hints shared by all scopes and store wrappers.
    /// Domain owners use stable names and subscribe independently.
    pub fn work_signal(&self, domain: &'static str) -> crate::WorkSignal {
        self.writer.work_signal(domain)
    }

    /// Fence outstanding PostgreSQL operations before reading after an
    /// ambiguous commit. The current write class controls admission.
    pub async fn settle_dispatched_writes(&self) -> Result<(), DbErr> {
        self.writer.settle_dispatched_writes(self.write_class).await
    }

    /// Stable across scheduling-class clones. Intended for process-local test
    /// instrumentation; it conveys no database authority or durable identity.
    #[doc(hidden)]
    pub fn runtime_identity(&self) -> usize {
        self.writer.runtime_identity()
    }

    pub fn new(reader: DatabaseConnection, writer: DatabaseConnection) -> Self {
        Self::from_executor(reader, PostgresWriteExecutor::new(writer))
    }

    pub fn new_with_observer(
        reader: DatabaseConnection,
        writer: DatabaseConnection,
        observer: Arc<dyn PostgresWriteObserver>,
    ) -> Self {
        Self::from_executor(
            reader,
            PostgresWriteExecutor::with_observer(writer, observer),
        )
    }

    pub fn from_executor(reader: DatabaseConnection, writer: PostgresWriteExecutor) -> Self {
        Self::from_executor_with_read_observer(reader, writer, noop_read_observer())
    }

    pub fn from_executor_with_read_observer(
        reader: DatabaseConnection,
        writer: PostgresWriteExecutor,
        read_observer: Arc<dyn PostgresReadObserver>,
    ) -> Self {
        Self {
            reader: PostgresReadPool::with_observer(reader, read_observer),
            writer,
            read_class: PostgresReadClass::Interactive,
            write_class: PostgresWriteClass::Interactive,
        }
    }

    /// Compatibility constructor for isolated tests and embedders which own a
    /// single connection. Gateway production startup always uses [`Self::new`]
    /// with two independently opened pools.
    pub fn from_single_connection(connection: DatabaseConnection) -> Self {
        Self::new(connection.clone(), connection)
    }

    pub fn with_critical_writes(&self) -> Self {
        let mut scoped = self.clone();
        scoped.write_class = PostgresWriteClass::Critical;
        scoped
    }

    pub fn with_interactive_writes(&self) -> Self {
        let mut scoped = self.clone();
        scoped.write_class = PostgresWriteClass::Interactive;
        scoped
    }

    pub fn maintenance(&self) -> Self {
        let mut scoped = self.clone();
        scoped.read_class = PostgresReadClass::Maintenance;
        scoped.write_class = PostgresWriteClass::Maintenance;
        scoped
    }

    pub const fn read_class(&self) -> PostgresReadClass {
        self.read_class
    }

    pub const fn write_class(&self) -> PostgresWriteClass {
        self.write_class
    }

    pub fn writer_max_connections(&self) -> u32 {
        self.writer.max_connections()
    }

    pub fn reader_max_connections(&self) -> u32 {
        self.reader.max_connections()
    }

    pub async fn reader_is_read_only(&self) -> Result<bool, DbErr> {
        self.reader.is_read_only(self.read_class).await
    }

    pub async fn validate_reader(&self) -> Result<(), DbErr> {
        self.reader.ping(self.read_class).await?;
        if !self.reader.is_read_only(self.read_class).await? {
            return Err(DbErr::Custom(
                "Gateway read pool requires read-only transactions".to_owned(),
            ));
        }
        Ok(())
    }

    /// Opens a consistent, read-only snapshot on the reader pool. Pure read
    /// workflows which need transaction-level consistency must use this API;
    /// [`TransactionTrait::begin`] intentionally remains the write boundary.
    pub async fn begin_read(&self) -> Result<PostgresReadTransaction, DbErr> {
        self.reader.begin(self.read_class).await
    }

    fn writer_connection(&self) -> PostgresWriteConnection {
        self.writer.connection(self.write_class)
    }

    /// Close both physical pools. Both close attempts are made so a reader
    /// shutdown error cannot leave the writer pool running.
    pub async fn close(self) -> Result<(), DbErr> {
        let reader_result = self.reader.connection.close().await;
        let writer_result = self.writer.close().await;
        reader_result.and(writer_result)
    }

    async fn query_one_routed(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        match statement_route(statement.sql.as_str()) {
            StatementRoute::Read => {
                self.reader
                    .query_one_confirmed_read(self.read_class, statement)
                    .await
            }
            StatementRoute::Write => self.writer_connection().query_one_raw(statement).await,
        }
    }

    async fn query_all_routed(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match statement_route(statement.sql.as_str()) {
            StatementRoute::Read => {
                self.reader
                    .query_all_confirmed_read(self.read_class, statement)
                    .await
            }
            StatementRoute::Write => self.writer_connection().query_all_raw(statement).await,
        }
    }
}

impl From<DatabaseConnection> for PostgresDatabase {
    fn from(connection: DatabaseConnection) -> Self {
        Self::from_single_connection(connection)
    }
}

impl From<&DatabaseConnection> for PostgresDatabase {
    fn from(connection: &DatabaseConnection) -> Self {
        Self::from_single_connection(connection.clone())
    }
}

impl From<&PostgresDatabase> for PostgresDatabase {
    fn from(database: &PostgresDatabase) -> Self {
        database.clone()
    }
}

#[async_trait]
impl ConnectionTrait for PostgresDatabase {
    fn get_database_backend(&self) -> DbBackend {
        ConnectionTrait::get_database_backend(&self.writer_connection())
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.writer_connection().execute_raw(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.writer_connection().execute_unprepared(sql).await
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.query_one_routed(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.query_all_routed(statement).await
    }

    fn support_returning(&self) -> bool {
        self.writer_connection().support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.writer_connection().is_mock_connection()
    }
}

impl StreamTrait for PostgresDatabase {
    type Stream<'a> = PostgresQueryStream;

    fn get_database_backend(&self) -> DbBackend {
        StreamTrait::get_database_backend(&self.writer_connection())
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            match statement_route(statement.sql.as_str()) {
                StatementRoute::Read => {
                    self.reader
                        .stream_confirmed_read(self.read_class, statement)
                        .await
                }
                StatementRoute::Write => self.writer_connection().stream_raw(statement).await,
            }
        })
    }
}

#[async_trait]
impl TransactionTrait for PostgresDatabase {
    type Transaction = PostgresTransaction;

    async fn begin(&self) -> Result<Self::Transaction, DbErr> {
        self.writer_connection().begin().await
    }

    async fn begin_with_config(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<Self::Transaction, DbErr> {
        self.writer_connection()
            .begin_with_config(isolation_level, access_mode)
            .await
    }

    async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction, DbErr> {
        self.writer_connection().begin_with_options(options).await
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
        self.writer_connection().transaction(callback).await
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
        self.writer_connection()
            .transaction_with_config(callback, isolation_level, access_mode)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresDatabase, PostgresReadClass, StatementRoute, statement_route};
    use crate::{
        PostgresReadEvent, PostgresReadObserver, PostgresReadOutcome, PostgresWriteExecutor,
    };
    use futures_util::StreamExt;
    use sea_orm::sea_query::{Alias, Expr, LockBehavior, LockType, Query};
    use sea_orm::{
        ConnectionTrait, DbBackend, DbErr, RuntimeErr, Statement, StreamTrait, TransactionTrait,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingReadObserver {
        events: Mutex<Vec<PostgresReadEvent>>,
    }

    impl PostgresReadObserver for RecordingReadObserver {
        fn observe(&self, event: PostgresReadEvent) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }
    }

    impl RecordingReadObserver {
        fn events(&self) -> Vec<PostgresReadEvent> {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    async fn test_pools() -> (sea_orm::DatabaseConnection, sea_orm::DatabaseConnection) {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let reader = fixture
            .connect_with_options(|options| {
                options
                    .max_connections(4)
                    .map_sqlx_postgres_opts(|postgres| {
                        postgres.options([
                            ("default_transaction_read_only", "on"),
                            ("search_path", crate::MANAGED_SEARCH_PATH),
                        ])
                    });
            })
            .await
            .expect("open read-only test pool");
        let writer = fixture
            .connect_with_limit(1)
            .await
            .expect("open test writer");
        (reader, writer)
    }

    async fn test_database() -> PostgresDatabase {
        let (reader, writer) = test_pools().await;
        PostgresDatabase::new(reader, writer)
    }

    async fn observed_test_database() -> (PostgresDatabase, Arc<RecordingReadObserver>) {
        let (reader, writer) = test_pools().await;
        let observer = Arc::new(RecordingReadObserver::default());
        let database = PostgresDatabase::from_executor_with_read_observer(
            reader,
            PostgresWriteExecutor::new(writer),
            observer.clone(),
        );
        (database, observer)
    }

    #[test]
    fn routes_plain_queries_to_reader() {
        assert_eq!(statement_route(" SELECT 1"), StatementRoute::Read);
        assert_eq!(statement_route("VALUES (1)"), StatementRoute::Read);
        assert_eq!(
            statement_route("-- leading comment\nSELECT 'delete is data'"),
            StatementRoute::Read
        );
        assert_eq!(statement_route("EXPLAIN SELECT 1"), StatementRoute::Read);
    }

    #[test]
    fn routes_mutations_and_unknown_sql_to_writer() {
        assert_eq!(
            statement_route("INSERT INTO item DEFAULT VALUES RETURNING id"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route("UPDATE item SET id = 1"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route("DELETE FROM item RETURNING id"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route(
                "MERGE INTO item USING (VALUES (1)) AS input(id) ON item.id = input.id WHEN MATCHED THEN DELETE RETURNING item.id"
            ),
            StatementRoute::Write
        );
        assert_eq!(statement_route("TRUNCATE item"), StatementRoute::Write);
        assert_eq!(statement_route(""), StatementRoute::Write);
    }

    #[test]
    fn routes_cte_by_its_bodies_and_outer_statement() {
        assert_eq!(
            statement_route(
                "WITH rows AS (SELECT 'update ignored' AS value) SELECT value FROM rows"
            ),
            StatementRoute::Read
        );
        assert_eq!(
            statement_route("WITH rows AS (SELECT 1) UPDATE item SET value = 2 RETURNING value"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route("WITH rows AS (SELECT 1) INSERT INTO item SELECT * FROM rows"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route("WITH rows AS (SELECT 1) DELETE FROM item RETURNING id"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route(
                "/* select */ WITH RECURSIVE rows(value) AS (VALUES (1)) SELECT value FROM rows"
            ),
            StatementRoute::Read
        );
        assert_eq!(
            statement_route(
                "WITH a AS (SELECT 1), b AS (DELETE FROM item RETURNING id) SELECT * FROM b"
            ),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route("WITH a AS (SELECT * FROM item FOR KEY SHARE) SELECT * FROM a"),
            StatementRoute::Write
        );
    }

    #[pioneer_postgres_test::postgres_test]
    async fn plain_reads_and_reading_ctes_use_the_read_only_session() {
        let database = test_database().await;
        let read = Query::select()
            .expr_as(
                Expr::cust("current_setting('transaction_read_only')"),
                Alias::new("mode"),
            )
            .to_owned();
        let row = database.query_one(&read).await.unwrap().unwrap();
        assert_eq!(row.try_get::<String>("", "mode").unwrap(), "on");

        for sql in [
            "SELECT current_setting('transaction_read_only') AS mode,
                'FOR UPDATE' AS \"FOR SHARE\" /* outer /* nested */ FOR NO KEY UPDATE */",
            "WITH \"update\"(value) AS NOT MATERIALIZED
                (SELECT $tag$FOR UPDATE; )$tag$),
                extra AS (WITH quoted AS (SELECT E'it\\'s FOR SHARE') SELECT * FROM quoted)
             SELECT current_setting('transaction_read_only') AS mode FROM \"update\", extra
             -- FOR KEY SHARE",
            "WITH RECURSIVE chain(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM chain WHERE n < 2)
             SELECT current_setting('transaction_read_only') AS mode FROM chain WHERE n = 2",
            "SELECT current_setting('transaction_read_only') AS mode
             FROM (VALUES (1)) AS lengths(update)
             WHERE substring('x' FOR update) = 'x'",
            "SELECT current_setting('transaction_read_only') AS mode,
                    'turn'::text COLLATE \"C\" AS kind
             WHERE pg_visible_in_snapshot('10'::xid8, pg_current_snapshot()::pg_snapshot)",
        ] {
            let rows = database
                .query_all_raw(Statement::from_string(DbBackend::Postgres, sql))
                .await
                .unwrap_or_else(|error| panic!("read probe failed: {error}"));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].try_get::<String>("", "mode").unwrap(), "on");
        }

        let unsupported =
            "WITH RECURSIVE chain(n) AS (VALUES (1) UNION ALL SELECT n + 1 FROM chain WHERE n < 2)
            SEARCH BREADTH FIRST BY n SET traversal
            CYCLE n SET cycle_flag USING visited
            SELECT current_setting('transaction_read_only') AS mode FROM chain WHERE n = 2";
        let rows = database
            .query_all_raw(Statement::from_string(DbBackend::Postgres, unsupported))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].try_get::<String>("", "mode").unwrap(), "off");
    }

    #[test]
    fn checks_executed_expressions_and_qualified_callables() {
        for sql in [
            "SELECT id INTO copied FROM workspace",
            "SELECT COALESCE((SELECT pg_catalog.nextval('probe')), 0)",
            "SELECT state FROM pg_advisory_xact_lock(1) AS state",
            "SELECT COALESCE((SELECT an_unknown_function()), 0)",
            "SELECT other.lower('a')",
            r#"SELECT pg_catalog."COUNT"(*)"#,
            "SELECT 1 OPERATOR(other.+) 2",
            r#"SELECT 1 OPERATOR("PG_CATALOG".+) 2"#,
            "SELECT NULL::other.custom_type",
            "SELECT 1 UNION SELECT nextval('probe')",
            "SELECT 1 INTERSECT SELECT nextval('probe')",
            "SELECT 1 EXCEPT SELECT nextval('probe')",
            "EXPLAIN (ANALYZE TRUE) SELECT nextval('probe')",
            "SELECT 1; SELECT 2",
            "SELECT (",
        ] {
            assert_eq!(statement_route(sql), StatementRoute::Write, "{sql}");
        }
        for sql in [
            "SELECT pg_catalog.lower('a')",
            "SELECT public.thread_episodic_search_keys('test')",
            "SELECT 1 OPERATOR(PG_CATALOG.+) 2",
            r#"SELECT NULL::vector OPERATOR("public".<=>) NULL::vector"#,
            "SELECT pg_visible_in_snapshot('10'::xid8, pg_current_snapshot()::pg_snapshot)",
            "EXPLAIN ANALYZE SELECT 1",
        ] {
            assert_eq!(statement_route(sql), StatementRoute::Read, "{sql}");
        }
    }

    #[pioneer_postgres_test::postgres_test]
    async fn full_episodic_search_runs_on_reader_with_application_bindings() {
        let (database, observer) = observed_test_database().await;
        let sql = include_str!("../../crud-memory/src/episodic_search.sql");
        let query = Statement::from_sql_and_values(
            DbBackend::Postgres,
            sql,
            [
                "absent-workspace".into(),
                Option::<String>::None.into(),
                Option::<String>::None.into(),
                false.into(),
                Option::<String>::None.into(),
                Option::<String>::None.into(),
                Option::<i64>::None.into(),
                true.into(),
                "search term".into(),
                "provider".into(),
                "model".into(),
                3_i64.into(),
                false.into(),
                sea_orm::prelude::PgVector::from(vec![1.0, 0.0, 0.0]).into(),
                60.0_f64.into(),
                20_i64.into(),
                0_i64.into(),
                200_i32.into(),
            ],
        );
        let rows = database.query_all_raw(query).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!rows[0].try_get::<bool>("", "incomplete").unwrap());
        assert!(observer.events().iter().any(|event| matches!(
            event,
            PostgresReadEvent::OperationFinished {
                outcome: PostgresReadOutcome::Ok,
                ..
            }
        )));
        database.close().await.unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn unknown_and_effectful_calls_use_writer_and_never_run_in_read_transaction() {
        let database = test_database().await;
        database
            .execute_unprepared(
                "CREATE SEQUENCE route_probe_sequence;
             CREATE FUNCTION route_probe_mode() RETURNS text
             LANGUAGE sql STABLE AS $$ SELECT current_setting('transaction_read_only') $$;",
            )
            .await
            .unwrap();
        let effectful = Statement::from_string(
            DbBackend::Postgres,
            "SELECT COALESCE((SELECT pg_catalog.nextval('route_probe_sequence')), 0) AS value,
                    current_setting('transaction_read_only') AS mode",
        );
        let unknown =
            Statement::from_string(DbBackend::Postgres, "SELECT route_probe_mode() AS mode");
        let read = database.begin_read().await.unwrap();
        for statement in [effectful.clone(), unknown.clone()] {
            let error = read.query_one_raw(statement).await.unwrap_err();
            assert!(error.to_string().contains("database read pool"));
        }
        read.commit().await.unwrap();

        let row = database.query_one_raw(effectful).await.unwrap().unwrap();
        assert_eq!(row.try_get::<i64>("", "value").unwrap(), 1);
        assert_eq!(row.try_get::<String>("", "mode").unwrap(), "off");
        let mut rows = database.stream_raw(unknown).await.unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.try_get::<String>("", "mode").unwrap(), "off");
        assert!(rows.next().await.is_none());
        drop(rows);
        database.close().await.unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn select_in_write_transaction_reads_its_uncommitted_insert() {
        let database = test_database().await;
        database
            .execute_unprepared("CREATE TABLE route_probe (value bigint)")
            .await
            .unwrap();
        let tx = database.begin().await.unwrap();
        tx.execute_unprepared("INSERT INTO route_probe VALUES (42)")
            .await
            .unwrap();
        let select = Statement::from_string(DbBackend::Postgres, "SELECT value FROM route_probe");
        let row = tx.query_one_raw(select.clone()).await.unwrap().unwrap();
        assert_eq!(row.try_get::<i64>("", "value").unwrap(), 42);
        assert!(
            database
                .query_one_raw(select.clone())
                .await
                .unwrap()
                .is_none()
        );
        tx.rollback().await.unwrap();
        assert!(database.query_one_raw(select).await.unwrap().is_none());
        database.close().await.unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn row_locks_use_writer_and_survive_until_the_same_transaction_commits() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let reader = fixture.connect_read_only().await.unwrap();
        let database =
            PostgresDatabase::new(reader.clone(), fixture.connect_with_limit(1).await.unwrap());
        let competitor =
            PostgresDatabase::new(reader, fixture.connect_with_limit(1).await.unwrap());
        database
            .execute_unprepared(
                "CREATE TABLE lock_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
                 INSERT INTO lock_probe VALUES (1, 10)",
            )
            .await
            .unwrap();
        for strength in [
            LockType::Update,
            LockType::NoKeyUpdate,
            LockType::Share,
            LockType::KeyShare,
        ] {
            let locking_read = Query::select()
                .expr_as(
                    Expr::cust("current_setting('transaction_read_only')"),
                    Alias::new("mode"),
                )
                .from(Alias::new("lock_probe"))
                .lock_with_behavior(strength, LockBehavior::Nowait)
                .to_owned();
            let row = database.query_one(&locking_read).await.unwrap().unwrap();
            assert_eq!(
                row.try_get::<String>("", "mode").unwrap(),
                "off",
                "{strength:?}"
            );
        }

        let transaction = database.begin().await.unwrap();
        let nowait = Statement::from_string(
            DbBackend::Postgres,
            "SELECT value FROM (SELECT value FROM lock_probe WHERE id = 1 FOR /* lock */ UPDATE NOWAIT) AS locked",
        );
        let initial = transaction
            .query_one_raw(nowait.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(initial.try_get::<i32>("", "value").unwrap(), 10);
        let conflict = competitor
            .query_one_raw(nowait.clone())
            .await
            .expect_err("the open transaction owns the row lock");
        let DbErr::Query(RuntimeErr::SqlxError(error)) = conflict else {
            panic!("expected a PostgreSQL row-lock error, got {conflict}");
        };
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("55P03")
        );
        {
            let mut skipped = competitor
                .stream_raw(Statement::from_string(
                    DbBackend::Postgres,
                    "WITH unlocked AS (SELECT value FROM lock_probe FOR UPDATE SKIP LOCKED)
                     SELECT value FROM unlocked",
                ))
                .await
                .unwrap();
            assert!(skipped.next().await.transpose().unwrap().is_none());
        }

        transaction
            .execute_unprepared("UPDATE lock_probe SET value = 20 WHERE id = 1")
            .await
            .unwrap();
        let own_write = transaction
            .query_one_raw(nowait.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(own_write.try_get::<i32>("", "value").unwrap(), 20);
        transaction.commit().await.unwrap();

        let released = competitor.query_one_raw(nowait).await.unwrap().unwrap();
        assert_eq!(released.try_get::<i32>("", "value").unwrap(), 20);
    }

    #[pioneer_postgres_test::postgres_test]
    async fn row_returning_mutations_use_writer_for_queries_and_streams() {
        let database = test_database().await;
        database
            .execute_unprepared(
                "CREATE TABLE returning_probe (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
                 INSERT INTO returning_probe VALUES (1, 10), (2, 20)",
            )
            .await
            .unwrap();
        let changed = database
            .query_all_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "WITH changed AS (
                     WITH step AS (SELECT $1::integer AS increment)
                     UPDATE returning_probe SET value = value + step.increment
                     FROM step RETURNING returning_probe.id, value
                 )
                 SELECT value FROM changed ORDER BY id",
                [1_i32.into()],
            ))
            .await
            .unwrap();
        assert_eq!(changed.len(), 2);
        assert_eq!(changed[0].try_get::<i32>("", "value").unwrap(), 11);
        assert_eq!(changed[1].try_get::<i32>("", "value").unwrap(), 21);

        {
            let mut rows = database
                .stream_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE returning_probe SET value = $1 WHERE id = $2 RETURNING value",
                    [22_i32.into(), 2_i32.into()],
                ))
                .await
                .unwrap();
            assert_eq!(
                rows.next()
                    .await
                    .unwrap()
                    .unwrap()
                    .try_get::<i32>("", "value")
                    .unwrap(),
                22
            );
            assert!(rows.next().await.is_none());
        }
        let saved = database
            .query_all_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT value, current_setting('transaction_read_only') AS mode
                 FROM returning_probe ORDER BY id",
            ))
            .await
            .unwrap();
        assert_eq!(saved.len(), 2);
        assert_eq!(saved[0].try_get::<i32>("", "value").unwrap(), 11);
        assert_eq!(saved[1].try_get::<i32>("", "value").unwrap(), 22);
        assert_eq!(saved[0].try_get::<String>("", "mode").unwrap(), "on");
    }

    #[pioneer_postgres_test::postgres_test]
    async fn database_scopes_carry_consistent_read_and_write_classes() {
        let database = test_database().await;

        assert_eq!(database.read_class(), PostgresReadClass::Interactive);
        assert_eq!(
            database.write_class(),
            crate::PostgresWriteClass::Interactive
        );
        assert_eq!(
            database.with_critical_writes().read_class(),
            PostgresReadClass::Interactive
        );
        assert_eq!(
            database.with_critical_writes().write_class(),
            crate::PostgresWriteClass::Critical
        );
        assert_eq!(
            database.maintenance().read_class(),
            PostgresReadClass::Maintenance
        );
        assert_eq!(
            database.maintenance().write_class(),
            crate::PostgresWriteClass::Maintenance
        );
        let background_critical = database.maintenance().with_critical_writes();
        assert_eq!(
            background_critical.read_class(),
            PostgresReadClass::Maintenance
        );
        assert_eq!(
            background_critical.write_class(),
            crate::PostgresWriteClass::Critical
        );
    }

    #[pioneer_postgres_test::postgres_test]
    async fn read_transaction_rejects_returning_writes_in_queries_and_streams() {
        let database = test_database().await;
        let reader = database.begin_read().await.expect("open test reader");

        let query_error = reader
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "DELETE FROM item RETURNING id",
            ))
            .await
            .expect_err("row-returning write must be rejected");
        assert!(query_error.to_string().contains("database read pool"));

        let stream_error = match reader
            .stream_raw(Statement::from_string(
                DbBackend::Postgres,
                "INSERT INTO item DEFAULT VALUES RETURNING id",
            ))
            .await
        {
            Ok(_) => panic!("streaming write must be rejected"),
            Err(error) => error,
        };
        assert!(stream_error.to_string().contains("database read pool"));
        reader.commit().await.unwrap();
        database.close().await.unwrap();
    }

    #[pioneer_postgres_test::postgres_test]
    async fn read_transaction_accepts_queries_and_rejects_all_mutations() {
        let database = test_database().await;
        let transaction = database.begin_read().await.expect("begin read transaction");

        let row = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 7::bigint AS value".to_owned(),
            ))
            .await
            .expect("query through read transaction")
            .expect("read transaction row");
        assert_eq!(row.try_get::<i64>("", "value").unwrap(), 7);

        let execute_error = transaction
            .execute_unprepared("CREATE TABLE forbidden (id INTEGER)")
            .await
            .expect_err("read transaction execute must be rejected");
        assert!(execute_error.to_string().contains("database read pool"));

        let returning_error = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "DELETE FROM forbidden RETURNING id".to_owned(),
            ))
            .await
            .expect_err("row-returning mutation must be rejected");
        assert!(returning_error.to_string().contains("database read pool"));

        let cte_error = transaction
            .query_all_raw(Statement::from_string(
                DbBackend::Postgres,
                "WITH removed AS (DELETE FROM workspace RETURNING id) SELECT id FROM removed",
            ))
            .await
            .expect_err("read transaction must reject a modifying CTE");
        assert!(cte_error.to_string().contains("database read pool"));

        let lock_error = match transaction
            .stream_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT id FROM workspace FOR KEY SHARE SKIP LOCKED",
            ))
            .await
        {
            Ok(_) => panic!("read transaction must reject a locking query"),
            Err(error) => error,
        };
        assert!(lock_error.to_string().contains("database read pool"));

        transaction.commit().await.expect("commit read transaction");
        database.close().await.expect("close test database");
    }

    #[pioneer_postgres_test::postgres_test]
    async fn held_read_snapshot_does_not_occupy_or_block_the_writer() {
        let database = test_database().await;
        database
            .execute_unprepared(
                "CREATE TABLE read_transaction_probe (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
            )
            .await
            .expect("create read transaction probe");
        let transaction = database.begin_read().await.expect("begin read snapshot");
        let initial = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count FROM read_transaction_probe".to_owned(),
            ))
            .await
            .expect("query initial snapshot")
            .expect("initial count row");
        assert_eq!(initial.try_get::<i64>("", "count").unwrap(), 0);

        tokio::time::timeout(
            Duration::from_secs(1),
            database.execute_unprepared(
                "INSERT INTO read_transaction_probe (id, value) VALUES (1, 'writer-progress')",
            ),
        )
        .await
        .expect("reader snapshot must not occupy writer admission")
        .expect("writer must progress while read snapshot is held");

        let snapshot = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count FROM read_transaction_probe".to_owned(),
            ))
            .await
            .expect("query held snapshot")
            .expect("held snapshot count row");
        assert_eq!(snapshot.try_get::<i64>("", "count").unwrap(), 0);
        transaction.commit().await.expect("commit read snapshot");

        let current = database
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT COUNT(*) AS count FROM read_transaction_probe".to_owned(),
            ))
            .await
            .expect("query current state")
            .expect("current count row");
        assert_eq!(current.try_get::<i64>("", "count").unwrap(), 1);
        database.close().await.expect("close test pools");
    }

    #[pioneer_postgres_test::postgres_test]
    async fn maintenance_read_transaction_holds_limiter_until_completion() {
        let database = test_database().await;
        let maintenance = database.maintenance();
        let transaction = maintenance
            .begin_read()
            .await
            .expect("begin maintenance read transaction");
        transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 1::bigint AS value".to_owned(),
            ))
            .await
            .expect("query maintenance read transaction");

        let waiting = tokio::spawn({
            let maintenance = maintenance.clone();
            async move {
                maintenance
                    .query_one_raw(Statement::from_string(
                        DbBackend::Postgres,
                        "SELECT 2::bigint AS value".to_owned(),
                    ))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "second maintenance read must wait for transaction completion"
        );

        let interactive = tokio::time::timeout(
            Duration::from_secs(1),
            database.query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 3::bigint AS value".to_owned(),
            )),
        )
        .await
        .expect("maintenance transaction must not block interactive reads")
        .expect("run interactive read")
        .expect("interactive read row");
        assert_eq!(interactive.try_get::<i64>("", "value").unwrap(), 3);

        transaction
            .commit()
            .await
            .expect("commit maintenance read transaction");
        let resumed = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("waiting maintenance read must resume")
            .expect("join waiting maintenance read")
            .expect("run waiting maintenance read")
            .expect("waiting maintenance row");
        assert_eq!(resumed.try_get::<i64>("", "value").unwrap(), 2);
        database.close().await.expect("close test database");
    }

    #[pioneer_postgres_test::postgres_test]
    async fn reader_observer_reports_class_outcome_and_maintenance_lifecycle() {
        let (database, observer) = observed_test_database().await;

        database
            .query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 1::bigint AS value".to_owned(),
            ))
            .await
            .expect("run observed interactive read");
        database
            .maintenance()
            .query_all_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 2::bigint AS value".to_owned(),
            ))
            .await
            .expect("run observed maintenance read");

        let events = observer.events();
        assert!(matches!(
            events.first(),
            Some(PostgresReadEvent::OperationFinished {
                class: PostgresReadClass::Interactive,
                outcome: PostgresReadOutcome::Ok,
                ..
            })
        ));
        assert!(matches!(
            events.get(1),
            Some(PostgresReadEvent::AdmissionEnqueued {
                class: PostgresReadClass::Maintenance,
                queue_depth: 1,
                active: 0,
            })
        ));
        assert!(matches!(
            events.get(2),
            Some(PostgresReadEvent::AdmissionAcquired {
                class: PostgresReadClass::Maintenance,
                queue_depth: 0,
                active: 1,
                ..
            })
        ));
        assert!(matches!(
            events.get(3),
            Some(PostgresReadEvent::AdmissionReleased {
                class: PostgresReadClass::Maintenance,
                queue_depth: 0,
                active: 0,
                ..
            })
        ));
        assert!(matches!(
            events.get(4),
            Some(PostgresReadEvent::OperationFinished {
                class: PostgresReadClass::Maintenance,
                outcome: PostgresReadOutcome::Ok,
                ..
            })
        ));
        assert_eq!(events.len(), 5);
    }

    #[pioneer_postgres_test::postgres_test]
    async fn reader_ping_uses_the_typed_maintenance_limiter() {
        let (database, observer) = observed_test_database().await;

        database
            .reader
            .ping(PostgresReadClass::Maintenance)
            .await
            .expect("ping observed maintenance reader");

        let events = observer.events();
        assert!(matches!(
            events.as_slice(),
            [
                PostgresReadEvent::AdmissionEnqueued {
                    class: PostgresReadClass::Maintenance,
                    queue_depth: 1,
                    active: 0,
                },
                PostgresReadEvent::AdmissionAcquired {
                    class: PostgresReadClass::Maintenance,
                    queue_depth: 0,
                    active: 1,
                    ..
                },
                PostgresReadEvent::AdmissionReleased {
                    class: PostgresReadClass::Maintenance,
                    queue_depth: 0,
                    active: 0,
                    ..
                },
                PostgresReadEvent::OperationFinished {
                    class: PostgresReadClass::Maintenance,
                    outcome: PostgresReadOutcome::Ok,
                    ..
                },
            ]
        ));
    }

    #[pioneer_postgres_test::postgres_test]
    async fn maintenance_stream_reserves_one_slot_without_blocking_interactive_reads() {
        let database = test_database().await;
        let maintenance = database.maintenance();
        let mut held_stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 1::bigint AS value UNION ALL SELECT 2::bigint AS value".to_owned(),
            ))
            .await
            .expect("open maintenance stream");

        let first = held_stream
            .next()
            .await
            .expect("first stream row")
            .expect("read first stream row");
        assert_eq!(first.try_get::<i64>("", "value").unwrap(), 1);

        let second_maintenance = tokio::spawn({
            let maintenance = maintenance.with_critical_writes();
            async move {
                maintenance
                    .query_one_raw(Statement::from_string(
                        DbBackend::Postgres,
                        "SELECT 3::bigint AS value".to_owned(),
                    ))
                    .await
            }
        });
        tokio::task::yield_now().await;
        assert!(
            !second_maintenance.is_finished(),
            "a second maintenance read must wait for the stream permit"
        );

        let interactive = tokio::time::timeout(
            Duration::from_secs(1),
            database.query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 4::bigint AS value".to_owned(),
            )),
        )
        .await
        .expect("interactive read must not wait for maintenance limiter")
        .expect("run interactive read")
        .expect("interactive row");
        assert_eq!(interactive.try_get::<i64>("", "value").unwrap(), 4);

        drop(held_stream);
        let resumed = tokio::time::timeout(Duration::from_secs(1), second_maintenance)
            .await
            .expect("maintenance read must resume after stream drop")
            .expect("join maintenance read")
            .expect("run maintenance read")
            .expect("maintenance row");
        assert_eq!(resumed.try_get::<i64>("", "value").unwrap(), 3);
    }

    #[pioneer_postgres_test::postgres_test]
    async fn completed_maintenance_stream_releases_permit_before_drop() {
        let database = test_database().await;
        let maintenance = database.maintenance();
        let mut completed_stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 1::bigint AS value".to_owned(),
            ))
            .await
            .expect("open maintenance stream");
        assert!(completed_stream.next().await.is_some());
        assert!(completed_stream.next().await.is_none());

        tokio::time::timeout(
            Duration::from_secs(1),
            maintenance.query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 2::bigint AS value".to_owned(),
            )),
        )
        .await
        .expect("EOF must release maintenance permit while wrapper remains alive")
        .expect("run maintenance read after EOF");

        drop(completed_stream);
    }

    #[pioneer_postgres_test::postgres_test]
    async fn cancelling_waiting_maintenance_read_does_not_leak_capacity() {
        let (database, observer) = observed_test_database().await;
        let maintenance = database.maintenance();
        let held_stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 1::bigint AS value".to_owned(),
            ))
            .await
            .expect("open maintenance stream");

        let waiting = tokio::spawn({
            let maintenance = maintenance.clone();
            async move {
                maintenance
                    .query_one_raw(Statement::from_string(
                        DbBackend::Postgres,
                        "SELECT 2::bigint AS value".to_owned(),
                    ))
                    .await
            }
        });
        tokio::task::yield_now().await;
        waiting.abort();
        let _ = waiting.await;
        assert!(observer.events().iter().any(|event| matches!(
            event,
            PostgresReadEvent::AdmissionCancelled {
                class: PostgresReadClass::Maintenance,
                queue_depth: 0,
                active: 1,
                ..
            }
        )));
        drop(held_stream);

        tokio::time::timeout(
            Duration::from_secs(1),
            maintenance.query_one_raw(Statement::from_string(
                DbBackend::Postgres,
                "SELECT 3::bigint AS value".to_owned(),
            )),
        )
        .await
        .expect("cancelled waiter must not consume maintenance capacity")
        .expect("run maintenance read after cancellation");
        assert!(observer.events().iter().any(|event| matches!(
            event,
            PostgresReadEvent::OperationFinished {
                class: PostgresReadClass::Maintenance,
                outcome: PostgresReadOutcome::Cancelled,
                ..
            }
        )));
    }
}
