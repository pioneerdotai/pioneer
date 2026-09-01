use async_trait::async_trait;
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr,
    ExecResult, IsolationLevel, QueryResult, Statement, StreamTrait, TransactionError,
    TransactionOptions, TransactionSession, TransactionStream, TransactionTrait,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::Instrument;

use crate::reader::{
    MaintenanceReadLimiter, SqliteMaintenanceReadPermit, SqliteReadClass, SqliteReadObserver,
    SqliteReadOperation, SqliteReadOutcome, noop_read_observer, reader_pool_span,
};
use crate::writer::{
    SqliteQueryStream, SqliteTransaction, SqliteWriteClass, SqliteWriteConnection,
    SqliteWriteExecutor, SqliteWriteObserver,
};

/// The private owner of the physical SQLite read pool.
///
/// The Gateway opens the underlying connections with SQLite's read-only flag
/// and `PRAGMA query_only = ON`. This wrapper adds an API-level guard:
/// mutation entry points and non-read statements are rejected before they can
/// reach the pool. It deliberately never crosses the crate boundary: every
/// query must carry a typed read class through [`SqliteDatabase`].
#[derive(Clone)]
struct SqliteReadPool {
    connection: DatabaseConnection,
    maintenance: Arc<MaintenanceReadLimiter>,
    observer: Arc<dyn SqliteReadObserver>,
}

impl std::fmt::Debug for SqliteReadPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteReadPool")
    }
}

impl SqliteReadPool {
    #[cfg(test)]
    fn new(connection: DatabaseConnection) -> Self {
        Self::with_observer(connection, noop_read_observer())
    }

    fn with_observer(
        connection: DatabaseConnection,
        observer: Arc<dyn SqliteReadObserver>,
    ) -> Self {
        Self {
            connection,
            maintenance: MaintenanceReadLimiter::new(observer.clone()),
            observer,
        }
    }

    fn reject_write() -> DbErr {
        DbErr::Custom("write attempted through the SQLite read pool".to_owned())
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
            .get_sqlite_connection_pool()
            .options()
            .get_max_connections()
    }

    async fn query_only_enabled(&self, class: SqliteReadClass) -> Result<bool, DbErr> {
        let mut operation = SqliteReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            let row = self
                .connection
                .query_one_raw(Statement::from_string(
                    DbBackend::Sqlite,
                    "PRAGMA query_only".to_owned(),
                ))
                .instrument(reader_pool_span(class))
                .await?
                .ok_or_else(|| DbErr::Custom("PRAGMA query_only returned no row".to_owned()))?;
            Ok(row.try_get::<i64>("", "query_only")? != 0)
        }
        .await;
        operation.finish(if result.is_ok() {
            SqliteReadOutcome::Ok
        } else {
            SqliteReadOutcome::Error
        });
        result
    }

    async fn ping(&self, class: SqliteReadClass) -> Result<(), DbErr> {
        let mut operation = SqliteReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            self.connection
                .ping()
                .instrument(reader_pool_span(class))
                .await
        }
        .await;
        operation.finish(if result.is_ok() {
            SqliteReadOutcome::Ok
        } else {
            SqliteReadOutcome::Error
        });
        result
    }

    async fn acquire(
        &self,
        class: SqliteReadClass,
    ) -> Result<Option<SqliteMaintenanceReadPermit>, DbErr> {
        match class {
            SqliteReadClass::Interactive => Ok(None),
            SqliteReadClass::Maintenance => self.maintenance.acquire().await.map(Some),
        }
    }

    async fn query_one_raw(
        &self,
        class: SqliteReadClass,
        statement: Statement,
    ) -> Result<Option<QueryResult>, DbErr> {
        Self::ensure_read_statement(&statement)?;
        let mut operation = SqliteReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            self.connection
                .query_one_raw(statement)
                .instrument(reader_pool_span(class))
                .await
        }
        .await;
        operation.finish(if result.is_ok() {
            SqliteReadOutcome::Ok
        } else {
            SqliteReadOutcome::Error
        });
        result
    }

    async fn query_all_raw(
        &self,
        class: SqliteReadClass,
        statement: Statement,
    ) -> Result<Vec<QueryResult>, DbErr> {
        Self::ensure_read_statement(&statement)?;
        let mut operation = SqliteReadOperation::start(self.observer.clone(), class);
        let result = async {
            let _permit = self.acquire(class).await?;
            self.connection
                .query_all_raw(statement)
                .instrument(reader_pool_span(class))
                .await
        }
        .await;
        operation.finish(if result.is_ok() {
            SqliteReadOutcome::Ok
        } else {
            SqliteReadOutcome::Error
        });
        result
    }

    async fn stream_raw(
        &self,
        class: SqliteReadClass,
        statement: Statement,
    ) -> Result<SqliteQueryStream, DbErr> {
        Self::ensure_read_statement(&statement)?;
        let mut operation = SqliteReadOperation::start(self.observer.clone(), class);
        let result = async {
            let permit = self.acquire(class).await?;
            let stream = self
                .connection
                .stream_raw(statement)
                .instrument(reader_pool_span(class))
                .await?;
            Ok((stream, permit))
        }
        .await;
        match result {
            Ok((stream, permit)) => Ok(SqliteQueryStream::read(stream, permit, operation)),
            Err(error) => {
                operation.finish(SqliteReadOutcome::Error);
                Err(error)
            }
        }
    }

    async fn begin(&self, class: SqliteReadClass) -> Result<SqliteReadTransaction, DbErr> {
        let mut operation = SqliteReadOperation::start(self.observer.clone(), class);
        let result = async {
            let permit = self.acquire(class).await?;
            let transaction = self
                .connection
                .begin_with_config(None, Some(AccessMode::ReadOnly))
                .instrument(reader_pool_span(class))
                .await?;
            Ok((transaction, permit))
        }
        .await;
        match result {
            Ok((transaction, permit)) => {
                Ok(SqliteReadTransaction::new(transaction, permit, operation))
            }
            Err(error) => {
                operation.finish(SqliteReadOutcome::Error);
                Err(error)
            }
        }
    }
}

/// Read-only snapshot transaction acquired from the physical reader pool.
///
/// Unlike [`SqliteTransaction`], this type never owns writer admission. Its
/// mutation APIs reject all calls, and maintenance admission is retained until
/// commit, rollback, or drop.
pub struct SqliteReadTransaction {
    inner: Option<DatabaseTransaction>,
    _maintenance_permit: Option<SqliteMaintenanceReadPermit>,
    operation: Option<SqliteReadOperation>,
}

impl std::fmt::Debug for SqliteReadTransaction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteReadTransaction")
    }
}

impl SqliteReadTransaction {
    fn new(
        inner: DatabaseTransaction,
        maintenance_permit: Option<SqliteMaintenanceReadPermit>,
        operation: SqliteReadOperation,
    ) -> Self {
        Self {
            inner: Some(inner),
            _maintenance_permit: maintenance_permit,
            operation: Some(operation),
        }
    }

    fn inner(&self) -> &DatabaseTransaction {
        self.inner
            .as_ref()
            .expect("SQLite read transaction was already completed")
    }

    pub async fn commit(mut self) -> Result<(), DbErr> {
        let result = self
            .inner
            .take()
            .expect("SQLite read transaction was already completed")
            .commit()
            .await;
        self._maintenance_permit.take();
        if let Some(operation) = self.operation.as_mut() {
            operation.finish(if result.is_ok() {
                SqliteReadOutcome::Ok
            } else {
                SqliteReadOutcome::Error
            });
        }
        self.operation.take();
        result
    }

    pub async fn rollback(mut self) -> Result<(), DbErr> {
        let result = self
            .inner
            .take()
            .expect("SQLite read transaction was already completed")
            .rollback()
            .await;
        self._maintenance_permit.take();
        if let Some(operation) = self.operation.as_mut() {
            operation.finish(if result.is_ok() {
                SqliteReadOutcome::Cancelled
            } else {
                SqliteReadOutcome::Error
            });
        }
        self.operation.take();
        result
    }
}

#[async_trait]
impl TransactionSession for SqliteReadTransaction {
    async fn commit(self) -> Result<(), DbErr> {
        SqliteReadTransaction::commit(self).await
    }

    async fn rollback(self) -> Result<(), DbErr> {
        SqliteReadTransaction::rollback(self).await
    }
}

#[async_trait]
impl ConnectionTrait for SqliteReadTransaction {
    fn get_database_backend(&self) -> DbBackend {
        ConnectionTrait::get_database_backend(self.inner())
    }

    async fn execute_raw(&self, _statement: Statement) -> Result<ExecResult, DbErr> {
        Err(SqliteReadPool::reject_write())
    }

    async fn execute_unprepared(&self, _sql: &str) -> Result<ExecResult, DbErr> {
        Err(SqliteReadPool::reject_write())
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        SqliteReadPool::ensure_read_statement(&statement)?;
        self.inner().query_one_raw(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        SqliteReadPool::ensure_read_statement(&statement)?;
        self.inner().query_all_raw(statement).await
    }

    fn support_returning(&self) -> bool {
        false
    }

    fn is_mock_connection(&self) -> bool {
        self.inner().is_mock_connection()
    }
}

impl StreamTrait for SqliteReadTransaction {
    type Stream<'a> = TransactionStream<'a>;

    fn get_database_backend(&self) -> DbBackend {
        StreamTrait::get_database_backend(self.inner())
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            SqliteReadPool::ensure_read_statement(&statement)?;
            self.inner().stream_raw(statement).await
        })
    }
}

/// Typed SQLite runtime with independent read and write contours.
///
/// Calls which return rows are classified from the actual SQL so that
/// `INSERT`, `UPDATE`, and `DELETE ... RETURNING` still use the writer. All
/// transactions and execution APIs are unconditionally routed to the writer.
#[derive(Clone, Debug)]
pub struct SqliteDatabase {
    reader: SqliteReadPool,
    writer: SqliteWriteExecutor,
    read_class: SqliteReadClass,
    write_class: SqliteWriteClass,
}

impl SqliteDatabase {
    pub fn new(reader: DatabaseConnection, writer: DatabaseConnection) -> Self {
        Self::from_executor(reader, SqliteWriteExecutor::new(writer))
    }

    pub fn new_with_observer(
        reader: DatabaseConnection,
        writer: DatabaseConnection,
        observer: Arc<dyn SqliteWriteObserver>,
    ) -> Self {
        Self::from_executor(reader, SqliteWriteExecutor::with_observer(writer, observer))
    }

    pub fn from_executor(reader: DatabaseConnection, writer: SqliteWriteExecutor) -> Self {
        Self::from_executor_with_read_observer(reader, writer, noop_read_observer())
    }

    pub fn from_executor_with_read_observer(
        reader: DatabaseConnection,
        writer: SqliteWriteExecutor,
        read_observer: Arc<dyn SqliteReadObserver>,
    ) -> Self {
        Self {
            reader: SqliteReadPool::with_observer(reader, read_observer),
            writer,
            read_class: SqliteReadClass::Interactive,
            write_class: SqliteWriteClass::Interactive,
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
        scoped.write_class = SqliteWriteClass::Critical;
        scoped
    }

    pub fn with_interactive_writes(&self) -> Self {
        let mut scoped = self.clone();
        scoped.write_class = SqliteWriteClass::Interactive;
        scoped
    }

    pub fn maintenance(&self) -> Self {
        let mut scoped = self.clone();
        scoped.read_class = SqliteReadClass::Maintenance;
        scoped.write_class = SqliteWriteClass::Maintenance;
        scoped
    }

    pub const fn read_class(&self) -> SqliteReadClass {
        self.read_class
    }

    pub const fn write_class(&self) -> SqliteWriteClass {
        self.write_class
    }

    pub fn writer_max_connections(&self) -> u32 {
        self.writer.max_connections()
    }

    pub fn reader_max_connections(&self) -> u32 {
        self.reader.max_connections()
    }

    pub async fn reader_query_only_enabled(&self) -> Result<bool, DbErr> {
        self.reader.query_only_enabled(self.read_class).await
    }

    pub async fn validate_reader(&self) -> Result<(), DbErr> {
        self.reader.ping(self.read_class).await?;
        if !self.reader.query_only_enabled(self.read_class).await? {
            return Err(DbErr::Custom(
                "SQLite read pool is not protected by PRAGMA query_only".to_owned(),
            ));
        }
        Ok(())
    }

    /// Opens a consistent, read-only snapshot on the reader pool. Pure read
    /// workflows which need transaction-level consistency must use this API;
    /// [`TransactionTrait::begin`] intentionally remains the write boundary.
    pub async fn begin_read(&self) -> Result<SqliteReadTransaction, DbErr> {
        self.reader.begin(self.read_class).await
    }

    /// Execute a row-returning statement on the serialized writer. This is
    /// reserved for SQLite extension functions whose SQL surface is `SELECT`
    /// but whose execution mutates database state.
    pub async fn query_one_write_raw(
        &self,
        statement: Statement,
    ) -> Result<Option<QueryResult>, DbErr> {
        self.writer_connection().query_one_raw(statement).await
    }

    fn writer_connection(&self) -> SqliteWriteConnection {
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
            StatementRoute::Read => self.reader.query_one_raw(self.read_class, statement).await,
            StatementRoute::Write => self.writer_connection().query_one_raw(statement).await,
        }
    }

    async fn query_all_routed(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match statement_route(statement.sql.as_str()) {
            StatementRoute::Read => self.reader.query_all_raw(self.read_class, statement).await,
            StatementRoute::Write => self.writer_connection().query_all_raw(statement).await,
        }
    }
}

impl From<DatabaseConnection> for SqliteDatabase {
    fn from(connection: DatabaseConnection) -> Self {
        Self::from_single_connection(connection)
    }
}

impl From<&DatabaseConnection> for SqliteDatabase {
    fn from(connection: &DatabaseConnection) -> Self {
        Self::from_single_connection(connection.clone())
    }
}

impl From<&SqliteDatabase> for SqliteDatabase {
    fn from(database: &SqliteDatabase) -> Self {
        database.clone()
    }
}

#[async_trait]
impl ConnectionTrait for SqliteDatabase {
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

impl StreamTrait for SqliteDatabase {
    type Stream<'a> = SqliteQueryStream;

    fn get_database_backend(&self) -> DbBackend {
        StreamTrait::get_database_backend(&self.writer_connection())
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            match statement_route(statement.sql.as_str()) {
                StatementRoute::Read => self.reader.stream_raw(self.read_class, statement).await,
                StatementRoute::Write => self.writer_connection().stream_raw(statement).await,
            }
        })
    }
}

#[async_trait]
impl TransactionTrait for SqliteDatabase {
    type Transaction = SqliteTransaction;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementRoute {
    Read,
    Write,
}

/// Classify the outer SQLite statement without recording or exporting SQL.
/// Unknown statements intentionally fall back to the writer. `WITH` needs
/// top-level token scanning because SQLite permits it before both SELECT and
/// mutating statements.
fn statement_route(sql: &str) -> StatementRoute {
    let mut scanner = SqlScanner::new(sql);
    let Some(first) = scanner.next_top_level_word() else {
        return StatementRoute::Write;
    };

    if first.eq_ignore_ascii_case("select")
        || first.eq_ignore_ascii_case("values")
        || first.eq_ignore_ascii_case("explain")
    {
        return StatementRoute::Read;
    }
    if !first.eq_ignore_ascii_case("with") {
        return StatementRoute::Write;
    }

    while let Some(token) = scanner.next_top_level_word() {
        if token.eq_ignore_ascii_case("select") || token.eq_ignore_ascii_case("values") {
            return StatementRoute::Read;
        }
        if token.eq_ignore_ascii_case("insert")
            || token.eq_ignore_ascii_case("update")
            || token.eq_ignore_ascii_case("delete")
            || token.eq_ignore_ascii_case("replace")
        {
            return StatementRoute::Write;
        }
    }
    StatementRoute::Write
}

struct SqlScanner<'a> {
    bytes: &'a [u8],
    offset: usize,
    depth: usize,
}

impl<'a> SqlScanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            bytes: sql.as_bytes(),
            offset: 0,
            depth: 0,
        }
    }

    fn next_top_level_word(&mut self) -> Option<&'a str> {
        while self.offset < self.bytes.len() {
            match self.bytes[self.offset] {
                b'(' => {
                    self.depth = self.depth.saturating_add(1);
                    self.offset += 1;
                }
                b')' => {
                    self.depth = self.depth.saturating_sub(1);
                    self.offset += 1;
                }
                b'\'' | b'"' | b'`' => self.skip_quoted(self.bytes[self.offset]),
                b'[' => self.skip_bracketed_identifier(),
                b'-' if self.bytes.get(self.offset + 1) == Some(&b'-') => self.skip_line_comment(),
                b'/' if self.bytes.get(self.offset + 1) == Some(&b'*') => self.skip_block_comment(),
                byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                    let start = self.offset;
                    self.offset += 1;
                    while self.bytes.get(self.offset).is_some_and(|byte| {
                        byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$'
                    }) {
                        self.offset += 1;
                    }
                    if self.depth == 0 {
                        return std::str::from_utf8(&self.bytes[start..self.offset]).ok();
                    }
                }
                _ => self.offset += 1,
            }
        }
        None
    }

    fn skip_quoted(&mut self, delimiter: u8) {
        self.offset += 1;
        while self.offset < self.bytes.len() {
            if self.bytes[self.offset] == delimiter {
                if self.bytes.get(self.offset + 1) == Some(&delimiter) {
                    self.offset += 2;
                    continue;
                }
                self.offset += 1;
                break;
            }
            self.offset += 1;
        }
    }

    fn skip_bracketed_identifier(&mut self) {
        self.offset += 1;
        while self.offset < self.bytes.len() {
            if self.bytes[self.offset] == b']' {
                self.offset += 1;
                break;
            }
            self.offset += 1;
        }
    }

    fn skip_line_comment(&mut self) {
        self.offset += 2;
        while self.offset < self.bytes.len() && self.bytes[self.offset] != b'\n' {
            self.offset += 1;
        }
    }

    fn skip_block_comment(&mut self) {
        self.offset += 2;
        while self.offset + 1 < self.bytes.len() {
            if self.bytes[self.offset] == b'*' && self.bytes[self.offset + 1] == b'/' {
                self.offset += 2;
                return;
            }
            self.offset += 1;
        }
        self.offset = self.bytes.len();
    }
}

#[cfg(test)]
mod tests {
    use super::{SqliteDatabase, SqliteReadClass, SqliteReadPool, StatementRoute, statement_route};
    use crate::{SqliteReadEvent, SqliteReadObserver, SqliteReadOutcome, SqliteWriteExecutor};
    use futures_util::StreamExt;
    use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement, StreamTrait};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct RecordingReadObserver {
        events: Mutex<Vec<SqliteReadEvent>>,
    }

    impl SqliteReadObserver for RecordingReadObserver {
        fn observe(&self, event: SqliteReadEvent) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }
    }

    impl RecordingReadObserver {
        fn events(&self) -> Vec<SqliteReadEvent> {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    async fn test_database() -> SqliteDatabase {
        let mut reader_options = ConnectOptions::new("sqlite::memory:");
        reader_options.max_connections(4);
        let reader = Database::connect(reader_options)
            .await
            .expect("open test reader pool");

        let mut writer_options = ConnectOptions::new("sqlite::memory:");
        writer_options.max_connections(1);
        let writer = Database::connect(writer_options)
            .await
            .expect("open test writer");

        SqliteDatabase::new(reader, writer)
    }

    async fn observed_test_database() -> (SqliteDatabase, Arc<RecordingReadObserver>) {
        let mut reader_options = ConnectOptions::new("sqlite::memory:");
        reader_options.max_connections(4);
        let reader = Database::connect(reader_options)
            .await
            .expect("open observed test reader pool");

        let mut writer_options = ConnectOptions::new("sqlite::memory:");
        writer_options.max_connections(1);
        let writer = Database::connect(writer_options)
            .await
            .expect("open observed test writer");

        let observer = Arc::new(RecordingReadObserver::default());
        let database = SqliteDatabase::from_executor_with_read_observer(
            reader,
            SqliteWriteExecutor::new(writer),
            observer.clone(),
        );
        (database, observer)
    }

    async fn file_backed_test_database() -> (SqliteDatabase, PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "pioneer-sqlite-read-transaction-{}-{}.sqlite",
            std::process::id(),
            rand::random::<u64>()
        ));
        let mut writer_options =
            ConnectOptions::new(format!("sqlite://{}?mode=rwc", path.display()));
        writer_options.max_connections(1);
        let writer = Database::connect(writer_options)
            .await
            .expect("open file-backed test writer");
        writer
            .execute_unprepared("PRAGMA journal_mode = WAL")
            .await
            .expect("enable WAL for file-backed test database");
        writer
            .execute_unprepared(
                "CREATE TABLE read_transaction_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
            )
            .await
            .expect("create file-backed read transaction probe");

        let mut reader_options =
            ConnectOptions::new(format!("sqlite://{}?mode=ro", path.display()));
        reader_options.max_connections(4);
        let reader = Database::connect(reader_options)
            .await
            .expect("open file-backed test reader");
        (SqliteDatabase::new(reader, writer), path)
    }

    async fn close_file_backed_test_database(database: SqliteDatabase, path: PathBuf) {
        database
            .close()
            .await
            .expect("close file-backed test pools");
        for candidate in [
            path.clone(),
            PathBuf::from(format!("{}-wal", path.display())),
            PathBuf::from(format!("{}-shm", path.display())),
        ] {
            match std::fs::remove_file(&candidate) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => panic!("remove test database {}: {error}", candidate.display()),
            }
        }
    }

    #[test]
    fn routes_plain_queries_to_reader() {
        assert_eq!(statement_route(" SELECT 1"), StatementRoute::Read);
        assert_eq!(statement_route("VALUES (1)"), StatementRoute::Read);
        assert_eq!(
            statement_route("-- leading comment\nSELECT 'delete is data'"),
            StatementRoute::Read
        );
        assert_eq!(
            statement_route("EXPLAIN QUERY PLAN SELECT 1"),
            StatementRoute::Read
        );
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
            statement_route("REPLACE INTO item(id) VALUES (1) RETURNING id"),
            StatementRoute::Write
        );
        assert_eq!(
            statement_route("PRAGMA journal_mode"),
            StatementRoute::Write
        );
        assert_eq!(statement_route(""), StatementRoute::Write);
    }

    #[test]
    fn routes_cte_by_its_outer_statement() {
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
    }

    #[tokio::test]
    async fn database_scopes_carry_consistent_read_and_write_classes() {
        let database = test_database().await;

        assert_eq!(database.read_class(), SqliteReadClass::Interactive);
        assert_eq!(database.write_class(), crate::SqliteWriteClass::Interactive);
        assert_eq!(
            database.with_critical_writes().read_class(),
            SqliteReadClass::Interactive
        );
        assert_eq!(
            database.with_critical_writes().write_class(),
            crate::SqliteWriteClass::Critical
        );
        assert_eq!(
            database.maintenance().read_class(),
            SqliteReadClass::Maintenance
        );
        assert_eq!(
            database.maintenance().write_class(),
            crate::SqliteWriteClass::Maintenance
        );
        let background_critical = database.maintenance().with_critical_writes();
        assert_eq!(
            background_critical.read_class(),
            SqliteReadClass::Maintenance
        );
        assert_eq!(
            background_critical.write_class(),
            crate::SqliteWriteClass::Critical
        );
    }

    #[tokio::test]
    async fn private_reader_rejects_every_write_entry_point() {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("open test reader");
        let reader = SqliteReadPool::new(connection);

        let query_error = reader
            .query_one_raw(
                SqliteReadClass::Interactive,
                Statement::from_string(
                    DbBackend::Sqlite,
                    "DELETE FROM item RETURNING id".to_owned(),
                ),
            )
            .await
            .expect_err("row-returning write must be rejected");
        assert!(query_error.to_string().contains("SQLite read pool"));

        let stream_error = match reader
            .stream_raw(
                SqliteReadClass::Maintenance,
                Statement::from_string(
                    DbBackend::Sqlite,
                    "INSERT INTO item DEFAULT VALUES RETURNING id".to_owned(),
                ),
            )
            .await
        {
            Ok(_) => panic!("streaming write must be rejected"),
            Err(error) => error,
        };
        assert!(stream_error.to_string().contains("SQLite read pool"));
    }

    #[tokio::test]
    async fn read_transaction_accepts_queries_and_rejects_all_mutations() {
        let database = test_database().await;
        let transaction = database.begin_read().await.expect("begin read transaction");

        let row = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 7 AS value".to_owned(),
            ))
            .await
            .expect("query through read transaction")
            .expect("read transaction row");
        assert_eq!(row.try_get::<i64>("", "value").unwrap(), 7);

        let execute_error = transaction
            .execute_unprepared("CREATE TABLE forbidden (id INTEGER)")
            .await
            .expect_err("read transaction execute must be rejected");
        assert!(execute_error.to_string().contains("SQLite read pool"));

        let returning_error = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "DELETE FROM forbidden RETURNING id".to_owned(),
            ))
            .await
            .expect_err("row-returning mutation must be rejected");
        assert!(returning_error.to_string().contains("SQLite read pool"));

        transaction.commit().await.expect("commit read transaction");
        database.close().await.expect("close test database");
    }

    #[tokio::test]
    async fn held_read_snapshot_does_not_occupy_or_block_the_writer() {
        let (database, path) = file_backed_test_database().await;
        let transaction = database.begin_read().await.expect("begin read snapshot");
        let initial = transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
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
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM read_transaction_probe".to_owned(),
            ))
            .await
            .expect("query held snapshot")
            .expect("held snapshot count row");
        assert_eq!(snapshot.try_get::<i64>("", "count").unwrap(), 0);
        transaction.commit().await.expect("commit read snapshot");

        let current = database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM read_transaction_probe".to_owned(),
            ))
            .await
            .expect("query current state")
            .expect("current count row");
        assert_eq!(current.try_get::<i64>("", "count").unwrap(), 1);
        close_file_backed_test_database(database, path).await;
    }

    #[tokio::test]
    async fn maintenance_read_transaction_holds_limiter_until_completion() {
        let database = test_database().await;
        let maintenance = database.maintenance();
        let transaction = maintenance
            .begin_read()
            .await
            .expect("begin maintenance read transaction");
        transaction
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 1 AS value".to_owned(),
            ))
            .await
            .expect("query maintenance read transaction");

        let waiting = tokio::spawn({
            let maintenance = maintenance.clone();
            async move {
                maintenance
                    .query_one_raw(Statement::from_string(
                        DbBackend::Sqlite,
                        "SELECT 2 AS value".to_owned(),
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
                DbBackend::Sqlite,
                "SELECT 3 AS value".to_owned(),
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

    #[tokio::test]
    async fn reader_observer_reports_class_outcome_and_maintenance_lifecycle() {
        let (database, observer) = observed_test_database().await;

        database
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 1 AS value".to_owned(),
            ))
            .await
            .expect("run observed interactive read");
        database
            .maintenance()
            .query_all_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 2 AS value".to_owned(),
            ))
            .await
            .expect("run observed maintenance read");

        let events = observer.events();
        assert!(matches!(
            events.first(),
            Some(SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Interactive,
                outcome: SqliteReadOutcome::Ok,
                ..
            })
        ));
        assert!(matches!(
            events.get(1),
            Some(SqliteReadEvent::AdmissionEnqueued {
                class: SqliteReadClass::Maintenance,
                queue_depth: 1,
                active: 0,
            })
        ));
        assert!(matches!(
            events.get(2),
            Some(SqliteReadEvent::AdmissionAcquired {
                class: SqliteReadClass::Maintenance,
                queue_depth: 0,
                active: 1,
                ..
            })
        ));
        assert!(matches!(
            events.get(3),
            Some(SqliteReadEvent::AdmissionReleased {
                class: SqliteReadClass::Maintenance,
                queue_depth: 0,
                active: 0,
                ..
            })
        ));
        assert!(matches!(
            events.get(4),
            Some(SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                outcome: SqliteReadOutcome::Ok,
                ..
            })
        ));
        assert_eq!(events.len(), 5);
    }

    #[tokio::test]
    async fn reader_ping_uses_the_typed_maintenance_limiter() {
        let (database, observer) = observed_test_database().await;

        database
            .reader
            .ping(SqliteReadClass::Maintenance)
            .await
            .expect("ping observed maintenance reader");

        let events = observer.events();
        assert!(matches!(
            events.as_slice(),
            [
                SqliteReadEvent::AdmissionEnqueued {
                    class: SqliteReadClass::Maintenance,
                    queue_depth: 1,
                    active: 0,
                },
                SqliteReadEvent::AdmissionAcquired {
                    class: SqliteReadClass::Maintenance,
                    queue_depth: 0,
                    active: 1,
                    ..
                },
                SqliteReadEvent::AdmissionReleased {
                    class: SqliteReadClass::Maintenance,
                    queue_depth: 0,
                    active: 0,
                    ..
                },
                SqliteReadEvent::OperationFinished {
                    class: SqliteReadClass::Maintenance,
                    outcome: SqliteReadOutcome::Ok,
                    ..
                },
            ]
        ));
    }

    #[tokio::test]
    async fn maintenance_stream_reserves_one_slot_without_blocking_interactive_reads() {
        let database = test_database().await;
        let maintenance = database.maintenance();
        let mut held_stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 1 AS value UNION ALL SELECT 2 AS value".to_owned(),
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
                        DbBackend::Sqlite,
                        "SELECT 3 AS value".to_owned(),
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
                DbBackend::Sqlite,
                "SELECT 4 AS value".to_owned(),
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

    #[tokio::test]
    async fn completed_maintenance_stream_releases_permit_before_drop() {
        let database = test_database().await;
        let maintenance = database.maintenance();
        let mut completed_stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 1 AS value".to_owned(),
            ))
            .await
            .expect("open maintenance stream");
        assert!(completed_stream.next().await.is_some());
        assert!(completed_stream.next().await.is_none());

        tokio::time::timeout(
            Duration::from_secs(1),
            maintenance.query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 2 AS value".to_owned(),
            )),
        )
        .await
        .expect("EOF must release maintenance permit while wrapper remains alive")
        .expect("run maintenance read after EOF");

        drop(completed_stream);
    }

    #[tokio::test]
    async fn cancelling_waiting_maintenance_read_does_not_leak_capacity() {
        let (database, observer) = observed_test_database().await;
        let maintenance = database.maintenance();
        let held_stream = maintenance
            .stream_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 1 AS value".to_owned(),
            ))
            .await
            .expect("open maintenance stream");

        let waiting = tokio::spawn({
            let maintenance = maintenance.clone();
            async move {
                maintenance
                    .query_one_raw(Statement::from_string(
                        DbBackend::Sqlite,
                        "SELECT 2 AS value".to_owned(),
                    ))
                    .await
            }
        });
        tokio::task::yield_now().await;
        waiting.abort();
        let _ = waiting.await;
        assert!(observer.events().iter().any(|event| matches!(
            event,
            SqliteReadEvent::AdmissionCancelled {
                class: SqliteReadClass::Maintenance,
                queue_depth: 0,
                active: 1,
                ..
            }
        )));
        drop(held_stream);

        tokio::time::timeout(
            Duration::from_secs(1),
            maintenance.query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT 3 AS value".to_owned(),
            )),
        )
        .await
        .expect("cancelled waiter must not consume maintenance capacity")
        .expect("run maintenance read after cancellation");
        assert!(observer.events().iter().any(|event| matches!(
            event,
            SqliteReadEvent::OperationFinished {
                class: SqliteReadClass::Maintenance,
                outcome: SqliteReadOutcome::Cancelled,
                ..
            }
        )));
    }
}
