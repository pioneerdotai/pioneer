use async_trait::async_trait;
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, DbErr,
    ExecResult, IsolationLevel, QueryResult, Statement, StreamTrait, TransactionError,
    TransactionOptions, TransactionTrait,
};
use std::future::Future;
use std::pin::Pin;

/// A pool dedicated to ordinary SQLite reads.
///
/// The Gateway opens the underlying connections with SQLite's read-only flag
/// and `PRAGMA query_only = ON`. This wrapper adds an API-level guard:
/// mutation entry points and non-read statements are rejected before they can
/// reach the pool.
#[derive(Clone, Debug)]
pub struct SqliteReadPool {
    connection: DatabaseConnection,
}

impl SqliteReadPool {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self { connection }
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

    pub fn max_connections(&self) -> u32 {
        self.connection
            .get_sqlite_connection_pool()
            .options()
            .get_max_connections()
    }

    pub async fn query_only_enabled(&self) -> Result<bool, DbErr> {
        let row = self
            .connection
            .query_one_raw(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA query_only".to_owned(),
            ))
            .await?
            .ok_or_else(|| DbErr::Custom("PRAGMA query_only returned no row".to_owned()))?;
        Ok(row.try_get::<i64>("", "query_only")? != 0)
    }
}

#[async_trait]
impl ConnectionTrait for SqliteReadPool {
    fn get_database_backend(&self) -> DbBackend {
        self.connection.get_database_backend()
    }

    async fn execute_raw(&self, _statement: Statement) -> Result<ExecResult, DbErr> {
        Err(Self::reject_write())
    }

    async fn execute_unprepared(&self, _sql: &str) -> Result<ExecResult, DbErr> {
        Err(Self::reject_write())
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        Self::ensure_read_statement(&statement)?;
        self.connection.query_one_raw(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        Self::ensure_read_statement(&statement)?;
        self.connection.query_all_raw(statement).await
    }

    fn support_returning(&self) -> bool {
        self.connection.support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.connection.is_mock_connection()
    }
}

impl StreamTrait for SqliteReadPool {
    type Stream<'a> = <DatabaseConnection as StreamTrait>::Stream<'a>;

    fn get_database_backend(&self) -> DbBackend {
        self.connection.get_database_backend()
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            Self::ensure_read_statement(&statement)?;
            self.connection.stream_raw(statement).await
        })
    }
}

/// The single SQLite writer contour. Transactions always originate here, so
/// every query made through a write transaction remains on the writer.
#[derive(Clone, Debug)]
pub struct SqliteWriter {
    connection: DatabaseConnection,
}

impl SqliteWriter {
    pub fn new(connection: DatabaseConnection) -> Self {
        Self { connection }
    }

    pub fn max_connections(&self) -> u32 {
        self.connection
            .get_sqlite_connection_pool()
            .options()
            .get_max_connections()
    }
}

#[async_trait]
impl ConnectionTrait for SqliteWriter {
    fn get_database_backend(&self) -> DbBackend {
        self.connection.get_database_backend()
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.connection.execute_raw(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.connection.execute_unprepared(sql).await
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.connection.query_one_raw(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.connection.query_all_raw(statement).await
    }

    fn support_returning(&self) -> bool {
        self.connection.support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.connection.is_mock_connection()
    }
}

impl StreamTrait for SqliteWriter {
    type Stream<'a> = <DatabaseConnection as StreamTrait>::Stream<'a>;

    fn get_database_backend(&self) -> DbBackend {
        self.connection.get_database_backend()
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        self.connection.stream_raw(statement)
    }
}

#[async_trait]
impl TransactionTrait for SqliteWriter {
    type Transaction = DatabaseTransaction;

    async fn begin(&self) -> Result<Self::Transaction, DbErr> {
        self.connection.begin().await
    }

    async fn begin_with_config(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<Self::Transaction, DbErr> {
        self.connection
            .begin_with_config(isolation_level, access_mode)
            .await
    }

    async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction, DbErr> {
        self.connection.begin_with_options(options).await
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
        self.connection.transaction(callback).await
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
        self.connection
            .transaction_with_config(callback, isolation_level, access_mode)
            .await
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
    writer: SqliteWriter,
}

impl SqliteDatabase {
    pub fn new(reader: DatabaseConnection, writer: DatabaseConnection) -> Self {
        Self {
            reader: SqliteReadPool::new(reader),
            writer: SqliteWriter::new(writer),
        }
    }

    /// Compatibility constructor for isolated tests and embedders which own a
    /// single connection. Gateway production startup always uses [`Self::new`]
    /// with two independently opened pools.
    pub fn from_single_connection(connection: DatabaseConnection) -> Self {
        Self::new(connection.clone(), connection)
    }

    pub fn reader(&self) -> &SqliteReadPool {
        &self.reader
    }

    pub fn writer(&self) -> &SqliteWriter {
        &self.writer
    }

    /// Close both physical pools. Both close attempts are made so a reader
    /// shutdown error cannot leave the writer pool running.
    pub async fn close(self) -> Result<(), DbErr> {
        let reader_result = self.reader.connection.close().await;
        let writer_result = self.writer.connection.close().await;
        reader_result.and(writer_result)
    }

    async fn query_one_routed(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        match statement_route(statement.sql.as_str()) {
            StatementRoute::Read => self.reader.query_one_raw(statement).await,
            StatementRoute::Write => self.writer.query_one_raw(statement).await,
        }
    }

    async fn query_all_routed(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match statement_route(statement.sql.as_str()) {
            StatementRoute::Read => self.reader.query_all_raw(statement).await,
            StatementRoute::Write => self.writer.query_all_raw(statement).await,
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
        ConnectionTrait::get_database_backend(&self.writer)
    }

    async fn execute_raw(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.writer.execute_raw(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.writer.execute_unprepared(sql).await
    }

    async fn query_one_raw(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.query_one_routed(statement).await
    }

    async fn query_all_raw(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.query_all_routed(statement).await
    }

    fn support_returning(&self) -> bool {
        self.writer.support_returning()
    }

    fn is_mock_connection(&self) -> bool {
        self.writer.is_mock_connection()
    }
}

impl StreamTrait for SqliteDatabase {
    type Stream<'a> = <DatabaseConnection as StreamTrait>::Stream<'a>;

    fn get_database_backend(&self) -> DbBackend {
        StreamTrait::get_database_backend(&self.writer)
    }

    fn stream_raw<'a>(
        &'a self,
        statement: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream<'a>, DbErr>> + 'a + Send>> {
        Box::pin(async move {
            match statement_route(statement.sql.as_str()) {
                StatementRoute::Read => self.reader.stream_raw(statement).await,
                StatementRoute::Write => self.writer.stream_raw(statement).await,
            }
        })
    }
}

#[async_trait]
impl TransactionTrait for SqliteDatabase {
    type Transaction = DatabaseTransaction;

    async fn begin(&self) -> Result<Self::Transaction, DbErr> {
        self.writer.begin().await
    }

    async fn begin_with_config(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<Self::Transaction, DbErr> {
        self.writer
            .begin_with_config(isolation_level, access_mode)
            .await
    }

    async fn begin_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Self::Transaction, DbErr> {
        self.writer.begin_with_options(options).await
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
        self.writer.transaction(callback).await
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
        self.writer
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
    use super::{StatementRoute, statement_route};

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
}
