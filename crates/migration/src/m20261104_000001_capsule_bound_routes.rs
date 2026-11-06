//! SQLite CHECK replacement needs installed DDL/trigger inspection (no builder
//! equivalent). SeaORM's transaction API cannot pin an out-of-transaction PRAGMA;
//! use its re-exported SQLx pool locally so FK settings and DDL share one lease.
use sea_orm_migration::prelude::*;
use sea_orm_migration::sea_orm::{DatabaseConnectionType, DatabaseExecutor, sqlx};
use sqlx::{AssertSqlSafe, Connection, Row, Sqlite, SqliteConnection, pool::PoolConnection};

#[derive(DeriveMigrationName)]
pub struct Migration;

const OLD: &str = "route_kind IN ('execution_bound', 'identity_bound')";
const NEW: &str = "route_kind IN ('execution_bound', 'identity_bound', 'capsule_bound')";

// Arm as soon as the physical connection is acquired, for both FK modes.
// Error, panic or cancellation closes it unless the original FK setting and
// absence of an open transaction have both been verified. SQLx transaction
// Drop alone cannot establish that its tracked depth returned to zero.
struct MigrationLease {
    connection: PoolConnection<Sqlite>,
    discard: bool,
}
impl Drop for MigrationLease {
    fn drop(&mut self) {
        if self.discard {
            self.connection.close_on_drop();
        }
    }
}

fn database_error(error: sqlx::Error) -> DbErr {
    DbErr::Custom(format!("capsule route schema operation failed: {error}"))
}

async fn execute(db: &mut SqliteConnection, sql: String) -> Result<(), sqlx::Error> {
    // Only installed schema DDL and migration-owned builder output enter here.
    sqlx::raw_sql(AssertSqlSafe(sql)).execute(db).await?;
    Ok(())
}

async fn rebuild_on_connection(db: &mut SqliteConnection, up: bool) -> Result<(), DbErr> {
    let mut txn = db.begin().await.map_err(database_error)?;
    let result: Result<(), DbErr> = async {
        if !up && sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM agent_delegation_route WHERE route_kind = 'capsule_bound' LIMIT 1")
            .fetch_optional(&mut *txn).await.map_err(database_error)?.is_some() {
            return Err(DbErr::Custom("cannot downgrade while capsule-bound routes exist; revoke alone does not remove provenance".into()));
        }
        let ddl = sqlx::query_scalar::<_, String>(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'agent_delegation_route'")
            .fetch_optional(&mut *txn).await.map_err(database_error)?
            .ok_or_else(|| DbErr::Custom("route table is missing".into()))?;
        let (from, to) = if up { (OLD, NEW) } else { (NEW, OLD) };
        // Retry after schema commit but before the migration marker is safe.
        if ddl.matches(to).count() == 1 { return Ok(()); }
        if ddl.matches(from).count() != 1 {
            return Err(DbErr::Custom("unrecognized route kind CHECK; refusing lossy rebuild".into()));
        }
        let objects = sqlx::query(
            "SELECT sql FROM sqlite_master WHERE tbl_name = 'agent_delegation_route' AND type IN ('index', 'trigger') AND sql IS NOT NULL ORDER BY type, name")
            .fetch_all(&mut *txn).await.map_err(database_error)?;
        let body = ddl.find('(').ok_or_else(|| DbErr::Custom("invalid route table DDL".into()))?;
        execute(&mut txn, format!("CREATE TABLE agent_delegation_route_capsule_upgrade {}", ddl[body..].replace(from, to))).await.map_err(database_error)?;
        for sql in [
            "INSERT INTO agent_delegation_route_capsule_upgrade SELECT * FROM agent_delegation_route".to_owned(),
            "DROP TABLE agent_delegation_route".to_owned(),
            ddl.replace(from, to),
            "INSERT INTO agent_delegation_route SELECT * FROM agent_delegation_route_capsule_upgrade".to_owned(),
            "DROP TABLE agent_delegation_route_capsule_upgrade".to_owned(),
        ] { execute(&mut txn, sql).await.map_err(database_error)?; }
        for row in objects {
            execute(&mut txn, row.try_get("sql").map_err(database_error)?).await.map_err(database_error)?;
        }
        let index = if up {
            Index::create().name("idx_agent_route_capsule_active")
                .table(Alias::new("agent_delegation_route"))
                .col(Alias::new("source_capsule_id"))
                .col(Alias::new("route_kind"))
                .col(Alias::new("status"))
                .col(Alias::new("expires_at")).to_string(SqliteQueryBuilder)
        } else {
            Index::drop().name("idx_agent_route_capsule_active")
                .table(Alias::new("agent_delegation_route")).to_string(SqliteQueryBuilder)
        };
        execute(&mut txn, index).await.map_err(database_error)?;
        Ok(())
    }.await;
    match result {
        Ok(()) => txn.commit().await.map_err(database_error),
        Err(error) => {
            txn.rollback().await.map_err(database_error)?;
            Err(error)
        }
    }
}

async fn rebuild(manager: &SchemaManager<'_>, up: bool) -> Result<(), DbErr> {
    // An enclosing transaction cannot disable FK enforcement. Reject before
    // acquiring another connection or changing any connection/session state.
    let DatabaseExecutor::Connection(db) = manager.get_connection() else {
        return Err(DbErr::Custom(
            "capsule route rebuild requires a standalone SQLite pool".into(),
        ));
    };
    if !matches!(
        &db.inner,
        DatabaseConnectionType::SqlxSqlitePoolConnection(_)
    ) {
        return Err(DbErr::Custom(
            "capsule route rebuild requires a SQLx SQLite pool".into(),
        ));
    }
    let mut lease = MigrationLease {
        connection: db
            .get_sqlite_connection_pool()
            .acquire()
            .await
            .map_err(database_error)?,
        discard: true,
    };
    if lease.connection.is_in_transaction() {
        return Err(DbErr::Custom(
            "capsule route rebuild cannot enter an active transaction".into(),
        ));
    }
    let original_fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *lease.connection)
        .await
        .map_err(database_error)?;
    // Keep the lease armed even when FK is already OFF: failed commit/rollback
    // may leave SQLx tracking an open transaction in either mode.
    if original_fk != 0 {
        execute(&mut lease.connection, "PRAGMA foreign_keys=OFF".into())
            .await
            .map_err(database_error)?;
    }
    let enabled: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *lease.connection)
        .await
        .map_err(database_error)?;
    if enabled != 0 {
        return Err(DbErr::Custom(
            "cannot disable FK on migration connection".into(),
        ));
    }
    let outcome = rebuild_on_connection(&mut lease.connection, up).await;
    if original_fk != 0 {
        execute(&mut lease.connection, "PRAGMA foreign_keys=ON".into())
            .await
            .map_err(database_error)?;
    }
    let restored: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut *lease.connection)
        .await
        .map_err(database_error)?;
    if restored != original_fk || lease.connection.is_in_transaction() {
        return Err(DbErr::Custom(
            "migration connection state was not restored".into(),
        ));
    }
    lease.discard = false;
    outcome
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    fn use_transaction(&self) -> Option<bool> {
        Some(false)
    }
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rebuild(manager, true).await
    }
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rebuild(manager, false).await
    }
}
