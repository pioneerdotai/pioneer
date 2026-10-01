use super::{HookRunStoreCauseClass, HookRunStoreDiagnostic, append_audit_error};
use pioneer_sqlite::{SqliteDatabase, sqlite_connection_url};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DbBackend, DbErr, RuntimeErr, SqlxError, Statement,
    TransactionTrait,
};

// Follow the existing managed SQLite fixture pattern: one physical connection
// per handle, with all statements and transactions through SqliteDatabase.
async fn open_fixture_database(url: &str) -> SqliteDatabase {
    let mut options = ConnectOptions::new(url);
    options.max_connections(1).sqlx_logging(false);
    let database = SqliteDatabase::from_single_connection(
        Database::connect(options)
            .await
            .expect("open isolated SQLite fixture"),
    );
    database
        .execute_unprepared("PRAGMA busy_timeout = 0")
        .await
        .unwrap();
    database
}

fn assert_typed_diagnostic(error: DbErr, code: i32, class: HookRunStoreCauseClass) {
    let DbErr::Exec(RuntimeErr::SqlxError(sqlx)) = &error else {
        panic!("fixture must produce SeaORM's typed SQLx execution error");
    };
    let SqlxError::Database(database) = sqlx.as_ref() else {
        panic!("fixture must produce a typed database error");
    };
    assert!(
        database
            .try_downcast_ref::<sea_orm::SqlxSqliteError>()
            .is_some()
    );
    assert_eq!(database.code().unwrap().parse::<i32>().unwrap(), code);
    for error in [
        anyhow::Error::new(error.clone()),
        anyhow::Error::new(error)
            .context("SELECT SQL_CANARY /private/PATH_CANARY ID_CANARY AUDIT_PAYLOAD_CANARY"),
    ] {
        let safe = append_audit_error(error);
        assert_eq!(
            safe.diagnostic(),
            HookRunStoreDiagnostic {
                cause_class: class,
                sqlite_primary_code: Some(code & 0xff),
                sqlite_extended_code: Some(code),
            }
        );
        for canary in [
            "SQL_CANARY",
            "PATH_CANARY",
            "ID_CANARY",
            "AUDIT_PAYLOAD_CANARY",
        ] {
            assert!(!format!("{safe:?} {safe}").contains(canary));
        }
        assert!(std::error::Error::source(&safe).is_none());
    }
}

#[tokio::test]
async fn typed_busy_and_locked_remain_distinct_under_anyhow_context() {
    for (shared_cache, code, class) in [
        (false, 5, HookRunStoreCauseClass::SqliteBusy),
        (true, 262, HookRunStoreCauseClass::SqliteLocked),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let mut url = sqlite_connection_url(&directory.path().join("PATH_CANARY.sqlite"));
        if shared_cache {
            url.push_str("&cache=shared");
        }
        let owner = open_fixture_database(&url).await;
        let contender = open_fixture_database(&url).await;
        owner
            .execute_unprepared("CREATE TABLE diagnostic_probe (id INTEGER PRIMARY KEY)")
            .await
            .unwrap();
        // Only necessary DB calls while the fixture lock is held. Schema lock
        // fails during prepare, before SQLx's shared-cache unlock-notify loop.
        let transaction = owner.begin().await.unwrap();
        transaction
            .execute_unprepared(if shared_cache {
                "CREATE TABLE pending_schema (id INTEGER PRIMARY KEY)"
            } else {
                "INSERT INTO diagnostic_probe VALUES (1)"
            })
            .await
            .unwrap();
        let error = contender
            .execute_unprepared("INSERT INTO diagnostic_probe VALUES (2)")
            .await
            .unwrap_err();
        transaction.rollback().await.unwrap();
        assert_typed_diagnostic(error, code, class);
        contender.close().await.unwrap();
        owner.close().await.unwrap();
    }
}

#[tokio::test]
async fn typed_cantopen_retains_code_and_discards_path_under_anyhow_context() {
    let directory = tempfile::tempdir().unwrap();
    let missing_file = directory
        .path()
        .join("missing_PARENT_CANARY")
        .join("PATH_CANARY.sqlite");
    let database = open_fixture_database("sqlite::memory:").await;
    let error = database
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "ATTACH DATABASE ? AS diagnostic_probe",
            [missing_file.to_string_lossy().into_owned().into()],
        ))
        .await
        .unwrap_err();
    assert_typed_diagnostic(error, 14, HookRunStoreCauseClass::SqliteCantOpen);
    database.close().await.unwrap();
}

#[derive(Debug)]
struct OtherDatabaseError;

impl std::fmt::Display for OtherDatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "database locked SQL_CANARY /private/PATH_CANARY ID_CANARY AUDIT_PAYLOAD_CANARY",
        )
    }
}

impl std::error::Error for OtherDatabaseError {}

impl sea_orm::sqlx::error::DatabaseError for OtherDatabaseError {
    fn message(&self) -> &str {
        "database locked AUDIT_PAYLOAD_CANARY"
    }
    fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
        Some("14".into())
    }
    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sea_orm::sqlx::error::ErrorKind {
        sea_orm::sqlx::error::ErrorKind::Other
    }
}

#[test]
fn a_database_code_without_sqlite_type_is_unclassified() {
    let sqlx = SqlxError::Database(Box::new(OtherDatabaseError));
    let error = DbErr::Exec(RuntimeErr::SqlxError(std::sync::Arc::new(sqlx)));
    let safe = append_audit_error(anyhow::Error::new(error).context("PATH_CANARY"));
    assert_eq!(safe.diagnostic(), HookRunStoreDiagnostic::default());
    for canary in [
        "SQL_CANARY",
        "PATH_CANARY",
        "ID_CANARY",
        "AUDIT_PAYLOAD_CANARY",
    ] {
        assert!(!format!("{safe:?} {safe}").contains(canary));
    }
    assert!(std::error::Error::source(&safe).is_none());
}
