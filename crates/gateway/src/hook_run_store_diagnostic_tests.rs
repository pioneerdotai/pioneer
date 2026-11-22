use super::{HookRunStoreCauseClass, HookRunStoreDiagnostic, append_audit_error};
use crate::tests::postgres_failures::{
    lock_unavailable, rejected_snapshot, rejected_statement, sqlstate,
};
use sea_orm::{DbErr, RuntimeErr, SqlxError};

fn assert_typed_diagnostic(error: DbErr, code: &str, class: HookRunStoreCauseClass) {
    assert_eq!(sqlstate(&error), code);
    for error in [
        anyhow::Error::new(error.clone()),
        anyhow::Error::new(error)
            .context("SELECT SQL_CANARY /private/PATH_CANARY ID_CANARY AUDIT_PAYLOAD_CANARY"),
    ] {
        let safe = append_audit_error(error);
        assert_eq!(
            safe.diagnostic(),
            HookRunStoreDiagnostic { cause_class: class }
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

#[pioneer_postgres_test::postgres_test]
async fn typed_lock_and_serialization_failures_keep_their_class_under_anyhow_context()
-> anyhow::Result<()> {
    assert_typed_diagnostic(
        lock_unavailable(false).await?,
        "55P03",
        HookRunStoreCauseClass::PostgresContention,
    );
    assert_typed_diagnostic(
        rejected_snapshot().await?,
        "40001",
        HookRunStoreCauseClass::PostgresContention,
    );
    Ok(())
}

#[pioneer_postgres_test::postgres_test]
async fn typed_constraint_failure_has_safe_diagnostics() -> anyhow::Result<()> {
    assert_typed_diagnostic(
        rejected_statement().await?,
        "23505",
        HookRunStoreCauseClass::PostgresConstraint,
    );
    Ok(())
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
        Some("55P03".into())
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
fn a_database_code_without_postgres_type_is_unclassified() {
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
