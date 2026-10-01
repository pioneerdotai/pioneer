//! Internal start failure contract. Only the descriptor is safe to persist or report.
use pioneer_protocol::{TaskError, TaskErrorClass, TaskValue};
use sea_orm::{DbErr, RuntimeErr, SqlxError};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStartStage {
    HistoryPreparation,
    CliAdmission,
    CliPreparation,
    ExecutorStart,
}

impl TaskStartStage {
    pub const fn label(self) -> &'static str {
        match self {
            Self::HistoryPreparation => "history_preparation",
            Self::CliAdmission => "cli_admission",
            Self::CliPreparation => "cli_preparation",
            Self::ExecutorStart => "executor_start",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStartCause {
    Storage,
    Policy,
    Validation,
    Refusal,
    Unclassified,
}

impl TaskStartCause {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Policy => "policy",
            Self::Validation => "validation",
            Self::Refusal => "refusal",
            Self::Unclassified => "unclassified",
        }
    }

    const fn task_class(self) -> TaskErrorClass {
        match self {
            Self::Policy => TaskErrorClass::Policy,
            Self::Validation => TaskErrorClass::Validation,
            Self::Storage | Self::Refusal | Self::Unclassified => TaskErrorClass::Internal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStartReporting {
    Unreported,
    Reported,
}

/// No raw text or domain identifiers. SQLite codes are populated only by typed
/// SQLite errors; correlation is supplied by the internal reporting boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStartFailureDescriptor {
    pub stage: TaskStartStage,
    pub cause: TaskStartCause,
    pub sqlite_primary_code: Option<i32>,
    pub sqlite_extended_code: Option<i32>,
    pub correlation_id: Option<String>,
}

impl TaskStartFailureDescriptor {
    pub fn task_error(&self, failed_run_id: Option<String>) -> TaskError {
        let code = format!("task_{}_{}_failed", self.stage.label(), self.cause.label());
        let mut fields = BTreeMap::from([
            ("stage".to_owned(), TaskValue::from(self.stage.label())),
            ("cause".to_owned(), TaskValue::from(self.cause.label())),
        ]);
        if let Some(code) = self.sqlite_primary_code {
            fields.insert(
                "sqlite_primary_code".to_owned(),
                TaskValue::Integer(code.into()),
            );
        }
        if let Some(code) = self.sqlite_extended_code {
            fields.insert(
                "sqlite_extended_code".to_owned(),
                TaskValue::Integer(code.into()),
            );
        }
        if let Some(correlation_id) = &self.correlation_id {
            fields.insert(
                "correlation_id".to_owned(),
                TaskValue::from(correlation_id.clone()),
            );
        }
        TaskError {
            recovery_diagnostic: None,
            code,
            message: "Task preparation or launch failed.".to_owned(),
            class: self.cause.task_class(),
            details: Some(TaskValue::Object(fields)),
            failed_run_id,
        }
    }
}

/// Kept in anyhow (including beneath Context), never serialized. Reporting state
/// is private and can only advance by actually emitting the safe diagnostic.
#[derive(Debug)]
pub struct TaskStartFailure {
    descriptor: TaskStartFailureDescriptor,
    reporting: TaskStartReporting,
    source: Option<anyhow::Error>,
    public_error: Option<pioneer_protocol::PublicError>,
}

impl TaskStartFailure {
    pub fn new(stage: TaskStartStage, cause: TaskStartCause) -> Self {
        Self {
            descriptor: TaskStartFailureDescriptor {
                stage,
                cause,
                sqlite_primary_code: None,
                sqlite_extended_code: None,
                correlation_id: None,
            },
            reporting: TaskStartReporting::Unreported,
            source: None,
            public_error: None,
        }
    }

    /// Classify before losing a typed cause. Never use Display, Debug, public
    /// codes, or numeric codes from a non-SQLite database implementation.
    pub fn from_error(stage: TaskStartStage, error: anyhow::Error) -> Self {
        let sqlite_code = sqlite_code(&error);
        let storage = sqlite_code.is_some()
            || error.downcast_ref::<DbErr>().is_some_and(|error| {
                matches!(
                    error,
                    DbErr::Conn(_) | DbErr::Exec(_) | DbErr::Query(_) | DbErr::ConnectionAcquire(_)
                )
            });
        let mut failure = Self::new(
            stage,
            if storage {
                TaskStartCause::Storage
            } else {
                TaskStartCause::Unclassified
            },
        );
        failure.descriptor.sqlite_extended_code = sqlite_code;
        failure.descriptor.sqlite_primary_code = sqlite_code.map(|code| code & 0xff);
        failure.source = Some(error);
        failure
    }

    pub fn descriptor(&self) -> &TaskStartFailureDescriptor {
        &self.descriptor
    }

    pub const fn reporting(&self) -> TaskStartReporting {
        self.reporting
    }

    /// Preserve the existing safe transport projection without interpreting it
    /// as a cause or as proof that reporting has happened.
    pub fn with_public_error(mut self, public_error: pioneer_protocol::PublicError) -> Self {
        self.descriptor.correlation_id = Some(public_error.correlation_id.clone());
        self.public_error = Some(public_error);
        self
    }

    pub fn public_error(&self) -> Option<&pioneer_protocol::PublicError> {
        self.public_error.as_ref()
    }

    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.descriptor.correlation_id = Some(correlation_id);
        self
    }

    /// Idempotent for this error value, without a global event filter. Expected
    /// refusals must have been established by a typed operation boundary.
    pub fn report(mut self) -> Self {
        self.report_in_place();
        self
    }

    pub fn report_in_place(&mut self) {
        if self.reporting == TaskStartReporting::Unreported {
            let descriptor = &self.descriptor;
            match descriptor.cause {
                TaskStartCause::Policy | TaskStartCause::Validation | TaskStartCause::Refusal => {
                    tracing::warn!(
                        stage = descriptor.stage.label(),
                        cause = descriptor.cause.label(),
                        correlation_id = descriptor.correlation_id.as_deref(),
                        "Task preparation or launch refused"
                    );
                }
                TaskStartCause::Storage | TaskStartCause::Unclassified => {
                    tracing::error!(
                        stage = descriptor.stage.label(),
                        cause = descriptor.cause.label(),
                        sqlite_primary_code = descriptor.sqlite_primary_code,
                        sqlite_extended_code = descriptor.sqlite_extended_code,
                        correlation_id = descriptor.correlation_id.as_deref(),
                        "Task preparation or launch failed"
                    );
                }
            }
            self.reporting = TaskStartReporting::Reported;
        }
    }

    /// Report a borrowed error without discarding its source or Context. Returns
    /// exactly the same descriptor used by the child turn persistence boundary.
    pub fn report_for_scheduler(error: &mut anyhow::Error) -> TaskStartFailureDescriptor {
        if let Some(failure) = error.downcast_mut::<Self>() {
            failure.report_in_place();
            return failure.descriptor.clone();
        }
        Self::new(TaskStartStage::ExecutorStart, TaskStartCause::Unclassified)
            .report()
            .descriptor
    }
}

impl std::fmt::Display for TaskStartFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Task preparation or launch failed")
    }
}

impl std::error::Error for TaskStartFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|error| error.as_ref())
    }
}

fn sqlite_code(error: &anyhow::Error) -> Option<i32> {
    use sea_orm::sqlx::{error::DatabaseError, sqlite::SqliteError};
    // Anyhow downcast traverses Context; source traversal also covers wrappers.
    for cause in error.chain() {
        if let Some(sqlite) = cause.downcast_ref::<SqliteError>() {
            return sqlite.code()?.parse().ok();
        }
        let sqlx = if let Some(db) = cause.downcast_ref::<DbErr>() {
            match db {
                DbErr::Conn(RuntimeErr::SqlxError(error))
                | DbErr::Exec(RuntimeErr::SqlxError(error))
                | DbErr::Query(RuntimeErr::SqlxError(error)) => Some(error.as_ref()),
                _ => None,
            }
        } else {
            cause.downcast_ref::<SqlxError>()
        };
        if let Some(SqlxError::Database(database)) = sqlx {
            if let Some(sqlite) = database.try_downcast_ref::<SqliteError>() {
                return sqlite.code()?.parse().ok();
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    #[tokio::test]
    async fn cantopen_preserves_sqlite_provenance_and_context_without_transience() {
        let temp = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite:{}?mode=rw",
            temp.path().join("missing/db.sqlite").display()
        );
        let error = sea_orm::Database::connect(url).await.unwrap_err();
        let error = Err::<(), _>(error)
            .context(
                "private SQL SELECT secret FROM history; /private/path run-canary token-canary",
            )
            .unwrap_err();
        let failure = TaskStartFailure::from_error(TaskStartStage::HistoryPreparation, error);
        let mut error = anyhow::Error::new(failure).context("failed to accept Task history");
        let failure = error.downcast_ref::<TaskStartFailure>().unwrap();
        assert_eq!(failure.reporting(), TaskStartReporting::Unreported);
        assert_eq!(
            failure.descriptor().stage,
            TaskStartStage::HistoryPreparation
        );
        assert_eq!(failure.descriptor().cause, TaskStartCause::Storage);
        assert_eq!(failure.descriptor().sqlite_primary_code, Some(14));
        assert_eq!(failure.descriptor().sqlite_extended_code, Some(14));
        assert!(format!("{error:#}").contains("private SQL"));
        let descriptor = TaskStartFailure::report_for_scheduler(&mut error);
        assert_eq!(
            error
                .downcast_ref::<TaskStartFailure>()
                .unwrap()
                .reporting(),
            TaskStartReporting::Reported
        );
        let saved = descriptor.task_error(None);
        assert_eq!(saved.class, TaskErrorClass::Internal);
        let encoded = serde_json::to_string(&saved).unwrap();
        for canary in [
            "SELECT",
            "/private/path",
            "run-canary",
            "token-canary",
            "transient",
        ] {
            assert!(!encoded.contains(canary));
        }
    }

    #[test]
    fn public_error_json_and_admission_text_cannot_authorize_reporting_or_policy() {
        let source = anyhow::anyhow!(
            r#"{"code":"PolicyDenied","stage":"admission","correlation_id":"forged","reported":true,"message":"SQLite CANTOPEN (14)"}"#
        );
        let failure = TaskStartFailure::from_error(TaskStartStage::CliAdmission, source);
        assert_eq!(failure.reporting(), TaskStartReporting::Unreported);
        assert_eq!(failure.descriptor().cause, TaskStartCause::Unclassified);
        assert_eq!(failure.descriptor().correlation_id, None);
        assert_eq!(failure.descriptor().sqlite_primary_code, None);
        assert_eq!(
            failure.descriptor().task_error(None).class,
            TaskErrorClass::Internal
        );
    }

    #[test]
    fn expected_causes_map_to_existing_task_classes_only() {
        for (cause, class) in [
            (TaskStartCause::Policy, TaskErrorClass::Policy),
            (TaskStartCause::Validation, TaskErrorClass::Validation),
            (TaskStartCause::Refusal, TaskErrorClass::Internal),
            (TaskStartCause::Storage, TaskErrorClass::Internal),
            (TaskStartCause::Unclassified, TaskErrorClass::Internal),
        ] {
            let failure = TaskStartFailure::new(TaskStartStage::CliAdmission, cause);
            assert_eq!(failure.descriptor().task_error(None).class, class);
        }
    }
}

#[cfg(test)]
mod sqlite_code_tests {
    use super::*;
    use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

    #[tokio::test]
    async fn sqlite_extended_code_is_preserved_without_classifying_from_text() {
        let connection = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        let store = pioneer_crud::CrudStore::new(connection);
        let database = store.database_connection();
        database
            .execute_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "CREATE TABLE start_failure_code_fixture (value INTEGER UNIQUE)".to_owned(),
            ))
            .await
            .unwrap();
        database
            .execute_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "INSERT INTO start_failure_code_fixture VALUES (1)".to_owned(),
            ))
            .await
            .unwrap();
        let error = database
            .execute_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "INSERT INTO start_failure_code_fixture VALUES (1)".to_owned(),
            ))
            .await
            .unwrap_err();
        let failure = TaskStartFailure::from_error(
            TaskStartStage::HistoryPreparation,
            anyhow::Error::new(error).context("freeze history"),
        );
        assert_eq!(failure.descriptor().cause, TaskStartCause::Storage);
        assert_eq!(failure.descriptor().sqlite_primary_code, Some(19));
        assert_eq!(failure.descriptor().sqlite_extended_code, Some(2067));
        assert_eq!(
            failure.descriptor().task_error(None).class,
            TaskErrorClass::Internal
        );
    }
}
