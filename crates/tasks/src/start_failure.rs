//! Internal start failure contract. Only the descriptor is safe to persist or report.
use pioneer_protocol::{TaskError, TaskErrorClass, TaskValue};
use sea_orm::{DbErr, RuntimeErr, SqlxError};
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

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
pub enum TaskStartReason {
    RuntimeNotReady,
    ReadinessSnapshotMissing,
    CapabilityProjectionInvalid,
    CapabilityResolutionUncertain,
    LaunchSourceUnavailable,
    ProviderAuthorityUnavailable,
}
impl TaskStartReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::RuntimeNotReady => "runtime_not_ready",
            Self::ReadinessSnapshotMissing => "readiness_snapshot_missing",
            Self::CapabilityProjectionInvalid => "capability_projection_invalid",
            Self::CapabilityResolutionUncertain => "capability_resolution_uncertain",
            Self::LaunchSourceUnavailable => "launch_source_unavailable",
            Self::ProviderAuthorityUnavailable => "provider_authority_unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStartReporting {
    Unreported,
    Reported,
}

/// No raw text or domain identifiers. Diagnostic codes come from typed
/// database errors, and correlation is allocated at the typed failure boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStartFailureDescriptor {
    pub stage: TaskStartStage,
    pub cause: TaskStartCause,
    pub reason: Option<TaskStartReason>,
    pub postgres_sqlstate: Option<String>,

    pub correlation_id: Option<String>,
}

impl TaskStartFailureDescriptor {
    pub fn task_error(&self, failed_run_id: Option<String>) -> TaskError {
        let code = format!("task_{}_{}_failed", self.stage.label(), self.cause.label());
        let mut fields = BTreeMap::from([
            ("stage".to_owned(), TaskValue::from(self.stage.label())),
            ("cause".to_owned(), TaskValue::from(self.cause.label())),
        ]);
        if let Some(reason) = self.reason {
            fields.insert("reason".to_owned(), TaskValue::from(reason.label()));
        }
        if let Some(code) = &self.postgres_sqlstate {
            fields.insert(
                "postgres_sqlstate".to_owned(),
                TaskValue::from(code.clone()),
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
    reported: Arc<AtomicBool>,
    source: Option<anyhow::Error>,
    public_error: Option<pioneer_protocol::PublicError>,
}

impl TaskStartFailure {
    pub fn new(stage: TaskStartStage, cause: TaskStartCause) -> Self {
        Self {
            descriptor: TaskStartFailureDescriptor {
                stage,
                cause,
                reason: None,
                postgres_sqlstate: None,

                correlation_id: Some(pioneer_protocol::generate_id(21)),
            },
            reported: Arc::new(AtomicBool::new(false)),
            source: None,
            public_error: None,
        }
    }

    /// Classify before losing a typed cause. Only driver error types can
    /// supply a diagnostic database code.
    pub fn from_error(stage: TaskStartStage, error: anyhow::Error) -> Self {
        let mut failure = Self::from_borrowed_error(stage, &error);
        failure.source = Some(error);
        failure
    }

    /// Classify an existing source without consuming or formatting it.
    pub fn from_borrowed_error(stage: TaskStartStage, error: &anyhow::Error) -> Self {
        if let Some(failure) = error.downcast_ref::<Self>().or_else(|| {
            error
                .chain()
                .find_map(|source| source.downcast_ref::<Self>())
        }) {
            return Self {
                descriptor: failure.descriptor.clone(),
                reported: Arc::clone(&failure.reported),
                source: None,
                public_error: failure.public_error.clone(),
            };
        }
        let database_code = database_code(error);
        let storage = database_code.is_some()
            || error.chain().any(|cause| {
                cause.downcast_ref::<DbErr>().is_some_and(|error| {
                    matches!(
                        error,
                        DbErr::Conn(_)
                            | DbErr::Exec(_)
                            | DbErr::Query(_)
                            | DbErr::ConnectionAcquire(_)
                    )
                })
            });
        let mut failure = Self::new(
            stage,
            if storage {
                TaskStartCause::Storage
            } else {
                TaskStartCause::Unclassified
            },
        );
        failure.descriptor.postgres_sqlstate = database_code;
        failure
    }

    pub fn with_reason(mut self, reason: TaskStartReason) -> Self {
        self.descriptor.reason.get_or_insert(reason);
        self
    }

    pub fn descriptor(&self) -> &TaskStartFailureDescriptor {
        &self.descriptor
    }

    pub fn reporting(&self) -> TaskStartReporting {
        if self.reported.load(Ordering::Acquire) {
            TaskStartReporting::Reported
        } else {
            TaskStartReporting::Unreported
        }
    }

    /// Preserve the existing safe transport projection without interpreting it
    /// as a cause or as proof that reporting has happened.
    pub fn with_public_error(mut self, mut public_error: pioneer_protocol::PublicError) -> Self {
        if let Some(correlation) = &self.descriptor.correlation_id {
            public_error.correlation_id = correlation.clone();
        } else {
            self.descriptor.correlation_id = Some(public_error.correlation_id.clone());
        }
        self.public_error = Some(public_error);
        self
    }

    pub fn public_error(&self) -> Option<&pioneer_protocol::PublicError> {
        self.public_error.as_ref()
    }

    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        if self.reporting() == TaskStartReporting::Unreported {
            self.descriptor.correlation_id = Some(correlation_id);
        }
        self
    }

    /// Idempotent for this error value, without a global event filter. Expected
    /// refusals must have been established by a typed operation boundary.
    pub fn report(mut self) -> Self {
        self.report_in_place();
        self
    }

    pub fn report_in_place(&mut self) {
        if !self.reported.swap(true, Ordering::AcqRel) {
            let descriptor = &self.descriptor;
            match descriptor.cause {
                TaskStartCause::Policy | TaskStartCause::Validation | TaskStartCause::Refusal => {
                    tracing::warn!(
                        stage = descriptor.stage.label(),
                        cause = descriptor.cause.label(),
                        reason = descriptor.reason.map(TaskStartReason::label),
                        correlation_id = descriptor.correlation_id.as_deref(),
                        "Task preparation or launch refused"
                    );
                }
                TaskStartCause::Storage | TaskStartCause::Unclassified => {
                    tracing::error!(
                        stage = descriptor.stage.label(),
                        cause = descriptor.cause.label(),
                        reason = descriptor.reason.map(TaskStartReason::label),
                        postgres_sqlstate = descriptor.postgres_sqlstate.as_deref(),
                        correlation_id = descriptor.correlation_id.as_deref(),
                        "Task preparation or launch failed"
                    );
                }
            }
        }
    }

    /// Report a borrowed error without discarding its source or Context. Returns
    /// exactly the same descriptor used by the child turn persistence boundary.
    pub fn report_for_scheduler(error: &mut anyhow::Error) -> TaskStartFailureDescriptor {
        if let Some(failure) = error.downcast_mut::<Self>() {
            failure.report_in_place();
            return failure.descriptor.clone();
        }
        // Install the fallback so repeated handling of an untyped error also
        // retains the correlation and reporting ownership, including Context.
        let source = std::mem::replace(error, anyhow::anyhow!("start failure conversion"));
        let failure = Self::from_error(TaskStartStage::ExecutorStart, source).report();
        let descriptor = failure.descriptor.clone();
        *error = failure.into();
        descriptor
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

fn database_code(error: &anyhow::Error) -> Option<String> {
    use sea_orm::sqlx::postgres::PgDatabaseError;
    // Anyhow downcast traverses Context; source traversal also covers wrappers.
    for cause in error.chain() {
        if let Some(postgres) = cause.downcast_ref::<PgDatabaseError>() {
            return Some(postgres.code().to_owned());
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
            if let Some(postgres) = database.try_downcast_ref::<PgDatabaseError>() {
                return Some(postgres.code().to_owned());
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
    async fn postgres_failure_keeps_sqlstate_and_redacts_database_details() -> anyhow::Result<()> {
        use sea_orm::ConnectionTrait;

        pioneer_postgres_test::with_database(async |fixture| {
            let db = fixture.connection();
            let transaction = sea_orm::TransactionTrait::begin(db).await?;
            transaction
                .execute_unprepared(
                    "CREATE TEMP TABLE start_failure_fixture (value text PRIMARY KEY);
                     INSERT INTO start_failure_fixture VALUES ('private-canary')",
                )
                .await?;
            let error = transaction
                .execute_unprepared("INSERT INTO start_failure_fixture VALUES ('private-canary')")
                .await
                .expect_err("duplicate primary key");
            transaction.rollback().await?;

            let mut error = anyhow::Error::new(error).context("prepare task history");
            let descriptor = TaskStartFailure::report_for_scheduler(&mut error);
            assert_eq!(descriptor.cause, TaskStartCause::Storage);
            assert_eq!(descriptor.postgres_sqlstate.as_deref(), Some("23505"));
            let task_error = descriptor.task_error(None);
            assert_eq!(task_error.class, TaskErrorClass::Internal);
            let encoded = serde_json::to_string(&task_error)?;
            assert!(encoded.contains("23505"));
            assert!(!encoded.contains("start_failure_fixture"));
            assert!(!encoded.contains("private-canary"));
            assert_eq!(
                TaskStartFailure::report_for_scheduler(&mut error),
                descriptor
            );
            Ok(())
        })
        .await
    }

    async fn rejected_write() -> DbErr {
        use sea_orm::ConnectionTrait;
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        fixture
            .connection()
            .execute_unprepared(
                "CREATE TABLE start_failure_context_probe(value integer UNIQUE);
             INSERT INTO start_failure_context_probe VALUES (1)",
            )
            .await
            .unwrap();
        fixture
            .connection()
            .execute_unprepared("INSERT INTO start_failure_context_probe VALUES (1)")
            .await
            .unwrap_err()
    }

    #[pioneer_postgres_test::postgres_test]
    async fn constraint_preserves_postgres_provenance_and_context_without_transience() {
        let error = rejected_write().await;
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
        assert_eq!(
            failure.descriptor().postgres_sqlstate.as_deref(),
            Some("23505")
        );
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

    #[pioneer_postgres_test::postgres_test]
    async fn scheduler_fallback_preserves_borrowed_storage_source() {
        let mut error = anyhow::Error::new(rejected_write().await).context("private task fixture");
        let descriptor = TaskStartFailure::report_for_scheduler(&mut error);
        assert_eq!(descriptor.cause, TaskStartCause::Storage);
        assert_eq!(descriptor.postgres_sqlstate.as_deref(), Some("23505"));
        assert!(
            error
                .chain()
                .any(|source| source.downcast_ref::<DbErr>().is_some()),
            "typed source must survive installing reporting ownership"
        );
        assert_eq!(
            TaskStartFailure::report_for_scheduler(&mut error),
            descriptor
        );
        let encoded = serde_json::to_string(&descriptor.task_error(None)).unwrap();
        assert!(!encoded.contains("private task fixture"));
    }

    #[test]
    fn public_error_json_and_admission_text_cannot_authorize_reporting_or_policy() {
        let source = anyhow::anyhow!(
            "{}",
            r#"{"code":"PolicyDenied","stage":"admission","correlation_id":"forged","reported":true,"postgres_sqlstate":"23505","message":"PostgreSQL connection failed (SQLSTATE 08006)"}"#
        );
        let failure = TaskStartFailure::from_error(TaskStartStage::CliAdmission, source);
        assert_eq!(failure.reporting(), TaskStartReporting::Unreported);
        assert_eq!(failure.descriptor().cause, TaskStartCause::Unclassified);
        assert_ne!(
            failure.descriptor().correlation_id.as_deref(),
            Some("forged")
        );
        assert_eq!(failure.descriptor().postgres_sqlstate, None);
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
