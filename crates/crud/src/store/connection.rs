use crate::ProjectionReceiptCleanupOutcome;
use crate::store::events::turn_projector::TurnProjector;
use anyhow::{Context, Result, bail};
use pioneer_crud_access::policy;
use pioneer_crud_compaction::frozen_lifetime;
use pioneer_crud_conversations::projection_receipts;
use pioneer_crud_conversations::turn_finalization;
use pioneer_crud_execution::execution_heartbeat;
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    PostgresReadClass, PostgresWriteClass, retry_with_backoff,
};
use pioneer_protocol::Turn;
use sea_orm::{ConnectionTrait, Statement};
use std::future::Future;
use std::time::Duration;

#[derive(Clone)]
pub struct CrudStore {
    pub(crate) recovery_work: std::sync::Arc<tokio::sync::Notify>,
    pub(crate) execution_heartbeats: execution_heartbeat::Registries,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) delivery_commit_test_hooks:
        pioneer_crud_tasks::delivery_transitions::DeliveryCommitTestHooks,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) episodic_claim_test_state: pioneer_crud_memory::EpisodicClaimTestState,
    pub(crate) finalization_cursor:
        std::sync::Arc<tokio::sync::Mutex<turn_finalization::PreparedPass>>,
    pub(crate) episodic_work: std::sync::Arc<tokio::sync::Notify>,
    pub(crate) html_preview_io_work: std::sync::Arc<tokio::sync::Notify>,
    pub(crate) frozen_readers: frozen_lifetime::ReaderCounts,
    pub(crate) connection: PostgresDatabase,
    pub(crate) turn_projector: TurnProjector,
}

impl CrudStore {
    pub fn new(connection: impl Into<PostgresDatabase>) -> Self {
        Self {
            execution_heartbeats: Default::default(),
            recovery_work: Default::default(),
            connection: connection.into(),
            finalization_cursor: Default::default(),
            episodic_work: std::sync::Arc::new(tokio::sync::Notify::new()),
            html_preview_io_work: Default::default(),
            frozen_readers: Default::default(),
            #[cfg(any(test, feature = "test-support"))]
            episodic_claim_test_state: Default::default(),
            #[cfg(any(test, feature = "test-support"))]
            delivery_commit_test_hooks: Default::default(),
            turn_projector: TurnProjector::new(),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn make_next_episodic_claim_commit_ambiguous_for_test(&self) {
        self.episodic_claim_test_state
            .make_next_episodic_claim_commit_ambiguous_for_test()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn inject_episodic_claim_fault_for_test(&self, job_id: &str, point: &'static str) {
        self.episodic_claim_test_state
            .inject_episodic_claim_fault_for_test(job_id, point)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn episodic_claim_discoveries_for_test(&self) -> u64 {
        self.episodic_claim_test_state
            .episodic_claim_discoveries_for_test()
    }

    /// Coalesced post-commit wake shared by scoped handles. Durable jobs remain
    /// the recovery obligation; this notification only advances a live runtime.
    pub fn thread_episodic_work_notification(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.episodic_work.clone()
    }

    /// Returns a store whose reads and writes are both classified as
    /// maintenance. Keeping both contours on one scoped handle prevents a
    /// background worker from limiting its writes while bypassing the read
    /// limiter.
    pub fn with_maintenance_access(&self) -> Self {
        let mut maintenance = self.clone();
        maintenance.connection = maintenance.connection.maintenance();
        maintenance
    }

    /// Returns a background control-plane store: discovery reads remain
    /// bounded by the maintenance limiter while authority-preserving writes
    /// retain critical writer priority.
    pub fn with_maintenance_reads_and_critical_writes(&self) -> Self {
        let mut scoped = self.clone();
        scoped.connection = scoped.connection.maintenance().with_critical_writes();
        scoped
    }

    pub fn with_critical_writes(&self) -> Self {
        let mut critical = self.clone();
        critical.connection = critical.connection.with_critical_writes();
        critical
    }

    pub fn with_interactive_writes(&self) -> Self {
        let mut interactive = self.clone();
        interactive.connection = interactive.connection.with_interactive_writes();
        interactive
    }

    /// Installer inspection only: never create the migration table or run
    /// migrations. A missing/unreadable history is an unknown compatibility.
    // Migration bookkeeping is excluded from Entity generation.
    pub async fn inspect_applied_migration_names(&self) -> Result<Vec<String>> {
        let rows = self
            .connection
            .query_all_raw(Statement::from_string(
                self.connection.get_database_backend(),
                "SELECT version FROM seaql_migrations ORDER BY version LIMIT 4097",
            ))
            .await
            .context("failed to inspect applied migration history")?;
        anyhow::ensure!(
            rows.len() <= 4096,
            "migration history exceeds inspection bound"
        );
        rows.into_iter()
            .map(|row| row.try_get("", "version").map_err(Into::into))
            .collect()
    }

    pub fn database_connection(&self) -> PostgresDatabase {
        self.connection.clone()
    }

    /// Runs one bounded maintenance database quantum. Each mutation performed
    /// by the operation obtains its own writer-executor reservation (or keeps
    /// one for the lifetime of its transaction); a pure read must never
    /// reserve the writer. This wrapper only preserves the whole-quantum lock
    /// retry policy.
    pub async fn run_background_database_quantum<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if self.connection.read_class() != PostgresReadClass::Maintenance
            || self.connection.write_class() != PostgresWriteClass::Maintenance
        {
            bail!("background database quantum requires maintenance-scoped reads and writes");
        }
        self.run_scoped_database_quantum(operation).await
    }

    /// Retains the caller's scheduling scope while applying the same bounded
    /// PostgreSQL conflict retry policy used by background database quanta.
    pub(crate) async fn run_scoped_database_quantum<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            || operation(),
            pioneer_crud_tasks::retry::is_retryable,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    /// Capture a finite pass so a continuous suffix cannot hide changed old
    /// streams from the next pass. No receipt payload is loaded here.
    pub async fn projection_receipt_cleanup_upper_bound(&self) -> Result<Option<String>> {
        projection_receipts::ProjectionReceiptRepository::new(&self.connection)
            .projection_receipt_cleanup_upper_bound()
            .await
    }

    /// Performs at most one bounded receipt cleanup batch for one stream.
    /// Discovery uses maintenance readers. Revalidation and deletion share a
    /// short write transaction. A persisted per-stream boundary makes restart safe.
    pub async fn cleanup_projection_receipts_quantum(
        &self,
        after_turn_id: Option<&str>,
        through_turn_id: Option<&str>,
    ) -> Result<ProjectionReceiptCleanupOutcome> {
        projection_receipts::ProjectionReceiptRepository::new(&self.connection)
            .cleanup_projection_receipts_quantum(after_turn_id, through_turn_id)
            .await
    }

    /// Drain one notified Turn without inventorying other streams. The sequence
    /// frontier is carried by the worker across bounded, fair batch portions.
    pub async fn cleanup_projection_receipts_for_turn(
        &self,
        turn_id: &str,
        through_sequence: Option<i64>,
    ) -> Result<ProjectionReceiptCleanupOutcome> {
        projection_receipts::ProjectionReceiptRepository::new(&self.connection)
            .cleanup_projection_receipts_for_turn(turn_id, through_sequence)
            .await
    }

    pub(crate) async fn retry_write_on_conflict<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        // Retry the complete operation after PostgreSQL aborts its transaction.
        // Each attempt acquires database capacity inside the write boundary.
        let mut operation = operation;
        retry_with_backoff(
            || operation(),
            |error| {
                pioneer_crud_tasks::retry::is_retryable(error)
                    || pioneer_crud_artifacts::html_preview_io::is_conflict(error)
            },
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
}

pub(crate) async fn rollback_preserving_error(
    transaction: pioneer_postgres_pools::PostgresTransaction,
    error: anyhow::Error,
) -> anyhow::Error {
    let _ = transaction.rollback().await;
    error
}
