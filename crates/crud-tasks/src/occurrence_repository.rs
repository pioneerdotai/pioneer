//! Scoped Task occurrence repair, including claim admission and exact ACK.
use crate::task_actor_contract::{
    self as contracts, TerminalTaskOccurrenceMismatch, TerminalTaskOccurrenceMismatchPage,
};
use crate::task_occurrence_reconcile as queue;
use crate::task_occurrence_reconcile::{
    TaskOccurrenceClaimDeferral, TaskOccurrenceClaimFailure, TaskOccurrenceClock,
    TaskOccurrenceReconcileCandidate, TaskOccurrenceReconcileClaim,
};
use crate::values::unix_to_datetime;
use anyhow::{Context, Result};
use pioneer_entity::task_occurrence_reconcile_pending as pending;
use pioneer_protocol::generate_id;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    retry_with_backoff,
};
use sea_orm::{EntityTrait, TransactionTrait};
use std::{future::Future, time::Duration};

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskOccurrenceTerminalRepairOutcome {
    Changed,
    AlreadyConsistent,
    NotFound,
    NotRepairable,
    StaleClaim,
}

#[derive(Clone)]
pub struct TaskOccurrenceRepository {
    connection: SqliteDatabase,
}

impl TaskOccurrenceRepository {
    pub fn new(connection: SqliteDatabase) -> Self {
        Self { connection }
    }

    fn with_maintenance_access(&self) -> Self {
        Self::new(self.connection.maintenance())
    }

    async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            crate::retry::is_retryable,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
}

impl TaskOccurrenceRepository {
    pub async fn expand_task_occurrence_scope(
        &self,
        clock: &TaskOccurrenceClock<'_>,
    ) -> Result<u64> {
        let maintenance = self.with_maintenance_access();
        queue::expand_scope(&maintenance.connection, clock).await
    }

    pub async fn discover_task_occurrence_reconcile(
        &self,
        now: i64,
    ) -> Result<Vec<TaskOccurrenceReconcileCandidate>> {
        let maintenance = self.with_maintenance_access();
        queue::discover(
            &maintenance.connection,
            now,
            queue::TASK_OCCURRENCE_RUN_QUOTA,
        )
        .await
    }

    pub async fn has_pending_task_occurrence_reconcile(&self) -> Result<bool> {
        let maintenance = self.with_maintenance_access();
        queue::has_pending(&maintenance.connection).await
    }

    pub async fn claim_task_occurrence_reconcile(
        &self,
        candidate: &TaskOccurrenceReconcileCandidate,
        clock: &TaskOccurrenceClock<'_>,
    ) -> Result<Option<TaskOccurrenceReconcileClaim>> {
        let token = generate_id(DB_ID_LEN);
        let maintenance = self.with_maintenance_access();
        // An ambiguous claim commit never gets an operation-wide retry.
        match queue::claim(&maintenance.connection, candidate, token, clock).await {
            Ok(claim) => Ok(claim),
            Err(failure) => {
                let (deferral, deferral_error) = match failure.snapshot.as_ref() {
                    Some(snapshot) => {
                        match queue::defer_failed_claim(&maintenance.connection, snapshot, clock)
                            .await
                        {
                            Ok(outcome) => (outcome, None),
                            Err(error) => (TaskOccurrenceClaimDeferral::Failed, Some(error)),
                        }
                    }
                    None => (TaskOccurrenceClaimDeferral::NoSnapshot, None),
                };
                Err(TaskOccurrenceClaimFailure {
                    phase: failure.phase,
                    deferral,
                    error: failure.error,
                    deferral_error,
                }
                .into())
            }
        }
    }

    pub async fn reconcile_claimed_task_occurrence(
        &self,
        claim: &TaskOccurrenceReconcileClaim,
        now: i64,
    ) -> Result<TaskOccurrenceTerminalRepairOutcome> {
        let repair = self.with_maintenance_access();
        if !queue::owns(&repair.connection, claim).await? {
            return Ok(TaskOccurrenceTerminalRepairOutcome::StaleClaim);
        }
        let prepared =
            match contracts::load_terminal_occurrence_metadata(&repair.connection, &claim.run_id)
                .await
            {
                Ok(facts) => facts,
                Err(error) => {
                    if !queue::owns(&repair.connection, claim).await? {
                        return Ok(TaskOccurrenceTerminalRepairOutcome::StaleClaim);
                    }
                    return Err(error);
                }
            };
        // No decode/serialization or other preparation under writer capacity.
        // This commit is not retried if its durable outcome is unknown.
        repair
            .commit_terminal_task_occurrence(&claim.run_id, now, Some((claim, &prepared)))
            .await
    }

    /// Explicit repair preserves the diagnostic API. It has no discovery or
    /// fallback path, and revalidates the same literal authority predicate.
    pub async fn compare_and_repair_terminal_task_occurrence(
        &self,
        run_id: &str,
        now: i64,
    ) -> Result<TaskOccurrenceTerminalRepairOutcome> {
        self.run_serialized_write(|| self.commit_terminal_task_occurrence(run_id, now, None))
            .await
    }

    pub(crate) async fn commit_terminal_task_occurrence(
        &self,
        run_id: &str,
        now: i64,
        prepared: Option<(
            &TaskOccurrenceReconcileClaim,
            &contracts::TerminalOccurrenceMetadata,
        )>,
    ) -> Result<TaskOccurrenceTerminalRepairOutcome> {
        let change = crate::work_change::TaskWorkChange::for_source(&self.connection, "r", run_id);
        let result = async {
            let tx = self.connection.begin().await?;
            let result = async {
                if let Some((claim, _)) = prepared
                    && !queue::owns(&tx, claim).await?
                {
                    return Ok(TaskOccurrenceTerminalRepairOutcome::StaleClaim);
                }
                let current = contracts::load_terminal_occurrence_metadata(&tx, run_id).await?;
                if let Some((_, facts)) = prepared
                    && facts != &current
                {
                    // Task scope refresh may not yet have expanded to this run.
                    // Compare values, including bindings, rather than timestamps.
                    return Ok(TaskOccurrenceTerminalRepairOutcome::StaleClaim);
                }
                let outcome = match current.expected_status() {
                    Some(status) => {
                        let occurrence = current
                            .occurrence
                            .as_ref()
                            .expect("predicate requires occurrence");
                        if occurrence.status == contracts::task_occurrence_status_to_db(&status) {
                            TaskOccurrenceTerminalRepairOutcome::AlreadyConsistent
                        } else if contracts::repair_terminal_task_occurrence_status(
                            &tx,
                            occurrence,
                            status,
                            now.max(occurrence.updated_at.timestamp()),
                        )
                        .await?
                        {
                            TaskOccurrenceTerminalRepairOutcome::Changed
                        } else {
                            // M=true failure keeps the claimed, delayed obligation.
                            return Ok(TaskOccurrenceTerminalRepairOutcome::NotRepairable);
                        }
                    }
                    None if current.run.is_none() => TaskOccurrenceTerminalRepairOutcome::NotFound,
                    None if current.execution.is_none() => {
                        TaskOccurrenceTerminalRepairOutcome::NotRepairable
                    }
                    None if current.occurrence.is_none() => {
                        TaskOccurrenceTerminalRepairOutcome::NotFound
                    }
                    None => TaskOccurrenceTerminalRepairOutcome::NotRepairable,
                };
                if prepared.is_some() {
                    // Our own occurrence UPDATE refreshes generation/clears token.
                    // Recheck consistency and ACK the CURRENT snapshot atomically.
                    let consistent = if outcome == TaskOccurrenceTerminalRepairOutcome::Changed {
                        !contracts::load_terminal_occurrence_metadata(&tx, run_id)
                            .await?
                            .is_mismatch()
                    } else {
                        !current.is_mismatch()
                    };
                    if consistent {
                        if let Some(row) = pending::Entity::find_by_id(run_id.to_owned())
                            .one(&tx)
                            .await?
                        {
                            queue::acknowledge(&tx, &row).await?;
                        }
                    }
                }
                Ok(outcome)
            }
            .await;
            match result {
                Ok(outcome) => {
                    tx.commit().await?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    Err(error)
                }
            }
        }
        .await;
        if let Ok(value) = &result {
            change.completed(matches!(
                value,
                TaskOccurrenceTerminalRepairOutcome::Changed
            ));
        }
        result
    }

    pub async fn upsert_task_occurrence_contract(
        &self,
        contract: &pioneer_protocol::TaskOccurrenceContract,
        now: i64,
    ) -> Result<()> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [contract.task_id.clone()],
        );
        let before = self
            .get_task_occurrence_contract(&contract.occurrence_id)
            .await?;
        let result = async {
            self.run_serialized_write(|| async {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin Task occurrence contract transition")?;
                contracts::upsert_task_occurrence_contract(&transaction, contract, now).await?;
                if matches!(
                    contract.status,
                    pioneer_protocol::TaskOccurrenceStatus::Delivered
                        | pioneer_protocol::TaskOccurrenceStatus::Failed
                        | pioneer_protocol::TaskOccurrenceStatus::Cancelled
                ) && let Some(root_id) = contract.work_graph_root_execution_id.as_deref()
                {
                    pioneer_crud_execution::agent_execution::close_agent_root_scope_if_drained(
                        &transaction,
                        root_id,
                        unix_to_datetime(now),
                    )
                    .await?;
                }
                transaction
                    .commit()
                    .await
                    .context("failed to commit Task occurrence contract transition")
            })
            .await
        }
        .await;
        if result.is_ok() {
            let after = self
                .get_task_occurrence_contract(&contract.occurrence_id)
                .await?;
            change.completed(before != after);
        }
        result
    }

    pub async fn get_task_occurrence_contract(
        &self,
        occurrence_id: &str,
    ) -> Result<Option<pioneer_protocol::TaskOccurrenceContract>> {
        contracts::find_task_occurrence_contract(&self.connection, occurrence_id).await
    }

    pub async fn get_task_occurrence_contract_by_run(
        &self,
        run_id: &str,
    ) -> Result<Option<pioneer_protocol::TaskOccurrenceContract>> {
        contracts::find_task_occurrence_by_run_id(&self.connection, run_id).await
    }

    pub async fn list_terminal_task_occurrence_mismatches(
        &self,
        limit: u64,
    ) -> Result<Vec<TerminalTaskOccurrenceMismatch>> {
        contracts::list_terminal_task_occurrence_mismatches(&self.connection, limit).await
    }

    pub async fn scan_terminal_task_occurrence_mismatches(
        &self,
        after_occurrence_id: Option<&str>,
        limit: u64,
    ) -> Result<TerminalTaskOccurrenceMismatchPage> {
        contracts::scan_terminal_task_occurrence_mismatches(
            &self.connection,
            after_occurrence_id,
            limit,
        )
        .await
    }
}

#[cfg(test)]
#[path = "occurrence_repository_tests.rs"]
mod tests;
