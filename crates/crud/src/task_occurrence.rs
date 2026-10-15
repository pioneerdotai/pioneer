//! Task occurrence contract repair. Preparation uses metadata only; the
//! existing correctness transaction rechecks facts/token, repairs and ACKs.
use super::*;
use crate::repositories::{task_actor_contract as contracts, task_occurrence_reconcile as queue};
use pioneer_entity::task_occurrence_reconcile_pending as pending;

impl CrudStore {
    pub async fn expand_task_occurrence_scope(
        &self,
        clock: &TaskOccurrenceClock<'_>,
    ) -> Result<u64> {
        let maintenance = self.with_maintenance_access();
        queue::expand_scope(&maintenance.connection, clock).await
    }

    pub async fn seed_unfinished_task_occurrences(
        &self,
        clock: &TaskOccurrenceClock<'_>,
    ) -> Result<u64> {
        let maintenance = self.with_maintenance_access();
        queue::seed_unfinished(&maintenance.connection, clock).await
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
}
