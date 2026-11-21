//! Task occurrence contract repair. Preparation uses metadata only; the
//! existing correctness transaction rechecks facts/token, repairs and ACKs.
use crate::{
    CrudStore, TaskOccurrenceClock, TaskOccurrenceReconcileCandidate, TaskOccurrenceReconcileClaim,
    TaskOccurrenceTerminalRepairOutcome,
};
use anyhow::Result;
use pioneer_crud_tasks::occurrence_repository::TaskOccurrenceRepository;

impl CrudStore {
    pub(crate) fn task_occurrence_repository(&self) -> TaskOccurrenceRepository {
        TaskOccurrenceRepository::new(self.connection.clone())
    }
}

impl CrudStore {
    pub async fn expand_task_occurrence_scope(
        &self,
        clock: &TaskOccurrenceClock<'_>,
    ) -> Result<u64> {
        self.task_occurrence_repository()
            .expand_task_occurrence_scope(clock)
            .await
    }

    pub async fn discover_task_occurrence_reconcile(
        &self,
        now: i64,
    ) -> Result<Vec<TaskOccurrenceReconcileCandidate>> {
        self.task_occurrence_repository()
            .discover_task_occurrence_reconcile(now)
            .await
    }

    pub async fn has_pending_task_occurrence_reconcile(&self) -> Result<bool> {
        self.task_occurrence_repository()
            .has_pending_task_occurrence_reconcile()
            .await
    }

    pub async fn claim_task_occurrence_reconcile(
        &self,
        candidate: &TaskOccurrenceReconcileCandidate,
        clock: &TaskOccurrenceClock<'_>,
    ) -> Result<Option<TaskOccurrenceReconcileClaim>> {
        self.task_occurrence_repository()
            .claim_task_occurrence_reconcile(candidate, clock)
            .await
    }

    pub async fn reconcile_claimed_task_occurrence(
        &self,
        claim: &TaskOccurrenceReconcileClaim,
        now: i64,
    ) -> Result<TaskOccurrenceTerminalRepairOutcome> {
        self.task_occurrence_repository()
            .reconcile_claimed_task_occurrence(claim, now)
            .await
    }

    /// Explicit repair preserves the diagnostic API. It has no discovery or
    /// fallback path, and revalidates the same literal authority predicate.
    pub async fn compare_and_repair_terminal_task_occurrence(
        &self,
        run_id: &str,
        now: i64,
    ) -> Result<TaskOccurrenceTerminalRepairOutcome> {
        self.task_occurrence_repository()
            .compare_and_repair_terminal_task_occurrence(run_id, now)
            .await
    }
}

impl CrudStore {
    pub async fn next_task_occurrence_reconcile_deadline(
        &self,
        now: i64,
    ) -> anyhow::Result<Option<i64>> {
        pioneer_crud_tasks::task_occurrence_reconcile::next_deadline(&self.connection, now).await
    }
}
