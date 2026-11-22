//! Canonical occurrence repair shared by Task-event fanout and the durable
//! background tracker. Preparation owns no writer capacity; commit fences the
//! claim and every source fact before entering the existing event projector.
use crate::store::events::event_preparation::{
    PreparedProjectedTurnEvent, TURN_EVENT_PROJECTION_LEASE_SECS,
    prepare_projected_turn_event_for_permanent_storage,
};
use crate::{
    CrudStore, TaskRunOccurrenceClock, TaskRunOccurrenceReconcileCandidate,
    TaskRunOccurrenceReconcileClaim, TaskRunOccurrenceTerminalizationOutcome,
};
use anyhow::{Context, Result};
use pioneer_crud_execution::time::unix_to_datetime;
use pioneer_crud_tasks::occurrence_reconcile as queue;
use pioneer_crud_tasks::run_occurrence::{
    OccurrencePreparation, OccurrenceRepairPreparation, OccurrenceSourceFacts,
    TaskRunOccurrenceQueue,
};
pub use pioneer_crud_tasks::run_occurrence::{
    TaskRunOccurrenceRepairPhase, TaskRunOccurrenceRepairReason,
};
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

pub(crate) type Preparation = OccurrenceRepairPreparation<Box<PreparedOccurrence>>;

#[derive(Clone)]
pub(crate) struct PreparedOccurrence {
    facts: Box<OccurrenceSourceFacts>,
    terminal_event: PreparedProjectedTurnEvent,
    created_at: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
}

impl CrudStore {
    pub async fn discover_task_run_occurrence_reconcile(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskRunOccurrenceReconcileCandidate>> {
        TaskRunOccurrenceQueue::new(&self.connection)
            .discover_task_run_occurrence_reconcile(now, limit)
            .await
    }

    pub async fn claim_task_run_occurrence_reconcile(
        &self,
        candidate: &TaskRunOccurrenceReconcileCandidate,
        clock: &TaskRunOccurrenceClock<'_>,
    ) -> Result<Option<TaskRunOccurrenceReconcileClaim>> {
        TaskRunOccurrenceQueue::new(&self.connection)
            .claim_task_run_occurrence_reconcile(candidate, clock)
            .await
    }

    /// A bounded service-table observation, including rows still in backoff.
    /// False describes this read's snapshot, not all future source writes.
    pub async fn has_pending_task_run_occurrence_reconcile(&self) -> Result<bool> {
        TaskRunOccurrenceQueue::new(&self.connection)
            .has_pending_task_run_occurrence_reconcile()
            .await
    }

    pub async fn reconcile_claimed_task_run_occurrence(
        &self,
        claim: &TaskRunOccurrenceReconcileClaim,
        now: i64,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
        let repair = self.with_maintenance_reads_and_critical_writes();
        TaskRunOccurrenceQueue::new(&repair.connection)
            .reconcile_claim(
                claim,
                || repair.prepare_task_run_occurrence(claim.run_id.clone(), now, true),
                |prepared| {
                    // Retry only this repair with the same preparation and token.
                    let repair = &repair;
                    async move {
                        repair
                            .retry_write_on_conflict(|| {
                                repair.commit_task_run_occurrence(
                                    *prepared.clone(),
                                    Some(claim.clone()),
                                )
                            })
                            .await
                    }
                },
            )
            .await
    }

    /// Task-event fanout and background repair use the same preparation/commit.
    pub async fn compare_and_materialize_task_run_occurrence_terminal(
        &self,
        run_id: &str,
        fallback_completed_at: i64,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
        self.retry_write_on_conflict(|| async {
            match self
                .prepare_task_run_occurrence(run_id.to_owned(), fallback_completed_at, false)
                .await?
            {
                Preparation::Unchanged(outcome) => Ok(outcome),
                Preparation::Ready(prepared) => {
                    self.commit_task_run_occurrence(*prepared, None).await
                }
            }
        })
        .await
    }

    pub(crate) async fn prepare_task_run_occurrence(
        &self,
        run_id: String,
        fallback_completed_at: i64,
        background: bool,
    ) -> Result<Preparation> {
        let (facts, event, completed_at) = match OccurrenceSourceFacts::prepare(
            &self.connection,
            run_id,
            fallback_completed_at,
            background,
        )
        .await?
        {
            OccurrencePreparation::Ready {
                facts,
                event,
                completed_at,
            } => (facts, event, completed_at),
            OccurrencePreparation::Unchanged(outcome) => {
                return Ok(Preparation::Unchanged(outcome));
            }
        };
        let created_at = unix_to_datetime(completed_at);
        let claim_expires_at =
            unix_to_datetime(completed_at.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
        let terminal_event =
            prepare_projected_turn_event_for_permanent_storage(&self.connection, event, created_at)
                .await
                .context(TaskRunOccurrenceRepairReason::TerminalEventPreparation)?;
        Ok(Preparation::Ready(Box::new(PreparedOccurrence {
            facts,
            terminal_event,
            created_at,
            claim_expires_at,
        })))
    }

    pub(crate) async fn commit_task_run_occurrence(
        &self,
        prepared: PreparedOccurrence,
        claim: Option<TaskRunOccurrenceReconcileClaim>,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_source(
            &self.connection,
            "r",
            &prepared.facts.run_id,
        );
        let result = async {
            let PreparedOccurrence {
                facts,
                terminal_event,
                created_at,
                claim_expires_at,
            } = prepared;
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin TaskRun occurrence terminalization transaction")?;

            let result = async {
                if let Some(outcome) = facts.revalidate(&transaction, claim.as_ref()).await? {
                    return Ok(outcome);
                }
                self.append_and_project_turn_event_in_transaction(
                    &transaction,
                    terminal_event,
                    created_at,
                    claim_expires_at,
                    false,
                )
                .await?;

                // Unknown legacy producers may not retire the queue row when the
                // projection becomes consistent. ACK this exact claim in the same
                // transaction; a superseding generation/token remains protected.
                if let Some(claim) = claim.as_ref() {
                    queue::remove_if_consistent(&transaction, claim).await?;
                }
                Ok(TaskRunOccurrenceTerminalizationOutcome::Changed)
            }
            .await;

            match result {
                Ok(outcome) => {
                    transaction.commit().await.context(
                        "failed to commit TaskRun occurrence terminalization transaction",
                    )?;
                    Ok(outcome)
                }
                Err(error) => Err(crate::store::connection::rollback_preserving_error(
                    transaction,
                    error,
                )
                .await),
            }
        }
        .await;
        if let Ok(value) = &result {
            change.completed(matches!(
                value,
                TaskRunOccurrenceTerminalizationOutcome::Changed
            ));
        }
        result
    }
}

impl CrudStore {
    /// Call after the existing Gateway child/parent/delivery bounded pass.
    /// Errors remain errors; a saturated quantum can additionally continue
    /// immediately before consulting these durable deadlines.
    pub async fn next_task_lifecycle_deadline(&self, now: i64) -> anyhow::Result<Option<i64>> {
        let child = self.next_task_child_work_deadline().await?;
        let parent =
            pioneer_crud_tasks::occurrence_reconcile::next_deadline(&self.connection).await?;
        let delivery =
            pioneer_crud_tasks::delivery::next_work_deadline(&self.connection, now, 300).await?;
        Ok(child.into_iter().chain(parent).chain(delivery).min())
    }
}
