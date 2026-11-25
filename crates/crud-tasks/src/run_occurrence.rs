//! Claim admission and source-fact validation for TaskRun occurrence repairs.
use crate::occurrence_reconcile::{
    self as queue, TaskRunOccurrenceClaimDeferral, TaskRunOccurrenceClaimFailure,
    TaskRunOccurrenceClock, TaskRunOccurrenceReconcileCandidate, TaskRunOccurrenceReconcileClaim,
};
use crate::{status::task_run_status_from_db, task_run, values::optional_typed_json_from_db};
use anyhow::{Context, Result, bail};
use pioneer_crud_access::turn_execution::{turn_kind_from_db, turn_status_from_db};
use pioneer_crud_conversations::{
    events::TurnEventPayload, protocol_projection::turn_from_db_model, thread_metadata as thread,
    turn_reads as turn,
};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{TaskRunStatus, TurnKind, TurnStatus, generate_id};
use sea_orm::{DatabaseTransaction, TransactionTrait};
use std::{fmt, future::Future};

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone, Copy)]
pub enum TaskRunOccurrenceRepairPhase {
    Ownership,
    Preparation,
    Commit,
    QueueCleanup,
}
impl TaskRunOccurrenceRepairPhase {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ownership => "repair_ownership",
            Self::Preparation => "repair_preparation",
            Self::Commit => "repair_commit",
            Self::QueueCleanup => "repair_queue_cleanup",
        }
    }
}
impl fmt::Display for TaskRunOccurrenceRepairPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TaskRunOccurrenceRepairReason {
    RunStatus,
    RunErrorDecode,
    TurnStatus,
    TurnDecode,
    TerminalEventPreparation,
}
impl TaskRunOccurrenceRepairReason {
    pub const fn label(self) -> &'static str {
        match self {
            Self::RunStatus => "run_status_invalid",
            Self::RunErrorDecode => "run_error_decode",
            Self::TurnStatus => "turn_status_invalid",
            Self::TurnDecode => "turn_decode",
            Self::TerminalEventPreparation => "terminal_event_preparation",
        }
    }
}
impl fmt::Display for TaskRunOccurrenceRepairReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Result of reconciling the canonical TaskRun occurrence Turn with its
/// authoritative TaskRun aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskRunOccurrenceTerminalizationOutcome {
    /// The preparation or durable generation/token was superseded. Normal deferral.
    StaleClaim,
    Changed,
    AlreadyConsistent,
    NotFound,
    InvalidBinding,
}

pub enum OccurrenceRepairPreparation<P> {
    Unchanged(TaskRunOccurrenceTerminalizationOutcome),
    Ready(P),
}

pub struct TaskRunOccurrenceQueue<'a> {
    connection: &'a PostgresDatabase,
}

impl<'a> TaskRunOccurrenceQueue<'a> {
    pub fn new(connection: &'a PostgresDatabase) -> Self {
        Self { connection }
    }

    pub async fn discover_task_run_occurrence_reconcile(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskRunOccurrenceReconcileCandidate>> {
        let maintenance = self.connection.maintenance();
        queue::discover(&maintenance, now, limit).await
    }

    pub async fn claim_task_run_occurrence_reconcile(
        &self,
        candidate: &TaskRunOccurrenceReconcileCandidate,
        clock: &TaskRunOccurrenceClock<'_>,
    ) -> Result<Option<TaskRunOccurrenceReconcileClaim>> {
        // Token generation precedes both the reader and the writer acquisition.
        let token = generate_id(DB_ID_LEN);
        let maintenance = self.connection.maintenance();
        // No operation-wide retry: an ambiguous commit must never cause a
        // second reservation. The persisted due time handles later passes.
        match queue::claim(&maintenance, candidate, token, clock).await {
            Ok(claim) => Ok(claim),
            Err(failure) => {
                let (deferral, deferral_error) = match failure.snapshot.as_ref() {
                    Some(snapshot) => {
                        match queue::defer_failed_claim(&maintenance, snapshot, clock).await {
                            Ok(outcome) => (outcome, None),
                            Err(error) => (TaskRunOccurrenceClaimDeferral::Failed, Some(error)),
                        }
                    }
                    None => (TaskRunOccurrenceClaimDeferral::NoSnapshot, None),
                };
                Err(TaskRunOccurrenceClaimFailure {
                    phase: failure.phase,
                    deferral,
                    error: failure.error,
                    deferral_error,
                }
                .into())
            }
        }
    }

    /// Reconcile only the exact claim. The caller owns canonical publication.
    pub async fn reconcile_claim<P, Prepare, Prepared, Commit, Committed>(
        &self,
        claim: &TaskRunOccurrenceReconcileClaim,
        prepare: Prepare,
        commit: Commit,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome>
    where
        Prepare: FnOnce() -> Prepared,
        Prepared: Future<Output = Result<OccurrenceRepairPreparation<P>>>,
        Commit: FnOnce(P) -> Committed,
        Committed: Future<Output = Result<TaskRunOccurrenceTerminalizationOutcome>>,
    {
        if !queue::owns(self.connection, claim)
            .await
            .context(TaskRunOccurrenceRepairPhase::Ownership)?
        {
            return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
        }
        let prepared = match prepare().await {
            Ok(prepared) => prepared,
            Err(error) => {
                // A concurrent source write may invalidate the claim even
                // before decoding finishes. Supersession is a normal outcome,
                // including when the new state is deliberately not repairable.
                if !queue::owns(self.connection, claim)
                    .await
                    .context(TaskRunOccurrenceRepairPhase::Ownership)?
                {
                    return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
                }
                return Err(error.context(TaskRunOccurrenceRepairPhase::Preparation));
            }
        };
        let outcome = match prepared {
            OccurrenceRepairPreparation::Ready(prepared) => commit(prepared)
                .await
                .context(TaskRunOccurrenceRepairPhase::Commit)?,
            OccurrenceRepairPreparation::Unchanged(outcome) => outcome,
        };
        if matches!(
            outcome,
            TaskRunOccurrenceTerminalizationOutcome::Changed
                | TaskRunOccurrenceTerminalizationOutcome::StaleClaim
        ) {
            return Ok(outcome);
        }
        // A no-op has no domain write-set; conditional cleanup is Maintenance.
        let maintenance = self.connection.maintenance();
        let tx = maintenance
            .begin()
            .await
            .context(TaskRunOccurrenceRepairPhase::QueueCleanup)?;
        let current = queue::owns(&tx, claim)
            .await
            .context(TaskRunOccurrenceRepairPhase::QueueCleanup)?;
        if current {
            queue::remove_if_consistent(&tx, claim)
                .await
                .context(TaskRunOccurrenceRepairPhase::QueueCleanup)?;
        }
        tx.commit()
            .await
            .context(TaskRunOccurrenceRepairPhase::QueueCleanup)?;
        Ok(if current {
            outcome
        } else {
            TaskRunOccurrenceTerminalizationOutcome::StaleClaim
        })
    }

    pub async fn has_pending_task_run_occurrence_reconcile(&self) -> Result<bool> {
        let maintenance = self.connection.maintenance();
        queue::has_pending(&maintenance).await
    }
}

pub enum OccurrencePreparation {
    Unchanged(TaskRunOccurrenceTerminalizationOutcome),
    Ready {
        facts: Box<OccurrenceSourceFacts>,
        event: TurnEventPayload,
        completed_at: i64,
    },
}

#[derive(Clone)]
pub struct OccurrenceSourceFacts {
    pub run_id: String,
    prepared_run_model: pioneer_entity::task_run::Model,
    prepared_turn_model: pioneer_entity::turn::Model,
    prepared_thread_model: pioneer_entity::thread::Model,
    desired_status: TurnStatus,
}

impl OccurrenceSourceFacts {
    pub async fn prepare(
        db: &PostgresDatabase,
        run_id: String,
        fallback_completed_at: i64,
        background: bool,
    ) -> Result<OccurrencePreparation> {
        let Some(prepared_run_model) = task_run::find_run_by_id(db, run_id.as_str()).await? else {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        };
        let run_status = task_run_status_from_db(&prepared_run_model.status)
            .context(TaskRunOccurrenceRepairReason::RunStatus)?;
        if queue::has_pending_revision_retry(&prepared_run_model) {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent,
            ));
        }
        let terminal_error = if matches!(
            run_status,
            TaskRunStatus::Failed
                | TaskRunStatus::TimedOut
                | TaskRunStatus::Blocked
                | TaskRunStatus::Cancelled
        ) {
            optional_typed_json_from_db::<pioneer_protocol::TaskError>(
                prepared_run_model.error_json.clone(),
            )
            .context(TaskRunOccurrenceRepairReason::RunErrorDecode)?
        } else {
            None
        };
        let desired_error = terminal_error.map(|error| error.message);
        let Some(desired_status) = queue::terminal_turn_status(run_status) else {
            if background {
                return Ok(OccurrencePreparation::Unchanged(
                    TaskRunOccurrenceTerminalizationOutcome::StaleClaim,
                ));
            }
            bail!("active TaskRun cannot terminalize its occurrence Turn");
        };
        let Some(prepared_turn_model) = turn::find_turn_by_id(db, run_id.as_str()).await? else {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        };
        if turn_kind_from_db(prepared_turn_model.turn_kind.as_str()) != Some(TurnKind::TaskRun) {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::InvalidBinding,
            ));
        }
        let current_status = turn_status_from_db(prepared_turn_model.status.as_str())
            .context(TaskRunOccurrenceRepairReason::TurnStatus)?;
        if current_status == desired_status {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent,
            ));
        }
        let Some(prepared_thread_model) =
            thread::find_thread_by_id(db, prepared_turn_model.thread_id.as_str()).await?
        else {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        };
        if !queue::workspace_exists(db, &prepared_thread_model.workspace_id).await? {
            return Ok(OccurrencePreparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        }
        let Some(mut terminal_turn) = turn_from_db_model(prepared_turn_model.clone())
            .context(TaskRunOccurrenceRepairReason::TurnDecode)?
        else {
            bail!("occurrence Turn `{}` has an unknown status", run_id);
        };
        terminal_turn.status = desired_status;
        terminal_turn.error = desired_error;
        let terminal_event = match desired_status {
            TurnStatus::Completed => {
                TurnEventPayload::TurnCompleted(pioneer_protocol::TurnCompletedNotification {
                    workspace_id: prepared_thread_model.workspace_id.clone(),
                    thread_id: prepared_thread_model.id.clone(),
                    turn: terminal_turn,
                })
            }
            TurnStatus::Failed | TurnStatus::Interrupted => {
                TurnEventPayload::TurnFailed(pioneer_protocol::TurnFailedNotification {
                    workspace_id: prepared_thread_model.workspace_id.clone(),
                    thread_id: prepared_thread_model.id.clone(),
                    turn: terminal_turn,
                })
            }
            TurnStatus::Blocked => {
                TurnEventPayload::TurnBlocked(pioneer_protocol::TurnBlockedNotification {
                    workspace_id: prepared_thread_model.workspace_id.clone(),
                    thread_id: prepared_thread_model.id.clone(),
                    turn: terminal_turn,
                    resume: None,
                })
            }
            TurnStatus::InProgress => unreachable!("TaskRun terminal status mapping"),
        };
        let completed_at = prepared_run_model
            .completed_at
            .map(|value| value.timestamp())
            .unwrap_or(fallback_completed_at);
        Ok(OccurrencePreparation::Ready {
            facts: Box::new(OccurrenceSourceFacts {
                run_id,
                prepared_run_model,
                prepared_turn_model,
                prepared_thread_model,
                desired_status,
            }),
            event: terminal_event,
            completed_at,
        })
    }

    /// A returned outcome settles the repair without a canonical event append.
    pub async fn revalidate(
        &self,
        transaction: &DatabaseTransaction,
        claim: Option<&TaskRunOccurrenceReconcileClaim>,
    ) -> Result<Option<TaskRunOccurrenceTerminalizationOutcome>> {
        let Self {
            run_id,
            prepared_run_model,
            prepared_turn_model,
            prepared_thread_model,
            desired_status,
        } = self;
        if let Some(claim) = claim
            && !queue::owns(transaction, claim).await?
        {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::StaleClaim));
        }
        let Some(run_model) = task_run::find_run_by_id(transaction, run_id.as_str()).await? else {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::NotFound));
        };
        // Complete set of Run facts used by this preparation: identity,
        // terminal mapping, timestamp and serialized error. Heartbeat and
        // updated_at do not fence these facts and need not invalidate them.
        if run_model.id != prepared_run_model.id
            || run_model.status != prepared_run_model.status
            || run_model.completed_at != prepared_run_model.completed_at
            || run_model.error_json != prepared_run_model.error_json
        {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::StaleClaim));
        }

        // The occurrence identity is canonical and does not depend on
        // optional/legacy lineage rows: Turn.id is exactly TaskRun.id.
        let Some(turn_model) = turn::find_turn_by_id(transaction, run_id.as_str()).await? else {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::NotFound));
        };
        if turn_kind_from_db(turn_model.turn_kind.as_str()) != Some(TurnKind::TaskRun) {
            return Ok(Some(
                TaskRunOccurrenceTerminalizationOutcome::InvalidBinding,
            ));
        }
        let current_status = turn_status_from_db(turn_model.status.as_str())
            .with_context(|| format!("occurrence Turn `{run_id}` has an unknown status"))?;
        if current_status == *desired_status {
            return Ok(Some(
                TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent,
            ));
        }
        // The event contains the decoded Turn; compare the entire source
        // model, including all serialized prompt/collaboration/security
        // fields, without repeating deserialization under the writer.
        if turn_model != *prepared_turn_model {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::StaleClaim));
        }
        let thread_model =
            thread::find_thread_by_id(transaction, turn_model.thread_id.as_str()).await?;
        let Some(thread_model) = thread_model else {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::NotFound));
        };
        if thread_model != *prepared_thread_model {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::StaleClaim));
        }
        if !queue::workspace_exists(transaction, &thread_model.workspace_id).await? {
            return Ok(Some(TaskRunOccurrenceTerminalizationOutcome::NotFound));
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "run_occurrence_queue_tests.rs"]
mod tests;
