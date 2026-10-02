//! Canonical occurrence repair shared by Task-event fanout and the durable
//! background tracker. Preparation owns no writer capacity; commit fences the
//! claim and every source fact before entering the existing event projector.
use super::*;
use crate::repositories::task_run_occurrence_reconcile as queue;

pub(crate) enum Preparation {
    Unchanged(TaskRunOccurrenceTerminalizationOutcome),
    Ready(Box<PreparedOccurrence>),
}

#[derive(Clone)]
pub(crate) struct PreparedOccurrence {
    run_id: String,
    prepared_run_model: pioneer_entity::task_run::Model,
    prepared_turn_model: pioneer_entity::turn::Model,
    prepared_thread_model: pioneer_entity::thread::Model,
    desired_status: TurnStatus,
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
        let maintenance = self.with_maintenance_access();
        queue::discover(&maintenance.connection, now, limit).await
    }

    pub async fn claim_task_run_occurrence_reconcile(
        &self,
        candidate: &TaskRunOccurrenceReconcileCandidate,
        clock: &TaskRunOccurrenceClock<'_>,
    ) -> Result<Option<TaskRunOccurrenceReconcileClaim>> {
        // Token generation precedes both the reader and the writer acquisition.
        let token = generate_id(DB_ID_LEN);
        let maintenance = self.with_maintenance_access();
        // No operation-wide retry: an ambiguous commit must never cause a
        // second reservation. The persisted due time handles later passes.
        match queue::claim(&maintenance.connection, candidate, token, clock).await {
            Ok(claim) => Ok(claim),
            Err(failure) => {
                let (deferral, deferral_error) = match failure.snapshot.as_ref() {
                    Some(snapshot) => {
                        match queue::defer_failed_claim(&maintenance.connection, snapshot, clock)
                            .await
                        {
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

    /// A bounded service-table observation, including rows still in backoff.
    /// False describes this read's snapshot, not all future source writes.
    pub async fn has_pending_task_run_occurrence_reconcile(&self) -> Result<bool> {
        let maintenance = self.with_maintenance_access();
        queue::has_pending(&maintenance.connection).await
    }

    pub async fn reconcile_claimed_task_run_occurrence(
        &self,
        claim: &TaskRunOccurrenceReconcileClaim,
        now: i64,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
        let repair = self.with_maintenance_reads_and_critical_writes();
        if !queue::owns(&repair.connection, claim).await? {
            return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
        }
        let prepared = match repair
            .prepare_task_run_occurrence(claim.run_id.clone(), now, true)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                // A concurrent source write may invalidate the claim even
                // before decoding finishes. Supersession is a normal outcome,
                // including when the new state is deliberately not repairable.
                if !queue::owns(&repair.connection, claim).await? {
                    return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
                }
                return Err(error);
            }
        };
        let outcome = match prepared {
            Preparation::Ready(prepared) => {
                // Retry only this single repair with the same preparation/token.
                repair
                    .run_serialized_write(|| {
                        repair.commit_task_run_occurrence(*prepared.clone(), Some(claim.clone()))
                    })
                    .await?
            }
            Preparation::Unchanged(outcome) => outcome,
        };
        if matches!(
            outcome,
            TaskRunOccurrenceTerminalizationOutcome::Changed
                | TaskRunOccurrenceTerminalizationOutcome::StaleClaim
        ) {
            return Ok(outcome);
        }
        // A no-op has no domain write-set; conditional cleanup is Maintenance.
        let maintenance = self.with_maintenance_access();
        let tx = maintenance.connection.begin().await?;
        let current = queue::owns(&tx, claim).await?;
        if current {
            queue::remove_if_consistent(&tx, claim).await?;
        }
        tx.commit().await?;
        Ok(if current {
            outcome
        } else {
            TaskRunOccurrenceTerminalizationOutcome::StaleClaim
        })
    }

    /// Task-event fanout and background repair use the same preparation/commit.
    pub async fn compare_and_materialize_task_run_occurrence_terminal(
        &self,
        run_id: &str,
        fallback_completed_at: i64,
    ) -> Result<TaskRunOccurrenceTerminalizationOutcome> {
        self.run_serialized_write(|| async {
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
        let Some(prepared_run_model) =
            task_run::find_run_by_id(&self.connection, run_id.as_str()).await?
        else {
            return Ok(Preparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        };
        let run_status = task_run_status_from_db(&prepared_run_model.status)
            .context("unknown TaskRun status")?;
        let desired_error = if matches!(
            run_status,
            TaskRunStatus::Failed
                | TaskRunStatus::TimedOut
                | TaskRunStatus::Blocked
                | TaskRunStatus::Cancelled
        ) {
            optional_typed_json_from_db::<pioneer_protocol::TaskError>(
                prepared_run_model.error_json.clone(),
            )?
            .map(|error| error.message)
        } else {
            None
        };
        let (desired_status, desired_error) = match run_status {
            TaskRunStatus::Succeeded => (TurnStatus::Completed, None),
            TaskRunStatus::Failed | TaskRunStatus::TimedOut => {
                (TurnStatus::Failed, desired_error.clone())
            }
            TaskRunStatus::Blocked => (TurnStatus::Blocked, desired_error.clone()),
            TaskRunStatus::Cancelled => (TurnStatus::Interrupted, desired_error.clone()),
            TaskRunStatus::Queued
            | TaskRunStatus::Starting
            | TaskRunStatus::Running
            | TaskRunStatus::Waiting
            | TaskRunStatus::WaitingReview => {
                if background {
                    return Ok(Preparation::Unchanged(
                        TaskRunOccurrenceTerminalizationOutcome::StaleClaim,
                    ));
                }
                bail!("active TaskRun cannot terminalize its occurrence Turn");
            }
        };
        let Some(prepared_turn_model) =
            turn::find_turn_by_id(&self.connection, run_id.as_str()).await?
        else {
            return Ok(Preparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        };
        if turn_kind_from_db(prepared_turn_model.turn_kind.as_str()) != Some(TurnKind::TaskRun) {
            return Ok(Preparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::InvalidBinding,
            ));
        }
        let current_status = turn_status_from_db(prepared_turn_model.status.as_str())
            .with_context(|| format!("occurrence Turn `{}` has an unknown status", run_id))?;
        if current_status == desired_status {
            return Ok(Preparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent,
            ));
        }
        let Some(prepared_thread_model) =
            thread::find_thread_by_id(&self.connection, prepared_turn_model.thread_id.as_str())
                .await?
        else {
            return Ok(Preparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        };
        if !queue::workspace_exists(&self.connection, &prepared_thread_model.workspace_id).await? {
            return Ok(Preparation::Unchanged(
                TaskRunOccurrenceTerminalizationOutcome::NotFound,
            ));
        }
        let Some(mut terminal_turn) = turn_from_db_model(prepared_turn_model.clone())? else {
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
        let created_at = unix_to_datetime(completed_at);
        let claim_expires_at =
            unix_to_datetime(completed_at.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
        let terminal_event = prepare_projected_turn_event_for_permanent_storage(
            &self.connection,
            terminal_event,
            created_at,
        )
        .await?;

        Ok(Preparation::Ready(Box::new(PreparedOccurrence {
            run_id,
            prepared_run_model,
            prepared_turn_model,
            prepared_thread_model,
            desired_status,
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
        let PreparedOccurrence {
            run_id,
            prepared_run_model,
            prepared_turn_model,
            prepared_thread_model,
            desired_status,
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
            if let Some(claim) = claim.as_ref()
                && !queue::owns(&transaction, claim).await?
            {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
            }
            let Some(run_model) = task_run::find_run_by_id(&transaction, run_id.as_str()).await?
            else {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::NotFound);
            };
            // Complete set of Run facts used by this preparation: identity,
            // terminal mapping, timestamp and serialized error. Heartbeat and
            // updated_at do not fence these facts and need not invalidate them.
            if run_model.id != prepared_run_model.id
                || run_model.status != prepared_run_model.status
                || run_model.completed_at != prepared_run_model.completed_at
                || run_model.error_json != prepared_run_model.error_json
            {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
            }

            // The occurrence identity is canonical and does not depend on
            // optional/legacy lineage rows: Turn.id is exactly TaskRun.id.
            let Some(turn_model) = turn::find_turn_by_id(&transaction, run_id.as_str()).await?
            else {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::NotFound);
            };
            if turn_kind_from_db(turn_model.turn_kind.as_str()) != Some(TurnKind::TaskRun) {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::InvalidBinding);
            }
            let current_status = turn_status_from_db(turn_model.status.as_str())
                .with_context(|| format!("occurrence Turn `{run_id}` has an unknown status"))?;
            if current_status == desired_status {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::AlreadyConsistent);
            }
            // The event contains the decoded Turn; compare the entire source
            // model, including all serialized prompt/collaboration/security
            // fields, without repeating deserialization under the writer.
            if turn_model != prepared_turn_model {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
            }
            let thread_model =
                thread::find_thread_by_id(&transaction, turn_model.thread_id.as_str()).await?;
            let Some(thread_model) = thread_model else {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::NotFound);
            };
            if thread_model != prepared_thread_model {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::StaleClaim);
            }
            if !queue::workspace_exists(&transaction, &thread_model.workspace_id).await? {
                return Ok(TaskRunOccurrenceTerminalizationOutcome::NotFound);
            }
            self.append_and_project_turn_event_in_transaction(
                &transaction,
                terminal_event,
                created_at,
                claim_expires_at,
                false,
            )
            .await?;

            Ok(TaskRunOccurrenceTerminalizationOutcome::Changed)
        }
        .await;

        match result {
            Ok(outcome) => {
                transaction
                    .commit()
                    .await
                    .context("failed to commit TaskRun occurrence terminalization transaction")?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }
}
