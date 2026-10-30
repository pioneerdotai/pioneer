//! Durable soft-stop admission fence. Preparation reads a snapshot outside the
//! writer; commit revalidates every fact used to close a task or stop a trigger.
use crate::repositories::{task, task_actor_contract, task_run_execution, task_trigger};
use crate::task_events::{DeferredAppendedTaskEvent, materialize_task_events};
use crate::util::unix_to_datetime;
use crate::{
    AppendedTaskEvent, CrudStore, task_from_db_model, task_run_from_db_model,
    task_trigger_from_db_model,
};
use anyhow::{Context, Result};
use pioneer_protocol::{TaskEventPayload, TaskFinishRequest, TaskRunStatus, TaskTriggerStatus};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};

#[derive(Clone, Debug, PartialEq, sea_orm::FromQueryResult)]
pub(crate) struct FinishActive {
    pub id: String,
    pub run_number: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FinishRuns {
    pub active: Option<FinishActive>,
    pub outcome: Option<pioneer_entity::task_run::Model>,
}

// Queries return at most two rows irrespective of the accumulated run history.
// A queued run is never admitted work, including a queued retry.
pub(crate) async fn finish_runs<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
    first: i64,
    excluding: Option<&str>,
) -> Result<FinishRuns> {
    use pioneer_entity::task_run::{Column, Entity};
    let mut query = Entity::find()
        .filter(Column::TaskId.eq(task_id.to_owned()))
        .filter(Column::RunNumber.gte(first));
    if let Some(id) = excluding {
        query = query.filter(Column::Id.ne(id.to_owned()));
    }
    let active = query
        .clone()
        .filter(Column::Status.is_in(["starting", "running", "waiting_review"]))
        .order_by_asc(Column::RunNumber)
        .select_only()
        .columns([Column::Id, Column::RunNumber])
        .into_model::<FinishActive>()
        .one(db)
        .await?;
    let outcome = query.filter(Column::Status.is_in(["succeeded", "failed", "blocked", "cancelled", "timed_out"]))
        // Queue disposal is bookkeeping, never a real cancellation of work.
        .filter(sea_orm::sea_query::Expr::cust("NOT (status = 'cancelled' AND started_at IS NULL AND COALESCE(json_extract(error_json, '$.code'), '') = 'task_run_cancelled' AND COALESCE(json_extract(error_json, '$.message'), '') = 'task finished before this run started')"))
        .order_by_asc(sea_orm::sea_query::Expr::cust("CASE status WHEN 'succeeded' THEN 1 ELSE 0 END"))
        .order_by_desc(Column::RunNumber).one(db).await?;
    Ok(FinishRuns { active, outcome })
}

async fn first_pending_retry_source<C: ConnectionTrait>(
    db: &C,
    task_id: &str,
) -> Result<Option<FinishActive>> {
    use pioneer_entity::task_run::{Column, Entity};
    let queued_sources = sea_orm::sea_query::Query::select()
        .column(Column::RetryOfRunId)
        .from(Entity)
        .and_where(Column::TaskId.eq(task_id.to_owned()))
        .and_where(Column::Status.eq("queued"))
        .and_where(Column::RetryOfRunId.is_not_null())
        .to_owned();
    Ok(Entity::find()
        .filter(Column::TaskId.eq(task_id.to_owned()))
        .filter(Column::Id.in_subquery(queued_sources))
        .order_by_asc(Column::RunNumber)
        .select_only()
        .columns([Column::Id, Column::RunNumber])
        .into_model::<FinishActive>()
        .one(db)
        .await?)
}

pub(crate) fn outcome_event(
    task_id: &str,
    run: Option<&pioneer_entity::task_run::Model>,
    at: i64,
) -> Result<TaskEventPayload> {
    let run = run.cloned().map(task_run_from_db_model).transpose()?;
    Ok(match run {
        Some(run) if run.status == TaskRunStatus::Blocked => TaskEventPayload::TaskBlocked {
            task_id: task_id.into(),
            error: run.error,
            blocked_at: at,
        },
        Some(run) if matches!(run.status, TaskRunStatus::Failed | TaskRunStatus::TimedOut) => {
            TaskEventPayload::TaskFailed {
                task_id: task_id.into(),
                error: run.error,
                completed_at: at,
            }
        }
        Some(run) if run.status == TaskRunStatus::Cancelled => TaskEventPayload::TaskCancelled {
            task_id: task_id.into(),
            reason: run.error.map(|e| e.message),
            completed_at: at,
        },
        run => TaskEventPayload::TaskCompleted {
            task_id: task_id.into(),
            result: run.and_then(|r| r.result),
            completed_at: at,
        },
    })
}

impl CrudStore {
    pub async fn request_task_finish(
        &self,
        task_id: &str,
        reason: Option<String>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<(bool, Vec<AppendedTaskEvent>)> {
        self.run_serialized_write(|| {
            self.request_task_finish_once(task_id, reason.clone(), at, action.clone())
        })
        .await
    }

    async fn request_task_finish_once(
        &self,
        task_id: &str,
        reason: Option<String>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<(bool, Vec<AppendedTaskEvent>)> {
        // Optimistic retries only repeat database preparation; no external work
        // is performed while holding the writer or between snapshot and commit.
        for _ in 0..16 {
            let model = task::find_task_by_id(&self.connection, task_id)
                .await?
                .context("Task not found")?;
            let mut task_model = task_from_db_model(model.clone())?;
            let already_terminal = task_model.status.is_terminal();
            if task_model.finish_request().is_some() {
                let events = if let Some(action) = action.clone() {
                    self.append_task_events_with_agent_action(Vec::new(), at, action)
                        .await?
                } else {
                    Vec::new()
                };
                return Ok((already_terminal, events));
            }
            let all_runs = finish_runs(&self.connection, task_id, 0, None).await?;
            // Without active work, preserve the most recent real run's outcome.
            // With parallel work, all runs from the oldest admission contribute.
            let latest = if all_runs.active.is_none() {
                pioneer_entity::task_run::Entity::find()
                    .filter(pioneer_entity::task_run::Column::TaskId.eq(task_id.to_owned()))
                    .filter(pioneer_entity::task_run::Column::Status.ne("queued"))
                    .order_by_desc(pioneer_entity::task_run::Column::RunNumber)
                    .one(&self.connection)
                    .await?
            } else {
                None
            };
            let pending_retry = first_pending_retry_source(&self.connection, task_id).await?;
            let first = all_runs
                .active
                .as_ref()
                .map(|r| r.run_number)
                .into_iter()
                .chain(pending_retry.as_ref().map(|r| r.run_number))
                .min()
                .or_else(|| latest.as_ref().map(|r| r.run_number))
                .unwrap_or(0);
            let runs = finish_runs(&self.connection, task_id, first, None).await?;
            let triggers = task_trigger::list_triggers_by_task(&self.connection, task_id).await?;
            task_model
                .metadata
                .get_or_insert_with(Default::default)
                .finish_request = Some(TaskFinishRequest {
                requested_at: at,
                reason: reason.clone(),
                first_run_number: first,
            });
            task_model.revision = task_model
                .revision
                .checked_add(1)
                .context("Task revision overflow")?;
            task_model.updated_at = at;
            let mut events = vec![TaskEventPayload::TaskUpdated {
                task: task_model,
                trigger: None,
                agent_spec: None,
                changed_fields: vec!["finishRequest".into()],
                updated_at: at,
            }];
            for trigger in &triggers {
                let mut trigger = task_trigger_from_db_model(trigger.clone())?;
                trigger.status = TaskTriggerStatus::Cancelled;
                trigger.next_fire_at = None;
                trigger.updated_at = at;
                events.push(TaskEventPayload::TaskRescheduled {
                    task_id: task_id.into(),
                    trigger,
                    rescheduled_at: at,
                    reason: pioneer_protocol::TaskRescheduleReason::UserRequested,
                });
            }
            if !already_terminal && runs.active.is_none() {
                events.push(outcome_event(task_id, runs.outcome.as_ref(), at)?);
            }
            let prepared = self.prepare_task_events_for_write(events).await?;
            let tx = self.connection.begin().await?;
            let result: Result<Option<Vec<DeferredAppendedTaskEvent>>> = async {
                if task::find_task_by_id(&tx, task_id).await? != Some(model)
                    || task_trigger::list_triggers_by_task(&tx, task_id).await? != triggers
                    || finish_runs(&tx, task_id, 0, None).await? != all_runs
                    || finish_runs(&tx, task_id, first, None).await? != runs
                    || first_pending_retry_source(&tx, task_id).await? != pending_retry
                    || (all_runs.active.is_none()
                        && pioneer_entity::task_run::Entity::find()
                            .filter(pioneer_entity::task_run::Column::TaskId.eq(task_id.to_owned()))
                            .filter(pioneer_entity::task_run::Column::Status.ne("queued"))
                            .order_by_desc(pioneer_entity::task_run::Column::RunNumber)
                            .one(&tx)
                            .await?
                            != latest)
                {
                    return Ok(None);
                }
                let events = self
                    .append_task_events_in_connection(&tx, prepared, at)
                    .await?;
                if let Some(action) = action.clone() {
                    crate::repositories::agent_domain::commit_agent_action(&tx, &action).await?;
                }
                Ok(Some(events))
            }
            .await;
            match result {
                Ok(Some(events)) => {
                    tx.commit().await?;
                    return Ok((already_terminal, materialize_task_events(events)));
                }
                Ok(None) => {
                    tx.rollback().await?;
                }
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            }
        }
        anyhow::bail!("Task changed repeatedly during finish preparation")
    }

    /// Bounded discovery also includes terminal Tasks and retries whose
    /// ready_at is still in the future. Disposal revalidates the durable fence.
    pub async fn list_task_finish_queue_candidates(&self, limit: u64) -> Result<Vec<String>> {
        crate::repositories::task_run::list_task_finish_queue_candidates(&self.connection, limit)
            .await
    }

    /// Bounded, restart-safe queue disposal after the durable fence. Scheduler
    /// also calls this when rediscovering a queued run after Gateway recovery.
    pub async fn discard_finished_task_queue(
        &self,
        task_id: &str,
        at: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.run_serialized_write(|| self.discard_finished_task_queue_once(task_id, at))
            .await
    }

    async fn discard_finished_task_queue_once(
        &self,
        task_id: &str,
        at: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        use pioneer_entity::task_run::{Column, Entity};
        let Some(task_model) = task::find_task_by_id(&self.connection, task_id).await? else {
            return Ok(Vec::new());
        };
        if task_from_db_model(task_model)?.finish_request().is_none() {
            return Ok(Vec::new());
        }
        let runs = Entity::find()
            .filter(Column::TaskId.eq(task_id.to_owned()))
            .filter(Column::Status.eq("queued"))
            .order_by_asc(Column::RunNumber)
            .limit(16)
            .all(&self.connection)
            .await?;
        let mut retry_sources = Vec::new();
        for run in &runs {
            if let Some(source_id) = run.retry_of_run_id.as_deref() {
                let source =
                    crate::repositories::task_run::find_run_by_id(&self.connection, source_id)
                        .await?
                        .context("queued retry source disappeared")?;
                let source = task_run_from_db_model(source)?;
                let terminal = TaskEventPayload::RunFailed {
                    task_id: source.task_id,
                    run_id: source.id,
                    error: source.error,
                    completed_at: source
                        .completed_at
                        .context("retry source has no completion time")?,
                };
                retry_sources.push((
                    run.id.clone(),
                    self.prepare_task_terminal_transition(terminal).await?,
                ));
            }
        }
        let events = runs
            .iter()
            .map(|r| TaskEventPayload::RunCancelled {
                task_id: task_id.into(),
                run_id: r.id.clone(),
                reason: Some("task finished before this run started".into()),
                cancelled_at: at,
            })
            .collect();
        let prepared = self.prepare_task_events_for_write(events).await?;
        let tx = self.connection.begin().await?;
        let result: Result<Vec<DeferredAppendedTaskEvent>> = async {
            // No new admission may cross the marker; these exact queued runs
            // cannot become starting after the snapshot.
            let mut eligible = Vec::new();
            for event in prepared {
                let run_id = event
                    .payload()
                    .run_id()
                    .context("queue disposal has no run")?;
                if crate::repositories::task_run::find_run_by_id(&tx, run_id)
                    .await?
                    .is_some_and(|run| run.status == "queued")
                {
                    eligible.push(event);
                }
            }
            let mut events = self
                .append_task_events_in_connection(&tx, eligible, at)
                .await?;
            let mut settled_events = Vec::new();
            for event in &events {
                let run_id = event
                    .payload
                    .run_id()
                    .context("queue disposal has no run")?;
                let run = runs
                    .iter()
                    .find(|run| run.id == run_id)
                    .context("queue disposal run missing")?;
                if let Some(execution) =
                    task_run_execution::find_execution_by_run(&tx, &run.id).await?
                {
                    task_run_execution::mark_execution_terminal_json(
                        &tx,
                        &execution.id,
                        pioneer_protocol::TaskRunExecutionStatus::Cancelled,
                        unix_to_datetime(at),
                        None,
                        None,
                    )
                    .await?;
                }
                if let Some(position) = retry_sources
                    .iter()
                    .position(|(retry_id, _)| retry_id == &run.id)
                {
                    let (_, prepared) = retry_sources.remove(position);
                    let (source_events, _) = self
                        .settle_finished_retry_in_connection(&tx, prepared)
                        .await?;
                    settled_events.extend(source_events);
                }
                // A queued retry acquires its occurrence only when dispatched;
                // finish must discard it without minting that new attempt.
                if let Some(occurrence) =
                    task_actor_contract::find_task_occurrence_by_run_id(&tx, &run.id).await?
                {
                    if task_actor_contract::is_terminal_task_occurrence_status(&occurrence.status) {
                        continue;
                    }
                    task_actor_contract::finalize_task_occurrence(
                        &tx,
                        &run.id,
                        pioneer_protocol::TaskOccurrenceStatus::Cancelled,
                        Some("task finished before this run started".into()),
                        unix_to_datetime(at),
                    )
                    .await?;
                } else {
                    anyhow::ensure!(
                        run.retry_of_run_id.is_some(),
                        "queued Task run has no occurrence"
                    );
                }
            }
            events.extend(settled_events);
            Ok(events)
        }
        .await;
        match result {
            Ok(events) => {
                tx.commit().await?;
                Ok(materialize_task_events(events))
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}
