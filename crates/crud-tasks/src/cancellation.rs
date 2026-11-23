//! Revalidate cancellation and record exact actor receipts with their events.
use crate::delivery as task_delivery;
use crate::event_pipeline::{
    append_task_events_in_connection_with_delivery_policy, prepare_task_events_for_write,
};
use crate::event_records::DeferredAppendedTaskEvent;
use crate::event_store::TaskEventStore;
use crate::task_event::PreparedTaskEvent;
use crate::task_projector::TaskOutputBinder;
use anyhow::{Context, Result};
use pioneer_protocol::{TaskDeliveryAttemptStatus, TaskDeliveryStatus, TaskEventPayload};
use sea_orm::sea_query::{Expr, ExprTrait, Query, SelectStatement};
use sea_orm::{ConnectionTrait, DatabaseTransaction};
use std::collections::HashSet;

/// No cancellation event committed in this quantum. A concurrent webhook run
/// arrived after preparation; rebuild the cancellation plan from durable state.
#[derive(Debug)]
pub struct TaskCancellationRunsChanged;
impl std::fmt::Display for TaskCancellationRunsChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("webhook cancellation run set changed before commit")
    }
}
impl std::error::Error for TaskCancellationRunsChanged {}

pub struct PreparedTaskCancellation {
    events: Vec<PreparedTaskEvent>,
    run_fences: Vec<(SelectStatement, SelectStatement)>,
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn prepare_cancellation(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
    ) -> Result<PreparedTaskCancellation> {
        let mut refreshed = Vec::with_capacity(events.len());
        for event in events {
            if let TaskEventPayload::DeliveryCancelled {
                delivery: original,
                reason,
                ..
            } = event
            {
                let row = task_delivery::find_delivery_by_id(&self.connection, &original.id)
                    .await?
                    .context("cancellation delivery disappeared")?;
                // Validate the caller's original binding before refreshing it.
                task_delivery::prepare_delivery_projection(&original)?.validate_identity(&row)?;
                let mut delivery = task_delivery::task_delivery_from_db_model(row)?;
                let mut attempt = if delivery.status == TaskDeliveryStatus::Delivering {
                    let attempt = self
                        .get_task_delivery_attempt(&delivery.id, delivery.attempt_count)
                        .await?
                        .context("cancellation has no exact active attempt")?;
                    // This reader snapshot may already have been finalized. The
                    // writer preflight below detects that and retries the batch.
                    Some(attempt)
                } else {
                    None
                };
                if matches!(
                    delivery.status,
                    TaskDeliveryStatus::Cancelled
                        | TaskDeliveryStatus::Delivered
                        | TaskDeliveryStatus::Failed
                ) {
                    // Retain the original event for authority/identity validation
                    // in the writer; preflight will omit this terminal delivery.
                    refreshed.push(TaskEventPayload::DeliveryCancelled {
                        delivery: original,
                        attempt: None,
                        reason,
                    });
                    continue;
                }
                delivery.status = TaskDeliveryStatus::Cancelled;
                delivery.next_attempt_at = None;
                delivery.last_error = reason.clone();
                delivery.updated_at = at;
                if let Some(attempt) = &mut attempt {
                    attempt.status = TaskDeliveryAttemptStatus::Failed;
                    attempt.completed_at = Some(at);
                    attempt.error = reason
                        .clone()
                        .or_else(|| Some("task_delivery_cancelled".to_owned()));
                }
                refreshed.push(TaskEventPayload::DeliveryCancelled {
                    delivery,
                    attempt,
                    reason,
                });
            } else {
                refreshed.push(event);
            }
        }
        let mut prepared = prepare_task_events_for_write(&self.connection, refreshed).await?;
        for event in &mut prepared {
            if let TaskEventPayload::DeliveryCancelled { delivery, .. } = event.payload() {
                event.validate_delivery_fields()?;
                let boundary = crate::task_projector::prepare_delivery_authority_boundary(
                    &self.connection,
                    delivery,
                )
                .await?;
                event.set_delivery_boundary(boundary);
            }
        }
        // Build bounded existence queries outside writer admission. Only the
        // final TaskCancelled quantum needs this fence: prior quanta may already
        // have cancelled their runs, which are excluded by terminal status.
        // The transaction rechecks both webhook capability and unmatched live
        // runs, so no POST can land between this decision and cancellation.
        use pioneer_entity::{task_run as fenced_run, task_trigger as fenced_trigger};
        let mut run_fences = Vec::new();
        for event in &prepared {
            let TaskEventPayload::TaskCancelled { task_id, .. } = event.payload() else {
                continue;
            };
            let webhook = Query::select()
                .expr(Expr::val(1))
                .from(fenced_trigger::Entity)
                .and_where(Expr::col(fenced_trigger::Column::TaskId).eq(task_id.as_str()))
                .and_where(Expr::col(fenced_trigger::Column::WebhookSecretRef).is_not_null())
                .limit(1)
                .to_owned();
            let covered = prepared
                .iter()
                .filter_map(|event| match event.payload() {
                    TaskEventPayload::RunCancelled {
                        task_id: owner,
                        run_id,
                        ..
                    } if owner == task_id => Some(run_id.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut unmatched = Query::select()
                .expr(Expr::val(1))
                .from(fenced_run::Entity)
                .and_where(Expr::col(fenced_run::Column::TaskId).eq(task_id.as_str()))
                .and_where(Expr::col(fenced_run::Column::Status).is_not_in([
                    "succeeded",
                    "failed",
                    "blocked",
                    "cancelled",
                    "timed_out",
                ]))
                .limit(1)
                .to_owned();
            if !covered.is_empty() {
                unmatched.and_where(Expr::col(fenced_run::Column::Id).is_not_in(covered));
            }
            run_fences.push((webhook, unmatched));
        }
        Ok(PreparedTaskCancellation {
            events: prepared,
            run_fences,
        })
    }

    pub async fn append_cancellation_in_transaction(
        &self,
        tx: &DatabaseTransaction,
        cancellation: PreparedTaskCancellation,
        at: i64,
        actor_id: Option<&str>,
    ) -> Result<Vec<DeferredAppendedTaskEvent>> {
        let PreparedTaskCancellation {
            events: prepared,
            run_fences,
        } = cancellation;
        for (webhook, unmatched) in &run_fences {
            if tx.query_one(webhook).await?.is_some() && tx.query_one(unmatched).await?.is_some() {
                return Err(anyhow::Error::new(TaskCancellationRunsChanged));
            }
        }
        // Revalidate the actual transition inside the writer. A cancellation
        // losing to an earlier terminal commit must not read that result.
        use pioneer_entity::task_run as run;
        let mut transitioning = HashSet::new();
        if actor_id.is_some() {
            for event in &prepared {
                if let TaskEventPayload::RunCancelled {
                    task_id, run_id, ..
                } = event.payload()
                {
                    let query = Query::select()
                        .expr(Expr::val(1))
                        .from(run::Entity)
                        .and_where(Expr::col(run::Column::Id).eq(run_id.as_str()))
                        .and_where(Expr::col(run::Column::TaskId).eq(task_id.as_str()))
                        .and_where(Expr::col(run::Column::Status).is_not_in([
                            "succeeded",
                            "failed",
                            "blocked",
                            "cancelled",
                            "timed_out",
                        ]))
                        .to_owned();
                    if tx.query_one(&query).await?.is_some() {
                        transitioning.insert(run_id.clone());
                    }
                }
            }
        }
        let appended = append_task_events_in_connection_with_delivery_policy(
            tx,
            prepared,
            at,
            true,
            &self.outputs,
        )
        .await?;
        // Receipts commit with the exact transition and fanout journal.
        if let Some(actor) = actor_id {
            for event in &appended {
                if event.append_status.is_inserted()
                    && let TaskEventPayload::RunCancelled {
                        task_id, run_id, ..
                    } = event.payload.as_ref()
                    && transitioning.contains(run_id)
                {
                    let workspace = event
                        .workspace_id
                        .as_deref()
                        .context("cancelled run has no durable workspace")?;
                    crate::completion_inbox::mark_cancelled_task_run_read(
                        tx,
                        actor,
                        workspace,
                        task_id,
                        run_id,
                        &chrono::Utc::now().to_rfc3339(),
                    )
                    .await?;
                }
            }
        }
        Ok(appended)
    }
}
