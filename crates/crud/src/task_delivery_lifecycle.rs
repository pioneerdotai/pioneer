//! Short writer boundaries for worker ownership and delivery cancellation.
//! Payloads/projections are prepared outside writer admission. Only indexed
//! durable-state reads and the bounded atomic event write set run inside it.
use anyhow::{Context, Result, bail};
use pioneer_protocol::{TaskDeliveryAttempt, TaskDeliveryAttemptStatus, TaskDeliveryStatus};
use sea_orm::{ConnectionTrait, TransactionTrait};

use crate::repositories::{task_delivery, task_event};
use crate::{AppendedTaskEvent, CrudStore, TaskEventPayload};

/// A losing worker has no durable effect and must not be finalized again.
#[derive(Clone, Debug)]
pub enum TaskDeliveryTransitionOutcome<T> {
    Applied(T),
    Superseded,
}

#[derive(Clone, Copy, Debug)]
pub enum TaskDeliveryTransition {
    Start,
    Finish,
    Recover { cutoff: i64 },
}

#[derive(Debug)]
struct CancellationSnapshotChanged;
impl std::fmt::Display for CancellationSnapshotChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("delivery cancellation snapshot changed before commit")
    }
}
impl std::error::Error for CancellationSnapshotChanged {}

impl CrudStore {
    pub async fn get_task_delivery_attempt(
        &self,
        delivery_id: &str,
        number: u32,
    ) -> Result<Option<TaskDeliveryAttempt>> {
        task_delivery::find_attempt_by_number(&self.connection, delivery_id, number)
            .await?
            .map(crate::task_delivery_attempt_from_db_model)
            .transpose()
    }

    pub async fn transition_task_delivery(
        &self,
        event: TaskEventPayload,
        transition: TaskDeliveryTransition,
        at: i64,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        self.run_serialized_write(|| {
            self.transition_task_delivery_once(event.clone(), transition, at, None)
        })
        .await
    }

    /// Recovery preparation reads a selected delivery, exact attempt and retry.
    /// Revalidate these raw facts in the same transaction as the domain event.
    pub async fn recover_task_delivery(
        &self,
        event: TaskEventPayload,
        snapshot: &crate::DeliveryRecoverySnapshot,
        cutoff: i64,
        at: i64,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        self.transition_task_delivery_once(
            event,
            TaskDeliveryTransition::Recover { cutoff },
            at,
            Some(snapshot),
        )
        .await
    }

    async fn transition_task_delivery_once(
        &self,
        event: TaskEventPayload,
        transition: TaskDeliveryTransition,
        at: i64,
        recovery_snapshot: Option<&crate::DeliveryRecoverySnapshot>,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        let prepared = self
            .prepare_task_events_for_write(vec![event])
            .await?
            .pop()
            .context("delivery transition preparation returned no event")?;
        if matches!(transition, TaskDeliveryTransition::Start) {
            prepared.validate_delivery_start_fields()?;
        } else {
            prepared.validate_delivery_fields()?;
        }
        match (prepared.payload(), transition) {
            (
                TaskEventPayload::DeliveryStarted { delivery, attempt },
                TaskDeliveryTransition::Start,
            ) if delivery.status == TaskDeliveryStatus::Delivering
                && attempt.status == TaskDeliveryAttemptStatus::Started
                && attempt.started_at == at
                && delivery.updated_at == at => {}
            (
                TaskEventPayload::DeliveryDelivered { delivery, attempt },
                TaskDeliveryTransition::Finish,
            ) if delivery.status == TaskDeliveryStatus::Delivered
                && attempt.status == TaskDeliveryAttemptStatus::Delivered
                && attempt.completed_at == Some(at)
                && delivery.updated_at == at => {}
            (
                TaskEventPayload::DeliveryFailed { delivery, attempt },
                TaskDeliveryTransition::Finish | TaskDeliveryTransition::Recover { .. },
            ) if attempt.status == TaskDeliveryAttemptStatus::Failed
                && attempt.completed_at == Some(at)
                && delivery.updated_at == at
                && delivery.status
                    == if delivery.attempt_count < delivery.max_attempts {
                        TaskDeliveryStatus::Pending
                    } else {
                        TaskDeliveryStatus::Failed
                    } => {}
            _ => bail!("delivery lifecycle event does not match its operation"),
        }

        let delivery = match prepared.payload() {
            TaskEventPayload::DeliveryStarted { delivery, .. }
            | TaskEventPayload::DeliveryDelivered { delivery, .. }
            | TaskEventPayload::DeliveryFailed { delivery, .. } => delivery,
            _ => bail!("invalid delivery lifecycle event"),
        };
        let boundary =
            crate::task_projector::prepare_delivery_authority_boundary(&self.connection, delivery)
                .await?;
        #[cfg(any(test, feature = "test-support"))]
        self.pause_delivery_commit_for_test(match transition {
            TaskDeliveryTransition::Start => TaskDeliveryCommitTestKind::Start,
            TaskDeliveryTransition::Finish => TaskDeliveryCommitTestKind::Finish,
            TaskDeliveryTransition::Recover { .. } => TaskDeliveryCommitTestKind::Recovery,
        })
        .await;
        let tx = self.connection.begin().await?;
        let result = async {
            let (delivery, attempt) = match (prepared.payload(), transition) {
                (
                    TaskEventPayload::DeliveryStarted { delivery, attempt },
                    TaskDeliveryTransition::Start,
                )
                | (
                    TaskEventPayload::DeliveryDelivered { delivery, attempt },
                    TaskDeliveryTransition::Finish,
                )
                | (
                    TaskEventPayload::DeliveryFailed { delivery, attempt },
                    TaskDeliveryTransition::Finish | TaskDeliveryTransition::Recover { .. },
                ) => (delivery, attempt),
                _ => bail!("invalid delivery lifecycle operation"),
            };
            if attempt.delivery_id != delivery.id
                || attempt.attempt_number != delivery.attempt_count
            {
                bail!("delivery transition has inconsistent attempt ownership");
            }
            let row = task_delivery::find_delivery_by_id(&tx, &delivery.id)
                .await?
                .context("delivery transition has no durable delivery")?;
            // Immutable facts and authority errors never become Superseded.
            prepared.validate_delivery_identity(&row)?;
            task_delivery::validate_durable_delivery(&row)?;
            let authority = boundary
                .revalidate(&tx)
                .await?
                .check_existing(&tx, &row.status)
                .await?;
            if !matches!(
                row.status.as_str(),
                "pending" | "delivering" | "delivered" | "failed" | "cancelled"
            ) {
                bail!("delivery has an unknown durable status");
            }
            if let Some(snapshot) = recovery_snapshot {
                if snapshot.delivery.id != row.id {
                    bail!("recovery snapshot belongs to another delivery");
                }
                if !crate::repositories::task_delivery_recovery::matches(&tx, snapshot).await? {
                    return Ok(TaskDeliveryTransitionOutcome::Superseded);
                }
            }
            let attempt_row = match transition {
                TaskDeliveryTransition::Start => {
                    if row.status != "pending"
                        || row.attempt_count + 1 != i64::from(delivery.attempt_count)
                        || row
                            .next_attempt_at
                            .as_ref()
                            .is_some_and(|due| due.timestamp() > at)
                    {
                        validate_current_attempt(&tx, &row).await?;
                        return Ok(TaskDeliveryTransitionOutcome::Superseded);
                    }
                    // A retry may start only after its predecessor has closed.
                    validate_current_attempt(&tx, &row).await?;
                    if row.attempt_count >= row.max_attempts {
                        bail!("pending delivery has exhausted its attempt budget");
                    }
                    if task_delivery::find_attempt_by_number(
                        &tx,
                        &delivery.id,
                        delivery.attempt_count,
                    )
                    .await?
                    .is_some()
                    {
                        bail!("delivery next attempt already exists");
                    }
                    None
                }
                TaskDeliveryTransition::Finish | TaskDeliveryTransition::Recover { .. } => {
                    let owned = task_delivery::find_attempt_by_id(&tx, &attempt.id)
                        .await?
                        .context("delivery transition has no durable attempt")?;
                    task_delivery::validate_attempt_identity(&owned, attempt)?;
                    task_delivery::validate_durable_attempt(&owned)?;
                    if row.attempt_count < owned.attempt_number {
                        bail!("delivery attempt is ahead of its durable owner");
                    }
                    // A closed exact attempt proves that this result lost ownership.
                    // An open old attempt is corruption, not an expected race.
                    if owned.status != "started" {
                        if row.attempt_count == owned.attempt_number && row.status == "delivering" {
                            bail!("delivering delivery has a closed active attempt");
                        }
                        if !matches!(owned.status.as_str(), "delivered" | "failed") {
                            bail!("delivery attempt has an unknown durable status");
                        }
                        if row.attempt_count == owned.attempt_number {
                            validate_current_attempt_status(&row, &owned)?;
                        } else {
                            if owned.status != "failed" {
                                bail!("terminal successful attempt has a later attempt");
                            }
                            validate_current_attempt(&tx, &row).await?;
                        }
                        return Ok(TaskDeliveryTransitionOutcome::Superseded);
                    }
                    if row.status != "delivering" || row.attempt_count != owned.attempt_number {
                        bail!("started delivery attempt has no active durable owner");
                    }
                    if let TaskDeliveryTransition::Recover { cutoff } = transition
                        && row.updated_at.timestamp() > cutoff
                    {
                        return Ok(TaskDeliveryTransitionOutcome::Superseded);
                    }
                    Some(owned)
                }
            };
            let prepared = prepared
                .with_delivery_rows(row, attempt_row)
                .with_delivery_authority(Some(authority));
            let appended = self
                .append_task_events_in_connection(&tx, vec![prepared], at)
                .await?
                .pop()
                .context("delivery transition returned no event")?;
            if !appended.append_status.is_inserted() {
                bail!("active delivery transition unexpectedly reused an existing event");
            }
            Ok(TaskDeliveryTransitionOutcome::Applied(appended))
        }
        .await;
        match result {
            Ok(TaskDeliveryTransitionOutcome::Applied(event)) => {
                tx.commit().await?;
                Ok(TaskDeliveryTransitionOutcome::Applied(event))
            }
            Ok(TaskDeliveryTransitionOutcome::Superseded) => {
                tx.rollback().await?;
                Ok(TaskDeliveryTransitionOutcome::Superseded)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    /// Keeps delivery cancellation inside the caller's existing atomic Task
    /// batch (including its optional Agent action). Refreshes only delivery
    /// snapshots after a typed preflight conflict, outside the writer.
    pub async fn append_task_cancellation_events(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let mut appended = Vec::new();
        if action.is_some() {
            return self
                .append_task_cancellation_quantum(events, at, action)
                .await;
        }
        for chunk in events.chunks(crate::MAX_ATOMIC_TASK_EVENT_BATCH_SIZE) {
            appended.extend(
                self.append_task_cancellation_quantum(chunk.to_vec(), at, None)
                    .await?,
            );
        }
        Ok(appended)
    }

    async fn append_task_cancellation_quantum(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        crate::retry_with_backoff(
            || {
                self.run_serialized_write(|| {
                    self.append_task_cancellation_events_once(events.clone(), at, action.clone())
                })
            },
            |error| {
                error.chain().any(|cause| {
                    cause
                        .downcast_ref::<CancellationSnapshotChanged>()
                        .is_some()
                })
            },
            crate::DEFAULT_LOCK_RETRY_ATTEMPTS,
            std::time::Duration::from_millis(crate::DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    async fn append_task_cancellation_events_once(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<Vec<AppendedTaskEvent>> {
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
                let mut delivery = crate::task_delivery_from_db_model(row)?;
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
        let mut prepared = self.prepare_task_events_for_write(refreshed).await?;
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
        #[cfg(any(test, feature = "test-support"))]
        self.pause_delivery_commit_for_test(TaskDeliveryCommitTestKind::Cancellation)
            .await;
        let tx = self.connection.begin().await?;
        let result = async {
            let appended = self
                .append_task_events_with_delivery_cancellation(&tx, prepared, at)
                .await?;
            if let Some(action) = &action {
                crate::repositories::agent_domain::commit_agent_action(&tx, action).await?;
            }
            Ok(appended)
        }
        .await;
        match result {
            Ok(appended) => {
                tx.commit().await?;
                Ok(appended)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}

/// Sequential preflight sees earlier dependent events in this same batch.
/// Terminal delivery outcomes win; active attempts are closed atomically.
pub(crate) async fn preflight_cancellation<C: ConnectionTrait>(
    db: &C,
    mut prepared: task_event::PreparedTaskEvent,
) -> Result<Option<task_event::PreparedTaskEvent>> {
    let boundary = prepared.take_delivery_boundary();
    let TaskEventPayload::DeliveryCancelled {
        delivery, attempt, ..
    } = prepared.payload()
    else {
        return Ok(Some(prepared));
    };
    let row = task_delivery::find_delivery_by_id(db, &delivery.id)
        .await?
        .context("cancellation has no durable delivery")?;
    prepared.validate_delivery_identity(&row)?;
    task_delivery::validate_durable_delivery(&row)?;
    // Check authority even for a cancellation that has lost to a terminal state.
    let authority = boundary
        .context("cancellation authority boundary was not prepared")?
        .revalidate(db)
        .await?
        .check_existing(db, &row.status)
        .await?;
    if matches!(row.status.as_str(), "cancelled" | "delivered" | "failed") {
        validate_current_attempt(db, &row).await?;
        return Ok(None);
    }
    if !matches!(row.status.as_str(), "pending" | "delivering") {
        bail!("cancellation has an unknown delivery status");
    }
    if row.attempt_count != i64::from(delivery.attempt_count) {
        return Err(CancellationSnapshotChanged.into());
    }
    let current_attempt = if delivery.attempt_count == 0 {
        None
    } else {
        task_delivery::find_attempt_by_number(db, &delivery.id, delivery.attempt_count).await?
    };
    let attempt_row = match (row.status.as_str(), attempt, current_attempt) {
        ("delivering", Some(expected), Some(current)) => {
            task_delivery::validate_attempt_identity(&current, expected)?;
            task_delivery::validate_durable_attempt(&current)?;
            if current.status != "started" {
                bail!("delivering cancellation has a closed active attempt");
            }
            Some(current)
        }
        ("delivering", None, Some(current)) if current.status == "started" => {
            return Err(CancellationSnapshotChanged.into());
        }
        ("pending", None, current) => {
            if let Some(current) = &current {
                task_delivery::validate_durable_attempt(current)?;
            }
            if delivery.attempt_count > 0
                && current.as_ref().is_none_or(|row| row.status != "failed")
            {
                bail!("pending cancellation has no exact failed previous attempt");
            }
            None
        }
        ("pending", Some(_), _) => return Err(CancellationSnapshotChanged.into()),
        _ => bail!("cancellation has no exact active attempt"),
    };
    Ok(Some(
        prepared
            .with_delivery_rows(row, attempt_row)
            .with_delivery_authority(Some(authority)),
    ))
}

// Validate the exact current attempt only when it was not already read by
// the ownership preflight (old worker, terminal cancellation or next start).
async fn validate_current_attempt<C: ConnectionTrait>(
    db: &C,
    delivery: &pioneer_entity::task_delivery::Model,
) -> Result<()> {
    if delivery.attempt_count == 0 {
        return Ok(());
    }
    let current = task_delivery::find_attempt_by_number(
        db,
        &delivery.id,
        u32::try_from(delivery.attempt_count)?,
    )
    .await?
    .context("durable delivery has no exact current attempt")?;
    validate_current_attempt_status(delivery, &current)
}

fn validate_current_attempt_status(
    delivery: &pioneer_entity::task_delivery::Model,
    current: &pioneer_entity::task_delivery_attempt::Model,
) -> Result<()> {
    task_delivery::validate_durable_attempt(current)?;
    let expected = match delivery.status.as_str() {
        "delivering" => "started",
        "delivered" => "delivered",
        "pending" | "failed" | "cancelled" => "failed",
        _ => bail!("delivery has an unknown durable status"),
    };
    if current.delivery_id != delivery.id
        || current.attempt_number != delivery.attempt_count
        || current.status != expected
    {
        bail!("delivery and its exact current attempt disagree");
    }
    Ok(())
}

// One-shot test gates pause only after preparation has released all database
// resources and before requesting the writer. Scoped store clones share a gate.
#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskDeliveryCommitTestKind {
    Start,
    Finish,
    Recovery,
    RecoveryRetry,
    RecoveryPanic,
    Cancellation,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
pub(crate) struct TaskDeliveryCommitTestGate {
    kind: TaskDeliveryCommitTestKind,
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(any(test, feature = "test-support"))]
impl CrudStore {
    #[doc(hidden)]
    pub fn set_delivery_commit_gate_for_test(
        &self,
        kind: TaskDeliveryCommitTestKind,
        entered: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
    ) {
        *self
            .delivery_commit_test_gate
            .lock()
            .expect("delivery test gate poisoned") = Some(TaskDeliveryCommitTestGate {
            kind,
            entered,
            release,
        });
    }

    pub(crate) async fn pause_delivery_commit_for_test(&self, kind: TaskDeliveryCommitTestKind) {
        let gate = {
            let mut guard = self
                .delivery_commit_test_gate
                .lock()
                .expect("delivery test gate poisoned");
            if guard.as_ref().is_some_and(|gate| {
                gate.kind == kind
                    || (kind == TaskDeliveryCommitTestKind::Recovery
                        && gate.kind == TaskDeliveryCommitTestKind::RecoveryPanic)
            }) {
                guard.take()
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
            if gate.kind == TaskDeliveryCommitTestKind::RecoveryPanic {
                panic!("injected recovery preparation panic");
            }
        }
    }
}
