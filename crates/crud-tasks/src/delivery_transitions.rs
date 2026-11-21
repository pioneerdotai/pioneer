//! Owns attempt transitions, including exact recovery revalidation.
use crate::delivery as task_delivery;
use crate::delivery_event_checks::{validate_current_attempt, validate_current_attempt_status};
use crate::delivery_recovery::DeliveryRecoverySnapshot;
use crate::event_pipeline::{
    append_task_events_in_connection_with_delivery_policy, prepare_task_events_for_write,
};
use crate::event_records::AppendedTaskEvent;
use crate::event_store::TaskEventStore;
use crate::task_projector::TaskOutputBinder;
use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    TaskDeliveryAttempt, TaskDeliveryAttemptStatus, TaskDeliveryStatus, TaskEventPayload,
};
use sea_orm::TransactionTrait;

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

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn get_task_delivery_attempt(
        &self,
        delivery_id: &str,
        number: u32,
    ) -> Result<Option<TaskDeliveryAttempt>> {
        task_delivery::find_attempt_by_number(&self.connection, delivery_id, number)
            .await?
            .map(task_delivery::task_delivery_attempt_from_db_model)
            .transpose()
    }

    pub async fn transition_task_delivery(
        &self,
        event: TaskEventPayload,
        transition: TaskDeliveryTransition,
        at: i64,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [event.task_id().to_owned()],
        );
        let result = async {
            self.run_serialized_write(|| {
                self.transition_task_delivery_once(event.clone(), transition, at, None)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(matches!(value, TaskDeliveryTransitionOutcome::Applied(event) if event.append_status.is_inserted()));
        }
        result
    }

    pub async fn recover_task_delivery(
        &self,
        event: TaskEventPayload,
        snapshot: &DeliveryRecoverySnapshot,
        cutoff: i64,
        at: i64,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [event.task_id().to_owned()],
        );
        let result = async {
            self.transition_task_delivery_once(
                event,
                TaskDeliveryTransition::Recover { cutoff },
                at,
                Some(snapshot),
            )
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(matches!(value, TaskDeliveryTransitionOutcome::Applied(event) if event.append_status.is_inserted()));
        }
        result
    }

    async fn transition_task_delivery_once(
        &self,
        event: TaskEventPayload,
        transition: TaskDeliveryTransition,
        at: i64,
        recovery_snapshot: Option<&DeliveryRecoverySnapshot>,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        let prepared = prepare_task_events_for_write(&self.connection, vec![event])
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
        self.delivery_commit_test_hooks
            .pause_delivery_commit_for_test(match transition {
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
                if !crate::delivery_recovery::matches(&tx, snapshot).await? {
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
            let appended = append_task_events_in_connection_with_delivery_policy(
                &tx,
                vec![prepared],
                at,
                false,
                &self.outputs,
            )
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
                Ok(TaskDeliveryTransitionOutcome::Applied(event.into_owned()))
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
struct TaskDeliveryCommitTestGate {
    kind: TaskDeliveryCommitTestKind,
    entered: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Default)]
pub struct DeliveryCommitTestHooks {
    gate: std::sync::Arc<std::sync::Mutex<Option<TaskDeliveryCommitTestGate>>>,
}

#[cfg(any(test, feature = "test-support"))]
impl DeliveryCommitTestHooks {
    #[doc(hidden)]
    pub fn set_delivery_commit_gate_for_test(
        &self,
        kind: TaskDeliveryCommitTestKind,
        entered: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
    ) {
        *self.gate.lock().expect("delivery test gate poisoned") =
            Some(TaskDeliveryCommitTestGate {
                kind,
                entered,
                release,
            });
    }

    pub async fn pause_delivery_commit_for_test(&self, kind: TaskDeliveryCommitTestKind) {
        let gate = {
            let mut guard = self.gate.lock().expect("delivery test gate poisoned");
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
