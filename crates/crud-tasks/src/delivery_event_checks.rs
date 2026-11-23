//! Revalidates delivery ownership on the caller's transaction.
use crate::{delivery as task_delivery, task_event};
use anyhow::{Context, Result, bail};
use pioneer_protocol::TaskEventPayload;
use sea_orm::ConnectionTrait;

#[derive(Debug)]
pub struct CancellationSnapshotChanged;
impl std::fmt::Display for CancellationSnapshotChanged {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("delivery cancellation snapshot changed before commit")
    }
}
impl std::error::Error for CancellationSnapshotChanged {}

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
pub async fn validate_current_attempt<C: ConnectionTrait>(
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

pub fn validate_current_attempt_status(
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
