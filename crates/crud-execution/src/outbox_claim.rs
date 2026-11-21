//! Claims and lease fencing for the durable agent action outbox.

use anyhow::{Context, Result, anyhow, bail};
use chrono::Duration;
use futures_util::FutureExt;
use pioneer_entity::{
    agent_action, agent_action_outbox, agent_action_timeline_target, agent_execution,
    agent_turn_response_execution,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, TransactionSession,
    TransactionTrait,
};
use std::panic::AssertUnwindSafe;

// The two partial-index predicates pin this policy to 8. Changing the retry
// budget requires a migration replacing their predicates, plus the queries below.
pub const AGENT_ACTION_OUTBOX_MAX_ATTEMPTS: i64 = 8;
const _: () = assert!(AGENT_ACTION_OUTBOX_MAX_ATTEMPTS == 8);
const AGENT_ACTION_OUTBOX_MAX_RETRY_SECONDS: i64 = 30 * 60;
pub const AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS: &str = "waiting_for_durable_permit";
pub const AGENT_ACTION_OUTBOX_RUNTIME_WAIT_CLASS: &str = "waiting_for_native_thread_runtime";
pub const ACTION_TIMELINE_TARGET_TURN_INPUT: &str = "turn_input";
const AGENT_ACTION_OUTBOX_BATCH_LIMIT: u64 = 64;
const AGENT_ACTION_OUTBOX_FAILURE_CLASS: &str = "outbox_delivery_failed";
// An exhausted StartAgent still owes outcome reconciliation, never delivery.
pub const AGENT_START_RECONCILIATION_REQUIRED: &str = "outbox_start_reconciliation_required";
pub const AGENT_START_TERMINAL_FAILURE_CLASSES: [&str; 6] = [
    "outbox_detached_binding_rejected",
    "outbox_start_integrity_rejected",
    "outbox_start_authority_denied",
    "outbox_start_runtime_unavailable",
    "outbox_foreground_conflict",
    "outbox_start_outcome_unknown",
];
pub const AGENT_ACTION_OUTBOX_LEASE_SECONDS: i64 = 30;

/// Fixed diagnostics only: provider errors and user payloads never enter the ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentActionOutboxFailure {
    DeliveryFailed,
    StorageUnavailable,
    InvalidStart,
    InvalidDetachedBinding,
    AuthorityDenied,
    RuntimeUnavailable,
    PreparationUnavailable,
    ControlUnavailable,
    OutcomeUnknown,
    ForegroundConflict,
}
impl AgentActionOutboxFailure {
    pub const fn safe_name(self) -> &'static str {
        match self {
            Self::DeliveryFailed => "outbox_delivery_failed",
            Self::StorageUnavailable => "outbox_storage_unavailable",
            Self::InvalidStart => "outbox_start_integrity_rejected",
            Self::InvalidDetachedBinding => "outbox_detached_binding_rejected",
            Self::AuthorityDenied => "outbox_start_authority_denied",
            Self::RuntimeUnavailable => "outbox_start_runtime_unavailable",
            Self::PreparationUnavailable => "outbox_start_preparation_unavailable",
            Self::ControlUnavailable => "outbox_runtime_control_unavailable",
            Self::OutcomeUnknown => "outbox_start_outcome_unknown",
            Self::ForegroundConflict => "outbox_foreground_conflict",
        }
    }
}
impl std::fmt::Display for AgentActionOutboxFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.safe_name())
    }
}
impl std::error::Error for AgentActionOutboxFailure {}

/// Known-committed claims and independent candidate failures from one input quantum.
#[derive(Debug, Default)]
pub struct AgentActionOutboxClaimBatch {
    /// Raw discovered identities before claims, filtering or dispatch failures.
    /// Either range can fill before the combined 64-row budget is exhausted.
    pub has_more: bool,
    pub rows: Vec<agent_action_outbox::Model>,
    pub errors: Vec<anyhow::Error>,
}

// An immediate clock callback makes writer-admission timing testable without
// sleeps. It performs no preparation or I/O while the transaction is open.
pub async fn claim_agent_action_outbox_with_clock<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    limit: u64,
    clock: &(impl Fn() -> DateTimeWithTimeZone + Sync),
) -> Result<AgentActionOutboxClaimBatch> {
    if limit == 0 {
        return Ok(AgentActionOutboxClaimBatch::default());
    }
    if !(2..=AGENT_ACTION_OUTBOX_BATCH_LIMIT).contains(&limit) {
        bail!("Agent action outbox nonzero batch limit must be 2..=64");
    }
    let ids = crate::outbox_discovery::discover(db, clock(), limit).await?;
    let mut batch = AgentActionOutboxClaimBatch {
        has_more: ids.len() as u64 >= limit / 2,
        ..Default::default()
    };
    for id in ids {
        // The entire point operation is inside the boundary, including future
        // creation and polling. Dropping it releases transactions/reservations;
        // cancellation still drops the owning future rather than becoming Err.
        let outcome = AssertUnwindSafe(async {
            let transaction =
                match db.begin().await {
                    Ok(transaction) => transaction,
                    Err(error) => {
                        batch.errors.push(anyhow::Error::new(error).context(
                            "failed to begin outbox claim; no snapshot for durable deferral",
                        ));
                        return;
                    }
                };
            let now = clock(); // after writer admission, for each row
            // Original source payload is authoritative. Do not decode it under capacity.
            let mut candidate =
                match agent_action_outbox::Entity::find_by_id(id)
                    .lock_exclusive()
                    .one(&transaction)
                    .await
                {
                    Ok(Some(candidate)) => candidate,
                    Ok(None) => {
                        if let Err(error) = transaction.rollback().await {
                            batch.errors.push(error.into());
                        }
                        return;
                    }
                    Err(error) => {
                        batch.errors.push(anyhow::Error::new(error).context(
                            "failed to load outbox input; no snapshot for durable deferral",
                        ));
                        if let Err(error) = transaction.rollback().await {
                            batch.errors.push(error.into());
                        }
                        return;
                    }
                };
            let row = match transition_agent_action_outbox(&transaction, &mut candidate, now).await
            {
                Ok(row) => row,
                Err(error) => {
                    batch.errors.push(error);
                    // Only a known rollback permits a separate snapshot-fenced delay.
                    match transaction.rollback().await {
                        Ok(()) => {
                            match defer_unclaimed_agent_action_outbox(db, &candidate, clock).await {
                                Ok(true) => {}
                                Ok(false) => batch.errors.push(anyhow::anyhow!(
                                    "outbox claim deferral skipped: source snapshot changed"
                                )),
                                Err(error) => batch.errors.push(error.context(
                                    "outbox claim deferral not confirmed; durability unavailable",
                                )),
                            }
                        }
                        Err(error) => batch.errors.push(
                            anyhow::Error::new(error)
                                .context("outbox rollback not confirmed; no claim repair allowed"),
                        ),
                    }
                    return;
                }
            };
            // Unknown commit excludes only this row. Never repair, rediscover or
            // claim it again here; previously confirmed rows still reach dispatch.
            match transaction.commit().await {
                Ok(()) => {
                    if row {
                        batch.rows.push(candidate);
                    }
                }
                Err(error) => batch.errors.push(
                    anyhow::Error::new(error)
                        .context("agent action outbox claim commit outcome unknown"),
                ),
            }
        })
        .catch_unwind()
        .await;
        if outcome.is_err() {
            // Unwind is not a commit/rollback acknowledgement. No repair or
            // same-quantum retry; only prior known commits are dispatchable.
            batch
                .errors
                .push(anyhow!("outbox_claim_panic_outcome_unknown"));
        }
    }
    Ok(batch)
}

// A failed, rolled-back claim consumes no execution attempt. Preserve status
// and attempt budget, using the existing 30s lease as a retry delay. The exact
// original nullable lease fences any new ownership acquired after rollback.
pub async fn defer_unclaimed_agent_action_outbox<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    snapshot: &agent_action_outbox::Model,
    clock: &(impl Fn() -> DateTimeWithTimeZone + Sync),
) -> Result<bool> {
    let transaction = db.begin().await?;
    let now = clock();
    let lease_guard = match snapshot.next_attempt_at {
        Some(lease) => agent_action_outbox::Column::NextAttemptAt.eq(lease),
        None => agent_action_outbox::Column::NextAttemptAt.is_null(),
    };
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(Some(
                now + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS),
            )),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(AGENT_ACTION_OUTBOX_FAILURE_CLASS.to_owned())),
        )
        .filter(agent_action_outbox::Column::Id.eq(&snapshot.id))
        .filter(agent_action_outbox::Column::Status.eq(&snapshot.status))
        .filter(agent_action_outbox::Column::Attempts.eq(snapshot.attempts))
        .filter(lease_guard)
        .exec(&transaction)
        .await?;
    transaction.commit().await?;
    Ok(result.rows_affected == 1)
}

// One bounded point transition. Exact snapshot guards also fence a replaced
// lease with the same attempt count (e.g. after permit/runtime budget return).
pub async fn transition_agent_action_outbox<C: ConnectionTrait>(
    db: &C,
    candidate: &mut agent_action_outbox::Model,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    if !matches!(candidate.status.as_str(), "pending" | "failed")
        || candidate
            .next_attempt_at
            .as_ref()
            .is_some_and(|due| due > &now)
    {
        return Ok(false);
    }
    if candidate.status == "failed"
        && candidate.next_attempt_at.is_none()
        && candidate.last_error.as_deref()
            == Some(AgentActionOutboxFailure::InvalidDetachedBinding.safe_name())
    {
        return Ok(false);
    }
    let exhausted = candidate.attempts >= AGENT_ACTION_OUTBOX_MAX_ATTEMPTS;
    let reconcile_start = exhausted
        && agent_action::Entity::find_by_id(candidate.action_id.clone())
            .one(db)
            .await?
            .is_some_and(|action| action.action_kind == "start_agent");
    // Only locally finalized StartAgent refusals add an early terminal case.
    // Legacy immediate failures and other action kinds retain their behavior.
    if !reconcile_start
        && candidate.status == "failed"
        && candidate.next_attempt_at.is_none()
        && candidate
            .last_error
            .as_deref()
            .is_some_and(|cause| AGENT_START_TERMINAL_FAILURE_CLASSES.contains(&cause))
    {
        return Ok(false);
    }
    if exhausted
        && !reconcile_start
        && (candidate.status != "pending" || candidate.next_attempt_at.is_none())
    {
        return Ok(false);
    }
    let lease_guard = match candidate.next_attempt_at.clone() {
        Some(lease) => agent_action_outbox::Column::NextAttemptAt.eq(lease),
        None => agent_action_outbox::Column::NextAttemptAt.is_null(),
    };
    let update = agent_action_outbox::Entity::update_many()
        .filter(agent_action_outbox::Column::Id.eq(candidate.id.clone()))
        .filter(agent_action_outbox::Column::Status.eq(candidate.status.clone()))
        .filter(agent_action_outbox::Column::Attempts.eq(candidate.attempts))
        .filter(lease_guard);
    let (status, attempts, next_attempt_at, last_error) = if reconcile_start {
        (
            "pending",
            candidate.attempts,
            Some(now + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS)),
            Some(AGENT_START_RECONCILIATION_REQUIRED.to_owned()),
        )
    } else if exhausted {
        (
            "failed",
            candidate.attempts,
            None,
            Some("outbox delivery lease expired after the retry limit".to_owned()),
        )
    } else {
        (
            "pending",
            candidate.attempts + 1,
            Some(now + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS)),
            None,
        )
    };
    let result = update
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value(status),
        )
        .col_expr(
            agent_action_outbox::Column::Attempts,
            sea_orm::sea_query::Expr::value(attempts),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(next_attempt_at.clone()),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error.clone()),
        )
        .exec(db)
        .await
        .context("failed to transition agent action outbox input")?;
    if result.rows_affected != 1 || (exhausted && !reconcile_start) {
        return Ok(false);
    }
    candidate.status = status.to_owned();
    candidate.attempts = attempts;
    candidate.next_attempt_at = next_attempt_at;
    candidate.last_error = last_error;
    Ok(true)
}

pub async fn mark_agent_action_outbox_delivered<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    delivered_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("delivered"),
        )
        .col_expr(
            agent_action_outbox::Column::DeliveredAt,
            sea_orm::sea_query::Expr::value(Some(delivered_at)),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value::<Option<DateTimeWithTimeZone>>(None),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to mark agent domain outbox row delivered")?;
    Ok(result.rows_affected == 1)
}

pub async fn mark_agent_action_outbox_failed_with_cause<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    failed_at: DateTimeWithTimeZone,
    failure: AgentActionOutboxFailure,
    terminal: bool,
) -> Result<bool> {
    let exhausted_start = if !terminal && expected_attempts == AGENT_ACTION_OUTBOX_MAX_ATTEMPTS {
        match agent_action_outbox::Entity::find_by_id(outbox_id.to_owned())
            .one(db)
            .await?
        {
            Some(row) if row.attempts == expected_attempts => {
                agent_action::Entity::find_by_id(row.action_id)
                    .one(db)
                    .await?
                    .is_some_and(|action| action.action_kind == "start_agent")
            }
            _ => false,
        }
    } else {
        false
    };
    let next_attempt_at = if terminal {
        None
    } else if exhausted_start {
        Some(failed_at.clone() + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS))
    } else {
        agent_action_outbox_retry_at(expected_attempts, failed_at.clone())?
    };
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(
                if exhausted_start
                    && AGENT_START_TERMINAL_FAILURE_CLASSES.contains(&failure.safe_name())
                {
                    AGENT_START_RECONCILIATION_REQUIRED.to_owned()
                } else {
                    failure.safe_name().to_owned()
                },
            )),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(next_attempt_at),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to mark agent domain outbox row failed")?;
    Ok(result.rows_affected == 1)
}

pub async fn defer_agent_action_outbox_for_permit<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let restored_attempts = expected_attempts
        .checked_sub(1)
        .filter(|attempts| *attempts >= 0)
        .context("outbox permit deferral has an invalid attempt fence")?;
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            agent_action_outbox::Column::Attempts,
            sea_orm::sea_query::Expr::value(restored_attempts),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS.to_owned())),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(Some(
                deferred_at + Duration::seconds(AGENT_ACTION_OUTBOX_LEASE_SECONDS),
            )),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to defer agent domain outbox for a durable permit")?;
    Ok(result.rows_affected == 1)
}

pub async fn defer_agent_action_outbox_for_runtime<C: ConnectionTrait>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let restored_attempts = expected_attempts
        .checked_sub(1)
        .filter(|attempts| *attempts >= 0)
        .context("outbox runtime deferral has an invalid attempt fence")?;
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::Status,
            sea_orm::sea_query::Expr::value("failed"),
        )
        .col_expr(
            agent_action_outbox::Column::Attempts,
            sea_orm::sea_query::Expr::value(restored_attempts),
        )
        .col_expr(
            agent_action_outbox::Column::LastError,
            sea_orm::sea_query::Expr::value(Some(
                AGENT_ACTION_OUTBOX_RUNTIME_WAIT_CLASS.to_owned(),
            )),
        )
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(Some(deferred_at + Duration::milliseconds(100))),
        )
        .filter(agent_action_outbox::Column::Id.eq(outbox_id.to_owned()))
        .filter(agent_action_outbox::Column::Status.eq("pending"))
        .filter(agent_action_outbox::Column::Attempts.eq(expected_attempts))
        .exec(db)
        .await
        .context("failed to defer agent domain outbox for a busy native thread runtime")?;
    Ok(result.rows_affected == 1)
}

pub async fn wake_agent_action_outbox_for_execution<C: ConnectionTrait>(
    db: &C,
    execution_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let execution = agent_execution::Entity::find_by_id(execution_id.to_owned())
        .one(db)
        .await
        .context("failed to resolve promoted AgentExecution")?
        .context("promoted AgentExecution is missing")?;
    if execution.parent_task_id.is_some() {
        // Task candidate/reviewer executions are resumed by the Task runtime,
        // not by a StartAgent action outbox. They may legitimately respond to
        // multiple initial/revision Turns during one occurrence.
        return Ok(0);
    }
    let responses = agent_turn_response_execution::Entity::find()
        .filter(agent_turn_response_execution::Column::ExecutionId.eq(execution_id.to_owned()))
        .limit(2)
        .all(db)
        .await
        .context("failed to resolve promoted execution response Turn")?;
    if responses.is_empty() {
        return Ok(0);
    }
    if responses.len() != 1 {
        bail!("promoted non-Task execution responds to multiple Turns");
    }
    let target = agent_action_timeline_target::Entity::find_by_id(format!(
        "turn_input:{}",
        responses[0].turn_id
    ))
    .one(db)
    .await
    .context("failed to resolve promoted execution StartAgent action")?;
    let Some(target) = target else {
        return Ok(0);
    };
    if target.target_kind != ACTION_TIMELINE_TARGET_TURN_INPUT
        || target.turn_id != responses[0].turn_id
    {
        bail!("promoted execution has an inconsistent action timeline target");
    }
    let result = agent_action_outbox::Entity::update_many()
        .col_expr(
            agent_action_outbox::Column::NextAttemptAt,
            sea_orm::sea_query::Expr::value(Some(now)),
        )
        .filter(agent_action_outbox::Column::ActionId.eq(target.action_id))
        .filter(agent_action_outbox::Column::Status.eq("failed"))
        .filter(
            agent_action_outbox::Column::LastError
                .eq(AGENT_ACTION_OUTBOX_PERMIT_WAIT_CLASS.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to wake agent domain outbox after permit promotion")?;
    Ok(result.rows_affected)
}

fn agent_action_outbox_retry_at(
    attempt: i64,
    failed_at: DateTimeWithTimeZone,
) -> Result<Option<DateTimeWithTimeZone>> {
    if !(1..=AGENT_ACTION_OUTBOX_MAX_ATTEMPTS).contains(&attempt) {
        bail!("outbox failure acknowledgement has an invalid attempt fence");
    }
    if attempt == AGENT_ACTION_OUTBOX_MAX_ATTEMPTS {
        return Ok(None);
    }
    let exponent = u32::try_from(attempt.saturating_sub(1)).unwrap_or_default();
    let delay = std::cmp::Ord::min(
        AGENT_ACTION_OUTBOX_LEASE_SECONDS.saturating_mul(2_i64.saturating_pow(exponent)),
        AGENT_ACTION_OUTBOX_MAX_RETRY_SECONDS,
    );
    Ok(Some(failed_at + Duration::seconds(delay)))
}

#[cfg(test)]
mod tests;
