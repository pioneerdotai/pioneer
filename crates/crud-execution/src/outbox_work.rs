//! Runtime outbox admission and settlement with commit-aware work invalidation.
use anyhow::Result;
use pioneer_entity::agent_work_queue;
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

use crate::agent_queue::AgentQueueEntryInput;
use crate::gateway_control::{GatewayControlConnection, GatewayControlWake};
use crate::outbox_claim::{
    AgentActionOutboxClaimBatch, AgentActionOutboxFailure, claim_agent_action_outbox_with_clock,
};

pub async fn enqueue_agent_execution<C: GatewayControlConnection>(
    db: &C,
    input: &AgentQueueEntryInput,
) -> Result<agent_work_queue::Model> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Resources]);
    let (queue, inserted) = crate::agent_queue::enqueue_agent_execution(db, input).await?;
    if let Some(change) = change {
        change.completed(inserted);
    }
    Ok(queue)
}

pub async fn claim_agent_action_outbox<C: GatewayControlConnection + TransactionTrait>(
    db: &C,
    limit: u64,
) -> Result<AgentActionOutboxClaimBatch> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Actions]);
    let result =
        claim_agent_action_outbox_with_clock(db, limit, &|| chrono::Utc::now().fixed_offset())
            .await;
    if let Ok(batch) = &result {
        if batch.errors.is_empty() {
            if let Some(change) = change {
                change.completed(!batch.rows.is_empty());
            }
        }
    }
    result
}

pub async fn mark_agent_action_outbox_delivered<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    delivered_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Actions]);
    let result = crate::outbox_claim::mark_agent_action_outbox_delivered(
        db,
        outbox_id,
        expected_attempts,
        delivered_at,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

pub async fn mark_agent_action_outbox_failed<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    failed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    mark_agent_action_outbox_failed_with_cause(
        db,
        outbox_id,
        expected_attempts,
        failed_at,
        AgentActionOutboxFailure::DeliveryFailed,
        false,
    )
    .await
}

pub async fn mark_agent_action_outbox_failed_with_cause<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    failed_at: DateTimeWithTimeZone,
    failure: AgentActionOutboxFailure,
    terminal: bool,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Actions]);
    let result = crate::outbox_claim::mark_agent_action_outbox_failed_with_cause(
        db,
        outbox_id,
        expected_attempts,
        failed_at,
        failure,
        terminal,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

/// Release an outbox claim whose execution is still legitimately queued.
/// Waiting for root capacity is not a delivery attempt and therefore must not
/// consume the bounded failure budget. The scheduler explicitly wakes this
/// row when it grants the execution a permit; the short fallback deadline
/// also keeps recovery independent from an in-memory notification.
pub async fn defer_agent_action_outbox_for_permit<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Actions]);
    let result = crate::outbox_claim::defer_agent_action_outbox_for_permit(
        db,
        outbox_id,
        expected_attempts,
        deferred_at,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

/// Release a StartAgent outbox claim while another native Turn still owns the
/// target thread runtime. This is ordinary serialization, not a delivery
/// failure, so it restores the attempt counter and retries promptly after the
/// parent Turn can finish.
pub async fn defer_agent_action_outbox_for_runtime<C: GatewayControlConnection>(
    db: &C,
    outbox_id: &str,
    expected_attempts: i64,
    deferred_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Actions]);
    let result = crate::outbox_claim::defer_agent_action_outbox_for_runtime(
        db,
        outbox_id,
        expected_attempts,
        deferred_at,
    )
    .await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed);
        }
    }
    result
}

/// Make a capacity-deferred StartAgent row immediately eligible after the
/// exact execution is promoted. The failure-class fence prevents scheduler
/// activity from disturbing an unrelated delivery retry.
pub async fn wake_agent_action_outbox_for_execution<C: GatewayControlConnection>(
    db: &C,
    execution_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let change = db.gateway_control_change_for(&[GatewayControlWake::Actions]);
    let result =
        crate::outbox_claim::wake_agent_action_outbox_for_execution(db, execution_id, now).await;
    if let Ok(changed) = &result {
        if let Some(change) = change {
            change.completed(*changed > 0);
        }
    }
    result
}
