//! Claim and settle persisted turn deliveries on their owning service.
use anyhow::{Context, Result};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use std::{future::Future, time::Duration};

use crate::events::AppendedTurnEvent;
use crate::values::unix_to_datetime;
use crate::{turn_event_delivery, turn_event_reads};
use turn_event_delivery::{
    DeliveryOutcome as TurnEventDeliveryOutcome, DeliverySettlement as TurnEventDeliverySettlement,
};

#[derive(Debug, Clone)]
pub struct ClaimedTurnEventDeliveryRecord {
    pub id: String,
    pub consumer: String,
    pub attempt_count: i64,
    pub claim_token: String,
    pub event: AppendedTurnEvent,
}

/// One advisory discovery page and its conditional claims. The clock is the
/// earliest eligible row observed, including future retry/lease obligations.
#[derive(Debug, Default)]
pub struct TurnEventDeliveryClaimBatch {
    pub deliveries: Vec<ClaimedTurnEventDeliveryRecord>,
    pub next_eligible_at_unix: Option<i64>,
}

fn deadline_unix_seconds(value: DateTimeWithTimeZone) -> i64 {
    value
        .timestamp()
        .saturating_add(i64::from(value.timestamp_subsec_nanos() != 0))
}

async fn retry_write<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry_with_backoff(
        operation,
        is_retryable_database_conflict,
        DEFAULT_LOCK_RETRY_ATTEMPTS,
        Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
    )
    .await
}

pub async fn next_turn_event_delivery_deadline(
    db: &PostgresDatabase,
    consumer: &str,
) -> Result<Option<i64>> {
    Ok(turn_event_delivery::next_deadline(db, consumer)
        .await?
        .map(deadline_unix_seconds))
}

pub async fn claim_due_turn_event_deliveries_with_deadline(
    db: &PostgresDatabase,
    consumer: &str,
    now_unix: i64,
    limit: u64,
) -> Result<TurnEventDeliveryClaimBatch> {
    let changes = [db
        .work_signal("projection")
        .changing()
        .with_keys(&["delivery".into()])];
    let result = async {
        // One compact reader discovery supplies both due IDs and an eligible
        // timer. Empty polls need no second query, writer or payload load.
        let page = turn_event_delivery::discover(db, consumer, limit).await?;
        let next_eligible_at_unix = page.first().map(|(_, at)| deadline_unix_seconds(*at));
        let candidates = page
            .into_iter()
            .filter(|(_, at)| *at <= unix_to_datetime(now_unix))
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(TurnEventDeliveryClaimBatch {
                deliveries: Vec::new(),
                next_eligible_at_unix,
            });
        }
        let candidates = turn_event_delivery::prepare_claims(candidates);
        let consumer = consumer.to_owned();
        let claimed = retry_write(|| {
            let consumer = consumer.clone();
            let candidates = candidates.clone();
            async move {
                let now = unix_to_datetime(now_unix);
                let claim_expires_at = unix_to_datetime(now_unix.saturating_add(120));
                turn_event_delivery::claim_prepared(
                    db,
                    &consumer,
                    now,
                    claim_expires_at,
                    &candidates,
                )
                .await
            }
        })
        .await?;
        // Claim writes are committed before event reads/JSON decoding. A read
        // error leaves the durable lease recoverable and dispatches no claim.
        let ids = claimed
            .iter()
            .map(|c| c.row.event_id.clone())
            .collect::<Vec<_>>();
        let mut events = turn_event_reads::find_event_models_by_ids(db, &ids)
            .await?
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        let mut records = Vec::with_capacity(claimed.len());
        let mut invalid = Vec::new();
        for claim in claimed {
            let event = events
                .remove(&claim.row.event_id)
                .with_context(|| {
                    format!(
                        "turn event `{}` is missing for delivery `{}`",
                        claim.row.event_id, claim.row.id
                    )
                })
                .and_then(|row| row)
                .and_then(turn_event_reads::appended_event_from_model)
                .and_then(|event| {
                    anyhow::ensure!(
                        event.turn_id == claim.row.turn_id
                            && event.thread_id == claim.row.thread_id
                            && event.sequence == claim.row.sequence
                            && event.payload.thread_id() == claim.row.thread_id
                            && event.payload.turn_id() == claim.row.turn_id,
                        "delivery event owner/sequence changed"
                    );
                    Ok(event)
                });
            match event {
                Ok(event) => records.push(ClaimedTurnEventDeliveryRecord {
                    id: claim.row.id,
                    consumer: claim.row.consumer,
                    attempt_count: claim.row.attempt_count,
                    claim_token: claim.claim_token,
                    event,
                }),
                Err(error) => invalid.push(TurnEventDeliverySettlement {
                    id: claim.row.id,
                    claim_token: claim.claim_token,
                    at_unix: now_unix,
                    outcome: TurnEventDeliveryOutcome::Failed {
                        attempt_count: claim.row.attempt_count,
                        error: format!("{error:#}"),
                    },
                }),
            }
        }
        if !invalid.is_empty() {
            settle_turn_event_deliveries(db, &invalid).await?;
        }
        Ok(TurnEventDeliveryClaimBatch {
            deliveries: records,
            next_eligible_at_unix,
        })
    }
    .await;
    if result.is_ok() {
        for change in changes {
            change.completed(false);
        }
    }
    result
}

pub async fn settle_turn_event_deliveries(
    db: &PostgresDatabase,
    settlements: &[TurnEventDeliverySettlement],
) -> Result<u64> {
    anyhow::ensure!(
        settlements.len() <= turn_event_delivery::DELIVERY_BATCH_LIMIT,
        "delivery settlement batch exceeds bound"
    );
    if settlements.is_empty() {
        return Ok(0);
    }
    let changes = ["projection", "compaction"].map(|domain| {
        let change = db.work_signal(domain).changing();
        if domain == "projection" {
            change.with_keys(&["delivery".into()])
        } else {
            change
        }
    });
    let result = retry_write(|| turn_event_delivery::settle_batch(db, settlements)).await;
    if let Ok(changed) = &result {
        for change in changes {
            change.completed(*changed != 0);
        }
    }
    result
}

pub async fn complete_turn_event_delivery(
    db: &PostgresDatabase,
    id: &str,
    claim_token: &str,
    now_unix: i64,
) -> Result<bool> {
    let changes = ["projection", "compaction"].map(|domain| {
        let change = db.work_signal(domain).changing();
        if domain == "projection" {
            change.with_keys(&["delivery".into()])
        } else {
            change
        }
    });
    let id = id.to_owned();
    let claim_token = claim_token.to_owned();
    let result = retry_write(|| async {
        turn_event_delivery::mark_delivered(
            db,
            id.as_str(),
            claim_token.as_str(),
            unix_to_datetime(now_unix),
        )
        .await
    })
    .await;
    if let Ok(changed) = &result {
        for change in changes {
            change.completed(*changed);
        }
    }
    result
}

pub async fn defer_turn_event_delivery_for_foreground_start(
    db: &PostgresDatabase,
    id: &str,
    claim_token: &str,
    now_unix: i64,
) -> Result<bool> {
    let projection_changes = [
        db.work_signal("projection")
            .changing()
            .with_keys(&["delivery".into()]),
        db.work_signal("compaction").changing(),
    ];
    let projection_result = async {
        retry_write(|| async {
            turn_event_delivery::defer_for_foreground_start(
                db,
                id,
                claim_token,
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }
    .await;
    if let Ok(value) = &projection_result {
        let changed = *value;
        for change in projection_changes {
            change.completed(changed);
        }
    }
    projection_result
}

pub async fn fail_turn_event_delivery(
    db: &PostgresDatabase,
    id: &str,
    claim_token: &str,
    attempt_count: i64,
    error: String,
    now_unix: i64,
) -> Result<bool> {
    let projection_changes = [
        db.work_signal("projection")
            .changing()
            .with_keys(&["delivery".into()]),
        db.work_signal("compaction").changing(),
    ];
    let projection_result = async {
        let id = id.to_owned();
        let claim_token = claim_token.to_owned();
        retry_write(|| {
            let error = error.clone();
            let id = id.clone();
            let claim_token = claim_token.clone();
            async move {
                const MAX_ATTEMPTS: i64 = 10;
                let exhausted = attempt_count.saturating_add(1) >= MAX_ATTEMPTS;
                let exponent = u32::try_from(attempt_count.clamp(0, 8)).unwrap_or(8);
                let delay = 2_i64.saturating_pow(exponent).min(300);
                turn_event_delivery::mark_failed(
                    db,
                    id.as_str(),
                    claim_token.as_str(),
                    error,
                    unix_to_datetime(now_unix.saturating_add(delay)),
                    unix_to_datetime(now_unix),
                    exhausted,
                )
                .await
            }
        })
        .await
    }
    .await;
    if let Ok(value) = &projection_result {
        let changed = *value;
        for change in projection_changes {
            change.completed(changed);
        }
    }
    projection_result
}

#[cfg(test)]
#[path = "delivery_service_tests.rs"]
mod tests;
