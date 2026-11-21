//! Discovery and recovery of the canonical projection stream.

use super::event_projection::{TurnEventProjectionFailureOutcome, TurnEventProjectionOutcome};
use crate::store::events::event_preparation::{
    TURN_EVENT_PROJECTION_LEASE_SECS, deserialize_turn_event_projection_context,
};
use pioneer_crud_execution::time::unix_to_datetime;
use crate::{CrudStore, compaction};
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::event_projection;
use pioneer_crud_conversations::events;
use pioneer_crud_conversations::projection_stream;
pub use pioneer_crud_conversations::projection_stream::{
    TurnProjectionStreamHealth, TurnProjectionStreamStateRecord,
};
use pioneer_crud_execution::event_append as turn_event;
use pioneer_protocol::Turn;
use sea_orm::TransactionTrait;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TurnProjectionReplaySummary {
    /// Earliest eligible clock from this replay's last bounded discovery.
    /// Advisory only; invalidation and durable claims remain authoritative.
    pub next_eligible_at_unix: Option<i64>,
    pub claimed: usize,
    pub projected: usize,
    pub deferred: usize,
    pub failed: usize,
    pub exhausted: usize,
    pub quarantined: usize,
    pub missing_events: usize,
    pub exhausted_records: Vec<TurnProjectionReplayExhaustedRecord>,
    pub quarantined_streams: Vec<TurnProjectionStreamQuarantineRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnProjectionReplayExhaustedRecord {
    pub event_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub error_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnProjectionStreamQuarantineRecord {
    pub thread_id: String,
    pub turn_id: String,
    pub blocking_event_id: String,
    pub error_message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreTurnProjectionStreamOutcome {
    Restored,
    AlreadyHealthy,
    NotFound,
    BlockingEventMismatch,
}

impl CrudStore {
    pub async fn get_turn_event_projection_stream_state(
        &self,
        turn_id: &str,
    ) -> Result<Option<TurnProjectionStreamStateRecord>> {
        projection_stream::get_turn_event_projection_stream_state(&self.connection, turn_id).await
    }

    /// Re-enables a quarantined projection stream after an operator has
    /// repaired its causal blocker. The expected blocker fences stale repair
    /// commands. Resetting the exhausted head and clearing quarantine happen
    /// atomically so replay can never observe a healthy stream with an
    /// exhausted causal head.
    pub async fn restore_turn_event_projection_stream(
        &self,
        turn_id: &str,
        expected_blocking_event_id: &str,
        restored_at_unix: i64,
    ) -> Result<RestoreTurnProjectionStreamOutcome> {
        let receipt_keys = vec![turn_id.to_owned()];
        let receipt_change = self
            .connection
            .work_signal("maintenance.receipts")
            .changing();
        let changes = ["projection", "compaction", "maintenance.native"].map(|domain| {
            let change = self.connection.work_signal(domain).changing();
            if domain == "projection" {
                change.with_keys(&[format!("turn:{turn_id}")])
            } else {
                change
            }
        });
        let task_change = pioneer_crud_tasks::work_change::TaskWorkChange::new(&self.connection);
        let result = async {
        self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin projection stream restoration transaction")?;

            let result = async {
                let Some(stream) =
                    projection_stream::find(&transaction, turn_id).await?
                else {
                    return Ok(RestoreTurnProjectionStreamOutcome::NotFound);
                };
                if stream.status
                    == projection_stream::STREAM_STATUS_HEALTHY
                {
                    return Ok(RestoreTurnProjectionStreamOutcome::AlreadyHealthy);
                }
                if stream.status
                    != projection_stream::STREAM_STATUS_QUARANTINED
                {
                    bail!(
                        "projection stream for Turn `{turn_id}` has unknown health status `{}`",
                        stream.status
                    );
                }
                if stream.blocking_event_id.as_deref() != Some(expected_blocking_event_id) {
                    return Ok(RestoreTurnProjectionStreamOutcome::BlockingEventMismatch);
                }

                let blocker = event_projection::find_by_event_id(
                    &transaction,
                    expected_blocking_event_id,
                )
                .await?
                .with_context(|| {
                    format!(
                        "quarantine blocker `{expected_blocking_event_id}` for Turn `{turn_id}` is missing"
                    )
                })?;
                if blocker.turn_id != turn_id
                    || blocker.status
                        != event_projection::PROJECTION_STATUS_EXHAUSTED
                {
                    bail!(
                        "quarantine blocker `{expected_blocking_event_id}` is not the exhausted causal head for Turn `{turn_id}`"
                    );
                }

                let raw_event =
                    pioneer_crud_conversations::turn_event_reads::find_event_model_by_id(
                        &transaction, expected_blocking_event_id,
                    )
                        .await?
                        .with_context(|| {
                            format!(
                                "raw quarantine blocker `{expected_blocking_event_id}` for Turn `{turn_id}` is still missing"
                            )
                        })?;
                if raw_event.turn_id != turn_id {
                    bail!(
                        "raw quarantine blocker `{expected_blocking_event_id}` belongs to Turn `{}`, not `{turn_id}`",
                        raw_event.turn_id
                    );
                }

                let restored_at = unix_to_datetime(restored_at_unix);
                let reset = event_projection::reset_exhausted_as_pending(
                    &transaction,
                    expected_blocking_event_id,
                    turn_id,
                    restored_at,
                )
                .await?;
                if !reset {
                    bail!(
                        "exhausted quarantine blocker `{expected_blocking_event_id}` changed during restoration"
                    );
                }
                let restored = crate::store::events::projection_receipts::restore(
                    &transaction,
                    turn_id,
                    expected_blocking_event_id,
                    restored_at,
                )
                .await?;
                if !restored {
                    bail!("projection stream for Turn `{turn_id}` changed during restoration");
                }

                Ok(RestoreTurnProjectionStreamOutcome::Restored)
            }
            .await;

            match result {
                Ok(outcome) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit projection stream restoration transaction")?;
                    Ok(outcome)
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        })
        .await
        }.await;
        if let Ok(outcome) = &result {
            let changed = matches!(outcome, RestoreTurnProjectionStreamOutcome::Restored);
            task_change.completed(changed);
            receipt_change.completed_keys(if changed { &receipt_keys } else { &[] });
            for change in changes {
                change.completed(changed);
            }
        }
        result
    }

    pub async fn replay_due_turn_event_projections(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<TurnProjectionReplaySummary> {
        self.replay_turn_event_projection_frontier(now_unix, limit, None, now_unix)
            .await
    }

    /// Advisory discovery is restricted to the captured invalidation frontier.
    /// Claims, leases and publication use the current operation clock; only
    /// candidate eligibility uses the fixed pass cutoff. Transactional guards
    /// still revalidate ownership, stream health and predecessor ordering.
    pub async fn replay_turn_event_projection_frontier(
        &self,
        now_unix: i64,
        limit: u64,
        turn_ids: Option<&[String]>,
        eligible_before_unix: i64,
    ) -> Result<TurnProjectionReplaySummary> {
        anyhow::ensure!(
            turn_ids.is_none_or(|ids| ids.len() <= 64),
            "projection recovery frontier exceeds 64 Turns"
        );
        let eligible_before = unix_to_datetime(eligible_before_unix.min(now_unix));
        let now = unix_to_datetime(now_unix);
        let claim_expires_at =
            unix_to_datetime(now_unix.saturating_add(TURN_EVENT_PROJECTION_LEASE_SECS));
        let mut summary = TurnProjectionReplaySummary::default();
        let mut remaining = limit;

        // `claim_due` returns at most the immediate watermark successor of
        // each Turn. Refill after projecting those events so one healthy stream
        // can still use the replay budget without monopolizing the first batch.
        while remaining > 0 {
            let page =
                event_projection::discover_for_turns(&self.connection, remaining, turn_ids).await?;
            summary.next_eligible_at_unix = page
                .first()
                .map(|(_, at)| pioneer_crud_execution::time::deadline_unix_seconds(*at));
            let candidates = page
                .into_iter()
                .filter(|(_, at)| *at <= eligible_before)
                .map(|(id, _)| id)
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                break;
            }
            let claimed = self
                .run_serialized_write(|| {
                    event_projection::claim_candidates(
                        &self.connection,
                        now,
                        claim_expires_at,
                        candidates.clone(),
                    )
                })
                .await?;
            if claimed.is_empty() {
                break;
            }
            let claimed_count = u64::try_from(claimed.len()).unwrap_or(u64::MAX);
            summary.claimed = summary.claimed.saturating_add(claimed.len());

            for claimed_projection in claimed {
                let event_id = claimed_projection.state.event_id.clone();
                let thread_id = claimed_projection.state.thread_id.clone();
                let turn_id = claimed_projection.state.turn_id.clone();
                let claim_token = claimed_projection.claim_token.clone();
                let attempt_count = claimed_projection.state.attempt_count;

                let Some(appended_event) =
                    turn_event::find_event_by_id(&self.connection, &event_id)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to load raw turn event `{event_id}` for projection replay"
                            )
                        })?
                else {
                    let marked = self
                        .run_serialized_write(|| {
                            self.mark_turn_event_projection_failure_once(
                                event_id.clone(),
                                thread_id.clone(),
                                turn_id.clone(),
                                claim_token.clone(),
                                attempt_count,
                                "raw turn_event row is missing for projection replay".to_owned(),
                                now_unix,
                                true,
                            )
                        })
                        .await?;
                    summary.missing_events += 1;
                    if marked == TurnEventProjectionFailureOutcome::Quarantined {
                        summary.exhausted += 1;
                        summary.quarantined += 1;
                        summary
                            .exhausted_records
                        .push(TurnProjectionReplayExhaustedRecord {
                            event_id: event_id.clone(),
                            thread_id: thread_id.clone(),
                            turn_id: turn_id.clone(),
                            error_message: "raw turn_event row is missing for projection replay"
                                .to_owned(),
                        });
                        summary.quarantined_streams.push(
                            TurnProjectionStreamQuarantineRecord {
                                thread_id: thread_id.clone(),
                                turn_id: turn_id.clone(),
                                blocking_event_id: event_id.clone(),
                                error_message:
                                    "raw turn_event row is missing for projection replay"
                                        .to_owned(),
                            },
                        );
                    }
                    continue;
                };

                let projection_context = match deserialize_turn_event_projection_context(
                    claimed_projection.state.projection_context_json.as_str(),
                    event_id.as_str(),
                ) {
                    Ok(context) => context,
                    Err(error) => {
                        let error_message = format!("{error:#}");
                        let marked = self
                            .run_serialized_write(|| {
                                self.mark_turn_event_projection_failure_once(
                                    event_id.clone(),
                                    thread_id.clone(),
                                    turn_id.clone(),
                                    claim_token.clone(),
                                    attempt_count,
                                    error_message.clone(),
                                    now_unix,
                                    true,
                                )
                            })
                            .await?;
                        if marked == TurnEventProjectionFailureOutcome::Quarantined {
                            summary.exhausted += 1;
                            summary.quarantined += 1;
                            summary
                                .exhausted_records
                                .push(TurnProjectionReplayExhaustedRecord {
                                    event_id: event_id.clone(),
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    error_message: error_message.clone(),
                                });
                            summary.quarantined_streams.push(
                                TurnProjectionStreamQuarantineRecord {
                                    thread_id: thread_id.clone(),
                                    turn_id: turn_id.clone(),
                                    blocking_event_id: event_id.clone(),
                                    error_message,
                                },
                            );
                        }
                        continue;
                    }
                };

                let projected = self
                    .run_serialized_write(|| {
                        self.project_claimed_turn_event_once(
                            appended_event.clone(),
                            projection_context.clone(),
                            claim_token.clone(),
                            now,
                        )
                    })
                    .await;

                match projected {
                    Ok(TurnEventProjectionOutcome::Projected) => {
                        summary.projected += 1;
                    }
                    Ok(TurnEventProjectionOutcome::DeferredByPredecessor) => {
                        let released = self
                            .run_serialized_write(|| {
                                self.mark_turn_event_projection_pending_once(
                                    event_id.clone(),
                                    turn_id.clone(),
                                    claim_token.clone(),
                                    now_unix,
                                )
                            })
                            .await?;
                        if released {
                            summary.deferred += 1;
                        }
                    }
                    Err(error) => {
                        let error_message = format!("{error:#}");
                        let marked = self
                            .run_serialized_write(|| {
                                self.mark_turn_event_projection_failure_once(
                                    event_id.clone(),
                                    thread_id.clone(),
                                    turn_id.clone(),
                                    claim_token.clone(),
                                    attempt_count,
                                    error_message.clone(),
                                    now_unix,
                                    false,
                                )
                            })
                            .await?;
                        match marked {
                            TurnEventProjectionFailureOutcome::Quarantined => {
                                summary.exhausted += 1;
                                summary.quarantined += 1;
                                summary.exhausted_records.push(
                                    TurnProjectionReplayExhaustedRecord {
                                        event_id: event_id.clone(),
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        error_message: error_message.clone(),
                                    },
                                );
                                summary.quarantined_streams.push(
                                    TurnProjectionStreamQuarantineRecord {
                                        thread_id: thread_id.clone(),
                                        turn_id: turn_id.clone(),
                                        blocking_event_id: event_id.clone(),
                                        error_message,
                                    },
                                );
                            }
                            TurnEventProjectionFailureOutcome::Failed => {
                                summary.failed += 1;
                            }
                            TurnEventProjectionFailureOutcome::LostClaim => {}
                        }
                    }
                }
            }

            remaining = remaining.saturating_sub(claimed_count);
        }

        // A consumed budget is not EOF. Refresh the advisory head so completed
        // events do not manufacture an immediate timer/full discovery.
        if summary.claimed > 0 {
            summary.next_eligible_at_unix =
                event_projection::discover_for_turns(&self.connection, 1, turn_ids)
                    .await?
                    .first()
                    .map(|(_, at)| pioneer_crud_execution::time::deadline_unix_seconds(*at));
        }
        Ok(summary)
    }
}
