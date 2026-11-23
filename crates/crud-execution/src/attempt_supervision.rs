//! Scoped execution attempt deadlines, progress, and timeout transitions.
use crate::attempt_values::*;
use crate::gateway_control::{GatewayControlWake, control_work_change};
use crate::retry::retry_write_on_conflict;
use crate::time::unix_to_datetime;
use crate::turn_item_attempt::{self, TimeoutCandidateCursor};
use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_item::{turn_item_type_from_db, turn_item_type_to_db};
use pioneer_crud_conversations::{turn_event_reads, turn_liveness, turn_reads};
use pioneer_postgres_pools::PostgresDatabase;
use pioneer_protocol::{TurnItemExecutionClass, TurnItemTimeoutReason, TurnItemType};
use sea_orm::{ColumnTrait, Condition, EntityTrait, QueryFilter, QuerySelect, TransactionTrait};

#[derive(Debug, Clone)]
pub struct TimeoutCandidate {
    pub attempt_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub execution_class: TurnItemExecutionClass,
    pub attempt_number: i64,
    pub timeout_reason: TurnItemTimeoutReason,
    pub started_at_unix: i64,
    pub started_event_sequence: Option<i64>,
    pub last_heartbeat_at_unix: Option<i64>,
    pub lease_expires_at_unix: Option<i64>,
    pub idle_deadline_at_unix: Option<i64>,
    pub hard_deadline_at_unix: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLivenessRecord {
    pub turn_id: String,
    pub thread_id: String,
    pub last_activity_sequence: i64,
    pub last_activity_kind: String,
    pub last_activity_item_id: Option<String>,
    pub last_activity_item_type: Option<String>,
    pub last_activity_at_unix: i64,
}

#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
pub struct TurnItemAttemptDeadlines {
    pub lease_expires_at_unix: Option<i64>,
    pub idle_deadline_at_unix: Option<i64>,
    pub hard_deadline_at_unix: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct RunningAttemptDeadlineRepairCandidate {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub execution_class: TurnItemExecutionClass,
    pub started_at_unix: i64,
}

#[derive(Debug, Clone)]
pub struct RunningTurnItemAttempt {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub execution_class: TurnItemExecutionClass,
    pub started_at_unix: i64,
    pub last_heartbeat_at_unix: Option<i64>,
}

pub struct AttemptSupervision<'a> {
    connection: &'a PostgresDatabase,
}

impl<'a> AttemptSupervision<'a> {
    pub fn new(connection: &'a PostgresDatabase) -> Self {
        Self { connection }
    }

    pub async fn configure_turn_item_attempt_deadlines(
        &self,
        turn_id: &str,
        item_id: &str,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Turn(turn_id.into())];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            retry_write_on_conflict(|| async {
                turn_item_attempt::configure_running_attempt_deadlines(
                    self.connection,
                    turn_id,
                    item_id,
                    unix_to_datetime(heartbeat_at_unix),
                    lease_expires_at_unix.map(unix_to_datetime),
                    idle_deadline_at_unix.map(unix_to_datetime),
                    hard_deadline_at_unix.map(unix_to_datetime),
                )
                .await
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn heartbeat_turn_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
    ) -> Result<bool> {
        self.heartbeat_turn_item_attempt_with_activity_kind(
            turn_id,
            item_id,
            item_type,
            heartbeat_at_unix,
            lease_expires_at_unix,
            idle_deadline_at_unix,
            hard_deadline_at_unix,
            "item/heartbeat",
        )
        .await
    }

    pub async fn heartbeat_turn_item_attempt_with_activity_kind(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        heartbeat_at_unix: i64,
        lease_expires_at_unix: Option<i64>,
        idle_deadline_at_unix: Option<i64>,
        hard_deadline_at_unix: Option<i64>,
        activity_kind: &str,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Activity(turn_id.into())];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            retry_write_on_conflict(|| async {
                let heartbeat_at = unix_to_datetime(heartbeat_at_unix);
                let updated = turn_item_attempt::heartbeat_running_attempt(
                    self.connection,
                    turn_id,
                    item_id,
                    heartbeat_at,
                    lease_expires_at_unix.map(unix_to_datetime),
                    idle_deadline_at_unix.map(unix_to_datetime),
                    hard_deadline_at_unix.map(unix_to_datetime),
                )
                .await?;
                if !updated {
                    return Ok(false);
                }

                // An owner lease proves only that the supervising loop is alive;
                // it is not causal work progress and must not replace the Turn's
                // durable progress frontier. Confirmed external activity is
                // recorded through the explicit activity kind below.
                if activity_kind == "item/heartbeat" {
                    return Ok(true);
                }

                let Some(turn_model) =
                    turn_reads::find_turn_by_id(self.connection, turn_id).await?
                else {
                    return Ok(true);
                };
                let source_sequence =
                    turn_event_reads::latest_event_for_turn(self.connection, turn_id)
                        .await?
                        .map(|event| event.sequence)
                        .unwrap_or(0);
                turn_liveness::observe_activity(
                    self.connection,
                    turn_liveness::TurnLivenessObservation {
                        turn_id: turn_id.to_owned(),
                        thread_id: turn_model.thread_id,
                        activity_sequence: source_sequence,
                        activity_kind: activity_kind.to_owned(),
                        item_id: Some(item_id.to_owned()),
                        item_type: Some(turn_item_type_to_db(item_type).to_owned()),
                        observed_at: heartbeat_at,
                    },
                )
                .await?;
                Ok(true)
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn observe_turn_runtime_activity(
        &self,
        turn_id: &str,
        activity_kind: &str,
        observed_at_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![GatewayControlWake::Activity(turn_id.into())];
        let cli_change = control_work_change(self.connection, "gateway.control", &control_wakes);
        let cli_result = async {
            retry_write_on_conflict(|| async {
                let Some(turn_model) =
                    turn_reads::find_turn_by_id(self.connection, turn_id).await?
                else {
                    return Ok(false);
                };
                let source_sequence =
                    turn_event_reads::latest_event_for_turn(self.connection, turn_id)
                        .await?
                        .map(|event| event.sequence)
                        .unwrap_or(0);
                turn_liveness::observe_activity(
                    self.connection,
                    turn_liveness::TurnLivenessObservation {
                        turn_id: turn_id.to_owned(),
                        thread_id: turn_model.thread_id,
                        activity_sequence: source_sequence,
                        activity_kind: activity_kind.to_owned(),
                        item_id: None,
                        item_type: None,
                        observed_at: unix_to_datetime(observed_at_unix),
                    },
                )
                .await
            })
            .await
        }
        .await;
        if let Ok(changed) = &cli_result {
            cli_change.completed(*changed);
        }
        cli_result
    }

    pub async fn get_turn_liveness(&self, turn_id: &str) -> Result<Option<TurnLivenessRecord>> {
        Ok(turn_liveness::find_by_turn_id(self.connection, turn_id)
            .await?
            .map(|row| TurnLivenessRecord {
                turn_id: row.turn_id,
                thread_id: row.thread_id,
                last_activity_sequence: row.last_activity_sequence,
                last_activity_kind: row.last_activity_kind,
                last_activity_item_id: row.last_activity_item_id,
                last_activity_item_type: row.last_activity_item_type,
                last_activity_at_unix: row.last_activity_at.timestamp(),
            }))
    }

    pub async fn list_timeout_candidates(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        self.list_timeout_candidate_page(now_unix, limit, None)
            .await
            .map(|(rows, _)| rows)
    }

    pub async fn list_timeout_candidate_page(
        &self,
        now_unix: i64,
        limit: u64,
        after: Option<&TimeoutCandidateCursor>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<TimeoutCandidateCursor>)> {
        self.list_timeout_candidate_page_scoped(None, now_unix, limit, after)
            .await
    }

    pub async fn list_timeout_candidate_page_for_turn(
        &self,
        turn_id: &str,
        now_unix: i64,
        limit: u64,
        after: Option<&TimeoutCandidateCursor>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<TimeoutCandidateCursor>)> {
        self.list_timeout_candidate_page_scoped(Some(turn_id), now_unix, limit, after)
            .await
    }

    async fn list_timeout_candidate_page_scoped(
        &self,
        turn_id: Option<&str>,
        now_unix: i64,
        limit: u64,
        after: Option<&TimeoutCandidateCursor>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<TimeoutCandidateCursor>)> {
        let (rows, next) = turn_item_attempt::list_expired_running_attempt_page_scoped(
            self.connection,
            unix_to_datetime(now_unix),
            limit,
            after,
            turn_id,
        )
        .await?;

        let candidates = rows
            .into_iter()
            .map(|row| {
                let execution_class = row.execution_class.with_context(|| {
                    format!("expired attempt `{}` has no execution class", row.id)
                })?;
                Ok(TimeoutCandidate {
                    attempt_id: row.id,
                    turn_id: row.turn_id,
                    item_id: row.item_id,
                    item_type: row.item_type,
                    execution_class,
                    attempt_number: row.attempt_number,
                    timeout_reason: infer_timeout_reason(
                        row.lease_expires_at,
                        row.idle_deadline_at,
                        row.hard_deadline_at,
                        now_unix,
                    ),
                    started_at_unix: row.started_at.timestamp(),
                    started_event_sequence: row.started_event_sequence,
                    last_heartbeat_at_unix: row.last_heartbeat_at.map(|value| value.timestamp()),
                    lease_expires_at_unix: row.lease_expires_at.map(|value| value.timestamp()),
                    idle_deadline_at_unix: row.idle_deadline_at.map(|value| value.timestamp()),
                    hard_deadline_at_unix: row.hard_deadline_at.map(|value| value.timestamp()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((candidates, next))
    }

    pub async fn suppress_timeout_candidate_recovery(
        &self,
        candidate: &TimeoutCandidate,
        reason: &str,
        context: serde_json::Value,
        now_unix: i64,
    ) -> Result<bool> {
        retry_write_on_conflict(|| async {
            crate::timeout_recovery::suppress_timeout_recovery(
                self.connection,
                candidate.attempt_id.as_str(),
                reason,
                context.clone(),
                unix_to_datetime(now_unix),
            )
            .await
        })
        .await
    }

    pub async fn list_running_attempts_missing_deadlines(
        &self,
        limit: u64,
    ) -> Result<Vec<RunningAttemptDeadlineRepairCandidate>> {
        let rows = pioneer_entity::turn_item_attempt::Entity::find()
            .filter(pioneer_entity::turn_item_attempt::Column::Status.eq(ATTEMPT_STATUS_RUNNING))
            .filter(pioneer_entity::turn_item_attempt::Column::ExecutionClass.is_not_null())
            .filter(
                Condition::any()
                    .add(pioneer_entity::turn_item_attempt::Column::LeaseExpiresAt.is_null())
                    .add(pioneer_entity::turn_item_attempt::Column::IdleDeadlineAt.is_null())
                    .add(pioneer_entity::turn_item_attempt::Column::HardDeadlineAt.is_null()),
            )
            .limit(limit)
            .all(self.connection)
            .await
            .context("failed to list running attempts missing deadlines")?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let execution_class = row
                    .execution_class
                    .as_deref()
                    .and_then(turn_item_execution_class_from_db)
                    .with_context(|| {
                        format!("attempt `{}` has an invalid execution class", row.id)
                    })?;
                Ok(RunningAttemptDeadlineRepairCandidate {
                    turn_id: row.turn_id,
                    item_id: row.item_id,
                    item_type: turn_item_type_from_db(row.item_type.as_str())
                        .unwrap_or(TurnItemType::DynamicToolCall),
                    execution_class,
                    started_at_unix: row.started_at.timestamp(),
                })
            })
            .collect::<Result<Vec<_>>>()?)
    }

    pub async fn list_running_turn_item_attempts_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RunningTurnItemAttempt>> {
        let rows =
            turn_item_attempt::list_running_attempts_for_turn(self.connection, turn_id).await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                row.execution_class
                    .map(|execution_class| RunningTurnItemAttempt {
                        turn_id: row.turn_id,
                        item_id: row.item_id,
                        item_type: row.item_type,
                        execution_class,
                        started_at_unix: row.started_at.timestamp(),
                        last_heartbeat_at_unix: row
                            .last_heartbeat_at
                            .map(|value| value.timestamp()),
                    })
            })
            .collect())
    }

    pub async fn get_running_turn_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<RunningTurnItemAttempt>> {
        Ok(
            turn_item_attempt::find_latest_running_attempt(self.connection, turn_id, item_id)
                .await?
                .and_then(|row| {
                    row.execution_class
                        .map(|execution_class| RunningTurnItemAttempt {
                            turn_id: row.turn_id,
                            item_id: row.item_id,
                            item_type: row.item_type,
                            execution_class,
                            started_at_unix: row.started_at.timestamp(),
                            last_heartbeat_at_unix: row
                                .last_heartbeat_at
                                .map(|value| value.timestamp()),
                        })
                }),
        )
    }

    pub async fn list_running_turn_item_attempts_by_type(
        &self,
        item_type: TurnItemType,
        limit: u64,
    ) -> Result<Vec<RunningTurnItemAttempt>> {
        let rows = turn_item_attempt::list_running_attempts_by_item_type(
            self.connection,
            item_type,
            limit,
        )
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                row.execution_class
                    .map(|execution_class| RunningTurnItemAttempt {
                        turn_id: row.turn_id,
                        item_id: row.item_id,
                        item_type: row.item_type,
                        execution_class,
                        started_at_unix: row.started_at.timestamp(),
                        last_heartbeat_at_unix: row
                            .last_heartbeat_at
                            .map(|value| value.timestamp()),
                    })
            })
            .collect())
    }

    pub async fn list_running_turn_item_attempt_page_by_type(
        &self,
        item_type: TurnItemType,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        let upper = self
            .running_turn_item_attempt_upper_id_by_type(item_type)
            .await?
            .unwrap_or_default();
        self.list_running_turn_item_attempt_page_by_type_bounded(item_type, after_id, &upper, limit)
            .await
    }

    pub async fn running_turn_item_attempt_upper_id_by_type(
        &self,
        item_type: TurnItemType,
    ) -> Result<Option<String>> {
        turn_item_attempt::running_attempt_upper_id_by_item_type(self.connection, item_type).await
    }

    pub async fn list_running_turn_item_attempt_page_by_type_bounded(
        &self,
        item_type: TurnItemType,
        after_id: Option<&str>,
        upper_id: &str,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        self.list_running_turn_item_attempt_page_by_type_bounded_scoped(
            None, item_type, after_id, upper_id, limit,
        )
        .await
    }

    pub async fn list_running_turn_item_attempt_page_by_type_bounded_for_turn(
        &self,
        turn_id: &str,
        item_type: TurnItemType,
        after_id: Option<&str>,
        upper_id: &str,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        self.list_running_turn_item_attempt_page_by_type_bounded_scoped(
            Some(turn_id),
            item_type,
            after_id,
            upper_id,
            limit,
        )
        .await
    }

    async fn list_running_turn_item_attempt_page_by_type_bounded_scoped(
        &self,
        turn_id: Option<&str>,
        item_type: TurnItemType,
        after_id: Option<&str>,
        upper_id: &str,
        limit: u64,
    ) -> Result<(Vec<RunningTurnItemAttempt>, Option<String>)> {
        let (rows, next) =
            turn_item_attempt::list_running_attempt_page_by_item_type_bounded_scoped(
                self.connection,
                item_type,
                after_id,
                upper_id,
                limit,
                turn_id,
            )
            .await?;
        let attempts = rows
            .into_iter()
            .filter_map(|row| {
                row.execution_class
                    .map(|execution_class| RunningTurnItemAttempt {
                        turn_id: row.turn_id,
                        item_id: row.item_id,
                        item_type: row.item_type,
                        execution_class,
                        started_at_unix: row.started_at.timestamp(),
                        last_heartbeat_at_unix: row
                            .last_heartbeat_at
                            .map(|value| value.timestamp()),
                    })
            })
            .collect();
        Ok((attempts, next))
    }

    pub async fn list_unqueued_timeout_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        self.list_unqueued_timeout_candidate_page(limit, None, None)
            .await
            .map(|(rows, _)| rows)
    }

    pub async fn list_unqueued_timeout_candidate_page(
        &self,
        limit: u64,
        after: Option<&str>,
        upper: Option<&str>,
    ) -> Result<(Vec<TimeoutCandidate>, Option<String>)> {
        let (rows, next) = turn_item_attempt::list_timed_out_without_recovery_page(
            self.connection,
            limit,
            after,
            upper,
        )
        .await?;
        Ok((
            rows.into_iter()
                .map(|row| TimeoutCandidate {
                    attempt_id: row.id,
                    turn_id: row.turn_id,
                    item_id: row.item_id,
                    item_type: row.item_type,
                    execution_class: row.execution_class,
                    attempt_number: row.attempt_number,
                    timeout_reason: row.timeout_reason,
                    started_at_unix: row.started_at.timestamp(),
                    started_event_sequence: row.started_event_sequence,
                    last_heartbeat_at_unix: row.last_heartbeat_at.map(|value| value.timestamp()),
                    lease_expires_at_unix: row.lease_expires_at.map(|value| value.timestamp()),
                    idle_deadline_at_unix: row.idle_deadline_at.map(|value| value.timestamp()),
                    hard_deadline_at_unix: row.hard_deadline_at.map(|value| value.timestamp()),
                })
                .collect(),
            next,
        ))
    }

    pub async fn transition_timeout_candidate(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Turn(candidate.turn_id.clone()),
            GatewayControlWake::Supervision,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let now = unix_to_datetime(now_unix);
            let snapshot = turn_item_attempt::RunningAttemptSnapshot {
                id: candidate.attempt_id.clone(),
                turn_id: candidate.turn_id.clone(),
                item_id: candidate.item_id.clone(),
                item_type: candidate.item_type,
                execution_class: Some(candidate.execution_class),
                attempt_number: candidate.attempt_number,
                started_at: unix_to_datetime(candidate.started_at_unix),
                started_event_sequence: candidate.started_event_sequence,
                last_heartbeat_at: candidate.last_heartbeat_at_unix.map(unix_to_datetime),
                lease_expires_at: candidate.lease_expires_at_unix.map(unix_to_datetime),
                idle_deadline_at: candidate.idle_deadline_at_unix.map(unix_to_datetime),
                hard_deadline_at: candidate.hard_deadline_at_unix.map(unix_to_datetime),
            };
            let Some(prepared) = turn_item_attempt::prepare_running_attempt_timeout(
                self.connection,
                &snapshot,
                candidate.timeout_reason,
                now,
            )
            .await?
            else {
                return Ok(false);
            };
            retry_write_on_conflict(|| {
                let prepared = prepared.clone();
                async move {
                    let tx = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin timeout transition transaction")?;
                    let transitioned =
                        turn_item_attempt::transition_prepared_running_attempt_to_timed_out(
                            &tx, prepared,
                        )
                        .await?;

                    if !transitioned {
                        tx.rollback()
                            .await
                            .context("failed to rollback timeout transition transaction")?;
                        return Ok(false);
                    }

                    tx.commit()
                        .await
                        .context("failed to commit timeout transition transaction")?;

                    Ok(true)
                }
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn get_timed_out_attempt_candidate(
        &self,
        attempt_id: &str,
    ) -> Result<Option<TimeoutCandidate>> {
        let Some(row) =
            turn_item_attempt::find_timed_out_attempt_by_id(self.connection, attempt_id).await?
        else {
            return Ok(None);
        };
        Ok(Some(TimeoutCandidate {
            attempt_id: row.id,
            turn_id: row.turn_id,
            item_id: row.item_id,
            item_type: row.item_type,
            execution_class: row.execution_class,
            attempt_number: row.attempt_number,
            timeout_reason: row.timeout_reason,
            started_at_unix: row.started_at.timestamp(),
            started_event_sequence: row.started_event_sequence,
            last_heartbeat_at_unix: row.last_heartbeat_at.map(|value| value.timestamp()),
            lease_expires_at_unix: row.lease_expires_at.map(|value| value.timestamp()),
            idle_deadline_at_unix: row.idle_deadline_at.map(|value| value.timestamp()),
            hard_deadline_at_unix: row.hard_deadline_at.map(|value| value.timestamp()),
        }))
    }

    pub async fn reactivate_timed_out_attempt(
        &self,
        attempt_id: &str,
        heartbeat_at_unix: i64,
        deadlines: TurnItemAttemptDeadlines,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Timeouts,
            GatewayControlWake::Supervision,
        ];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let attempt_id = attempt_id.to_owned();
            let Some(prepared) = turn_item_attempt::prepare_timed_out_attempt_reactivation(
                self.connection,
                attempt_id.as_str(),
            )
            .await?
            else {
                return Ok(false);
            };
            let heartbeat_at = unix_to_datetime(heartbeat_at_unix);
            let deadlines = turn_item_attempt::AttemptDeadlines {
                lease_expires_at: deadlines.lease_expires_at_unix.map(unix_to_datetime),
                idle_deadline_at: deadlines.idle_deadline_at_unix.map(unix_to_datetime),
                hard_deadline_at: deadlines.hard_deadline_at_unix.map(unix_to_datetime),
            };
            retry_write_on_conflict(|| {
                let prepared = prepared.clone();
                let deadlines = deadlines.clone();
                async move {
                    let transaction =
                        self.connection.begin().await.context(
                            "failed to begin timed_out attempt reactivation transaction",
                        )?;
                    let reactivated = turn_item_attempt::reactivate_prepared_timed_out_attempt(
                        &transaction,
                        prepared,
                        heartbeat_at,
                        deadlines,
                    )
                    .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit timed_out attempt reactivation")?;
                    Ok(reactivated)
                }
            })
            .await
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }
}

fn infer_timeout_reason(
    lease_expires_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    idle_deadline_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    hard_deadline_at: Option<sea_orm::entity::prelude::DateTimeWithTimeZone>,
    now_unix: i64,
) -> TurnItemTimeoutReason {
    let now = unix_to_datetime(now_unix);
    if hard_deadline_at.is_some_and(|deadline| deadline <= now) {
        return TurnItemTimeoutReason::HardDeadlineExceeded;
    }
    if idle_deadline_at.is_some_and(|deadline| deadline <= now) {
        return TurnItemTimeoutReason::IdleDeadlineExceeded;
    }
    if lease_expires_at.is_some_and(|deadline| deadline <= now) {
        return TurnItemTimeoutReason::LeaseExpired;
    }
    TurnItemTimeoutReason::HardDeadlineExceeded
}

#[cfg(test)]
#[path = "attempt_supervision_tests.rs"]
mod tests;
