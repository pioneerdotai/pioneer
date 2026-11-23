//! Terminal Item payload and attempt repair over the scoped database.
use crate::attempt_values::*;
use crate::turn_item_terminal::{
    TurnItemTerminalState, terminalize_turn_item_payload, tool_call_status,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::{read_model_repair, turn_payload};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    PostgresReadClass, PostgresWriteClass, is_retryable_database_conflict, retry_with_backoff,
};
use pioneer_protocol::{ToolCallStatus, TurnItem, TurnItemTimeoutReason};
#[cfg(test)]
use sea_orm::ConnectionTrait;
use sea_orm::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    time::{Duration, Instant, SystemTime},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepairSummary {
    pub detected: usize,
    pub repaired: usize,
    pub remaining: usize,
}

/// Stable, bounded phases of deterministic read-model repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReadModelRepairStage {
    TerminalPayloadCheckpointLoad,
    TerminalPayloadCheckpointReset,
    TerminalPayloadFullScanBatchLoad,
    TerminalPayloadFullScanBatchEvaluate,
    TerminalPayloadFullScanBatchCommit,
    TerminalPayloadFullScanComplete,
    TerminalPayloadIncrementalBegin,
    TerminalPayloadIncrementalDirtyBatchLoad,
    TerminalPayloadIncrementalSourceBatchLoad,
    TerminalPayloadIncrementalBatchEvaluate,
    TerminalPayloadIncrementalBatchCommit,
    TerminalPayloadIncrementalComplete,
    TerminalTurnRunningAttempts,
    TerminalTasksMissingCompletedAt,
    TerminalRunsMissingCompletedAt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadModelRepairStageTiming {
    pub stage: ReadModelRepairStage,
    pub started_at: SystemTime,
    pub duration: Duration,
    pub operations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadModelRepairReport {
    pub summary: RepairSummary,
    pub stage_timings: Vec<ReadModelRepairStageTiming>,
}

#[derive(Debug)]
struct ReadModelRepairStageAggregate {
    started_at: SystemTime,
    duration: Duration,
    operations: u64,
}

#[derive(Debug, Default)]
pub struct ReadModelRepairTimings {
    stages: BTreeMap<ReadModelRepairStage, ReadModelRepairStageAggregate>,
}

impl ReadModelRepairTimings {
    pub async fn observe<T>(
        &mut self,
        stage: ReadModelRepairStage,
        operation: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let started_at = SystemTime::now();
        let started = Instant::now();
        let result = operation.await;
        self.record(stage, started_at, started.elapsed());
        result
    }

    fn observe_sync<T>(
        &mut self,
        stage: ReadModelRepairStage,
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let started_at = SystemTime::now();
        let started = Instant::now();
        let result = operation();
        self.record(stage, started_at, started.elapsed());
        result
    }

    fn record(&mut self, stage: ReadModelRepairStage, started_at: SystemTime, duration: Duration) {
        self.stages
            .entry(stage)
            .and_modify(|aggregate| {
                aggregate.duration = aggregate.duration.saturating_add(duration);
                aggregate.operations = aggregate.operations.saturating_add(1);
            })
            .or_insert(ReadModelRepairStageAggregate {
                started_at,
                duration,
                operations: 1,
            });
    }

    pub fn into_stage_timings(self) -> Vec<ReadModelRepairStageTiming> {
        self.stages
            .into_iter()
            .map(|(stage, aggregate)| ReadModelRepairStageTiming {
                stage,
                started_at: aggregate.started_at,
                duration: aggregate.duration,
                operations: aggregate.operations,
            })
            .collect()
    }
}

const READ_MODEL_REPAIR_BATCH_SIZE: u64 = 32;
pub const READ_MODEL_REPAIR_ALGORITHM_VERSION: i64 = 1;

fn is_terminal_turn_item_status(status: Option<&str>) -> bool {
    matches!(
        status,
        Some(
            TURN_ITEM_STATUS_COMPLETED
                | TURN_ITEM_STATUS_FAILED
                | TURN_ITEM_STATUS_TIMED_OUT
                | TURN_ITEM_STATUS_CANCELLED
        )
    )
}

pub fn repaired_terminal_turn_item_payload(
    row: &pioneer_entity::turn_item::Model,
) -> Result<Option<String>> {
    if !is_terminal_turn_item_status(row.status.as_deref()) {
        return Ok(None);
    }
    let mut item: TurnItem = turn_payload::decode_item(&row).with_context(|| {
        format!(
            "failed to decode turn_item payload during repair (turn `{}` item `{}`)",
            row.turn_id, row.item_id
        )
    })?;
    if tool_call_status(&item) != Some(ToolCallStatus::InProgress) {
        return Ok(None);
    }
    let terminal_state = match row.status.as_deref() {
        Some(TURN_ITEM_STATUS_COMPLETED) => TurnItemTerminalState::Completed,
        Some(TURN_ITEM_STATUS_TIMED_OUT) => TurnItemTerminalState::TimedOut {
            reason: TurnItemTimeoutReason::HardDeadlineExceeded,
        },
        Some(TURN_ITEM_STATUS_CANCELLED) => TurnItemTerminalState::Cancelled {
            reason: Some("read_model_invariant_repair".to_owned()),
        },
        _ => TurnItemTerminalState::Failed {
            reason: Some("read_model_invariant_repair".to_owned()),
        },
    };
    terminalize_turn_item_payload(&mut item, terminal_state);
    serde_json::to_string(&item)
        .context("failed to encode repaired turn_item payload")
        .map(Some)
}

impl RepairSummary {
    pub fn merge(&mut self, phase: Self) {
        self.detected = self.detected.saturating_add(phase.detected);
        self.repaired = self.repaired.saturating_add(phase.repaired);
        self.remaining = self.remaining.saturating_add(phase.remaining);
    }
}

/// A canonical repair may refresh an episodic source in the same transaction.
pub trait EpisodicRepair: Sync {
    type Prepared: Clone + Send + Sync;

    fn prepare<'a>(
        &'a self,
        row: &'a pioneer_entity::turn_item::Model,
        payload_json: &'a str,
    ) -> impl Future<Output = Result<Option<Self::Prepared>>> + Send + 'a;

    fn commit<'a>(
        &'a self,
        transaction: &'a DatabaseTransaction,
        prepared: &'a Self::Prepared,
        now: DateTimeWithTimeZone,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    fn committed(&self);
}

pub struct ExecutionReadModelRepair<'a, E> {
    connection: &'a PostgresDatabase,
    episodic: E,
}

impl<'a, E: EpisodicRepair> ExecutionReadModelRepair<'a, E> {
    pub fn new(connection: &'a PostgresDatabase, episodic: E) -> Self {
        Self {
            connection,
            episodic,
        }
    }

    async fn run_background_database_quantum<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if self.connection.read_class() != PostgresReadClass::Maintenance
            || self.connection.write_class() != PostgresWriteClass::Maintenance
        {
            bail!("background database quantum requires maintenance-scoped reads and writes");
        }
        retry_with_backoff(
            operation,
            |error| {
                is_retryable_database_conflict(error)
                    || error
                        .downcast_ref::<turn_payload::PreflightRace>()
                        .is_some()
            },
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn repair_terminal_turn_item_payloads(
        &self,
        timings: &mut ReadModelRepairTimings,
    ) -> Result<RepairSummary> {
        let mut summary = RepairSummary::default();
        let checkpoint = self
            .ensure_terminal_payload_repair_checkpoint(timings)
            .await?;
        if checkpoint.full_scan_status == read_model_repair::STATUS_RUNNING {
            summary.merge(
                self.run_terminal_payload_full_scan(checkpoint, timings)
                    .await?,
            );
        } else if checkpoint.full_scan_status != read_model_repair::STATUS_COMPLETED {
            bail!(
                "unsupported read-model repair full-scan status `{}`",
                checkpoint.full_scan_status
            );
        }

        let checkpoint = timings
            .observe(
                ReadModelRepairStage::TerminalPayloadCheckpointLoad,
                self.load_terminal_payload_repair_checkpoint(),
            )
            .await?
            .context("terminal payload repair checkpoint disappeared after full scan")?;
        let resumed_incremental =
            checkpoint.incremental_status == read_model_repair::STATUS_RUNNING;
        if resumed_incremental {
            summary.merge(
                self.run_terminal_payload_incremental_pass(checkpoint, timings)
                    .await?,
            );
        } else if checkpoint.incremental_status != read_model_repair::STATUS_COMPLETED {
            bail!(
                "unsupported read-model repair incremental status `{}`",
                checkpoint.incremental_status
            );
        }

        // A crashed process may have left a pass whose high-watermark predates
        // later writes. Finish that pass first, then take one fresh snapshot in
        // the same startup so those writes do not wait for another restart.
        summary.merge(
            self.start_terminal_payload_incremental_pass(timings)
                .await?,
        );
        Ok(summary)
    }

    async fn load_terminal_payload_repair_checkpoint(
        &self,
    ) -> Result<Option<read_model_repair::Checkpoint>> {
        self.run_background_database_quantum(|| async {
            read_model_repair::load_checkpoint(
                self.connection,
                read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
            )
            .await
        })
        .await
    }

    async fn ensure_terminal_payload_repair_checkpoint(
        &self,
        timings: &mut ReadModelRepairTimings,
    ) -> Result<read_model_repair::Checkpoint> {
        let existing = timings
            .observe(
                ReadModelRepairStage::TerminalPayloadCheckpointLoad,
                self.load_terminal_payload_repair_checkpoint(),
            )
            .await?;
        if existing.as_ref().is_some_and(|checkpoint| {
            checkpoint.algorithm_version == READ_MODEL_REPAIR_ALGORITHM_VERSION
        }) {
            return existing.context("checked read-model repair checkpoint is missing");
        }

        timings
            .observe(
                ReadModelRepairStage::TerminalPayloadCheckpointReset,
                self.run_background_database_quantum(|| async {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin read-model repair checkpoint transaction")?;
                    let high_watermark = pioneer_entity::turn_item::Entity::find()
                        .filter(pioneer_entity::turn_item::Column::Status.is_in([
                            TURN_ITEM_STATUS_COMPLETED,
                            TURN_ITEM_STATUS_FAILED,
                            TURN_ITEM_STATUS_TIMED_OUT,
                            TURN_ITEM_STATUS_CANCELLED,
                        ]))
                        .order_by_desc(pioneer_entity::turn_item::Column::Id)
                        .one(&transaction)
                        .await
                        .context("failed to load terminal turn_item repair high-watermark")?
                        .map(|row| row.id);
                    read_model_repair::reset_full_scan(
                        &transaction,
                        read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                        READ_MODEL_REPAIR_ALGORITHM_VERSION,
                        high_watermark.as_deref(),
                    )
                    .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit read-model repair checkpoint transaction")?;
                    Ok(())
                }),
            )
            .await?;

        timings
            .observe(
                ReadModelRepairStage::TerminalPayloadCheckpointLoad,
                self.load_terminal_payload_repair_checkpoint(),
            )
            .await?
            .context("initialized read-model repair checkpoint is missing")
    }

    async fn run_terminal_payload_full_scan(
        &self,
        checkpoint: read_model_repair::Checkpoint,
        timings: &mut ReadModelRepairTimings,
    ) -> Result<RepairSummary> {
        if checkpoint.algorithm_version != READ_MODEL_REPAIR_ALGORITHM_VERSION {
            bail!("cannot run terminal payload full scan for a stale algorithm version");
        }
        let Some(high_watermark) = checkpoint.full_scan_high_watermark_id else {
            timings
                .observe(
                    ReadModelRepairStage::TerminalPayloadFullScanComplete,
                    self.run_background_database_quantum(|| async {
                        let transaction = self
                            .connection
                            .begin()
                            .await
                            .context("failed to begin empty read-model full-scan completion")?;
                        read_model_repair::complete_full_scan(
                            &transaction,
                            read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                            READ_MODEL_REPAIR_ALGORITHM_VERSION,
                        )
                        .await?;
                        transaction
                            .commit()
                            .await
                            .context("failed to commit empty read-model full scan")?;
                        Ok(())
                    }),
                )
                .await?;
            return Ok(RepairSummary::default());
        };

        let mut summary = RepairSummary::default();
        let mut cursor = checkpoint.full_scan_cursor_id;
        loop {
            let cursor_for_query = cursor.clone();
            let high_watermark_for_query = high_watermark.clone();
            let rows = timings
                .observe(
                    ReadModelRepairStage::TerminalPayloadFullScanBatchLoad,
                    self.run_background_database_quantum(|| {
                        let cursor = cursor_for_query.clone();
                        let high_watermark = high_watermark_for_query.clone();
                        async move {
                            let query = pioneer_entity::turn_item::Entity::find()
                                .filter(pioneer_entity::turn_item::Column::Status.is_in([
                                    TURN_ITEM_STATUS_COMPLETED,
                                    TURN_ITEM_STATUS_FAILED,
                                    TURN_ITEM_STATUS_TIMED_OUT,
                                    TURN_ITEM_STATUS_CANCELLED,
                                ]))
                                .filter(pioneer_entity::turn_item::Column::Id.lte(high_watermark))
                                .order_by_asc(pioneer_entity::turn_item::Column::Id)
                                .limit(READ_MODEL_REPAIR_BATCH_SIZE);
                            let query = if let Some(cursor) = cursor {
                                query.filter(pioneer_entity::turn_item::Column::Id.gt(cursor))
                            } else {
                                query
                            };
                            query
                                .all(self.connection)
                                .await
                                .context("failed to load terminal turn_item repair batch")
                        }
                    }),
                )
                .await?;
            if rows.is_empty() {
                break;
            }
            let next_cursor = rows
                .last()
                .map(|row| row.id.clone())
                .context("non-empty terminal turn_item repair batch has no cursor")?;

            let rows = turn_payload::hydrate_items(self.connection, rows).await?;
            // JSON decoding and terminalization are deliberately outside the
            // database quantum so foreground requests keep access to the pool.
            let repairs = timings.observe_sync(
                ReadModelRepairStage::TerminalPayloadFullScanBatchEvaluate,
                || {
                    let mut repairs = Vec::new();
                    for row in rows {
                        if let Some(payload_json) = repaired_terminal_turn_item_payload(&row)? {
                            repairs.push((row, payload_json));
                        }
                    }
                    Ok(repairs)
                },
            )?;
            summary.detected = summary.detected.saturating_add(repairs.len());

            let repair_count = repairs.len();
            let mut prepared_repairs = Vec::with_capacity(repair_count);
            for (row, payload_json) in repairs {
                let episodic = self.episodic.prepare(&row, &payload_json).await?;
                prepared_repairs.push((
                    row,
                    pioneer_crud_payloads::storage::PreparedPayload::new_turn_item(&payload_json)?,
                    episodic,
                ));
            }
            let repairs_for_write = prepared_repairs;
            let cursor_for_write = next_cursor.clone();
            let repaired = timings
                .observe(
                    ReadModelRepairStage::TerminalPayloadFullScanBatchCommit,
                    self.run_background_database_quantum(|| {
                        let repairs = repairs_for_write.clone();
                        let cursor = cursor_for_write.clone();
                        async move {
                            let transaction = self.connection.begin().await.context(
                                "failed to begin terminal turn_item full-scan transaction",
                            )?;
                            let repaired = self
                                .apply_terminal_turn_item_payload_repairs(&transaction, repairs)
                                .await?;
                            read_model_repair::advance_full_scan_cursor(
                                &transaction,
                                read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                                READ_MODEL_REPAIR_ALGORITHM_VERSION,
                                cursor.as_str(),
                            )
                            .await?;
                            transaction.commit().await.context(
                                "failed to commit terminal turn_item full-scan transaction",
                            )?;
                            Ok(repaired)
                        }
                    }),
                )
                .await?;
            summary.repaired = summary.repaired.saturating_add(repaired);
            if repaired > 0 {
                self.episodic.committed();
            }
            summary.remaining = summary
                .remaining
                .saturating_add(repair_count.saturating_sub(repaired));
            cursor = Some(next_cursor);
        }

        timings
            .observe(
                ReadModelRepairStage::TerminalPayloadFullScanComplete,
                self.run_background_database_quantum(|| async {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin read-model full-scan completion")?;
                    read_model_repair::complete_full_scan(
                        &transaction,
                        read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                        READ_MODEL_REPAIR_ALGORITHM_VERSION,
                    )
                    .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit read-model full-scan completion")?;
                    Ok(())
                }),
            )
            .await?;
        Ok(summary)
    }

    async fn start_terminal_payload_incremental_pass(
        &self,
        timings: &mut ReadModelRepairTimings,
    ) -> Result<RepairSummary> {
        let checkpoint = timings
            .observe(
                ReadModelRepairStage::TerminalPayloadCheckpointLoad,
                self.load_terminal_payload_repair_checkpoint(),
            )
            .await?
            .context("terminal payload repair checkpoint is missing")?;
        if checkpoint.incremental_status != read_model_repair::STATUS_COMPLETED {
            bail!("cannot start an incremental repair pass while another pass is running");
        }
        timings
            .observe(
                ReadModelRepairStage::TerminalPayloadIncrementalBegin,
                self.run_background_database_quantum(|| async {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin incremental repair checkpoint transaction")?;
                    let high_watermark =
                        read_model_repair::current_change_generation(&transaction).await?;
                    read_model_repair::begin_incremental_pass(
                        &transaction,
                        read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                        READ_MODEL_REPAIR_ALGORITHM_VERSION,
                        high_watermark,
                    )
                    .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit incremental repair checkpoint transaction")?;
                    Ok(())
                }),
            )
            .await?;
        let checkpoint = timings
            .observe(
                ReadModelRepairStage::TerminalPayloadCheckpointLoad,
                self.load_terminal_payload_repair_checkpoint(),
            )
            .await?
            .context("started incremental repair checkpoint is missing")?;
        self.run_terminal_payload_incremental_pass(checkpoint, timings)
            .await
    }

    async fn run_terminal_payload_incremental_pass(
        &self,
        checkpoint: read_model_repair::Checkpoint,
        timings: &mut ReadModelRepairTimings,
    ) -> Result<RepairSummary> {
        if checkpoint.algorithm_version != READ_MODEL_REPAIR_ALGORITHM_VERSION
            || checkpoint.incremental_status != read_model_repair::STATUS_RUNNING
        {
            bail!("cannot resume an incompatible incremental read-model repair pass");
        }
        let high_watermark = checkpoint
            .incremental_high_watermark_generation
            .context("running incremental repair has no high-watermark")?;
        let mut cursor = checkpoint
            .incremental_cursor_generation
            .unwrap_or(checkpoint.last_completed_generation);
        let mut summary = RepairSummary::default();

        loop {
            let dirty_rows = timings
                .observe(
                    ReadModelRepairStage::TerminalPayloadIncrementalDirtyBatchLoad,
                    self.run_background_database_quantum(|| async {
                        read_model_repair::list_dirty_turn_items(
                            self.connection,
                            cursor,
                            high_watermark,
                            READ_MODEL_REPAIR_BATCH_SIZE,
                        )
                        .await
                    }),
                )
                .await?;
            if dirty_rows.is_empty() {
                break;
            }
            let next_cursor = dirty_rows
                .last()
                .map(|row| row.generation)
                .context("non-empty dirty repair batch has no cursor")?;
            let ids = dirty_rows
                .iter()
                .map(|row| row.turn_item_id.clone())
                .collect::<Vec<_>>();
            let source_rows = timings
                .observe(
                    ReadModelRepairStage::TerminalPayloadIncrementalSourceBatchLoad,
                    self.run_background_database_quantum(|| {
                        let ids = ids.clone();
                        async move {
                            pioneer_entity::turn_item::Entity::find()
                                .filter(pioneer_entity::turn_item::Column::Id.is_in(ids))
                                .all(self.connection)
                                .await
                                .context("failed to load dirty turn_items for incremental repair")
                        }
                    }),
                )
                .await?;

            let source_rows = turn_payload::hydrate_items(self.connection, source_rows).await?;
            // Payload parsing remains outside both the database permit and the
            // write transaction. Only dirty terminal rows pay this CPU cost.
            let repairs = timings.observe_sync(
                ReadModelRepairStage::TerminalPayloadIncrementalBatchEvaluate,
                || {
                    let mut repairs = HashMap::new();
                    for row in source_rows {
                        if let Some(payload_json) = repaired_terminal_turn_item_payload(&row)? {
                            repairs.insert(row.id.clone(), (row, payload_json));
                        }
                    }
                    Ok(repairs)
                },
            )?;
            summary.detected = summary.detected.saturating_add(repairs.len());

            let mut prepared_repairs = HashMap::with_capacity(repairs.len());
            for (id, (row, payload_json)) in repairs {
                let episodic = self.episodic.prepare(&row, &payload_json).await?;
                prepared_repairs.insert(
                    id,
                    (
                        row,
                        pioneer_crud_payloads::storage::PreparedPayload::new_turn_item(
                            &payload_json,
                        )?,
                        episodic,
                    ),
                );
            }
            let repairs = prepared_repairs;
            let dirty_for_write = dirty_rows.clone();
            let repairs_for_write = repairs.clone();
            let previous_cursor = cursor;
            let repaired = timings
                .observe(
                    ReadModelRepairStage::TerminalPayloadIncrementalBatchCommit,
                    self.run_background_database_quantum(|| {
                        let dirty_rows = dirty_for_write.clone();
                        let repairs = repairs_for_write.clone();
                        async move {
                            let transaction = self.connection.begin().await.context(
                                "failed to begin incremental terminal payload repair transaction",
                            )?;
                            let mut repaired = 0usize;
                            for dirty in &dirty_rows {
                                if let Some((row, payload_json, episodic)) = repairs.get(&dirty.turn_item_id) {
                                    let applied = self
                                        .apply_one_terminal_turn_item_payload_repair(
                                            &transaction,
                                            row,
                                            payload_json,
                                            episodic.as_ref(),
                                        )
                                        .await?;
                                    if applied {
                                        repaired = repaired.saturating_add(1);
                                        // The repair UPDATE itself fires the dirty
                                        // trigger. No other writer can interleave
                                        // inside this transaction, so remove that
                                        // self-generated entry as well.
                                        read_model_repair::clear_dirty_turn_item(
                                            &transaction,
                                            dirty.turn_item_id.as_str(),
                                        )
                                        .await?;
                                    }
                                } else {
                                    read_model_repair::clear_dirty_turn_item_if_unchanged(
                                        &transaction,
                                        dirty.turn_item_id.as_str(),
                                        dirty.generation,
                                    )
                                    .await?;
                                }
                            }
                            let unconsumed =
                                read_model_repair::count_dirty_turn_items_in_window(
                                    &transaction,
                                    previous_cursor,
                                    next_cursor,
                                )
                                .await?;
                            if unconsumed != 0 {
                                bail!(
                                    "incremental terminal payload repair retained {unconsumed} rows before its next cursor"
                                );
                            }
                            read_model_repair::advance_incremental_cursor(
                                &transaction,
                                read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                                READ_MODEL_REPAIR_ALGORITHM_VERSION,
                                high_watermark,
                                next_cursor,
                            )
                            .await?;
                            transaction.commit().await.context(
                                "failed to commit incremental terminal payload repair transaction",
                            )?;
                            Ok(repaired)
                        }
                    }),
                )
                .await?;
            summary.repaired = summary.repaired.saturating_add(repaired);
            if repaired > 0 {
                self.episodic.committed();
            }
            summary.remaining = summary
                .remaining
                .saturating_add(repairs.len().saturating_sub(repaired));
            cursor = next_cursor;
        }

        timings
            .observe(
                ReadModelRepairStage::TerminalPayloadIncrementalComplete,
                self.run_background_database_quantum(|| async {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin incremental repair completion")?;
                    read_model_repair::complete_incremental_pass(
                        &transaction,
                        read_model_repair::TERMINAL_TURN_ITEM_PAYLOAD_REPAIR_KEY,
                        READ_MODEL_REPAIR_ALGORITHM_VERSION,
                        high_watermark,
                    )
                    .await?;
                    transaction
                        .commit()
                        .await
                        .context("failed to commit incremental repair completion")?;
                    Ok(())
                }),
            )
            .await?;
        Ok(summary)
    }

    async fn apply_terminal_turn_item_payload_repairs(
        &self,
        transaction: &DatabaseTransaction,
        repairs: Vec<(
            pioneer_entity::turn_item::Model,
            pioneer_crud_payloads::storage::PreparedPayload,
            Option<E::Prepared>,
        )>,
    ) -> Result<usize> {
        let mut repaired = 0usize;
        for (row, payload_json, episodic) in repairs {
            let applied = self
                .apply_one_terminal_turn_item_payload_repair(
                    transaction,
                    &row,
                    &payload_json,
                    episodic.as_ref(),
                )
                .await?;
            if applied {
                repaired = repaired.saturating_add(1);
            }
        }
        Ok(repaired)
    }

    pub async fn apply_one_terminal_turn_item_payload_repair(
        &self,
        transaction: &DatabaseTransaction,
        row: &pioneer_entity::turn_item::Model,
        payload: &pioneer_crud_payloads::storage::PreparedPayload,
        episodic: Option<&E::Prepared>,
    ) -> Result<bool> {
        use sea_orm::sea_query::ExprTrait;

        let now: DateTimeWithTimeZone = chrono::Utc::now().into();
        payload.insert(transaction).await?;
        let updated = pioneer_entity::turn_item::Entity::update_many()
            .filter(pioneer_entity::turn_item::Column::Id.eq(row.id.clone()))
            .filter(pioneer_entity::turn_item::Column::Status.eq(row.status.clone()))
            .filter(
                pioneer_entity::turn_item::Column::PortableIdentity
                    .eq(row.portable_identity.clone()),
            )
            .filter(pioneer_entity::turn_item::Column::PortableRevision.eq(row.portable_revision))
            .col_expr(
                pioneer_entity::turn_item::Column::Payload,
                Expr::val(Option::<String>::None).cast_as("json"),
            )
            .col_expr(
                pioneer_entity::turn_item::Column::PayloadPayloadId,
                Expr::val(payload.id()),
            )
            .col_expr(
                pioneer_entity::turn_item::Column::UpdatedAt,
                Expr::value(now),
            )
            .exec(transaction)
            .await
            .context("failed to conditionally repair turn_item payload")?;

        let applied = updated.rows_affected == 1;
        if !applied {
            pioneer_crud_payloads::storage::delete(transaction, payload.id()).await?;
        }
        if applied && let Some(episodic) = episodic {
            self.episodic.commit(transaction, episodic, now).await?;
        }
        Ok(applied)
    }

    pub async fn repair_terminal_turn_running_attempts(&self) -> Result<RepairSummary> {
        let high_watermark = self
            .run_background_database_quantum(|| async {
                pioneer_entity::turn_item_attempt::Entity::find()
                    .filter(
                        pioneer_entity::turn_item_attempt::Column::Status
                            .eq(ATTEMPT_STATUS_RUNNING),
                    )
                    .order_by_desc(pioneer_entity::turn_item_attempt::Column::Id)
                    .one(self.connection)
                    .await
                    .context("failed to load running attempt repair high-watermark")
                    .map(|row| row.map(|row| row.id))
            })
            .await?;
        let Some(high_watermark) = high_watermark else {
            return Ok(RepairSummary::default());
        };

        let mut summary = RepairSummary::default();
        let mut cursor: Option<String> = None;
        loop {
            let cursor_for_query = cursor.clone();
            let high_watermark_for_query = high_watermark.clone();
            let (rows, terminal_turn_ids) = self
                .run_background_database_quantum(|| {
                    let cursor = cursor_for_query.clone();
                    let high_watermark = high_watermark_for_query.clone();
                    async move {
                        let query = pioneer_entity::turn_item_attempt::Entity::find()
                            .filter(
                                pioneer_entity::turn_item_attempt::Column::Status
                                    .eq(ATTEMPT_STATUS_RUNNING),
                            )
                            .filter(
                                pioneer_entity::turn_item_attempt::Column::Id.lte(high_watermark),
                            )
                            .order_by_asc(pioneer_entity::turn_item_attempt::Column::Id)
                            .limit(READ_MODEL_REPAIR_BATCH_SIZE);
                        let query = if let Some(cursor) = cursor {
                            query.filter(pioneer_entity::turn_item_attempt::Column::Id.gt(cursor))
                        } else {
                            query
                        };
                        let rows = query
                            .all(self.connection)
                            .await
                            .context("failed to load running attempt repair batch")?;
                        let turn_ids = rows
                            .iter()
                            .map(|row| row.turn_id.clone())
                            .collect::<HashSet<_>>();
                        let terminal_turn_ids = if turn_ids.is_empty() {
                            HashSet::new()
                        } else {
                            pioneer_entity::turn::Entity::find()
                                .filter(pioneer_entity::turn::Column::Id.is_in(turn_ids))
                                .filter(pioneer_entity::turn::Column::Status.is_in([
                                    "completed",
                                    "failed",
                                    "interrupted",
                                    "blocked",
                                ]))
                                .all(self.connection)
                                .await
                                .context("failed to load terminal turns for attempt repair")?
                                .into_iter()
                                .map(|turn| turn.id)
                                .collect()
                        };
                        Ok((rows, terminal_turn_ids))
                    }
                })
                .await?;
            if rows.is_empty() {
                break;
            }
            cursor = rows.last().map(|row| row.id.clone());
            let candidates = rows
                .into_iter()
                .filter(|row| terminal_turn_ids.contains(row.turn_id.as_str()))
                .collect::<Vec<_>>();
            summary.detected = summary.detected.saturating_add(candidates.len());

            if !candidates.is_empty() {
                let candidates_for_write = candidates.clone();
                let (repaired_rows, resolved) = self
                    .run_background_database_quantum(|| {
                        let candidates = candidates_for_write.clone();
                        async move {
                            let transaction = self.connection.begin().await.context(
                                "failed to begin running attempt repair transaction",
                            )?;
                            let turn_ids = candidates
                                .iter()
                                .map(|row| row.turn_id.clone())
                                .collect::<HashSet<_>>();
                            let still_terminal = pioneer_entity::turn::Entity::find()
                                .filter(pioneer_entity::turn::Column::Id.is_in(turn_ids))
                                .filter(pioneer_entity::turn::Column::Status.is_in([
                                    "completed",
                                    "failed",
                                    "interrupted",
                                    "blocked",
                                ]))
                                .all(&transaction)
                                .await
                                .context("failed to revalidate terminal turns for repair")?
                                .into_iter()
                                .map(|turn| turn.id)
                                .collect::<HashSet<_>>();
                            let now: DateTimeWithTimeZone = chrono::Utc::now().into();
                            let mut repaired_rows = 0usize;
                            let mut resolved = 0usize;

                            for attempt in candidates {
                                if !still_terminal.contains(attempt.turn_id.as_str()) {
                                    // The parent is live again, so the invariant no longer applies.
                                    resolved = resolved.saturating_add(1);
                                    continue;
                                }
                                let attempt_result =
                                    pioneer_entity::turn_item_attempt::Entity::update_many()
                                        .filter(
                                            pioneer_entity::turn_item_attempt::Column::Id
                                                .eq(attempt.id.clone()),
                                        )
                                        .filter(
                                            pioneer_entity::turn_item_attempt::Column::Status
                                                .eq(ATTEMPT_STATUS_RUNNING),
                                        )
                                        .col_expr(
                                            pioneer_entity::turn_item_attempt::Column::Status,
                                            Expr::value(ATTEMPT_STATUS_INTERRUPTED),
                                        )
                                        .col_expr(
                                            pioneer_entity::turn_item_attempt::Column::FailureReason,
                                            Expr::value(Some(
                                                "read_model_invariant_repair".to_owned(),
                                            )),
                                        )
                                        .col_expr(
                                            pioneer_entity::turn_item_attempt::Column::UpdatedAt,
                                            Expr::value(now.clone()),
                                        )
                                        .exec(&transaction)
                                        .await
                                        .context(
                                            "failed to interrupt running attempt during repair",
                                        )?;
                                if attempt_result.rows_affected == 0 {
                                    // A foreground writer already moved the attempt out of running.
                                    resolved = resolved.saturating_add(1);
                                    continue;
                                }
                                repaired_rows = repaired_rows.saturating_add(1);
                                resolved = resolved.saturating_add(1);

                                let item_result = pioneer_entity::turn_item::Entity::update_many()
                                    .filter(
                                        pioneer_entity::turn_item::Column::TurnId
                                            .eq(attempt.turn_id.clone()),
                                    )
                                    .filter(
                                        pioneer_entity::turn_item::Column::ItemId
                                            .eq(attempt.item_id.clone()),
                                    )
                                    .filter(
                                        pioneer_entity::turn_item::Column::ActiveAttemptId
                                            .eq(Some(attempt.id.clone())),
                                    )
                                    .filter(
                                        pioneer_entity::turn_item::Column::ActiveAttemptStatus
                                            .eq(Some(ATTEMPT_STATUS_RUNNING)),
                                    )
                                    .col_expr(
                                        pioneer_entity::turn_item::Column::ActiveAttemptStatus,
                                        Expr::value(Some(ATTEMPT_STATUS_INTERRUPTED)),
                                    )
                                    .col_expr(
                                        pioneer_entity::turn_item::Column::UpdatedAt,
                                        Expr::value(now.clone()),
                                    )
                                    .exec(&transaction)
                                    .await
                                    .context("failed to repair active turn_item attempt status")?;
                                if item_result.rows_affected > 0 {
                                    repaired_rows = repaired_rows.saturating_add(1);
                                }
                            }
                            transaction.commit().await.context(
                                "failed to commit running attempt repair transaction",
                            )?;
                            Ok((repaired_rows, resolved))
                        }
                    })
                    .await?;
                summary.repaired = summary.repaired.saturating_add(repaired_rows);
                summary.remaining = summary
                    .remaining
                    .saturating_add(candidates.len().saturating_sub(resolved));
            }
        }
        Ok(summary)
    }
}

#[cfg(test)]
#[path = "read_model_repair_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadModelInvariantKind {
    TerminalToolPayloadInProgress,
    TimedOutToolPayloadInProgress,
    TerminalTurnHasRunningAttempts,
    TerminalTaskMissingCompletedAt,
    TerminalRunMissingCompletedAt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadModelInvariantViolation {
    pub kind: ReadModelInvariantKind,
    pub entity_id: String,
    pub details: String,
}

pub async fn list_execution_invariant_violations(
    connection: &PostgresDatabase,
) -> Result<Vec<ReadModelInvariantViolation>> {
    let terminal_turn_item_rows = pioneer_entity::turn_item::Entity::find()
        .filter(pioneer_entity::turn_item::Column::Status.is_in([
            TURN_ITEM_STATUS_COMPLETED,
            TURN_ITEM_STATUS_FAILED,
            TURN_ITEM_STATUS_TIMED_OUT,
            TURN_ITEM_STATUS_CANCELLED,
        ]))
        .all(connection)
        .await
        .context("failed to list terminal turn_item rows for invariant check")?;

    let terminal_turn_item_rows =
        turn_payload::hydrate_items(connection, terminal_turn_item_rows).await?;
    let mut violations = Vec::new();
    for row in terminal_turn_item_rows {
        let item: TurnItem = turn_payload::decode_item(&row).with_context(|| {
            format!(
                "failed to decode turn_item payload during invariant check (turn `{}` item `{}`)",
                row.turn_id, row.item_id
            )
        })?;

        if tool_call_status(&item) == Some(ToolCallStatus::InProgress) {
            violations.push(ReadModelInvariantViolation {
                kind: if row.status.as_deref() == Some(TURN_ITEM_STATUS_TIMED_OUT) {
                    ReadModelInvariantKind::TimedOutToolPayloadInProgress
                } else {
                    ReadModelInvariantKind::TerminalToolPayloadInProgress
                },
                entity_id: format!("{}:{}", row.turn_id, row.item_id),
                details: format!(
                    "turn_item.status=`{}` while tool payload status is in_progress",
                    row.status.as_deref().unwrap_or("null")
                ),
            });
        }
    }

    let running_attempt_rows = pioneer_entity::turn_item_attempt::Entity::find()
        .filter(pioneer_entity::turn_item_attempt::Column::Status.eq(ATTEMPT_STATUS_RUNNING))
        .all(connection)
        .await
        .context("failed to list running attempts for invariant check")?;

    for running_attempt in running_attempt_rows {
        let Some(turn_model) =
            pioneer_entity::turn::Entity::find_by_id(running_attempt.turn_id.clone())
                .one(connection)
                .await
                .context("failed to load turn for running attempt invariant check")?
        else {
            continue;
        };
        if matches!(
            turn_model.status.as_str(),
            "completed" | "failed" | "interrupted" | "blocked"
        ) {
            violations.push(ReadModelInvariantViolation {
                kind: ReadModelInvariantKind::TerminalTurnHasRunningAttempts,
                entity_id: running_attempt.id,
                details: format!(
                    "turn `{}` is `{}` while attempt for item `{}` remains running",
                    turn_model.id, turn_model.status, running_attempt.item_id
                ),
            });
        }
    }

    Ok(violations)
}
