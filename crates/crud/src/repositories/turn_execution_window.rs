use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_execution_checkpoint, turn_execution_window, turn_item};
use pioneer_protocol::{
    ExecutionWindowExhaustionReason, ExecutionWindowStatus, TurnItem, TurnStatus, generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter,
    QueryOrder, Set, Unchanged,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::convention::{
    DB_ID_LEN, execution_window_exhaustion_reason_from_db,
    execution_window_exhaustion_reason_to_db, execution_window_status_from_db,
    execution_window_status_to_db, turn_item_type_from_db, turn_status_from_db, turn_status_to_db,
};

pub const TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnExecutionCheckpointKind {
    WindowExhausted,
    TurnBlocked,
    StartupRecovery,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowRecord {
    pub id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    pub exhaustion_reason: Option<ExecutionWindowExhaustionReason>,
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    pub provider_token_count: u64,
    pub metadata_json: serde_json::Value,
    pub started_at: DateTimeWithTimeZone,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTurnExecutionWindowRecord {
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub window_index: u32,
    pub status: ExecutionWindowStatus,
    pub exhaustion_reason: Option<ExecutionWindowExhaustionReason>,
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    pub provider_token_count: u64,
    pub metadata_json: serde_json::Value,
    pub started_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionWindowStatsRecord {
    pub agent_round_count: u32,
    pub tool_call_count: u32,
    pub provider_token_count: u64,
    pub metadata_json: serde_json::Value,
    pub completed_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnExecutionWindowUsageAggregateRecord {
    pub total_windows: u32,
    pub latest_window_index: u32,
    pub total_agent_rounds: u64,
    pub total_tool_calls: u64,
    pub total_wall_clock_ms: u64,
    pub wall_clock_window_count: u32,
    pub total_provider_tokens: u64,
    pub provider_token_usage_unknown: bool,
    pub consecutive_no_progress_windows: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnExecutionWindowTerminalItemCountsRecord {
    pub agent_round_count: u32,
    pub tool_call_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnExecutionCheckpointRecord {
    pub id: String,
    pub window_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub checkpoint_kind: TurnExecutionCheckpointKind,
    pub payload_json: serde_json::Value,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TurnExecutionDataCleanupRecord {
    pub checkpoints_deleted: u64,
    pub windows_deleted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTurnExecutionCheckpointRecord {
    /// Stable wire/checkpoint identity. Native execution-window transitions
    /// provide it so a continuation fence survives acknowledgement retries;
    /// legacy callers may leave it empty and receive a generated id.
    pub id: Option<String>,
    pub window_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub checkpoint_kind: TurnExecutionCheckpointKind,
    pub payload_json: serde_json::Value,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedTurnExecutionWindowInsert {
    model: turn_execution_window::ActiveModel,
    id: String,
    turn_id: String,
    window_index: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedTurnExecutionWindowStatsTransition {
    expected_status: ExecutionWindowStatus,
    expected_status_db: String,
    target_status: ExecutionWindowStatus,
    target_status_db: String,
    reason_db: Option<String>,
    stats: TurnExecutionWindowStatsRecord,
    metadata_json: String,
    provider_token_count: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedTurnExecutionWindowStatusTransition {
    expected_status: ExecutionWindowStatus,
    expected_status_db: String,
    target_status: ExecutionWindowStatus,
    target_status_db: String,
    updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedTurnExecutionCheckpointInsert {
    id: String,
    window_id: String,
    workspace_id: String,
    thread_id: String,
    turn_id: String,
    checkpoint_kind_db: String,
    payload_json_db: String,
    created_at: DateTimeWithTimeZone,
}

impl PreparedTurnExecutionCheckpointInsert {
    pub(crate) fn payload_bytes(&self) -> usize {
        self.payload_json_db.len()
    }

    pub(crate) fn with_window_id(mut self, window_id: String) -> Self {
        self.window_id = window_id;
        self
    }

    pub(crate) fn matches_model(&self, model: &turn_execution_checkpoint::Model) -> bool {
        model.id == self.id
            && model.window_id == self.window_id
            && model.workspace_id == self.workspace_id
            && model.thread_id == self.thread_id
            && model.turn_id == self.turn_id
            && model.checkpoint_kind == self.checkpoint_kind_db
            && model.payload_json == self.payload_json_db
    }
}

pub fn window_record_from_model(
    model: turn_execution_window::Model,
) -> Result<TurnExecutionWindowRecord> {
    let status = execution_window_status_from_db(model.status.as_str())
        .with_context(|| format!("invalid turn_execution_window status `{}`", model.status))?;
    let exhaustion_reason = model
        .exhaustion_reason
        .as_deref()
        .map(execution_window_exhaustion_reason_from_db)
        .transpose()
        .with_context(|| {
            format!(
                "invalid turn_execution_window exhaustion_reason for window `{}`",
                model.id
            )
        })?;
    let metadata_json = serde_json::from_str(model.metadata_json.as_str()).with_context(|| {
        format!(
            "invalid turn_execution_window metadata_json for window `{}`",
            model.id
        )
    })?;

    Ok(TurnExecutionWindowRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        window_index: u32::try_from(model.window_index)
            .context("turn_execution_window window_index is out of range")?,
        status,
        exhaustion_reason,
        agent_round_count: u32::try_from(model.agent_round_count)
            .context("turn_execution_window agent_round_count is out of range")?,
        tool_call_count: u32::try_from(model.tool_call_count)
            .context("turn_execution_window tool_call_count is out of range")?,
        provider_token_count: u64::try_from(model.provider_token_count)
            .context("turn_execution_window provider_token_count is out of range")?,
        metadata_json,
        started_at: model.started_at,
        completed_at: model.completed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

pub fn checkpoint_record_from_model(
    model: turn_execution_checkpoint::Model,
) -> Result<TurnExecutionCheckpointRecord> {
    let checkpoint_kind =
        checkpoint_kind_from_db(model.checkpoint_kind.as_str()).with_context(|| {
            format!(
                "invalid turn_execution_checkpoint checkpoint_kind `{}`",
                model.checkpoint_kind
            )
        })?;
    let payload_json = serde_json::from_str(model.payload_json.as_str()).with_context(|| {
        format!(
            "invalid turn_execution_checkpoint payload_json for checkpoint `{}`",
            model.id
        )
    })?;

    Ok(TurnExecutionCheckpointRecord {
        id: model.id,
        window_id: model.window_id,
        workspace_id: model.workspace_id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        checkpoint_kind,
        payload_json,
        created_at: model.created_at,
    })
}

pub fn new_window_active_model(
    record: NewTurnExecutionWindowRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<turn_execution_window::ActiveModel> {
    Ok(turn_execution_window::ActiveModel {
        id: Set(generate_id(DB_ID_LEN)),
        workspace_id: Set(record.workspace_id),
        thread_id: Set(record.thread_id),
        turn_id: Set(record.turn_id),
        window_index: Set(i64::from(record.window_index)),
        status: Set(execution_window_status_to_db(record.status)),
        exhaustion_reason: Set(record
            .exhaustion_reason
            .map(execution_window_exhaustion_reason_to_db)),
        agent_round_count: Set(i64::from(record.agent_round_count)),
        tool_call_count: Set(i64::from(record.tool_call_count)),
        provider_token_count: Set(i64::try_from(record.provider_token_count)
            .context("provider_token_count exceeds i64")?),
        metadata_json: Set(serde_json::to_string(&record.metadata_json)
            .context("failed to serialize execution-window metadata_json")?),
        started_at: Set(record.started_at),
        completed_at: Set(None),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
}

pub(crate) fn prepare_turn_execution_window_insert(
    record: NewTurnExecutionWindowRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<PreparedTurnExecutionWindowInsert> {
    let turn_id = record.turn_id.clone();
    let window_index = record.window_index;
    let model = new_window_active_model(record, created_at, updated_at)?;
    let id = active_model_id(&model.id)?;
    Ok(PreparedTurnExecutionWindowInsert {
        model,
        id,
        turn_id,
        window_index,
    })
}

pub(crate) fn prepare_turn_execution_window_stats_transition(
    expected_status: ExecutionWindowStatus,
    target_status: ExecutionWindowStatus,
    reason: Option<ExecutionWindowExhaustionReason>,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<PreparedTurnExecutionWindowStatsTransition> {
    let metadata_json = serde_json::to_string(&stats.metadata_json)
        .context("failed to serialize execution-window metadata_json")?;
    let provider_token_count =
        i64::try_from(stats.provider_token_count).context("provider_token_count exceeds i64")?;
    Ok(PreparedTurnExecutionWindowStatsTransition {
        expected_status,
        expected_status_db: execution_window_status_to_db(expected_status),
        target_status,
        target_status_db: execution_window_status_to_db(target_status),
        reason_db: reason.map(execution_window_exhaustion_reason_to_db),
        stats,
        metadata_json,
        provider_token_count,
    })
}

pub(crate) fn prepare_turn_execution_window_status_transition(
    expected_status: ExecutionWindowStatus,
    target_status: ExecutionWindowStatus,
    updated_at: DateTimeWithTimeZone,
) -> PreparedTurnExecutionWindowStatusTransition {
    PreparedTurnExecutionWindowStatusTransition {
        expected_status,
        expected_status_db: execution_window_status_to_db(expected_status),
        target_status,
        target_status_db: execution_window_status_to_db(target_status),
        updated_at,
    }
}

pub(crate) fn prepare_turn_execution_checkpoint_insert(
    record: NewTurnExecutionCheckpointRecord,
) -> Result<PreparedTurnExecutionCheckpointInsert> {
    let payload_json_db = serialize_checkpoint_payload(&record.payload_json)?;
    let checkpoint_kind_db = checkpoint_kind_to_db(record.checkpoint_kind);
    Ok(PreparedTurnExecutionCheckpointInsert {
        id: record.id.unwrap_or_else(|| generate_id(DB_ID_LEN)),
        window_id: record.window_id,
        workspace_id: record.workspace_id,
        thread_id: record.thread_id,
        turn_id: record.turn_id,
        checkpoint_kind_db,
        payload_json_db,
        created_at: record.created_at,
    })
}

impl PreparedTurnExecutionCheckpointInsert {
    fn active_model(&self) -> turn_execution_checkpoint::ActiveModel {
        turn_execution_checkpoint::ActiveModel {
            id: Set(self.id.clone()),
            window_id: Set(self.window_id.clone()),
            workspace_id: Set(self.workspace_id.clone()),
            thread_id: Set(self.thread_id.clone()),
            turn_id: Set(self.turn_id.clone()),
            checkpoint_kind: Set(self.checkpoint_kind_db.clone()),
            payload_json: Set(self.payload_json_db.clone()),
            created_at: Set(self.created_at),
        }
    }
}

pub async fn create_turn_execution_window<C: ConnectionTrait>(
    db: &C,
    record: NewTurnExecutionWindowRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<TurnExecutionWindowRecord> {
    let prepared = prepare_turn_execution_window_insert(record, created_at, updated_at)?;
    let id = prepared.id.clone();
    insert_prepared_turn_execution_window(db, prepared).await?;

    get_turn_execution_window(db, id.as_str())
        .await?
        .context("inserted turn_execution_window row is missing")
}

pub(crate) async fn insert_prepared_turn_execution_window<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnExecutionWindowInsert,
) -> Result<()> {
    enforce_next_window_index(db, prepared.turn_id.as_str(), prepared.window_index).await?;
    turn_execution_window::Entity::insert(prepared.model)
        .exec(db)
        .await
        .context("failed to insert turn_execution_window row")?;
    Ok(())
}

pub async fn get_turn_execution_window<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
) -> Result<Option<TurnExecutionWindowRecord>> {
    turn_execution_window::Entity::find_by_id(window_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn_execution_window row")?
        .map(window_record_from_model)
        .transpose()
}

pub(crate) async fn get_turn_execution_window_model<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
) -> Result<Option<turn_execution_window::Model>> {
    turn_execution_window::Entity::find_by_id(window_id.to_owned())
        .one(db)
        .await
        .context("failed to query raw turn_execution_window row")
}

pub async fn list_turn_execution_windows<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<TurnExecutionWindowRecord>> {
    turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_execution_window::Column::WindowIndex)
        .all(db)
        .await
        .context("failed to list turn_execution_window rows")?
        .into_iter()
        .map(window_record_from_model)
        .collect()
}

pub async fn latest_turn_execution_window<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnExecutionWindowRecord>> {
    turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_execution_window::Column::WindowIndex)
        .one(db)
        .await
        .context("failed to query latest turn_execution_window row")?
        .map(window_record_from_model)
        .transpose()
}

pub(crate) async fn latest_turn_execution_window_model<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_execution_window::Model>> {
    turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_execution_window::Column::WindowIndex)
        .one(db)
        .await
        .context("failed to query latest raw turn_execution_window row")
}

pub(crate) async fn window_metadata_string_matches<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    key: &'static str,
    expected: &str,
) -> Result<bool> {
    let json_path = format!("$.{key}");
    Ok(
        turn_execution_window::Entity::find_by_id(window_id.to_owned())
            .filter(sea_orm::sea_query::Expr::cust_with_values(
                "json_extract(metadata_json, ?) = ?",
                vec![json_path, expected.to_owned()],
            ))
            .one(db)
            .await
            .with_context(|| format!("failed to validate execution-window metadata key `{key}`"))?
            .is_some(),
    )
}

pub(crate) async fn window_metadata_string_is_present<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    key: &'static str,
) -> Result<bool> {
    let json_path = format!("$.{key}");
    Ok(
        turn_execution_window::Entity::find_by_id(window_id.to_owned())
            .filter(sea_orm::sea_query::Expr::cust_with_values(
                "json_type(metadata_json, ?) = 'text'",
                vec![json_path],
            ))
            .one(db)
            .await
            .with_context(|| format!("failed to validate execution-window metadata key `{key}`"))?
            .is_some(),
    )
}

pub async fn aggregate_turn_execution_window_usage<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<TurnExecutionWindowUsageAggregateRecord> {
    let windows = list_turn_execution_windows(db, turn_id).await?;
    Ok(aggregate_turn_execution_window_records(windows.as_slice()))
}

pub fn aggregate_turn_execution_window_records(
    windows: &[TurnExecutionWindowRecord],
) -> TurnExecutionWindowUsageAggregateRecord {
    let mut aggregate = TurnExecutionWindowUsageAggregateRecord {
        total_windows: u32::try_from(windows.len()).unwrap_or(u32::MAX),
        ..TurnExecutionWindowUsageAggregateRecord::default()
    };

    for window in windows {
        aggregate.latest_window_index = aggregate.latest_window_index.max(window.window_index);
        aggregate.total_agent_rounds = aggregate
            .total_agent_rounds
            .saturating_add(u64::from(window.agent_round_count));
        aggregate.total_tool_calls = aggregate
            .total_tool_calls
            .saturating_add(u64::from(window.tool_call_count));
        aggregate.total_provider_tokens = aggregate
            .total_provider_tokens
            .saturating_add(window.provider_token_count);
        if window.agent_round_count > 0 && window.provider_token_count == 0 {
            aggregate.provider_token_usage_unknown = true;
        }

        let no_progress_recovery = window.agent_round_count == 0
            && window.tool_call_count == 0
            && matches!(
                window.exhaustion_reason,
                Some(
                    ExecutionWindowExhaustionReason::ProviderFailureContinuation
                        | ExecutionWindowExhaustionReason::RuntimeShutdownContinuation
                )
            );
        if no_progress_recovery {
            aggregate.consecutive_no_progress_windows =
                aggregate.consecutive_no_progress_windows.saturating_add(1);
        } else {
            aggregate.consecutive_no_progress_windows = 0;
        }

        if let Some(completed_at) = window.completed_at.as_ref() {
            let duration_ms = completed_at
                .signed_duration_since(window.started_at.clone())
                .num_milliseconds()
                .max(0);
            aggregate.total_wall_clock_ms = aggregate
                .total_wall_clock_ms
                .saturating_add(u64::try_from(duration_ms).unwrap_or(u64::MAX));
            aggregate.wall_clock_window_count = aggregate.wall_clock_window_count.saturating_add(1);
        }
    }

    aggregate
}

pub async fn mark_turn_execution_window_exhausted<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    reason: ExecutionWindowExhaustionReason,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<TurnExecutionWindowRecord> {
    update_window_with_stats(
        db,
        window_id,
        ExecutionWindowStatus::Exhausted,
        Some(reason),
        stats,
    )
    .await
}

pub async fn mark_turn_execution_window_checkpointed<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<TurnExecutionWindowRecord> {
    update_window_status_only(
        db,
        window_id,
        ExecutionWindowStatus::Checkpointed,
        updated_at,
    )
    .await
}

pub async fn mark_turn_execution_window_continued<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<TurnExecutionWindowRecord> {
    update_window_status_only(db, window_id, ExecutionWindowStatus::Continued, updated_at).await
}

pub async fn mark_turn_execution_window_completed<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<TurnExecutionWindowRecord> {
    update_window_with_stats(db, window_id, ExecutionWindowStatus::Completed, None, stats).await
}

pub async fn mark_turn_execution_window_failed<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<TurnExecutionWindowRecord> {
    update_window_with_stats(db, window_id, ExecutionWindowStatus::Failed, None, stats).await
}

pub async fn mark_turn_execution_window_interrupted<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<TurnExecutionWindowRecord> {
    update_window_with_stats(
        db,
        window_id,
        ExecutionWindowStatus::Interrupted,
        None,
        stats,
    )
    .await
}

pub async fn mark_turn_execution_window_blocked<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    reason: Option<ExecutionWindowExhaustionReason>,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<TurnExecutionWindowRecord> {
    update_window_with_stats(db, window_id, ExecutionWindowStatus::Blocked, reason, stats).await
}

pub(crate) async fn transition_window_with_prepared_stats<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    prepared: PreparedTurnExecutionWindowStatsTransition,
) -> Result<turn_execution_window::Model> {
    let affected = turn_execution_window::Entity::update_many()
        .col_expr(
            turn_execution_window::Column::Status,
            sea_orm::sea_query::Expr::value(prepared.target_status_db.clone()),
        )
        .col_expr(
            turn_execution_window::Column::ExhaustionReason,
            sea_orm::sea_query::Expr::value(prepared.reason_db.clone()),
        )
        .col_expr(
            turn_execution_window::Column::AgentRoundCount,
            sea_orm::sea_query::Expr::value(i64::from(prepared.stats.agent_round_count)),
        )
        .col_expr(
            turn_execution_window::Column::ToolCallCount,
            sea_orm::sea_query::Expr::value(i64::from(prepared.stats.tool_call_count)),
        )
        .col_expr(
            turn_execution_window::Column::ProviderTokenCount,
            sea_orm::sea_query::Expr::value(prepared.provider_token_count),
        )
        .col_expr(
            turn_execution_window::Column::MetadataJson,
            sea_orm::sea_query::Expr::value(prepared.metadata_json.clone()),
        )
        .col_expr(
            turn_execution_window::Column::CompletedAt,
            sea_orm::sea_query::Expr::value(Some(prepared.stats.completed_at.clone())),
        )
        .col_expr(
            turn_execution_window::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(prepared.stats.updated_at.clone()),
        )
        .filter(turn_execution_window::Column::Id.eq(window_id.to_owned()))
        .filter(turn_execution_window::Column::Status.eq(prepared.expected_status_db.as_str()))
        .exec(db)
        .await
        .with_context(|| format!("failed to transition execution window `{window_id}`"))?
        .rows_affected;

    let current = get_turn_execution_window_model(db, window_id)
        .await?
        .with_context(|| format!("execution window `{window_id}` is missing"))?;
    if affected == 1 {
        return Ok(current);
    }
    if current.status == prepared.target_status_db
        && current.exhaustion_reason == prepared.reason_db
        && current.agent_round_count == i64::from(prepared.stats.agent_round_count)
        && current.tool_call_count == i64::from(prepared.stats.tool_call_count)
        && current.provider_token_count == prepared.provider_token_count
        && current.metadata_json == prepared.metadata_json
        && current.completed_at.as_ref() == Some(&prepared.stats.completed_at)
    {
        return Ok(current);
    }
    anyhow::bail!(
        "execution window `{window_id}` cannot transition from {:?} to {:?}; expected {:?}",
        current.status,
        prepared.target_status,
        prepared.expected_status
    )
}

pub(crate) async fn transition_window_with_prepared_status<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    prepared: PreparedTurnExecutionWindowStatusTransition,
) -> Result<turn_execution_window::Model> {
    let affected = turn_execution_window::Entity::update_many()
        .col_expr(
            turn_execution_window::Column::Status,
            sea_orm::sea_query::Expr::value(prepared.target_status_db.as_str()),
        )
        .col_expr(
            turn_execution_window::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(prepared.updated_at.clone()),
        )
        .filter(turn_execution_window::Column::Id.eq(window_id.to_owned()))
        .filter(turn_execution_window::Column::Status.eq(prepared.expected_status_db.as_str()))
        .exec(db)
        .await
        .with_context(|| format!("failed to transition execution window `{window_id}` status"))?
        .rows_affected;
    let current = get_turn_execution_window_model(db, window_id)
        .await?
        .with_context(|| format!("execution window `{window_id}` is missing"))?;
    if affected == 1 || current.status == prepared.target_status_db {
        return Ok(current);
    }
    anyhow::bail!(
        "execution window `{window_id}` cannot transition from {:?} to {:?}; expected {:?}",
        current.status,
        prepared.target_status,
        prepared.expected_status
    )
}

pub async fn close_active_execution_windows_for_terminal_turns<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let active_window_statuses = [
        execution_window_status_to_db(ExecutionWindowStatus::Running),
        execution_window_status_to_db(ExecutionWindowStatus::Checkpointed),
    ];
    let terminal_turn_statuses = [
        turn_status_to_db(TurnStatus::Completed).to_owned(),
        turn_status_to_db(TurnStatus::Failed).to_owned(),
        turn_status_to_db(TurnStatus::Interrupted).to_owned(),
        turn_status_to_db(TurnStatus::Blocked).to_owned(),
    ];

    let terminal_turn_ids = turn::Entity::find()
        .filter(turn::Column::Status.is_in(terminal_turn_statuses))
        .all(db)
        .await
        .context("failed to list terminal turns for execution-window repair")?
        .into_iter()
        .map(|turn| turn.id)
        .collect::<Vec<_>>();
    if terminal_turn_ids.is_empty() {
        return Ok(0);
    }

    let windows = turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::TurnId.is_in(terminal_turn_ids))
        .filter(turn_execution_window::Column::Status.is_in(active_window_statuses))
        .all(db)
        .await
        .context("failed to list active execution windows for terminal turn repair")?;

    let mut repaired = 0u64;
    for window_model in windows {
        let window = window_record_from_model(window_model)?;
        let Some(turn_model) = turn::Entity::find_by_id(window.turn_id.clone())
            .one(db)
            .await
            .with_context(|| {
                format!(
                    "failed to query turn `{}` for execution-window repair",
                    window.turn_id
                )
            })?
        else {
            continue;
        };
        let Some(turn_status) = turn_status_from_db(turn_model.status.as_str()) else {
            continue;
        };
        let Some(window_status) = terminal_window_status_for_turn_status(turn_status) else {
            continue;
        };

        let counts = count_turn_execution_window_terminal_items_since(
            db,
            window.turn_id.as_str(),
            window.started_at.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "failed to count terminal items for execution-window repair on turn `{}`",
                window.turn_id
            )
        })?;
        let metadata_json = repaired_terminal_turn_window_metadata(
            &window.metadata_json,
            window_status,
            turn_model.error.as_deref(),
        );
        update_window_with_stats(
            db,
            window.id.as_str(),
            window_status,
            None,
            TurnExecutionWindowStatsRecord {
                agent_round_count: window.agent_round_count.max(counts.agent_round_count),
                tool_call_count: window.tool_call_count.max(counts.tool_call_count),
                provider_token_count: window.provider_token_count,
                metadata_json,
                completed_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .with_context(|| {
            format!(
                "failed to repair active execution window `{}` for terminal turn `{}`",
                window.id, window.turn_id
            )
        })?;
        repaired = repaired.saturating_add(1);
    }

    Ok(repaired)
}

pub async fn count_turn_execution_window_terminal_items<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<TurnExecutionWindowTerminalItemCountsRecord> {
    count_turn_execution_window_terminal_items_query(
        turn_item::Entity::find().filter(turn_item::Column::TurnId.eq(turn_id.to_owned())),
        db,
    )
    .await
}

pub async fn count_turn_execution_window_terminal_items_since<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    started_at: DateTimeWithTimeZone,
) -> Result<TurnExecutionWindowTerminalItemCountsRecord> {
    count_turn_execution_window_terminal_items_query(
        turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_item::Column::CreatedAt.gte(started_at)),
        db,
    )
    .await
}

async fn count_turn_execution_window_terminal_items_query<C: ConnectionTrait>(
    query: sea_orm::Select<turn_item::Entity>,
    db: &C,
) -> Result<TurnExecutionWindowTerminalItemCountsRecord> {
    let rows = query
        .all(db)
        .await
        .context("failed to list turn_item rows for execution-window terminal stats")?;

    let mut counts = TurnExecutionWindowTerminalItemCountsRecord::default();
    for row in rows {
        let Some(item_type) = turn_item_type_from_db(row.item_type.as_str()) else {
            continue;
        };
        if item_type == pioneer_protocol::TurnItemType::Reasoning {
            counts.agent_round_count = counts.agent_round_count.saturating_add(1);
        }
        if item_type.is_tool_item() {
            counts.tool_call_count = counts.tool_call_count.saturating_add(1);
        }
    }

    Ok(counts)
}

pub async fn list_turn_execution_window_items<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    started_at: DateTimeWithTimeZone,
) -> Result<Vec<TurnItem>> {
    let rows = turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::CreatedAt.gte(started_at))
        .order_by_asc(turn_item::Column::CreatedAt)
        .order_by_asc(turn_item::Column::ItemId)
        .all(db)
        .await
        .context("failed to list turn_item rows for execution-window checkpoint")?;

    rows.into_iter()
        .map(|row| {
            serde_json::from_str::<TurnItem>(row.payload.as_str()).with_context(|| {
                format!(
                    "failed to decode turn_item payload for execution-window checkpoint on turn `{turn_id}` item `{}`",
                    row.item_id
                )
            })
        })
        .collect()
}

pub async fn save_turn_execution_checkpoint<C: ConnectionTrait>(
    db: &C,
    record: NewTurnExecutionCheckpointRecord,
) -> Result<TurnExecutionCheckpointRecord> {
    let prepared = prepare_turn_execution_checkpoint_insert(record)?;
    let id = prepared.id.clone();
    insert_prepared_turn_execution_checkpoint(db, prepared).await?;

    get_turn_execution_checkpoint(db, id.as_str())
        .await?
        .context("inserted turn_execution_checkpoint row is missing")
}

pub(crate) async fn insert_prepared_turn_execution_checkpoint<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnExecutionCheckpointInsert,
) -> Result<()> {
    turn_execution_checkpoint::Entity::insert(prepared.active_model())
        .exec(db)
        .await
        .context("failed to insert turn_execution_checkpoint row")?;
    Ok(())
}

pub async fn get_turn_execution_checkpoint<C: ConnectionTrait>(
    db: &C,
    checkpoint_id: &str,
) -> Result<Option<TurnExecutionCheckpointRecord>> {
    turn_execution_checkpoint::Entity::find_by_id(checkpoint_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn_execution_checkpoint row")?
        .map(checkpoint_record_from_model)
        .transpose()
}

pub(crate) async fn get_turn_execution_checkpoint_model<C: ConnectionTrait>(
    db: &C,
    checkpoint_id: &str,
) -> Result<Option<turn_execution_checkpoint::Model>> {
    turn_execution_checkpoint::Entity::find_by_id(checkpoint_id.to_owned())
        .one(db)
        .await
        .context("failed to query raw turn_execution_checkpoint row")
}

pub async fn list_turn_execution_checkpoints_for_window<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
) -> Result<Vec<TurnExecutionCheckpointRecord>> {
    turn_execution_checkpoint::Entity::find()
        .filter(turn_execution_checkpoint::Column::WindowId.eq(window_id.to_owned()))
        .order_by_asc(turn_execution_checkpoint::Column::CreatedAt)
        .all(db)
        .await
        .context("failed to list turn_execution_checkpoint rows for window")?
        .into_iter()
        .map(checkpoint_record_from_model)
        .collect()
}

pub async fn latest_turn_execution_checkpoint_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnExecutionCheckpointRecord>> {
    turn_execution_checkpoint::Entity::find()
        .filter(turn_execution_checkpoint::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_execution_checkpoint::Column::CreatedAt)
        .one(db)
        .await
        .context("failed to query latest turn_execution_checkpoint row for turn")?
        .map(checkpoint_record_from_model)
        .transpose()
}

pub async fn delete_turn_execution_checkpoints_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    let deleted = turn_execution_checkpoint::Entity::delete_many()
        .filter(turn_execution_checkpoint::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete turn_execution_checkpoint rows for turn")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_turn_execution_checkpoints_for_window<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
) -> Result<u64> {
    let deleted = turn_execution_checkpoint::Entity::delete_many()
        .filter(turn_execution_checkpoint::Column::WindowId.eq(window_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete turn_execution_checkpoint rows for window")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_turn_execution_windows_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    let deleted = turn_execution_window::Entity::delete_many()
        .filter(turn_execution_window::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete turn_execution_window rows for turn")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_turn_execution_data_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<TurnExecutionDataCleanupRecord> {
    let checkpoints_deleted = delete_turn_execution_checkpoints_for_turn(db, turn_id).await?;
    let windows_deleted = delete_turn_execution_windows_for_turn(db, turn_id).await?;
    Ok(TurnExecutionDataCleanupRecord {
        checkpoints_deleted,
        windows_deleted,
    })
}

fn terminal_window_status_for_turn_status(status: TurnStatus) -> Option<ExecutionWindowStatus> {
    match status {
        TurnStatus::Completed => Some(ExecutionWindowStatus::Completed),
        TurnStatus::Failed => Some(ExecutionWindowStatus::Failed),
        TurnStatus::Interrupted => Some(ExecutionWindowStatus::Interrupted),
        TurnStatus::Blocked => Some(ExecutionWindowStatus::Blocked),
        TurnStatus::InProgress => None,
    }
}

fn repaired_terminal_turn_window_metadata(
    existing: &serde_json::Value,
    status: ExecutionWindowStatus,
    reason: Option<&str>,
) -> serde_json::Value {
    let mut metadata = existing.as_object().cloned().unwrap_or_default();
    metadata.insert(
        "repairedBy".to_owned(),
        serde_json::Value::String("bootstrap_terminal_turn_window_repair".to_owned()),
    );
    metadata.insert(
        "terminalStatus".to_owned(),
        serde_json::Value::String(execution_window_status_to_db(status)),
    );
    if let Some(reason) = reason.map(str::trim).filter(|reason| !reason.is_empty()) {
        metadata.insert(
            "terminalReason".to_owned(),
            serde_json::Value::String(reason.to_owned()),
        );
    }
    serde_json::Value::Object(metadata)
}

pub fn checkpoint_kind_to_db(kind: TurnExecutionCheckpointKind) -> String {
    enum_to_snake_string(kind)
}

pub fn checkpoint_kind_from_db(value: &str) -> Result<TurnExecutionCheckpointKind> {
    enum_from_snake_string(value, "execution checkpoint kind")
}

fn enum_to_snake_string<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .expect("execution checkpoint enum should serialize as a string")
}

fn enum_from_snake_string<T: DeserializeOwned>(value: &str, type_name: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|err| anyhow::anyhow!("unknown {type_name} `{value}`: {err}"))
}

pub(crate) fn serialize_checkpoint_payload(payload: &serde_json::Value) -> Result<String> {
    let encoded = serde_json::to_string(payload)
        .context("failed to serialize execution-checkpoint payload_json")?;
    if encoded.len() > TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES {
        anyhow::bail!(
            "execution checkpoint payload_json is too large: {} bytes exceeds {} bytes",
            encoded.len(),
            TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES
        );
    }
    Ok(encoded)
}

async fn enforce_next_window_index<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    window_index: u32,
) -> Result<()> {
    let latest = turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_execution_window::Column::WindowIndex)
        .one(db)
        .await
        .context("failed to query latest turn_execution_window index")?;

    let expected = latest
        .map(|window| window.window_index.saturating_add(1))
        .unwrap_or(1);
    let observed = i64::from(window_index);
    if observed != expected {
        anyhow::bail!(
            "turn_execution_window window_index must be monotonic for turn `{turn_id}`: expected {expected}, got {observed}"
        );
    }

    Ok(())
}

async fn update_window_with_stats<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    status: ExecutionWindowStatus,
    reason: Option<ExecutionWindowExhaustionReason>,
    stats: TurnExecutionWindowStatsRecord,
) -> Result<TurnExecutionWindowRecord> {
    let metadata_json = serde_json::to_string(&stats.metadata_json)
        .context("failed to serialize execution-window metadata_json")?;

    let model = turn_execution_window::ActiveModel {
        id: Unchanged(window_id.to_owned()),
        status: Set(execution_window_status_to_db(status)),
        exhaustion_reason: Set(reason.map(execution_window_exhaustion_reason_to_db)),
        agent_round_count: Set(i64::from(stats.agent_round_count)),
        tool_call_count: Set(i64::from(stats.tool_call_count)),
        provider_token_count: Set(i64::try_from(stats.provider_token_count)
            .context("provider_token_count exceeds i64")?),
        metadata_json: Set(metadata_json),
        completed_at: Set(Some(stats.completed_at)),
        updated_at: Set(stats.updated_at),
        ..Default::default()
    }
    .update(db)
    .await
    .with_context(|| format!("failed to update turn_execution_window `{window_id}`"))?;

    window_record_from_model(model)
}

async fn update_window_status_only<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    status: ExecutionWindowStatus,
    updated_at: DateTimeWithTimeZone,
) -> Result<TurnExecutionWindowRecord> {
    let model = turn_execution_window::ActiveModel {
        id: Unchanged(window_id.to_owned()),
        status: Set(execution_window_status_to_db(status)),
        updated_at: Set(updated_at),
        ..Default::default()
    }
    .update(db)
    .await
    .with_context(|| format!("failed to update turn_execution_window `{window_id}` status"))?;

    window_record_from_model(model)
}

fn active_model_id(value: &ActiveValue<String>) -> Result<String> {
    match value {
        ActiveValue::Set(id) | ActiveValue::Unchanged(id) => Ok(id.clone()),
        ActiveValue::NotSet => anyhow::bail!("turn_execution_window id is not set"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_window_status_and_reason_roundtrip_through_db_strings() {
        let status = ExecutionWindowStatus::Interrupted;
        let status_db = execution_window_status_to_db(status);
        assert_eq!(status_db, "interrupted");
        assert_eq!(execution_window_status_from_db(&status_db).unwrap(), status);

        let reason = ExecutionWindowExhaustionReason::MaxToolCallsPerWindow;
        let reason_db = execution_window_exhaustion_reason_to_db(reason);
        assert_eq!(reason_db, "max_tool_calls_per_window");
        assert_eq!(
            execution_window_exhaustion_reason_from_db(&reason_db).unwrap(),
            reason
        );
    }

    #[test]
    fn checkpoint_kind_roundtrips_through_db_string() {
        let kind = TurnExecutionCheckpointKind::WindowExhausted;
        let encoded = checkpoint_kind_to_db(kind);
        assert_eq!(encoded, "window_exhausted");
        assert_eq!(checkpoint_kind_from_db(&encoded).unwrap(), kind);
    }
}
