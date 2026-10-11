use anyhow::{Context, Result};
use pioneer_entity::{turn_execution_checkpoint, turn_execution_window, turn_item};
use pioneer_protocol::{
    ExecutionWindowExhaustionReason, ExecutionWindowStatus, TurnItem, TurnStatus, generate_id,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, Unchanged,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::convention::{
    DB_ID_LEN, execution_window_exhaustion_reason_from_db,
    execution_window_exhaustion_reason_to_db, execution_window_status_from_db,
    execution_window_status_to_db,
};

pub const TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES: usize = 128 * 1024;

pub(crate) fn semantic_checkpoint_id(turn_id: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(
        format!("native-semantic-v1:{turn_id}").as_bytes(),
    ))[..DB_ID_LEN]
        .to_owned()
}

pub(crate) async fn native_event_frontier<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<pioneer_protocol::ExecutionCheckpointNativeFrontier> {
    let row = db
        .query_one_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT coalesce((SELECT max(sequence) FROM turn_event WHERE turn_id=?1),0) AS events, \
         coalesce((SELECT max(sequence) FROM turn_llm_context WHERE turn_id=?1),-1) AS contexts",
            [turn_id.to_owned().into()],
        ))
        .await?
        .context("native event frontier unavailable")?;
    Ok(pioneer_protocol::ExecutionCheckpointNativeFrontier {
        schema_version: 1,
        event_sequence: row.try_get("", "events")?,
        context_sequence: row.try_get("", "contexts")?,
    })
}

/// Only this bounded checkpoint row is replaced. All preparation reads are
/// revalidated by scalar predicates and exact predecessor bytes in this write.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_semantic_checkpoint_commit(
    payload: &pioneer_protocol::ExecutionCheckpointPayload,
    next_json: String,
    previous_json: Option<String>,
    owner_id: &str,
    owner_generation: u64,
    context_sha256: &str,
    frontier: &pioneer_protocol::ExecutionCheckpointNativeFrontier,
    now: DateTimeWithTimeZone,
) -> Result<sea_orm::Statement> {
    anyhow::ensure!(
        next_json.len() <= TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES,
        "native semantic checkpoint exceeds byte limit"
    );
    Ok(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "INSERT INTO turn_execution_checkpoint \
        (id,window_id,workspace_id,thread_id,turn_id,checkpoint_kind,payload_json,created_at) \
        SELECT ?1,w.id,?2,?3,?4,'semantic_state',?5,?6 FROM turn_execution_window w \
        JOIN \"turn\" t ON t.id=w.turn_id JOIN thread th ON th.id=t.thread_id \
        JOIN turn_execution e ON e.turn_id=t.id JOIN native_cancellation_context c ON c.turn_id=t.id \
        WHERE w.turn_id=?4 AND w.workspace_id=?2 AND w.thread_id=?3 AND w.window_index=?7 \
        AND w.status='running' AND w.window_index=(SELECT max(window_index) FROM turn_execution_window WHERE turn_id=?4) \
        AND t.thread_id=?3 AND th.workspace_id=?2 AND t.status='in_progress' AND t.message_deleted_at IS NULL \
        AND e.workspace_id=?2 AND e.thread_id=?3 AND e.owner_id=?8 AND e.owner_generation=?9 \
        AND e.executor_kind IN ('native_agent','api_provider') AND e.status IN ('queued','starting','running','recovering') \
        AND c.workspace_id=?2 AND c.thread_id=?3 AND c.context_sha256=?10 AND c.accepted_event_id IS NULL \
        AND coalesce((SELECT max(sequence) FROM turn_event WHERE turn_id=?4),0)=?11 \
        AND coalesce((SELECT max(sequence) FROM turn_llm_context WHERE turn_id=?4),-1)=?12 \
        AND ((?13 IS NULL AND NOT EXISTS(SELECT 1 FROM turn_execution_checkpoint WHERE id=?1)) \
          OR EXISTS(SELECT 1 FROM turn_execution_checkpoint WHERE id=?1 AND turn_id=?4 AND workspace_id=?2 AND thread_id=?3 AND payload_json=?13)) \
        ON CONFLICT(id) DO UPDATE SET window_id=excluded.window_id,payload_json=excluded.payload_json,created_at=excluded.created_at \
        WHERE turn_execution_checkpoint.payload_json=?13 AND turn_execution_checkpoint.checkpoint_kind='semantic_state'",
        [
            semantic_checkpoint_id(&payload.turn_id).into(),
            payload.workspace_id.clone().into(),
            payload.thread_id.clone().into(),
            payload.turn_id.clone().into(),
            next_json.into(),
            now.into(),
            i64::from(payload.window.window_index).into(),
            owner_id.to_owned().into(),
            i64::try_from(owner_generation)?.into(),
            context_sha256.to_owned().into(),
            frontier.event_sequence.into(),
            frontier.context_sequence.into(),
            previous_json.into(),
        ],
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_semantic_recovery_close(
    window: &TurnExecutionWindowRecord,
    previous_json: String,
    stats: &TurnExecutionWindowStatsRecord,
    reason: ExecutionWindowExhaustionReason,
    owner_id: &str,
    owner_generation: u64,
    context_sha256: &str,
    frontier: &pioneer_protocol::ExecutionCheckpointNativeFrontier,
) -> Result<sea_orm::Statement> {
    let metadata = serde_json::to_string(&stats.metadata_json)?;
    anyhow::ensure!(
        metadata.len() <= TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES,
        "window metadata byte limit"
    );
    Ok(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE turn_execution_window SET status='checkpointed',exhaustion_reason=?1, \
        agent_round_count=max(agent_round_count,?2),tool_call_count=max(tool_call_count,?3), \
        provider_token_count=max(provider_token_count,?4),metadata_json=?5,completed_at=?6,updated_at=?7 \
        WHERE id=?8 AND turn_id=?9 AND workspace_id=?10 AND thread_id=?11 AND status='running' \
        AND window_index=(SELECT max(window_index) FROM turn_execution_window WHERE turn_id=?9) \
        AND metadata_json=?12 \
        AND EXISTS(SELECT 1 FROM \"turn\" t JOIN thread th ON th.id=t.thread_id \
          JOIN turn_execution e ON e.turn_id=t.id JOIN native_cancellation_context c ON c.turn_id=t.id \
          WHERE t.id=?9 AND t.thread_id=?11 AND th.workspace_id=?10 AND t.status='in_progress' \
          AND t.message_deleted_at IS NULL AND e.workspace_id=?10 AND e.thread_id=?11 \
          AND e.owner_id=?13 AND e.owner_generation=?14 AND e.status IN ('queued','starting','running','recovering') \
          AND e.executor_kind IN ('native_agent','api_provider') AND c.workspace_id=?10 AND c.thread_id=?11 \
          AND c.context_sha256=?15 AND c.accepted_event_id IS NULL) \
        AND EXISTS(SELECT 1 FROM turn_execution_checkpoint WHERE id=?16 AND window_id=?8 \
          AND workspace_id=?10 AND thread_id=?11 AND turn_id=?9 AND checkpoint_kind='semantic_state' AND payload_json=?17) \
        AND coalesce((SELECT max(sequence) FROM turn_event WHERE turn_id=?9),0)=?18 \
        AND coalesce((SELECT max(sequence) FROM turn_llm_context WHERE turn_id=?9),-1)=?19",
        [
            execution_window_exhaustion_reason_to_db(reason).into(),
            i64::from(stats.agent_round_count).into(),
            i64::from(stats.tool_call_count).into(),
            i64::try_from(stats.provider_token_count)?.into(),
            metadata.into(),
            stats.completed_at.into(),
            stats.updated_at.into(),
            window.id.clone().into(),
            window.turn_id.clone().into(),
            window.workspace_id.clone().into(),
            window.thread_id.clone().into(),
            serde_json::to_string(&window.metadata_json)?.into(),
            owner_id.to_owned().into(),
            i64::try_from(owner_generation)?.into(),
            context_sha256.to_owned().into(),
            semantic_checkpoint_id(&window.turn_id).into(),
            previous_json.into(),
            frontier.event_sequence.into(),
            frontier.context_sequence.into(),
        ],
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnExecutionCheckpointKind {
    SemanticState,
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

pub(crate) async fn window_usage_batch<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    after_index: u32,
) -> Result<Vec<turn_execution_window::Model>> {
    use sea_orm::QuerySelect;
    turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_execution_window::Column::WindowIndex.gt(i64::from(after_index)))
        .order_by_asc(turn_execution_window::Column::WindowIndex)
        .limit(32)
        .all(db)
        .await
        .context("failed to read execution-window usage batch")
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
        if window.agent_round_count > 0
            && (window
                .metadata_json
                .get("accountingSchemaVersion")
                .and_then(serde_json::Value::as_u64)
                != Some(1)
                || window
                    .metadata_json
                    .get("providerUsageAvailable")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true))
        {
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

pub(crate) async fn terminal_active_window_batch<C: ConnectionTrait>(
    db: &C,
    after_window_id: Option<&str>,
) -> Result<Vec<turn_execution_window::Model>> {
    use sea_orm::QuerySelect;
    let mut query = turn_execution_window::Entity::find()
        .filter(turn_execution_window::Column::Status.is_in(["running", "exhausted", "checkpointed"]))
        .filter(sea_orm::sea_query::Expr::cust(
            "EXISTS (SELECT 1 FROM \"turn\" WHERE \"turn\".id = turn_execution_window.turn_id AND \"turn\".status IN ('completed', 'failed', 'interrupted', 'blocked'))"))
        .order_by_asc(turn_execution_window::Column::Id).limit(32);
    if let Some(after) = after_window_id {
        query = query.filter(turn_execution_window::Column::Id.gt(after.to_owned()));
    }
    query
        .all(db)
        .await
        .context("failed to discover terminal execution-window repair batch")
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
    use sea_orm::{FromQueryResult, QuerySelect};
    #[derive(FromQueryResult)]
    struct Counts {
        agent_round_count: i64,
        tool_call_count: i64,
    }
    let counts = query.select_only()
        .column_as(sea_orm::sea_query::Expr::cust("COALESCE(SUM(CASE WHEN item_type = 'reasoning' THEN 1 ELSE 0 END), 0)"), "agent_round_count")
        .column_as(sea_orm::sea_query::Expr::cust("COALESCE(SUM(CASE WHEN item_type IN ('command_execution','file_change','web_search','web_fetch','download','dynamic_tool_call') THEN 1 ELSE 0 END), 0)"), "tool_call_count")
        .into_model::<Counts>().one(db).await
        .context("failed to count execution-window terminal items")?
        .context("missing execution-window item counts")?;
    Ok(TurnExecutionWindowTerminalItemCountsRecord {
        agent_round_count: u32::try_from(counts.agent_round_count).unwrap_or(u32::MAX),
        tool_call_count: u32::try_from(counts.tool_call_count).unwrap_or(u32::MAX),
    })
}

pub fn provider_usage_observation_tokens(payload: &str) -> (u64, bool) {
    provider_usage_observation_payload(payload).unwrap_or((0, false))
}

fn provider_usage_observation_payload(payload: &str) -> Option<(u64, bool)> {
    let item: TurnItem = serde_json::from_str(payload).ok()?;
    let TurnItem::SystemEvent {
        code,
        details: Some(details),
        ..
    } = item
    else {
        return None;
    };
    if code.as_deref() != Some("provider_usage") || details["schema_version"].as_u64() != Some(1) {
        return None;
    }
    let usage = details.get("usage")?;
    let input = usage
        .get("input_tokens")
        .and_then(serde_json::Value::as_u64);
    let output = usage
        .get("output_tokens")
        .and_then(serde_json::Value::as_u64);
    Some((
        input.unwrap_or(0).saturating_add(output.unwrap_or(0)),
        input.is_some()
            && output.is_some()
            && matches!(details["status"].as_str(), Some("completed" | "failed")),
    ))
}

pub(crate) async fn commit_checkpoint_usage_availability<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
    available: bool,
) -> Result<()> {
    let result = turn_execution_window::Entity::update_many()
        .col_expr(turn_execution_window::Column::MetadataJson,
            sea_orm::sea_query::Expr::cust_with_values(
                "json_set(metadata_json, '$.accountingSchemaVersion', 1, '$.providerUsageAvailable', json(?))",
                [if available { "true" } else { "false" }],
            ))
        .filter(turn_execution_window::Column::Id.eq(window_id.to_owned()))
        .filter(turn_execution_window::Column::Status.eq(execution_window_status_to_db(ExecutionWindowStatus::Checkpointed)))
        .exec(db).await.context("failed to commit checkpoint usage availability")?;
    anyhow::ensure!(result.rows_affected == 1, "checkpoint usage owner changed");
    Ok(())
}

pub async fn execution_window_usage_observation_batch<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    window_index: u32,
    after_item_id: Option<&str>,
) -> Result<Vec<(String, String)>> {
    use sea_orm::{FromQueryResult, QuerySelect};
    #[derive(FromQueryResult)]
    struct ObservationRow {
        item_id: String,
        payload: String,
    }
    let mut query = turn_item::Entity::find()
        .select_only()
        .column(turn_item::Column::ItemId)
        // Corrupt/oversize records become explicit Unknown without allocating
        // their unbounded payload while a reader is occupied.
        .column_as(
            sea_orm::sea_query::Expr::cust(
                "CASE WHEN length(CAST(payload AS BLOB)) <= 65536 THEN payload ELSE '' END",
            ),
            "payload",
        )
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemType.eq("system_event"))
        .filter(sea_orm::sea_query::Expr::cust(
            "json_extract(payload, '$.code') = 'provider_usage'",
        ))
        .filter(sea_orm::sea_query::Expr::cust_with_values(
            "json_extract(payload, '$.details.window_index') = ?",
            [i64::from(window_index)],
        ))
        .order_by_asc(turn_item::Column::ItemId)
        .limit(128);
    if let Some(after) = after_item_id {
        query = query.filter(turn_item::Column::ItemId.gt(after.to_owned()));
    }
    let rows = query
        .into_model::<ObservationRow>()
        .all(db)
        .await
        .context("failed to read bounded provider usage observations")?;
    Ok(rows
        .into_iter()
        .map(|row| (row.item_id, row.payload))
        .collect())
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

/// Preserve the selected row, including a poisoned newest row. Filtering it
/// out would allow recovery to use an older checkpoint with fewer obligations.
/// NULL in the required String column is an explicit unavailable read outcome;
/// oversized legacy TEXT never crosses the physical reader boundary.
fn bounded_checkpoint_query() -> sea_orm::Select<turn_execution_checkpoint::Entity> {
    use turn_execution_checkpoint::Column as C;
    turn_execution_checkpoint::Entity::find()
        .select_only()
        .columns([
            C::Id,
            C::WindowId,
            C::WorkspaceId,
            C::ThreadId,
            C::TurnId,
            C::CheckpointKind,
            C::CreatedAt,
        ])
        .column_as(
            sea_orm::sea_query::Expr::cust_with_values(
                "CASE WHEN length(CAST(payload_json AS BLOB)) <= ? THEN payload_json ELSE NULL END",
                [TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES as i64],
            ),
            C::PayloadJson,
        )
}

pub async fn get_turn_execution_checkpoint<C: ConnectionTrait>(
    db: &C,
    checkpoint_id: &str,
) -> Result<Option<TurnExecutionCheckpointRecord>> {
    bounded_checkpoint_query()
        .filter(turn_execution_checkpoint::Column::Id.eq(checkpoint_id.to_owned()))
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
    bounded_checkpoint_query()
        .filter(turn_execution_checkpoint::Column::Id.eq(checkpoint_id.to_owned()))
        .one(db)
        .await
        .context("failed to query raw turn_execution_checkpoint row")
}

pub async fn list_turn_execution_checkpoints_for_window<C: ConnectionTrait>(
    db: &C,
    window_id: &str,
) -> Result<Vec<TurnExecutionCheckpointRecord>> {
    bounded_checkpoint_query()
        .filter(turn_execution_checkpoint::Column::WindowId.eq(window_id.to_owned()))
        // The completed-window checkpoint supersedes the replaceable Running
        // projection even when recovery uses a coarser/backdated timestamp.
        .order_by(
            sea_orm::sea_query::Expr::cust(
                "CASE WHEN checkpoint_kind='semantic_state' THEN 0 ELSE 1 END",
            ),
            sea_orm::Order::Asc,
        )
        .order_by_asc(turn_execution_checkpoint::Column::CreatedAt)
        .order_by_asc(turn_execution_checkpoint::Column::Id)
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
    bounded_checkpoint_query()
        .filter(turn_execution_checkpoint::Column::TurnId.eq(turn_id.to_owned()))
        .order_by(
            sea_orm::sea_query::Expr::cust("(SELECT window_index FROM turn_execution_window WHERE id=turn_execution_checkpoint.window_id)"),
            sea_orm::Order::Desc,
        )
        .order_by(
            sea_orm::sea_query::Expr::cust("CASE WHEN checkpoint_kind='semantic_state' THEN 0 ELSE 1 END"),
            sea_orm::Order::Desc,
        )
        .order_by_desc(turn_execution_checkpoint::Column::CreatedAt)
        .order_by_desc(turn_execution_checkpoint::Column::Id)
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

pub(crate) fn terminal_window_status_for_turn_status(
    status: TurnStatus,
) -> Option<ExecutionWindowStatus> {
    match status {
        TurnStatus::Completed => Some(ExecutionWindowStatus::Completed),
        TurnStatus::Failed => Some(ExecutionWindowStatus::Failed),
        TurnStatus::Interrupted => Some(ExecutionWindowStatus::Interrupted),
        TurnStatus::Blocked => Some(ExecutionWindowStatus::Blocked),
        TurnStatus::InProgress => None,
    }
}

pub(crate) fn repaired_terminal_turn_window_metadata(
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

    #[tokio::test]
    async fn legacy_oversized_newest_checkpoint_is_unavailable_without_stale_fallback() {
        use migration::{Migrator, MigratorTrait};
        let connection = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&connection, None).await.unwrap();
        let store = crate::CrudStore::new(connection).with_maintenance_access();
        let db = store.database_connection();
        assert_eq!(
            db.read_class(),
            pioneer_sqlite::SqliteReadClass::Maintenance
        );
        let now = chrono::Utc::now().fixed_offset();
        let window = store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "workspace".to_owned(),
                    thread_id: "thread".to_owned(),
                    turn_id: "turn".to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: now,
                },
                now,
                now,
            )
            .await
            .unwrap();
        let first = store
            .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                id: Some("original".to_owned()),
                window_id: window.id.clone(),
                workspace_id: "workspace".to_owned(),
                thread_id: "thread".to_owned(),
                turn_id: "turn".to_owned(),
                checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                payload_json: serde_json::json!({"original": true}),
                created_at: now,
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .latest_turn_execution_checkpoint_for_turn("turn")
                .await
                .unwrap()
                .unwrap()
                .id,
            first.id
        );
        // Simulate a legacy writer; no production limit is disabled.
        let poison = serde_json::json!({"text": "界".repeat(TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES / 2)}).to_string();
        assert!(poison.len() > TURN_EXECUTION_CHECKPOINT_PAYLOAD_MAX_BYTES);
        db.execute_raw(sea_orm::Statement::from_sql_and_values(sea_orm::DatabaseBackend::Sqlite,
            "INSERT INTO turn_execution_checkpoint(id,window_id,workspace_id,thread_id,turn_id,checkpoint_kind,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
            ["poison".into(), window.id.clone().into(), "workspace".into(), "thread".into(), "turn".into(),
             "window_exhausted".into(), poison.into(), (now + chrono::Duration::seconds(1)).into()],
        )).await.unwrap();
        assert!(store.get_turn_execution_checkpoint("poison").await.is_err());
        assert!(
            get_turn_execution_checkpoint_model(&db, "poison")
                .await
                .is_err()
        );
        assert!(
            store
                .latest_turn_execution_checkpoint_for_turn("turn")
                .await
                .is_err()
        );
        assert!(
            store
                .list_turn_execution_checkpoints_for_window(&window.id)
                .await
                .is_err()
        );
        assert!(
            store
                .get_turn_execution_checkpoint("original")
                .await
                .unwrap()
                .is_some()
        );
    }

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

#[cfg(test)]
mod usage_accounting_tests {
    use super::provider_usage_observation_tokens;
    fn payload(usage: serde_json::Value, status: &str) -> String {
        serde_json::json!({"type":"systemEvent", "id":"observation", "level":"info",
            "message":"Provider usage observation", "code":"provider_usage",
            "details":{"schema_version":1,"window_index":1,"status":status,"usage":usage}})
        .to_string()
    }
    #[test]
    fn terminal_usage_reads_confirmed_lower_bound_and_unknown_separately() {
        assert_eq!(
            provider_usage_observation_tokens(&payload(
                serde_json::json!({"input_tokens":30}),
                "failed"
            )),
            (30, false)
        );
        assert_eq!(
            provider_usage_observation_tokens(&payload(
                serde_json::json!({"input_tokens":30,"output_tokens":2}),
                "started"
            )),
            (32, false)
        );
        assert_eq!(
            provider_usage_observation_tokens(&payload(
                serde_json::json!({"input_tokens":0,"output_tokens":0}),
                "completed"
            )),
            (0, true)
        );
        assert_eq!(provider_usage_observation_tokens("invalid"), (0, false));
    }
}
