use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_runtime_snapshot};
use pioneer_protocol::TurnStatus;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, Set};

use crate::convention::turn_status_to_db;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRuntimeSnapshotRecord {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub mode_json: String,
    pub model: String,
    pub provider_name: String,
    pub reasoning_effort: Option<String>,
    pub agent_skill_versions_json: Option<String>,
    pub ordinary_skill_catalog_json: Option<String>,
    pub provider_model_contract_json: Option<String>,
    pub initial_phase_json: Option<String>,
    pub hook_runtime_context_json: String,
    pub workspace_skill_policies_json: String,
    pub input_json: String,
    pub capabilities_json: String,
    pub resolved_artifacts_json: String,
    pub runtime_environment_json: String,
    pub history_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTurnRuntimeSnapshot {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub mode_json: String,
    pub model: String,
    pub provider_name: String,
    pub reasoning_effort: Option<String>,
    pub agent_skill_versions_json: Option<String>,
    pub ordinary_skill_catalog_json: Option<String>,
    pub provider_model_contract_json: Option<String>,
    pub initial_phase_json: Option<String>,
    pub hook_runtime_context_json: String,
    pub workspace_skill_policies_json: String,
    pub input_json: String,
    pub capabilities_json: String,
    pub resolved_artifacts_json: String,
    pub runtime_environment_json: String,
    pub history_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

fn validate_snapshot_storage_size(fields: &[&str]) -> Result<usize> {
    let mut remaining = pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES;
    for field in fields {
        remaining = remaining
            .checked_sub(field.len())
            .context("runtime snapshot exceeds accepted execution byte limit")?;
    }
    Ok(remaining)
}

impl NewTurnRuntimeSnapshot {
    pub fn validate_storage_size(&self) -> Result<()> {
        self.remaining_storage_bytes().map(|_| ())
    }
    pub fn remaining_storage_bytes(&self) -> Result<usize> {
        validate_snapshot_storage_size(&[
            &self.turn_id,
            &self.thread_id,
            &self.workspace_id,
            &self.mode_json,
            &self.model,
            &self.provider_name,
            self.reasoning_effort.as_deref().unwrap_or(""),
            self.agent_skill_versions_json.as_deref().unwrap_or(""),
            self.ordinary_skill_catalog_json.as_deref().unwrap_or(""),
            self.provider_model_contract_json.as_deref().unwrap_or(""),
            self.initial_phase_json.as_deref().unwrap_or(""),
            &self.hook_runtime_context_json,
            &self.workspace_skill_policies_json,
            &self.input_json,
            &self.capabilities_json,
            &self.resolved_artifacts_json,
            &self.runtime_environment_json,
            &self.history_json,
        ])
    }
}

impl TurnRuntimeSnapshotRecord {
    pub fn validate_storage_size(&self) -> Result<()> {
        self.remaining_storage_bytes().map(|_| ())
    }
    pub fn remaining_storage_bytes(&self) -> Result<usize> {
        validate_snapshot_storage_size(&[
            &self.turn_id,
            &self.thread_id,
            &self.workspace_id,
            &self.mode_json,
            &self.model,
            &self.provider_name,
            self.reasoning_effort.as_deref().unwrap_or(""),
            self.agent_skill_versions_json.as_deref().unwrap_or(""),
            self.ordinary_skill_catalog_json.as_deref().unwrap_or(""),
            self.provider_model_contract_json.as_deref().unwrap_or(""),
            self.initial_phase_json.as_deref().unwrap_or(""),
            &self.hook_runtime_context_json,
            &self.workspace_skill_policies_json,
            &self.input_json,
            &self.capabilities_json,
            &self.resolved_artifacts_json,
            &self.runtime_environment_json,
            &self.history_json,
        ])
    }
}

// SQLite byte lengths are checked before a driver materializes TEXT columns.
// Include the runtime-owned frozen locator as well as the encoded payload.
// Row preparation reserves its bytes before SQL; reads guard all stored TEXT.
const SNAPSHOT_STORAGE_BYTES_SQL: &str = "coalesce(length(cast(turn_id as blob)),0) + coalesce(length(cast(thread_id as blob)),0) + coalesce(length(cast(workspace_id as blob)),0) + coalesce(length(cast(mode_json as blob)),0) + coalesce(length(cast(model as blob)),0) + coalesce(length(cast(provider_name as blob)),0) + coalesce(length(cast(reasoning_effort as blob)),0) + coalesce(length(cast(agent_skill_versions_json as blob)),0) + coalesce(length(cast(hook_runtime_context_json as blob)),0) + coalesce(length(cast(workspace_skill_policies_json as blob)),0) + coalesce(length(cast(input_json as blob)),0) + coalesce(length(cast(capabilities_json as blob)),0) + coalesce(length(cast(resolved_artifacts_json as blob)),0) + coalesce(length(cast(runtime_environment_json as blob)),0) + coalesce(length(cast(history_json as blob)),0) + coalesce(length(cast(ordinary_skill_catalog_json as blob)),0) + coalesce(length(cast(provider_model_contract_json as blob)),0) + coalesce(length(cast(initial_phase_json as blob)),0) + coalesce(length(cast(frozen_manifest_id as blob)),0)";

// Encoded values are prepared before opening the admission transaction.
pub(crate) fn prepare_snapshot_row(
    snapshot: NewTurnRuntimeSnapshot,
    frozen_manifest_id: Option<String>,
) -> Result<turn_runtime_snapshot::ActiveModel> {
    anyhow::ensure!(
        snapshot.remaining_storage_bytes()? >= frozen_manifest_id.as_ref().map_or(0, String::len),
        "runtime snapshot frozen locator exceeds accepted execution byte limit"
    );
    Ok(turn_runtime_snapshot::ActiveModel {
        turn_id: Set(snapshot.turn_id),
        thread_id: Set(snapshot.thread_id),
        workspace_id: Set(snapshot.workspace_id),
        mode_json: Set(snapshot.mode_json),
        model: Set(snapshot.model),
        provider_name: Set(snapshot.provider_name),
        reasoning_effort: Set(snapshot.reasoning_effort),
        agent_skill_versions_json: Set(snapshot.agent_skill_versions_json),
        provider_model_contract_json: Set(snapshot.provider_model_contract_json),
        initial_phase_json: Set(snapshot.initial_phase_json),
        ordinary_skill_catalog_json: Set(snapshot.ordinary_skill_catalog_json),
        hook_runtime_context_json: Set(snapshot.hook_runtime_context_json),
        workspace_skill_policies_json: Set(snapshot.workspace_skill_policies_json),
        input_json: Set(snapshot.input_json),
        capabilities_json: Set(snapshot.capabilities_json),
        resolved_artifacts_json: Set(snapshot.resolved_artifacts_json),
        runtime_environment_json: Set(snapshot.runtime_environment_json),
        history_json: Set(snapshot.history_json),
        frozen_manifest_id: Set(frozen_manifest_id),
        created_at: Set(snapshot.created_at),
        updated_at: Set(snapshot.updated_at),
    })
}

pub(crate) async fn insert_snapshot_if_absent<C: ConnectionTrait>(
    db: &C,
    snapshot: turn_runtime_snapshot::ActiveModel,
    turn_id: &str,
    workspace_id: &str,
    root: &crate::frozen_lifetime::PreparedFrozenRoot,
) -> Result<TurnRuntimeSnapshotRecord> {
    root.verify(db, workspace_id).await?;
    turn_runtime_snapshot::Entity::insert(snapshot)
        .on_conflict(
            OnConflict::column(turn_runtime_snapshot::Column::TurnId)
                .do_nothing()
                .to_owned(),
        )
        .try_insert()
        .exec(db)
        .await
        .context("failed to insert immutable native runtime snapshot")?;
    find_turn_runtime_snapshot(db, turn_id)
        .await?
        .context("immutable native runtime snapshot is missing")
}

impl NewTurnRuntimeSnapshot {
    pub(crate) fn validate_admission_json(&self) -> Result<()> {
        self.validate_storage_size()?;
        for encoded in [
            Some(self.mode_json.as_str()),
            self.agent_skill_versions_json.as_deref(),
            self.ordinary_skill_catalog_json.as_deref(),
            self.provider_model_contract_json.as_deref(),
            self.initial_phase_json.as_deref(),
            Some(self.hook_runtime_context_json.as_str()),
            Some(self.workspace_skill_policies_json.as_str()),
            Some(self.input_json.as_str()),
            Some(self.capabilities_json.as_str()),
            Some(self.resolved_artifacts_json.as_str()),
            Some(self.runtime_environment_json.as_str()),
            Some(self.history_json.as_str()),
        ]
        .into_iter()
        .flatten()
        {
            let _: serde_json::Value = serde_json::from_str(encoded)
                .map_err(|_| anyhow::anyhow!("native snapshot candidate JSON is invalid"))?;
        }
        Ok(())
    }

    /// Called after the SQL transaction is released. JSON key ordering is not
    /// semantic; accepted array order and all resolved startup values are.
    pub(crate) fn same_admission_payload(
        &self,
        accepted: &TurnRuntimeSnapshotRecord,
    ) -> Result<bool> {
        self.validate_storage_size()?;
        accepted.validate_storage_size()?;
        if self.turn_id != accepted.turn_id
            || self.thread_id != accepted.thread_id
            || self.workspace_id != accepted.workspace_id
            || self.model != accepted.model
            || self.provider_name != accepted.provider_name
            || self.reasoning_effort != accepted.reasoning_effort
        {
            return Ok(false);
        }
        for (candidate, original) in [
            (
                self.provider_model_contract_json.as_deref(),
                accepted.provider_model_contract_json.as_deref(),
            ),
            (
                Some(self.mode_json.as_str()),
                Some(accepted.mode_json.as_str()),
            ),
            (
                self.agent_skill_versions_json.as_deref(),
                accepted.agent_skill_versions_json.as_deref(),
            ),
            (
                self.ordinary_skill_catalog_json.as_deref(),
                accepted.ordinary_skill_catalog_json.as_deref(),
            ),
            (
                Some(self.hook_runtime_context_json.as_str()),
                Some(accepted.hook_runtime_context_json.as_str()),
            ),
            (
                Some(self.workspace_skill_policies_json.as_str()),
                Some(accepted.workspace_skill_policies_json.as_str()),
            ),
            (
                Some(self.input_json.as_str()),
                Some(accepted.input_json.as_str()),
            ),
            (
                Some(self.capabilities_json.as_str()),
                Some(accepted.capabilities_json.as_str()),
            ),
            (
                Some(self.resolved_artifacts_json.as_str()),
                Some(accepted.resolved_artifacts_json.as_str()),
            ),
            (
                Some(self.runtime_environment_json.as_str()),
                Some(accepted.runtime_environment_json.as_str()),
            ),
            (
                Some(self.history_json.as_str()),
                Some(accepted.history_json.as_str()),
            ),
        ] {
            match (candidate, original) {
                (None, None) => {}
                (Some(candidate), Some(original)) => {
                    let candidate: serde_json::Value =
                        serde_json::from_str(candidate).map_err(|_| {
                            anyhow::anyhow!("native snapshot candidate JSON is invalid")
                        })?;
                    let original: serde_json::Value = serde_json::from_str(original)
                        .map_err(|_| anyhow::anyhow!("accepted native snapshot JSON is invalid"))?;
                    if candidate != original {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
        Ok(true)
    }
}

pub async fn upsert_turn_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot: turn_runtime_snapshot::ActiveModel,
    turn_id: &str,
    workspace_id: &str,
    root: &crate::frozen_lifetime::PreparedFrozenRoot,
) -> Result<TurnRuntimeSnapshotRecord> {
    root.verify(db, workspace_id).await?;
    turn_runtime_snapshot::Entity::insert(snapshot)
        .on_conflict(
            OnConflict::column(turn_runtime_snapshot::Column::TurnId)
                .update_columns([
                    turn_runtime_snapshot::Column::ThreadId,
                    turn_runtime_snapshot::Column::WorkspaceId,
                    turn_runtime_snapshot::Column::ModeJson,
                    turn_runtime_snapshot::Column::Model,
                    turn_runtime_snapshot::Column::ProviderName,
                    turn_runtime_snapshot::Column::ReasoningEffort,
                    turn_runtime_snapshot::Column::AgentSkillVersionsJson,
                    turn_runtime_snapshot::Column::OrdinarySkillCatalogJson,
                    turn_runtime_snapshot::Column::ProviderModelContractJson,
                    turn_runtime_snapshot::Column::InitialPhaseJson,
                    turn_runtime_snapshot::Column::HookRuntimeContextJson,
                    turn_runtime_snapshot::Column::WorkspaceSkillPoliciesJson,
                    turn_runtime_snapshot::Column::InputJson,
                    turn_runtime_snapshot::Column::CapabilitiesJson,
                    turn_runtime_snapshot::Column::ResolvedArtifactsJson,
                    turn_runtime_snapshot::Column::RuntimeEnvironmentJson,
                    turn_runtime_snapshot::Column::HistoryJson,
                    turn_runtime_snapshot::Column::FrozenManifestId,
                    turn_runtime_snapshot::Column::UpdatedAt,
                ])
                .to_owned(),
        )
        .exec(db)
        .await
        .context("failed to upsert turn_runtime_snapshot row")?;

    find_turn_runtime_snapshot(db, turn_id)
        .await?
        .context("upserted turn_runtime_snapshot row is missing")
}

pub async fn find_turn_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnRuntimeSnapshotRecord>> {
    let row = turn_runtime_snapshot::Entity::find_by_id(turn_id.to_owned())
        .filter(Expr::cust_with_values(
            format!("({SNAPSHOT_STORAGE_BYTES_SQL}) <= ?"),
            [pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES as i64],
        ))
        .one(db)
        .await
        .context("failed to query turn_runtime_snapshot row")?;
    if row.is_none()
        && turn_runtime_snapshot::Entity::find_by_id(turn_id.to_owned())
            .count(db)
            .await?
            > 0
    {
        anyhow::bail!(
            "runtime snapshot exceeds accepted execution byte limit or changed during bounded read"
        );
    }
    Ok(row.map(record_from_model))
}

pub async fn clear_agent_skill_versions_if_matches<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    expected_agent_skill_versions_json: &str,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let affected = turn_runtime_snapshot::Entity::update_many()
        .col_expr(
            turn_runtime_snapshot::Column::AgentSkillVersionsJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            turn_runtime_snapshot::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(turn_runtime_snapshot::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_runtime_snapshot::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            turn_runtime_snapshot::Column::AgentSkillVersionsJson
                .eq(expected_agent_skill_versions_json.to_owned()),
        )
        .exec(db)
        .await
        .context("failed to clear Agent skill pins from turn_runtime_snapshot row")?
        .rows_affected;
    Ok(affected == 1)
}

pub async fn delete_turn_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    let deleted = turn_runtime_snapshot::Entity::delete_by_id(turn_id.to_owned())
        .exec(db)
        .await
        .context("failed to delete turn_runtime_snapshot row")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_turn_runtime_snapshots_for_closed_turns<C: ConnectionTrait>(
    db: &C,
) -> Result<u64> {
    let closed_statuses = [
        turn_status_to_db(TurnStatus::Completed).to_owned(),
        turn_status_to_db(TurnStatus::Failed).to_owned(),
        turn_status_to_db(TurnStatus::Interrupted).to_owned(),
    ];

    let closed_turns = Query::select()
        .column(turn::Column::Id)
        .from(turn::Entity)
        .and_where(turn::Column::Status.is_in(closed_statuses))
        .to_owned();

    let deleted = turn_runtime_snapshot::Entity::delete_many()
        .filter(turn_runtime_snapshot::Column::TurnId.in_subquery(closed_turns))
        .exec(db)
        .await
        .context("failed to delete closed turn_runtime_snapshot rows")?;
    Ok(deleted.rows_affected)
}

fn record_from_model(model: turn_runtime_snapshot::Model) -> TurnRuntimeSnapshotRecord {
    TurnRuntimeSnapshotRecord {
        turn_id: model.turn_id,
        thread_id: model.thread_id,
        workspace_id: model.workspace_id,
        mode_json: model.mode_json,
        model: model.model,
        provider_name: model.provider_name,
        reasoning_effort: model.reasoning_effort,
        agent_skill_versions_json: model.agent_skill_versions_json,
        provider_model_contract_json: model.provider_model_contract_json,
        initial_phase_json: model.initial_phase_json,
        ordinary_skill_catalog_json: model.ordinary_skill_catalog_json,
        hook_runtime_context_json: model.hook_runtime_context_json,
        workspace_skill_policies_json: model.workspace_skill_policies_json,
        input_json: model.input_json,
        capabilities_json: model.capabilities_json,
        resolved_artifacts_json: model.resolved_artifacts_json,
        runtime_environment_json: model.runtime_environment_json,
        history_json: model.history_json,
        created_at: model.created_at,
        updated_at: model.updated_at,
    }
}

/// All bounds, hashing, JSON validation and owner reads precede this statement.
/// The compare/execution/context/window predicates close every stale gap in
/// the single domain write, using only physically bounded SQL scalars.
pub(crate) fn prepare_initial_phase_update(
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    window_index: u32,
    owner_id: &str,
    owner_generation: u64,
    cancellation_sha256: &str,
    previous_json: &str,
    next_json: &str,
    now: DateTimeWithTimeZone,
) -> Result<sea_orm::Statement> {
    anyhow::ensure!(
        previous_json.len() <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES
            && next_json.len() <= pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES,
        "initial phase storage exceeds execution byte limit"
    );
    let sql = format!(
        "UPDATE turn_runtime_snapshot SET initial_phase_json=?, updated_at=? \
        WHERE turn_id=? AND thread_id=? AND workspace_id=? AND initial_phase_json=? \
        AND ({SNAPSHOT_STORAGE_BYTES_SQL} - coalesce(length(cast(initial_phase_json as blob)),0) + ?) <= ? \
        AND EXISTS (SELECT 1 FROM \"turn\" t JOIN thread th ON th.id=t.thread_id \
          JOIN turn_execution e ON e.turn_id=t.id \
          WHERE t.id=turn_runtime_snapshot.turn_id AND t.thread_id=turn_runtime_snapshot.thread_id \
          AND th.workspace_id=turn_runtime_snapshot.workspace_id \
          AND t.status='in_progress' AND t.message_deleted_at IS NULL \
          AND e.thread_id=t.thread_id AND e.workspace_id=th.workspace_id \
          AND e.executor_kind IN ('native_agent','api_provider') \
          AND e.status IN ('queued','starting','running','recovering') \
          AND e.owner_id=? AND e.owner_generation=?) \
        AND EXISTS (SELECT 1 FROM native_cancellation_context c \
          WHERE c.turn_id=turn_runtime_snapshot.turn_id \
          AND c.thread_id=turn_runtime_snapshot.thread_id AND c.workspace_id=turn_runtime_snapshot.workspace_id \
          AND c.context_sha256=? AND c.accepted_event_id IS NULL) \
        AND EXISTS (SELECT 1 FROM turn_execution_window w \
          WHERE w.turn_id=turn_runtime_snapshot.turn_id AND w.thread_id=turn_runtime_snapshot.thread_id \
          AND w.workspace_id=turn_runtime_snapshot.workspace_id AND w.window_index=? AND w.status='running' \
          AND w.window_index=(SELECT max(window_index) FROM turn_execution_window WHERE turn_id=w.turn_id))"
    );
    Ok(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        sql,
        [
            next_json.to_owned().into(),
            now.into(),
            turn_id.to_owned().into(),
            thread_id.to_owned().into(),
            workspace_id.to_owned().into(),
            previous_json.to_owned().into(),
            (next_json.len() as i64).into(),
            (pioneer_protocol::TURN_EXECUTION_REQUEST_MAX_BYTES as i64).into(),
            owner_id.to_owned().into(),
            i64::try_from(owner_generation)
                .context("initial phase owner generation invalid")?
                .into(),
            cancellation_sha256.to_owned().into(),
            i64::from(window_index).into(),
        ],
    ))
}
