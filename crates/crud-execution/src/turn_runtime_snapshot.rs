use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_runtime_snapshot};
use pioneer_protocol::{ThreadMode, TurnStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, OnConflict, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QuerySelect, Value};

use pioneer_crud_conversations::values::{
    thread_mode_from_db, thread_mode_to_db, turn_status_to_db,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRuntimeSnapshotRecord {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub mode: ThreadMode,
    pub model: String,
    pub provider_name: String,
    pub reasoning_effort: Option<String>,
    pub agent_skill_versions_json: Option<String>,
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
    pub mode: ThreadMode,
    pub model: String,
    pub provider_name: String,
    pub reasoning_effort: Option<String>,
    pub agent_skill_versions_json: Option<String>,
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

pub async fn lock_identity<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<(String, String)>> {
    use turn_runtime_snapshot::Column as Col;
    Ok(turn_runtime_snapshot::Entity::find_by_id(turn_id)
        .select_only()
        .columns([Col::ThreadId, Col::WorkspaceId])
        .lock_exclusive()
        .into_tuple::<(String, String)>()
        .one(db)
        .await?)
}

pub async fn upsert_turn_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    snapshot: NewTurnRuntimeSnapshot,
    frozen_manifest_id: Option<&str>,
) -> Result<TurnRuntimeSnapshotRecord> {
    let turn_id = snapshot.turn_id.clone();
    use turn_runtime_snapshot::Column as Col;
    let mutable = [
        Col::ThreadId,
        Col::WorkspaceId,
        Col::Mode,
        Col::Model,
        Col::ProviderName,
        Col::ReasoningEffort,
        Col::AgentSkillVersionsJson,
        Col::HookRuntimeContextJson,
        Col::WorkspaceSkillPoliciesJson,
        Col::InputJson,
        Col::CapabilitiesJson,
        Col::ResolvedArtifactsJson,
        Col::RuntimeEnvironmentJson,
        Col::HistoryJson,
        Col::FrozenManifestId,
        Col::UpdatedAt,
    ];
    // Insert<ActiveModel> binds JSON through serde_json::Value, which changes
    // serialized bytes. This insert keeps the original JSON for CAS and history.
    let insert = Query::insert()
        .into_table(turn_runtime_snapshot::Entity)
        .columns([Col::TurnId, Col::CreatedAt].into_iter().chain(mutable))
        .values([
            Expr::val(snapshot.turn_id),
            Expr::val(snapshot.created_at),
            Expr::val(snapshot.thread_id),
            Expr::val(snapshot.workspace_id),
            Expr::val(thread_mode_to_db(snapshot.mode)),
            Expr::val(snapshot.model),
            Expr::val(snapshot.provider_name),
            Expr::val(snapshot.reasoning_effort),
            json_text(snapshot.agent_skill_versions_json),
            json_text(snapshot.hook_runtime_context_json),
            json_text(snapshot.workspace_skill_policies_json),
            json_text(snapshot.input_json),
            json_text(snapshot.capabilities_json),
            json_text(snapshot.resolved_artifacts_json),
            json_text(snapshot.runtime_environment_json),
            json_text(snapshot.history_json),
            Expr::val(frozen_manifest_id),
            Expr::val(snapshot.updated_at),
        ])?
        .on_conflict(
            OnConflict::column(Col::TurnId)
                .update_columns(mutable)
                .to_owned(),
        )
        .to_owned();
    db.execute(&insert)
        .await
        .context("failed to upsert turn_runtime_snapshot row")?;

    find_turn_runtime_snapshot(db, turn_id.as_str())
        .await?
        .context("upserted turn_runtime_snapshot row is missing")
}

pub async fn find_frozen_manifest_id(
    db: &impl ConnectionTrait,
    turn_id: &str,
) -> Result<Option<String>> {
    Ok(
        turn_runtime_snapshot::Entity::find_by_id(turn_id.to_owned())
            .select_only()
            .column(turn_runtime_snapshot::Column::FrozenManifestId)
            .into_tuple::<Option<String>>()
            .one(db)
            .await?
            .flatten(),
    )
}

pub async fn find_turn_runtime_snapshot<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnRuntimeSnapshotRecord>> {
    use turn_runtime_snapshot::Column as Col;
    let mut query = Query::select();
    query
        .from(turn_runtime_snapshot::Entity)
        .columns([
            Col::TurnId,
            Col::ThreadId,
            Col::WorkspaceId,
            Col::Mode,
            Col::Model,
            Col::ProviderName,
            Col::ReasoningEffort,
            Col::CreatedAt,
            Col::UpdatedAt,
        ])
        .and_where(Col::TurnId.eq(turn_id));
    for (column, name) in [
        (Col::AgentSkillVersionsJson, "agent_skill_versions_json"),
        (Col::HookRuntimeContextJson, "hook_runtime_context_json"),
        (
            Col::WorkspaceSkillPoliciesJson,
            "workspace_skill_policies_json",
        ),
        (Col::InputJson, "input_json"),
        (Col::CapabilitiesJson, "capabilities_json"),
        (Col::ResolvedArtifactsJson, "resolved_artifacts_json"),
        (Col::RuntimeEnvironmentJson, "runtime_environment_json"),
        (Col::HistoryJson, "history_json"),
    ] {
        query.expr_as(
            Expr::col(column).cast_as(Alias::new("text")),
            Alias::new(name),
        );
    }
    let row = db
        .query_one(&query)
        .await
        .context("failed to query turn_runtime_snapshot row")?;
    row.map(|row| {
        let mode: String = row.try_get("", "mode")?;
        Ok(TurnRuntimeSnapshotRecord {
            turn_id: row.try_get("", "turn_id")?,
            thread_id: row.try_get("", "thread_id")?,
            workspace_id: row.try_get("", "workspace_id")?,
            mode: thread_mode_from_db(&mode).context("invalid runtime snapshot mode")?,
            model: row.try_get("", "model")?,
            provider_name: row.try_get("", "provider_name")?,
            reasoning_effort: row.try_get("", "reasoning_effort")?,
            agent_skill_versions_json: row.try_get("", "agent_skill_versions_json")?,
            hook_runtime_context_json: row.try_get("", "hook_runtime_context_json")?,
            workspace_skill_policies_json: row.try_get("", "workspace_skill_policies_json")?,
            input_json: row.try_get("", "input_json")?,
            capabilities_json: row.try_get("", "capabilities_json")?,
            resolved_artifacts_json: row.try_get("", "resolved_artifacts_json")?,
            runtime_environment_json: row.try_get("", "runtime_environment_json")?,
            history_json: row.try_get("", "history_json")?,
            created_at: row.try_get("", "created_at")?,
            updated_at: row.try_get("", "updated_at")?,
        })
    })
    .transpose()
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
            Expr::cust("NULL"),
        )
        .col_expr(
            turn_runtime_snapshot::Column::UpdatedAt,
            Expr::value(updated_at),
        )
        .filter(turn_runtime_snapshot::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_runtime_snapshot::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(
            Expr::col(turn_runtime_snapshot::Column::AgentSkillVersionsJson)
                .cast_as(Alias::new("text"))
                .eq(expected_agent_skill_versions_json),
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
        // Task children retain a capsule until their existing reconciler sees
        // the parent receipt; startup must not bypass that owner.
        .and_where(Expr::cust(
            "NOT EXISTS (SELECT 1 FROM task_run_turn trt WHERE trt.turn_id = turn.id)",
        ))
        .to_owned();

    let deleted = turn_runtime_snapshot::Entity::delete_many()
        .filter(turn_runtime_snapshot::Column::TurnId.in_subquery(closed_turns))
        .exec(db)
        .await
        .context("failed to delete closed turn_runtime_snapshot rows")?;
    Ok(deleted.rows_affected)
}

fn json_text(value: impl Into<Value>) -> Expr {
    Expr::val(value)
        .cast_as(Alias::new("text"))
        .cast_as(Alias::new("json"))
}
