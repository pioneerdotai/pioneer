use anyhow::{Context, Result};
use pioneer_entity::{turn_mcp_binding, turn_mcp_projection};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

const TURN_MCP_BINDING_INSERT_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone)]
pub struct PreparedTurnMcpBinding {
    model: turn_mcp_binding::ActiveModel,
    turn_id: String,
    callable_name: String,
}

pub fn prepare_turn_mcp_binding(
    turn_id: &str,
    binding: &TurnMcpBindingRecord,
    created_at: DateTimeWithTimeZone,
) -> PreparedTurnMcpBinding {
    PreparedTurnMcpBinding {
        model: turn_mcp_binding::ActiveModel {
            id: Set(pioneer_protocol::generate_id(21)),
            turn_id: Set(turn_id.to_owned()),
            server_installation_id: Set(binding.server_installation_id.clone()),
            server_name: Set(binding.server_name.clone()),
            raw_tool_name: Set(binding.raw_tool_name.clone()),
            callable_name: Set(binding.callable_name.clone()),
            canonical_callable_name: Set(binding.canonical_callable_name.clone()),
            provider_callable_name: Set(binding.provider_callable_name.clone()),
            catalog_version: Set(binding.catalog_version.clone()),
            fingerprint: Set(binding.fingerprint.clone()),
            canonical_schema_fingerprint: Set(binding.canonical_schema_fingerprint.clone()),
            provider_schema_fingerprint: Set(binding.provider_schema_fingerprint.clone()),
            annotations_json: Set(binding.annotations_json.clone()),
            annotations_digest: Set(binding.annotations_digest.clone()),
            effective_timeout_ms: Set(binding.effective_timeout_ms),
            runtime_generation: Set(binding.runtime_generation),
            projection_activation_generation: Set(binding.projection_activation_generation),
            selection_reason: Set(binding.selection_reason.clone()),
            capability_id: Set(binding.capability_id.clone()),
            created_at: Set(created_at),
        },
        turn_id: turn_id.to_owned(),
        callable_name: binding.callable_name.clone(),
    }
}

pub async fn replace_turn_mcp_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    bindings: &[TurnMcpBindingRecord],
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    let prepared = bindings
        .iter()
        .map(|binding| prepare_turn_mcp_binding(turn_id, binding, created_at))
        .collect::<Vec<_>>();
    delete_turn_mcp_bindings(db, turn_id).await?;
    insert_prepared_turn_mcp_bindings(db, &prepared).await
}

pub async fn delete_turn_mcp_bindings<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<()> {
    turn_mcp_binding::Entity::delete_many()
        .filter(turn_mcp_binding::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to clear turn MCP bindings")?;
    Ok(())
}

pub async fn insert_prepared_turn_mcp_binding<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnMcpBinding,
) -> Result<()> {
    let PreparedTurnMcpBinding {
        model,
        turn_id,
        callable_name,
    } = prepared;
    turn_mcp_binding::Entity::insert(model)
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to insert turn MCP binding `{}` for turn `{turn_id}`",
                callable_name,
            )
        })?;
    Ok(())
}

pub async fn insert_prepared_turn_mcp_bindings<C: ConnectionTrait>(
    db: &C,
    prepared: &[PreparedTurnMcpBinding],
) -> Result<()> {
    for batch in prepared.chunks(TURN_MCP_BINDING_INSERT_BATCH_SIZE) {
        turn_mcp_binding::Entity::insert_many(batch.iter().map(|binding| binding.model.clone()))
            .exec(db)
            .await
            .context("failed to insert prepared turn MCP binding batch")?;
    }
    Ok(())
}

pub async fn set_projection_activation_generation<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    activation_generation: i64,
) -> Result<u64> {
    let outcome = turn_mcp_binding::Entity::update_many()
        .col_expr(
            turn_mcp_binding::Column::ProjectionActivationGeneration,
            Expr::value(activation_generation),
        )
        .filter(turn_mcp_binding::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to bind turn MCP projection activation generation")?;
    Ok(outcome.rows_affected)
}

pub async fn find_turn_mcp_projection<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_mcp_projection::Model>> {
    turn_mcp_projection::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn MCP projection header")
}

pub async fn list_turn_mcp_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_mcp_binding::Model>> {
    turn_mcp_binding::Entity::find()
        .filter(turn_mcp_binding::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_mcp_binding::Column::CallableName)
        .all(db)
        .await
        .context("failed to query turn MCP bindings")
}

pub async fn list_recent_turn_mcp_bindings_for_server<C: ConnectionTrait>(
    db: &C,
    server_installation_id: &str,
    limit: u64,
) -> Result<Vec<turn_mcp_binding::Model>> {
    turn_mcp_binding::Entity::find()
        .filter(
            turn_mcp_binding::Column::ServerInstallationId.eq(server_installation_id.to_owned()),
        )
        .order_by_desc(turn_mcp_binding::Column::CreatedAt)
        .order_by_asc(turn_mcp_binding::Column::CallableName)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to query turn MCP bindings for `{server_installation_id}`")
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnMcpBindingRecord {
    pub server_installation_id: String,
    pub server_name: String,
    pub raw_tool_name: String,
    pub callable_name: String,
    pub canonical_callable_name: String,
    pub provider_callable_name: String,
    pub catalog_version: String,
    pub fingerprint: String,
    pub canonical_schema_fingerprint: String,
    pub provider_schema_fingerprint: String,
    pub annotations_json: String,
    pub annotations_digest: String,
    pub effective_timeout_ms: i64,
    pub runtime_generation: i64,
    pub projection_activation_generation: i64,
    pub selection_reason: String,
    pub capability_id: Option<String>,
}

pub fn turn_mcp_binding_record_from_model(model: turn_mcp_binding::Model) -> TurnMcpBindingRecord {
    TurnMcpBindingRecord {
        server_installation_id: model.server_installation_id,
        server_name: model.server_name,
        raw_tool_name: model.raw_tool_name,
        callable_name: model.callable_name,
        canonical_callable_name: model.canonical_callable_name,
        provider_callable_name: model.provider_callable_name,
        catalog_version: model.catalog_version,
        fingerprint: model.fingerprint,
        canonical_schema_fingerprint: model.canonical_schema_fingerprint,
        provider_schema_fingerprint: model.provider_schema_fingerprint,
        annotations_json: model.annotations_json,
        annotations_digest: model.annotations_digest,
        effective_timeout_ms: model.effective_timeout_ms,
        runtime_generation: model.runtime_generation,
        projection_activation_generation: model.projection_activation_generation,
        selection_reason: model.selection_reason,
        capability_id: model.capability_id,
    }
}
