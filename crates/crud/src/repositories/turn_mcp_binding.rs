use anyhow::{Context, Result};
use pioneer_entity::{turn_mcp_binding, turn_mcp_projection};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn replace_turn_mcp_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    bindings: &[crate::TurnMcpBindingRecord],
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    delete_turn_mcp_bindings(db, turn_id).await?;

    for binding in bindings {
        insert_turn_mcp_binding(db, turn_id, binding, created_at).await?;
    }

    Ok(())
}

pub(crate) async fn delete_turn_mcp_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<()> {
    turn_mcp_binding::Entity::delete_many()
        .filter(turn_mcp_binding::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to clear turn MCP bindings")?;
    Ok(())
}

pub(crate) async fn insert_turn_mcp_binding<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    binding: &crate::TurnMcpBindingRecord,
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_mcp_binding::Entity::insert(turn_mcp_binding::ActiveModel {
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
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert turn MCP binding `{}` for turn `{turn_id}`",
            binding.callable_name
        )
    })?;
    Ok(())
}

pub(crate) async fn set_projection_activation_generation<C: ConnectionTrait>(
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
