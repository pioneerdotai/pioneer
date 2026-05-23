use anyhow::{Context, Result};
use pioneer_entity::turn_mcp_binding;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn replace_turn_mcp_bindings<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    bindings: &[crate::TurnMcpBindingRecord],
    created_at: DateTimeWithTimeZone,
) -> Result<()> {
    turn_mcp_binding::Entity::delete_many()
        .filter(turn_mcp_binding::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to clear turn MCP bindings")?;

    for binding in bindings {
        turn_mcp_binding::Entity::insert(turn_mcp_binding::ActiveModel {
            id: Set(pioneer_protocol::generate_id(21)),
            turn_id: Set(turn_id.to_owned()),
            server_installation_id: Set(binding.server_installation_id.clone()),
            server_name: Set(binding.server_name.clone()),
            raw_tool_name: Set(binding.raw_tool_name.clone()),
            callable_name: Set(binding.callable_name.clone()),
            catalog_version: Set(binding.catalog_version.clone()),
            fingerprint: Set(binding.fingerprint.clone()),
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
    }

    Ok(())
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
