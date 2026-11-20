use anyhow::{Context, Result};
use pioneer_entity::mcp_audit_event;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

pub async fn insert_mcp_audit_event<C: ConnectionTrait>(
    db: &C,
    record: &crate::McpAuditEventRecord,
) -> Result<()> {
    mcp_audit_event::Entity::insert(mcp_audit_event::ActiveModel {
        id: Set(pioneer_protocol::generate_id(21)),
        turn_id: Set(record.turn_id.clone()),
        server_installation_id: Set(record.server_installation_id.clone()),
        server_name: Set(record.server_name.clone()),
        raw_tool_name: Set(record.raw_tool_name.clone()),
        callable_name: Set(record.callable_name.clone()),
        catalog_version: Set(record.catalog_version.clone()),
        action: Set(record.action.clone()),
        decision: Set(record.decision.clone()),
        reason_code: Set(record.reason_code.clone()),
        details_json: Set(serde_json::from_str(record.details_json.as_str())
            .context("invalid MCP details_json")?),
        created_at: Set(
            sea_orm::prelude::DateTimeUtc::from_timestamp(record.created_at_unix, 0)
                .context("invalid MCP audit timestamp")?
                .fixed_offset(),
        ),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert MCP audit event `{}` for `{}`",
            record.action, record.server_name
        )
    })?;
    Ok(())
}

pub async fn list_recent_mcp_audit_events<C: ConnectionTrait>(
    db: &C,
    server_name: &str,
    limit: u64,
) -> Result<Vec<mcp_audit_event::Model>> {
    mcp_audit_event::Entity::find()
        .filter(mcp_audit_event::Column::ServerName.eq(server_name.to_owned()))
        .order_by_desc(mcp_audit_event::Column::CreatedAt)
        .order_by_asc(mcp_audit_event::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to query MCP audit events for `{server_name}`"))
}

pub async fn list_recent_mcp_audit_events_for_server_id<C: ConnectionTrait>(
    db: &C,
    server_installation_id: &str,
    limit: u64,
) -> Result<Vec<mcp_audit_event::Model>> {
    mcp_audit_event::Entity::find()
        .filter(mcp_audit_event::Column::ServerInstallationId.eq(server_installation_id.to_owned()))
        .order_by_desc(mcp_audit_event::Column::CreatedAt)
        .order_by_asc(mcp_audit_event::Column::Id)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to query MCP audit events for `{server_installation_id}`"))
}
