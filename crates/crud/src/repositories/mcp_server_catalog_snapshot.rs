use anyhow::{Context, Result};
use pioneer_entity::mcp_server_catalog_snapshot;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};

pub async fn upsert_mcp_server_catalog_snapshot<C: ConnectionTrait>(
    db: &C,
    record: &crate::McpServerCatalogSnapshotRecord,
    generated_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<()> {
    mcp_server_catalog_snapshot::Entity::insert(mcp_server_catalog_snapshot::ActiveModel {
        id: Set(pioneer_protocol::generate_id(21)),
        server_installation_id: Set(record.server_installation_id.clone()),
        catalog_version: Set(record.catalog_version.clone()),
        server_info_json: Set(record.server_info_json.clone()),
        server_instructions_hash: Set(record.server_instructions_hash.clone()),
        tools_json: Set(record.tools_json.clone()),
        resources_json: Set(record.resources_json.clone()),
        resource_templates_json: Set(record.resource_templates_json.clone()),
        prompts_json: Set(record.prompts_json.clone()),
        generated_at: Set(generated_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::column(mcp_server_catalog_snapshot::Column::ServerInstallationId)
            .update_columns([
                mcp_server_catalog_snapshot::Column::CatalogVersion,
                mcp_server_catalog_snapshot::Column::ServerInfoJson,
                mcp_server_catalog_snapshot::Column::ServerInstructionsHash,
                mcp_server_catalog_snapshot::Column::ToolsJson,
                mcp_server_catalog_snapshot::Column::ResourcesJson,
                mcp_server_catalog_snapshot::Column::ResourceTemplatesJson,
                mcp_server_catalog_snapshot::Column::PromptsJson,
                mcp_server_catalog_snapshot::Column::GeneratedAt,
                mcp_server_catalog_snapshot::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert MCP catalog snapshot for `{}`",
            record.server_installation_id
        )
    })?;
    Ok(())
}

pub async fn find_mcp_server_catalog_snapshot<C: ConnectionTrait>(
    db: &C,
    server_installation_id: &str,
) -> Result<Option<mcp_server_catalog_snapshot::Model>> {
    mcp_server_catalog_snapshot::Entity::find()
        .filter(
            mcp_server_catalog_snapshot::Column::ServerInstallationId
                .eq(server_installation_id.to_owned()),
        )
        .one(db)
        .await
        .with_context(|| {
            format!("failed to query MCP catalog snapshot for `{server_installation_id}`")
        })
}

pub async fn delete_mcp_server_catalog_snapshot<C: ConnectionTrait>(
    db: &C,
    server_installation_id: &str,
) -> Result<u64> {
    let result = mcp_server_catalog_snapshot::Entity::delete_many()
        .filter(
            mcp_server_catalog_snapshot::Column::ServerInstallationId
                .eq(server_installation_id.to_owned()),
        )
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete MCP catalog snapshot for `{server_installation_id}`")
        })?;
    Ok(result.rows_affected)
}
