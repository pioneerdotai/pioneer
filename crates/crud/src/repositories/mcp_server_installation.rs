use anyhow::{Context, Result};
use pioneer_entity::mcp_server_installation;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

pub async fn upsert_mcp_server_installation<C: ConnectionTrait>(
    db: &C,
    record: &crate::McpServerInstallationRecord,
    created_at: DateTimeWithTimeZone,
    updated_at: DateTimeWithTimeZone,
) -> Result<String> {
    let id = record
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(21));
    mcp_server_installation::Entity::insert(mcp_server_installation::ActiveModel {
        id: Set(id.clone()),
        scope_kind: Set(record.scope_kind.clone()),
        scope_key: Set(record.scope_key.clone()),
        name: Set(record.name.clone()),
        display_name: Set(record.display_name.clone()),
        source_kind: Set(record.source_kind.clone()),
        source_ref: Set(record.source_ref.clone()),
        transport_kind: Set(record.transport_kind.clone()),
        transport_json: Set(record.transport_json.clone()),
        auth_json: Set(record.auth_json.clone()),
        secret_refs_json: Set(record.secret_refs_json.clone()),
        enabled: Set(record.enabled),
        allow_implicit_invocation: Set(record.allow_implicit_invocation),
        required: Set(record.required),
        fingerprint: Set(record.fingerprint.clone()),
        created_at: Set(created_at),
        updated_at: Set(updated_at),
    })
    .on_conflict(
        OnConflict::columns([
            mcp_server_installation::Column::ScopeKind,
            mcp_server_installation::Column::ScopeKey,
            mcp_server_installation::Column::Name,
        ])
        .update_columns([
            mcp_server_installation::Column::DisplayName,
            mcp_server_installation::Column::SourceKind,
            mcp_server_installation::Column::SourceRef,
            mcp_server_installation::Column::TransportKind,
            mcp_server_installation::Column::TransportJson,
            mcp_server_installation::Column::AuthJson,
            mcp_server_installation::Column::SecretRefsJson,
            mcp_server_installation::Column::Enabled,
            mcp_server_installation::Column::AllowImplicitInvocation,
            mcp_server_installation::Column::Required,
            mcp_server_installation::Column::Fingerprint,
            mcp_server_installation::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert MCP server installation `{}` ({}/{})",
            record.name, record.scope_kind, record.scope_key
        )
    })?;

    let model = find_mcp_server_installation(
        db,
        record.scope_kind.as_str(),
        record.scope_key.as_str(),
        record.name.as_str(),
    )
    .await?
    .context("upserted MCP server installation was not found")?;
    Ok(model.id)
}

pub async fn list_mcp_server_installations<C: ConnectionTrait>(
    db: &C,
    scope_kind: &str,
    scope_key: &str,
) -> Result<Vec<mcp_server_installation::Model>> {
    mcp_server_installation::Entity::find()
        .filter(mcp_server_installation::Column::ScopeKind.eq(scope_kind.to_owned()))
        .filter(mcp_server_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .order_by_asc(mcp_server_installation::Column::Name)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to query MCP server installations for `{scope_kind}/{scope_key}`")
        })
}

pub async fn find_mcp_server_installation<C: ConnectionTrait>(
    db: &C,
    scope_kind: &str,
    scope_key: &str,
    name: &str,
) -> Result<Option<mcp_server_installation::Model>> {
    mcp_server_installation::Entity::find()
        .filter(mcp_server_installation::Column::ScopeKind.eq(scope_kind.to_owned()))
        .filter(mcp_server_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .filter(mcp_server_installation::Column::Name.eq(name.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to query MCP server installation `{name}` ({scope_kind}/{scope_key})")
        })
}

pub async fn delete_mcp_server_installation<C: ConnectionTrait>(
    db: &C,
    scope_kind: &str,
    scope_key: &str,
    name: &str,
) -> Result<()> {
    mcp_server_installation::Entity::delete_many()
        .filter(mcp_server_installation::Column::ScopeKind.eq(scope_kind.to_owned()))
        .filter(mcp_server_installation::Column::ScopeKey.eq(scope_key.to_owned()))
        .filter(mcp_server_installation::Column::Name.eq(name.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete MCP server installation `{name}` ({scope_kind}/{scope_key})")
        })?;
    Ok(())
}
