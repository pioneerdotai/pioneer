//! MCP installation, catalog, ownership and audit mutations in one transaction.
use anyhow::{Context, Result, bail};
use pioneer_entity::package_component;
use sea_orm::{DatabaseTransaction, prelude::DateTimeWithTimeZone};

use crate::plugins::{PluginNativeWrite, PluginOwnershipWrite};
use crate::{
    McpAuditEventRecord, McpServerInstallationRecord, mcp_audit_event, mcp_server_catalog_snapshot,
    mcp_server_installation, plugins,
};

pub async fn publish(
    transaction: &DatabaseTransaction,
    record: &McpServerInstallationRecord,
    audit: &McpAuditEventRecord,
    ownership: Option<&PluginOwnershipWrite>,
    native: Option<&PluginNativeWrite>,
    prepared_link: Option<package_component::ActiveModel>,
    now: DateTimeWithTimeZone,
) -> Result<String> {
    let existing = mcp_server_installation::find_mcp_server_installation(
        transaction,
        &record.scope_kind,
        &record.scope_key,
        &record.name,
    )
    .await?;
    let child_id = ownership
        .map(|write| write.child_id.as_str())
        .or_else(|| existing.as_ref().map(|row| row.id.as_str()))
        .or(record.id.as_deref())
        .unwrap_or("");
    if ownership.is_some()
        && (record.scope_kind != "workspace" || record.id.as_deref() != Some(child_id))
    {
        bail!("owned MCP publication requires reserved ID and workspace scope");
    }
    if native.is_some() {
        if record.scope_kind != "workspace"
            || existing.as_ref().map(|row| row.id.as_str()) != Some(child_id)
        {
            bail!("owned native settings cannot adopt an installation");
        }
        plugins::validate_native_write(transaction, native, &record.scope_key, "mcp", child_id)
            .await?;
    } else {
        plugins::validate_publication(
            transaction,
            ownership,
            &record.scope_key,
            "mcp",
            child_id,
            existing.as_ref().map(|row| row.id.as_str()),
        )
        .await?;
    }
    let installation_id =
        mcp_server_installation::upsert_mcp_server_installation(transaction, record, now, now)
            .await?;
    let mut audit = audit.clone();
    audit.server_installation_id = Some(installation_id.clone());
    mcp_audit_event::insert_mcp_audit_event(transaction, &audit).await?;
    if let Some(link) = prepared_link.clone() {
        if installation_id != ownership.expect("prepared ownership").child_id {
            bail!("MCP publication identity changed");
        }
        plugins::publish(transaction, link).await?;
    }
    if let Some(native) = native {
        plugins::publish_native_write(transaction, native, "mcp", false).await?;
    }
    Ok(installation_id)
}

pub async fn remove(
    transaction: &DatabaseTransaction,
    record: &McpServerInstallationRecord,
    audit: &McpAuditEventRecord,
    native: Option<&PluginNativeWrite>,
    package_removal: bool,
) -> Result<()> {
    let current = mcp_server_installation::find_mcp_server_installation(
        transaction,
        &record.scope_kind,
        &record.scope_key,
        &record.name,
    )
    .await?;
    if let Some(native) = native {
        let current = current.as_ref().context("MCP installation missing")?;
        if Some(current.id.as_str()) != record.id.as_deref() {
            bail!("MCP uninstall installation identity changed");
        }
        plugins::validate_native_write(
            transaction,
            Some(native),
            &record.scope_key,
            "mcp",
            &current.id,
        )
        .await?;
    } else if let Some(current) = current {
        plugins::validate_native_write(transaction, None, &record.scope_key, "mcp", &current.id)
            .await?;
    }
    if let Some(server_installation_id) = record.id.as_deref() {
        mcp_server_catalog_snapshot::delete_mcp_server_catalog_snapshot(
            transaction,
            server_installation_id,
        )
        .await?;
    }
    mcp_audit_event::insert_mcp_audit_event(transaction, audit).await?;
    mcp_server_installation::delete_mcp_server_installation(
        transaction,
        &record.scope_kind,
        &record.scope_key,
        &record.name,
    )
    .await?;
    if let Some(native) = native {
        plugins::publish_native_write(transaction, native, "mcp", true).await?;
        if package_removal {
            plugins::mark_package_removed(transaction, native, "mcp").await?;
        }
    }
    Ok(())
}
