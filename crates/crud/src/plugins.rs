use crate::{CrudStore, repositories::plugins};
use anyhow::{Result, bail};
use pioneer_entity::{plugin_component, plugin_installation};
pub use plugins::PluginOwnershipWrite;

impl CrudStore {
    pub async fn insert_plugin_installation(
        &self,
        parent: &plugin_installation::Model,
    ) -> Result<()> {
        if parent.state != "installing" || parent.revision != 1 {
            bail!("new plugin must be installing at revision 1");
        }
        if let Some(pending) = &parent.pending_json {
            if pending.len() > 65536 {
                bail!("plugin pending plan too large");
            }
            let _: serde_json::Value = serde_json::from_str(pending)?;
        }
        let row: plugin_installation::ActiveModel = parent.clone().into();
        self.run_serialized_write(|| {
            let row = row.clone();
            async move { plugins::insert_parent(&self.connection, row).await }
        })
        .await
    }
    pub async fn find_plugin_installation(
        &self,
        id: &str,
    ) -> Result<Option<plugin_installation::Model>> {
        plugins::find(&self.connection, id).await
    }
    pub async fn find_plugin_by_upload(
        &self,
        upload: &str,
    ) -> Result<Option<plugin_installation::Model>> {
        plugins::find_by_upload(&self.connection, upload).await
    }
    pub async fn list_plugin_components(&self, id: &str) -> Result<Vec<plugin_component::Model>> {
        plugins::components(&self.connection, id).await
    }
    pub async fn find_skill_plugin_owner(
        &self,
        id: &pioneer_protocol::SkillId,
    ) -> Result<Option<plugin_component::Model>> {
        plugins::owner(&self.connection, "skill", id.as_str()).await
    }
    pub async fn find_mcp_plugin_owner(&self, id: &str) -> Result<Option<plugin_component::Model>> {
        plugins::owner(&self.connection, "mcp", id).await
    }
}

/// Preserve pre-existing standalone names while reserving new plugin names.
/// Ownership is checked separately, before side effects and under the writer.
pub fn validate_standalone_mcp_name(name: &str, existing: bool) -> Result<()> {
    if name.starts_with("pplugin_") && !existing {
        bail!("reserved plugin MCP name");
    }
    Ok(())
}
