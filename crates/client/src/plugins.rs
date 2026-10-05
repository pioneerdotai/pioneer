//! Parent-only plugin state and selection. Desktop consumes these Rust types
//! directly; mobile can reuse the reducer/selector without shell dependencies.
use crate::composer::capabilities::{ComposerCapability, ComposerCapabilityKind};
use pioneer_protocol::{PluginItem, PluginsListResponse};

#[derive(Clone, Debug, Default)]
pub struct PluginCatalogState {
    pub plugins: Vec<PluginItem>,
    pub loading: bool,
    pub failed: bool,
    pub refresh_requested: bool,
}
impl PluginCatalogState {
    pub fn accept(&mut self, response: PluginsListResponse) {
        self.plugins = response.plugins;
        self.loading = false;
        self.failed = false;
    }
    pub fn fail(&mut self) {
        self.loading = false;
        self.failed = true;
    }
}
pub fn selectable_plugins<'a>(plugins: &'a [PluginItem], query: &str) -> Vec<&'a PluginItem> {
    let query = query.trim().to_lowercase();
    plugins
        .iter()
        .filter(|p| p.enabled && p.state == "installed" && p.name.to_lowercase().contains(&query))
        .collect()
}
pub fn plugin_capability(plugin: &PluginItem) -> Option<ComposerCapability> {
    (plugin.enabled && plugin.state == "installed").then(|| ComposerCapability {
        id: pioneer_protocol::plugin_capability_key(&plugin.id),
        label: plugin.name.clone(),
        kind: ComposerCapabilityKind::Plugin {
            plugin_id: plugin.id.clone(),
            expected_revision: plugin.revision,
        },
    })
}
pub fn replace_selected_plugins(
    current: &[ComposerCapability],
    plugins: &[PluginItem],
    selected: &std::collections::BTreeSet<String>,
) -> Vec<ComposerCapability> {
    let mut result: Vec<_> = current
        .iter()
        .filter(|c| !matches!(c.kind, ComposerCapabilityKind::Plugin { .. }))
        .cloned()
        .collect();
    for plugin in plugins.iter().filter(|p| selected.contains(&p.id)) {
        if let Some(capability) = plugin_capability(plugin) {
            result.push(capability);
        }
    }
    result
}

impl crate::core::ClientCore {
    pub fn watch_plugin_catalog(&self) -> tokio::sync::watch::Receiver<u64> {
        self.plugin_catalog_changes.subscribe()
    }
    pub fn plugins_management_allowed(&self, workspace: &str) -> bool {
        self.capability_management_allowed(workspace)
    }
    /// Blocking adapter for shell background callers. Bind reads to the chosen
    /// Gateway and discard responses after a connection/authorization change.
    pub fn read_plugins(&self, workspace: &str) -> anyhow::Result<PluginsListResponse> {
        let epoch = self.provider_runtime_epoch();
        let connection = epoch
            .2
            .ok_or_else(|| anyhow::anyhow!("gateway_not_connected"))?;
        let transport = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(connection);
        let result = crate::transport::ws::command_sender::plugins_list(
            &transport,
            pioneer_protocol::PluginsListParams {
                workspace_id: workspace.into(),
            },
        )?;
        anyhow::ensure!(
            self.provider_runtime_epoch() == epoch,
            "plugin_catalog_stale"
        );
        Ok(result)
    }
    pub fn read_plugin_details(&self, workspace: &str, id: &str) -> anyhow::Result<PluginItem> {
        let epoch = self.provider_runtime_epoch();
        let connection = epoch
            .2
            .ok_or_else(|| anyhow::anyhow!("gateway_not_connected"))?;
        let transport = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(connection);
        let result = crate::transport::ws::command_sender::plugins_details(
            &transport,
            pioneer_protocol::PluginsDetailsParams {
                workspace_id: workspace.into(),
                plugin_id: id.into(),
            },
        )?;
        anyhow::ensure!(
            self.provider_runtime_epoch() == epoch,
            "plugin_catalog_stale"
        );
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plugin(id: char, state: &str) -> PluginItem {
        PluginItem {
            id: id.to_string().repeat(21),
            name: "mixed".into(),
            version: None,
            enabled: true,
            state: state.into(),
            revision: 7,
            status: "partial".into(),
            diagnostics: vec![],
            components: vec![pioneer_protocol::PluginComponentItem {
                kind: "skill".into(),
                member_key: "one".into(),
                status: "installed".into(),
                diagnostic: None,
                skill_id: Some(pioneer_protocol::SkillId::new("S".repeat(21)).unwrap()),
                mcp_installation_id: None,
                runtime_status: None,
            }],
        }
    }
    #[test]
    fn mixed_selection_draft_and_history_keep_only_the_parent() {
        let parent = plugin('P', "installed");
        let selected = std::collections::BTreeSet::from([parent.id.clone()]);
        let chips = replace_selected_plugins(&[], &[parent.clone()], &selected);
        assert_eq!(chips.len(), 1);
        assert_eq!(
            chips[0].id,
            pioneer_protocol::plugin_capability_key(&parent.id)
        );
        let restored: ComposerCapability =
            serde_json::from_str(&serde_json::to_string(&chips[0]).unwrap()).unwrap();
        assert_eq!(restored, chips[0]);
        assert!(matches!(
            restored.kind,
            ComposerCapabilityKind::Plugin {
                expected_revision: 7,
                ..
            }
        ));
        assert!(
            crate::composer::capabilities::composer_capability_removal_reason(
                &restored,
                crate::composer::capabilities::ComposerCapabilityTarget::cli_submission()
            )
            .is_none()
        );
        let attachment = restored.to_user_message_attachment();
        let history: pioneer_protocol::UserMessageAttachment =
            serde_json::from_str(&serde_json::to_string(&attachment).unwrap()).unwrap();
        assert!(
            matches!(history, pioneer_protocol::UserMessageAttachment::Plugin { capability } if capability.plugin_id == parent.id && capability.expected_revision == 7)
        );
        // A source component must never produce a second chip.
        assert!(!chips.iter().any(|c| c.id.starts_with("skill:")));
        assert!(replace_selected_plugins(&chips, &[parent], &Default::default()).is_empty());
    }
    #[test]
    fn interrupted_disabled_and_installing_parents_are_not_selectable() {
        let mut disabled = plugin('D', "installed");
        disabled.enabled = false;
        let parents = [
            plugin('A', "installed"),
            plugin('B', "installing"),
            plugin('C', "interrupted"),
            disabled,
        ];
        assert_eq!(selectable_plugins(&parents, "mixed").len(), 1);
        assert!(plugin_capability(&parents[1]).is_none());
    }
}
