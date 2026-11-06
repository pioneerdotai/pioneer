//! Parent-only plugin state and selection. Desktop consumes these Rust types
//! directly; mobile can reuse the reducer/selector without shell dependencies.
use crate::composer::capabilities::{ComposerCapability, ComposerCapabilityKind};
use pioneer_protocol::PluginsListResponse;

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
    /// A timeout is an uncertain server outcome: callers always refetch details
    /// after an error instead of reverting the parent optimistically.
    pub fn set_plugin_enabled(
        &self,
        params: pioneer_protocol::PluginsSetEnabledParams,
    ) -> anyhow::Result<PluginItem> {
        let epoch = self.provider_runtime_epoch();
        let connection = epoch
            .2
            .ok_or_else(|| anyhow::anyhow!("gateway_not_connected"))?;
        let transport = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(connection);
        let result = crate::transport::ws::command_sender::plugins_set_enabled(&transport, params)?;
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

pub use pioneer_protocol::{
    PluginComponentItem, PluginComponentKey, PluginItem, PluginManagementIntent,
    PluginsMutateParams, PluginsSetEnabledParams, PluginsUpdatePreviewResponse,
};
/// UI-neutral current-action state. An error is an uncertain Gateway outcome;
/// the caller must refetch before enabling another mutation. No operation polling.
#[derive(Clone, Debug, Default)]
pub struct PluginManagementState {
    pub busy: bool,
    pub failed: bool,
    pub refresh_required: bool,
}
impl PluginManagementState {
    pub fn begin(&mut self) -> bool {
        if self.busy || self.refresh_required {
            return false;
        }
        self.busy = true;
        self.failed = false;
        true
    }
    pub fn complete(&mut self, success: bool) {
        self.busy = false;
        self.failed = !success;
        self.refresh_required = true;
    }
    pub fn refreshed(&mut self) {
        self.refresh_required = false;
    }
}
impl crate::core::ClientCore {
    /// Desktop calls this typed adapter directly; mobile can use the same intent
    /// in D. Bind the action to the selected Gateway and authorization epoch.
    pub fn mutate_plugin(
        &self,
        params: PluginsMutateParams,
    ) -> anyhow::Result<pioneer_protocol::PluginsMutationResponse> {
        let epoch = self.provider_runtime_epoch();
        let connection = epoch
            .2
            .ok_or_else(|| anyhow::anyhow!("gateway_not_connected"))?;
        let sender = self
            .transport_runtime()
            .ws_command_sender()
            .requests_for_connection(connection);
        let result = crate::transport::ws::command_sender::plugins_mutate(&sender, params)?;
        anyhow::ensure!(
            self.provider_runtime_epoch() == epoch,
            "plugin_catalog_stale"
        );
        Ok(result)
    }
}

#[cfg(test)]
mod management_tests {
    // NOT_RUN / NOT_COMPILED. A failed/uncertain action cannot be retried on a
    // stale projection; a successful refetch is required in every shell.
    #[test]
    fn uncertain_mutation_requires_refetch_and_does_not_stick_in_busy() {
        let mut state = super::PluginManagementState::default();
        assert!(state.begin());
        assert!(!state.begin());
        state.complete(false);
        assert!(!state.busy);
        assert!(state.failed);
        assert!(!state.begin());
        state.refreshed();
        assert!(state.begin());
        state.complete(true);
        assert!(!state.busy);
        assert!(!state.failed);
        assert!(!state.begin());
        state.refreshed();
        assert!(state.begin());
    }
}

pub mod runtime;

/// Parent-only presentation for immediate pickers. Selection retains its original
/// revision even when catalog labels/revisions refresh. Desktop picker migration:
/// use these rows in place of shell-local eligibility and selected-key checks.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct PluginPickerRow {
    pub plugin_id: String,
    pub key: String,
    pub label: String,
    pub selected: bool,
    pub selectable: bool,
    pub stale: bool,
}
pub fn plugin_picker_rows(
    current: &[ComposerCapability],
    plugins: &[PluginItem],
    query: &str,
) -> Vec<PluginPickerRow> {
    let query = query.trim().to_lowercase();
    let mut rows:Vec<_>=plugins.iter().filter(|p|p.name.to_lowercase().contains(&query)).map(|p|{
        let selected=current.iter().find(|c|matches!(&c.kind,ComposerCapabilityKind::Plugin{plugin_id,..} if plugin_id==&p.id));
        PluginPickerRow{plugin_id:p.id.clone(),key:pioneer_protocol::plugin_capability_key(&p.id),label:p.name.clone(),selected:selected.is_some(),selectable:plugin_capability(p).is_some(),stale:selected.is_some_and(|c|matches!(c.kind,ComposerCapabilityKind::Plugin{expected_revision,..} if expected_revision!=p.revision))}
    }).collect();
    for c in current {
        if let ComposerCapabilityKind::Plugin { plugin_id, .. } = &c.kind {
            if !plugins.iter().any(|p| &p.id == plugin_id)
                && c.label.to_lowercase().contains(&query)
            {
                rows.push(PluginPickerRow {
                    plugin_id: plugin_id.clone(),
                    key: c.id.clone(),
                    label: c.label.clone(),
                    selected: true,
                    selectable: false,
                    stale: true,
                });
            }
        }
    }
    rows
}
// Parent selection has no client-provided expansion. Keep already selected
// parents removable under unsupported providers; new selection uses the same
// proven Skills/MCP target policy. Gateway checks actual expanded capabilities.
pub(crate) fn plugin_picker_target_supported(
    target: crate::composer::capabilities::ComposerCapabilityTarget,
) -> bool {
    let policy = target.policy();
    policy.supports_skills && policy.supports_mcp_tools
}
impl crate::core::ClientCore {
    pub fn composer_plugin_picker(
        &self,
        thread: &str,
        draft: crate::composer::store::DraftId,
        query: &str,
    ) -> Vec<PluginPickerRow> {
        let Some(input) = self
            .composer_snapshot(thread)
            .filter(|p| p.draft_id() == draft)
        else {
            return vec![];
        };
        let catalog = self.composer_catalog_snapshot(thread);
        let mut rows = plugin_picker_rows(
            &input.domain().capabilities,
            catalog
                .as_ref()
                .filter(|p| p.draft_id == draft)
                .map_or(&[], |p| p.plugins.as_slice()),
            query,
        );
        if !plugin_picker_target_supported(input.domain().capability_target) {
            for row in &mut rows {
                row.selectable = false;
            }
        }
        rows
    }
}

#[cfg(test)]
mod picker_regressions {
    use super::*;
    // NOT_RUN / NOT_COMPILED: parent projection never upgrades authority.
    #[test]
    fn parent_picker_requires_proven_combined_target_support() {
        use crate::composer::capabilities::{ComposerCapabilityPolicy, ComposerCapabilityTarget};
        assert!(plugin_picker_target_supported(
            ComposerCapabilityTarget::native()
        ));
        for (skills, mcp) in [(false, false), (false, true), (true, false), (true, true)] {
            assert_eq!(
                plugin_picker_target_supported(ComposerCapabilityTarget::cli(
                    ComposerCapabilityPolicy::cli(skills, mcp)
                )),
                skills && mcp
            );
        }
    }
    #[test]
    fn refreshed_catalog_keeps_selected_revision_and_unknown_parent_removable() {
        let mut p = PluginItem {
            id: "P".repeat(21),
            name: "Mixed".into(),
            version: None,
            enabled: true,
            state: "installed".into(),
            revision: 7,
            status: "partial".into(),
            components: vec![],
            diagnostics: vec![],
        };
        let selected = plugin_capability(&p).unwrap();
        p.revision = 8;
        let rows = plugin_picker_rows(&[selected.clone()], &[p.clone()], "");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].selected && rows[0].stale);
        assert!(matches!(
            selected.kind,
            ComposerCapabilityKind::Plugin {
                expected_revision: 7,
                ..
            }
        ));
        let unknown = plugin_picker_rows(&[selected], &[], "");
        assert!(unknown[0].selected && unknown[0].stale && !unknown[0].selectable);
        p.enabled = false;
        assert!(!plugin_picker_rows(&[], &[p], "")[0].selectable);
    }
}
