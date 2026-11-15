//! Common installed-package management and composer projections.
pub mod runtime;
use crate::composer::capabilities::{ComposerCapability, ComposerCapabilityKind};
use crate::plugins::{
    PackageKind, PluginItem, PluginPickerRow, plugin_capability, plugin_picker_target_supported,
};

/// Parent projection shared by the two catalogs; domain stays in the capability.
pub fn package_capability(
    kind: pioneer_protocol::PackageKind,
    p: &PluginItem,
) -> Option<ComposerCapability> {
    if kind == pioneer_protocol::PackageKind::Plugin {
        return plugin_capability(p);
    }
    crate::apps::app_capability(p)
}

/// One projection for both catalogs; identity and eligibility come from the domain.
pub fn package_picker_rows(
    kind: PackageKind,
    current: &[ComposerCapability],
    packages: &[PluginItem],
    query: &str,
) -> Vec<PluginPickerRow> {
    let query = query.trim().to_lowercase();
    let belongs = |cap: &ComposerCapability| {
        matches!(
            (kind, &cap.kind),
            (PackageKind::App, ComposerCapabilityKind::App { .. })
                | (PackageKind::Plugin, ComposerCapabilityKind::Plugin { .. })
        )
    };
    let key = |id: &str| match kind {
        PackageKind::App => pioneer_protocol::app_capability_key(id),
        PackageKind::Plugin => pioneer_protocol::plugin_capability_key(id),
    };
    let mut rows = Vec::new();
    for parent in packages {
        let label = match kind {
            PackageKind::App => crate::apps::app_label(parent),
            PackageKind::Plugin => parent.name.clone(),
        };
        if !label.to_lowercase().contains(&query) {
            continue;
        }
        let identity = key(&parent.id);
        let selected = current.iter().find(|c| belongs(c) && c.id == identity);
        let stale = selected.is_some_and(|c| match &c.kind {
            ComposerCapabilityKind::App {
                expected_revision, ..
            }
            | ComposerCapabilityKind::Plugin {
                expected_revision, ..
            } => *expected_revision != parent.revision,
            _ => false,
        });
        rows.push(PluginPickerRow {
            plugin_id: parent.id.clone(),
            key: identity,
            label,
            description: parent.description.clone(),
            selected: selected.is_some(),
            selectable: package_capability(kind, parent).is_some(),
            stale,
        });
    }
    for cap in current.iter().filter(|cap| belongs(cap)) {
        if !packages.iter().any(|p| key(&p.id) == cap.id)
            && cap.label.to_lowercase().contains(&query)
        {
            let id = match &cap.kind {
                ComposerCapabilityKind::App { app_id, .. } => app_id,
                ComposerCapabilityKind::Plugin { plugin_id, .. } => plugin_id,
                _ => continue,
            };
            rows.push(PluginPickerRow {
                plugin_id: id.clone(),
                key: cap.id.clone(),
                label: cap.label.clone(),
                description: None,
                selected: true,
                selectable: false,
                stale: true,
            });
        }
    }
    rows
}

pub(crate) fn package_target_supported(
    kind: PackageKind,
    parent: &PluginItem,
    target: crate::composer::capabilities::ComposerCapabilityTarget,
) -> bool {
    if kind == PackageKind::Plugin {
        return plugin_picker_target_supported(target);
    }
    crate::apps::app_target_supported(parent, target)
}

#[cfg(test)]
mod app_target_regressions {
    use super::*;
    // NOT_RUN / NOT_COMPILED. An empty App stays an identity without fake tools.
    #[test]
    fn empty_app_is_selectable_without_skill_or_mcp_provider_support() {
        let parent:PluginItem=serde_json::from_value(serde_json::json!({"id":"A".repeat(21),"name":"empty-app","version":"1.0.0","enabled":true,"state":"installed","revision":1,"status":"installed","components":[],"diagnostics":[]})).unwrap();
        let target = crate::composer::capabilities::ComposerCapabilityTarget::cli(
            crate::composer::capabilities::ComposerCapabilityPolicy::unsupported_cli(),
        );
        assert!(package_target_supported(PackageKind::App, &parent, target));
        assert!(!package_target_supported(
            PackageKind::Plugin,
            &parent,
            target
        ));
        let wire = package_capability(PackageKind::App, &parent)
            .unwrap()
            .to_turn_capability();
        assert_eq!(wire.id, format!("app:{}", parent.id));
        assert!(matches!(
            wire.kind,
            pioneer_protocol::TurnCapabilityKind::App {
                expected_revision: 1,
                ..
            }
        ));
    }
}
