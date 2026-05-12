use super::*;

pub(super) fn memory_tool_bundle_contribution(
    index: usize,
    bundle_id: HookToolBundleId,
    bundle: &ToolExtensionBundle,
    policy: &MemoryTurnPolicy,
) -> ToolBundleContribution {
    let tool_names = bundle
        .specs
        .iter()
        .filter_map(|configured| HookToolName::new(configured.spec.name.clone()).ok())
        .collect::<Vec<_>>();
    ToolBundleContribution {
        contribution_id: HookContributionId::new(format!(
            "{MEMORY_TOOL_BUNDLE_CONTRIBUTION_ID_PREFIX}.{index}"
        ))
        .expect("static contribution id is valid"),
        bundle_id,
        domain: HookDomain::new("memory").expect("static domain is valid"),
        priority: 100,
        diagnostics: vec![memory_safe_info_diagnostic(
            "memory.tools_exposed",
            format!(
                "memory tool bundle exposed: source={} reason={} tools={}",
                policy.source.as_str(),
                policy.reason_code.as_str(),
                hook_tool_names_csv(&tool_names)
            ),
        )],
        tool_names,
    }
}
#[cfg(test)]
pub(crate) fn memory_tool_names(materialization: &MemoryToolMaterialization) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut names = Vec::new();
    for bundle in &materialization.bundles {
        for configured in &bundle.specs {
            let name = configured.spec.name.trim();
            if !name.is_empty() && seen.insert(name.to_owned()) {
                names.push(name.to_owned());
            }
        }
    }
    names
}

pub(crate) fn filter_memory_tool_materialization(
    materialization: MemoryToolMaterialization,
    policy: &MemoryTurnPolicy,
) -> MemoryToolMaterialization {
    let mut diagnostics = materialization.diagnostics;
    let mut removed_tools = Vec::new();
    let mut bundles = Vec::new();

    for bundle in materialization.bundles {
        let mut allowed_spec_names = HashSet::new();
        let specs = bundle
            .specs
            .into_iter()
            .filter(|configured| {
                let name = configured.spec.name.as_str();
                let allowed = policy.allows_memory_tool(name);
                if allowed {
                    allowed_spec_names.insert(name.to_owned());
                } else {
                    removed_tools.push(name.to_owned());
                }
                allowed
            })
            .collect::<Vec<_>>();

        let handlers = bundle
            .handlers
            .into_iter()
            .filter(|(name, _)| {
                let allowed = allowed_spec_names.contains(name);
                if !allowed && policy.allows_memory_tool(name.as_str()) {
                    removed_tools.push(name.clone());
                }
                allowed
            })
            .collect::<Vec<_>>();

        if !specs.is_empty() || !handlers.is_empty() {
            bundles.push(pioneer_tools::ToolExtensionBundle { specs, handlers });
        }
    }

    removed_tools.sort();
    removed_tools.dedup();
    if !removed_tools.is_empty() {
        diagnostics.push(format!(
            "memory.policy.tools_filtered: source={} reason={} removed={}",
            policy.source.as_str(),
            policy.reason_code.as_str(),
            removed_tools.join(",")
        ));
    }

    MemoryToolMaterialization {
        bundles,
        diagnostics,
    }
}

pub(super) fn hook_tool_names_csv(tool_names: &[HookToolName]) -> String {
    if tool_names.is_empty() {
        return "none".to_owned();
    }
    tool_names
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(",")
}
