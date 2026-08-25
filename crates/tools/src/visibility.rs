use crate::domain::builtin_tool_domain_map;
use crate::spec::ToolSpec;
use crate::tool_index::PREFLIGHT_CORE_TOOL_NAMES;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolVisibilitySource {
    Core,
    CurrentTurn,
    Preflight,
    Dynamic,
}

impl ToolVisibilitySource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::CurrentTurn => "current_turn",
            Self::Preflight => "preflight",
            Self::Dynamic => "dynamic",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolVisibilityDiagnosticCode {
    UnknownToolDropped,
    UnavailableToolDropped,
    PolicyBlockedToolDropped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolVisibilityDiagnostic {
    pub code: ToolVisibilityDiagnosticCode,
    pub tool_name: String,
    pub source: ToolVisibilitySource,
    pub reason: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FinalToolVisibilityInput {
    pub core_tools: Vec<String>,
    pub current_visible_tools: Vec<String>,
    pub preflight_visible_tools: Vec<String>,
    pub dynamic_tool_names: Vec<String>,
    pub registered_tool_names: Vec<String>,
    pub available_tool_names: Vec<String>,
    pub blocked_tool_names: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FinalToolVisibility {
    pub visible_tools: Vec<String>,
    pub diagnostics: Vec<ToolVisibilityDiagnostic>,
}

pub fn compute_final_tool_visibility(input: FinalToolVisibilityInput) -> FinalToolVisibility {
    let registered = normalized_set(input.registered_tool_names);
    let available = normalized_set(input.available_tool_names);
    let blocked = input
        .blocked_tool_names
        .into_iter()
        .filter_map(|(name, reason)| {
            let name = normalize_tool_name(name.as_str())?;
            Some((name, reason))
        })
        .collect::<BTreeMap<_, _>>();
    let mut result = FinalToolVisibility::default();
    let mut visible = BTreeSet::<String>::new();

    append_visible_tools(
        &mut result,
        &mut visible,
        input.core_tools,
        ToolVisibilitySource::Core,
        false,
        &registered,
        &available,
        &blocked,
    );
    append_visible_tools(
        &mut result,
        &mut visible,
        input.current_visible_tools,
        ToolVisibilitySource::CurrentTurn,
        true,
        &registered,
        &available,
        &blocked,
    );
    append_visible_tools(
        &mut result,
        &mut visible,
        input.preflight_visible_tools,
        ToolVisibilitySource::Preflight,
        true,
        &registered,
        &available,
        &blocked,
    );
    append_visible_tools(
        &mut result,
        &mut visible,
        input.dynamic_tool_names,
        ToolVisibilitySource::Dynamic,
        true,
        &registered,
        &available,
        &blocked,
    );

    result
}

pub fn materialized_dynamic_extension_tool_names(
    all_tool_names: impl IntoIterator<Item = String>,
    core_tool_names: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let core = normalized_set(core_tool_names);
    let static_core = PREFLIGHT_CORE_TOOL_NAMES
        .iter()
        .copied()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let domain_tools = builtin_tool_domain_map()
        .iter()
        .flat_map(|(_, names)| names.iter().copied())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::<String>::new();
    let mut dynamic = Vec::new();

    for name in all_tool_names {
        let Some(name) = normalize_tool_name(name.as_str()) else {
            continue;
        };
        if core.contains(name.as_str())
            || static_core.contains(name.as_str())
            || domain_tools.contains(name.as_str())
            || !seen.insert(name.clone())
        {
            continue;
        }
        dynamic.push(name);
    }

    dynamic
}

fn append_visible_tools(
    result: &mut FinalToolVisibility,
    visible: &mut BTreeSet<String>,
    names: Vec<String>,
    source: ToolVisibilitySource,
    diagnose_clamps: bool,
    registered: &BTreeSet<String>,
    available: &BTreeSet<String>,
    blocked: &BTreeMap<String, String>,
) {
    for raw_name in names {
        let Some(name) = normalize_tool_name(raw_name.as_str()) else {
            continue;
        };

        if !registered.contains(name.as_str()) {
            if diagnose_clamps {
                result.diagnostics.push(ToolVisibilityDiagnostic {
                    code: ToolVisibilityDiagnosticCode::UnknownToolDropped,
                    tool_name: name,
                    source,
                    reason: "tool is not registered for this turn".to_owned(),
                });
            }
            continue;
        }

        if !available.contains(name.as_str()) {
            if diagnose_clamps {
                result.diagnostics.push(ToolVisibilityDiagnostic {
                    code: ToolVisibilityDiagnosticCode::UnavailableToolDropped,
                    tool_name: name,
                    source,
                    reason: "tool has no available handler for this turn".to_owned(),
                });
            }
            continue;
        }

        if let Some(reason) = blocked.get(name.as_str()) {
            if diagnose_clamps {
                result.diagnostics.push(ToolVisibilityDiagnostic {
                    code: ToolVisibilityDiagnosticCode::PolicyBlockedToolDropped,
                    tool_name: name,
                    source,
                    reason: reason.clone(),
                });
            }
            continue;
        }

        if visible.insert(name.clone()) {
            result.visible_tools.push(name);
        }
    }
}

fn normalized_set(names: impl IntoIterator<Item = String>) -> BTreeSet<String> {
    names
        .into_iter()
        .filter_map(|name| normalize_tool_name(name.as_str()))
        .collect()
}

fn normalize_tool_name(name: &str) -> Option<String> {
    let name = name.trim();
    (!name.is_empty()).then(|| name.to_owned())
}

#[derive(Clone)]
pub struct ToolVisibilitySnapshot {
    all_specs: Arc<Vec<ToolSpec>>,
    visible_specs: Arc<RwLock<Vec<ToolSpec>>>,
}

impl ToolVisibilitySnapshot {
    pub fn new(all_specs: Vec<ToolSpec>) -> Self {
        let visible_specs = default_visible_specs(&all_specs);
        Self {
            all_specs: Arc::new(all_specs),
            visible_specs: Arc::new(RwLock::new(visible_specs)),
        }
    }

    pub async fn get(&self) -> Vec<ToolSpec> {
        self.visible_specs.read().await.clone()
    }

    pub async fn contains_name(&self, name: &str) -> bool {
        self.visible_specs
            .read()
            .await
            .iter()
            .any(|spec| spec.name == name)
    }

    pub async fn replace(&self, specs: Vec<ToolSpec>) {
        *self.visible_specs.write().await = specs;
    }

    pub async fn set_visible_by_name(&self, names: &[String]) {
        let mut seen = HashSet::<&str>::new();
        let mut selected = Vec::new();
        for name in names {
            if !seen.insert(name.as_str()) {
                continue;
            }
            if let Some(spec) = self.all_specs.iter().find(|spec| spec.name == *name) {
                selected.push(spec.clone());
            }
        }
        self.replace(selected).await;
    }

    pub fn all_specs(&self) -> &[ToolSpec] {
        self.all_specs.as_slice()
    }
}

fn default_visible_specs(all_specs: &[ToolSpec]) -> Vec<ToolSpec> {
    let all_tool_names = all_specs
        .iter()
        .map(|spec| spec.name.clone())
        .collect::<Vec<_>>();
    let default_visible_names = PREFLIGHT_CORE_TOOL_NAMES
        .iter()
        .copied()
        .map(str::to_owned)
        .chain(materialized_dynamic_extension_tool_names(
            all_tool_names,
            PREFLIGHT_CORE_TOOL_NAMES.iter().copied().map(str::to_owned),
        ))
        .collect::<HashSet<_>>();

    all_specs
        .iter()
        .filter(|spec| default_visible_names.contains(spec.name.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PayloadKind;

    fn spec(name: &str) -> ToolSpec {
        ToolSpec::new(
            name,
            "test tool",
            serde_json::json!({"type": "object"}),
            PayloadKind::Function,
        )
    }

    #[tokio::test]
    async fn visibility_snapshot_contains_only_currently_visible_names() {
        let snapshot = ToolVisibilitySnapshot::new(vec![spec("tool_a"), spec("tool_b")]);

        snapshot.set_visible_by_name(&["tool_a".to_owned()]).await;

        assert!(snapshot.contains_name("tool_a").await);
        assert!(!snapshot.contains_name("tool_b").await);
        assert!(!snapshot.contains_name("unknown_tool").await);
    }

    #[tokio::test]
    async fn visibility_snapshot_preserves_requested_visible_order() {
        let snapshot =
            ToolVisibilitySnapshot::new(vec![spec("tool_a"), spec("tool_b"), spec("tool_c")]);

        snapshot
            .set_visible_by_name(&[
                "tool_c".to_owned(),
                "tool_a".to_owned(),
                "tool_c".to_owned(),
                "missing_tool".to_owned(),
                "tool_b".to_owned(),
            ])
            .await;

        let visible = snapshot
            .get()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();
        assert_eq!(
            visible,
            vec![
                "tool_c".to_owned(),
                "tool_a".to_owned(),
                "tool_b".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn visibility_snapshot_defaults_to_core_plus_dynamic_extensions() {
        let snapshot = ToolVisibilitySnapshot::new(vec![
            spec("exec_command"),
            spec("read_file"),
            spec("list_dir"),
            spec("grep_files"),
            spec("apply_patch"),
            spec("request_tools"),
            spec("memory_search"),
            spec("task_create"),
            spec("artifact_prepare"),
            spec("computer_use"),
            spec("skill.weather"),
            spec("mcp.browser.open"),
        ]);

        let visible = snapshot
            .get()
            .await
            .into_iter()
            .map(|spec| spec.name)
            .collect::<Vec<_>>();

        assert_eq!(
            visible,
            vec![
                "exec_command".to_owned(),
                "read_file".to_owned(),
                "list_dir".to_owned(),
                "grep_files".to_owned(),
                "apply_patch".to_owned(),
                "request_tools".to_owned(),
                "skill.weather".to_owned(),
                "mcp.browser.open".to_owned()
            ]
        );
        assert!(snapshot.contains_name("request_tools").await);
        assert!(snapshot.contains_name("skill.weather").await);
        assert!(!snapshot.contains_name("memory_search").await);
        assert!(!snapshot.contains_name("task_create").await);
        assert!(!snapshot.contains_name("artifact_prepare").await);
        assert!(!snapshot.contains_name("computer_use").await);
        assert_eq!(snapshot.all_specs().len(), 12);
    }

    fn visibility_input() -> FinalToolVisibilityInput {
        FinalToolVisibilityInput {
            core_tools: vec!["exec_command".to_owned(), "request_tools".to_owned()],
            current_visible_tools: Vec::new(),
            preflight_visible_tools: Vec::new(),
            dynamic_tool_names: Vec::new(),
            registered_tool_names: vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "memory_search".to_owned(),
                "memory_get".to_owned(),
                "task_create".to_owned(),
                "artifact_prepare".to_owned(),
                "skill.weather".to_owned(),
            ],
            available_tool_names: vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "memory_search".to_owned(),
                "memory_get".to_owned(),
                "task_create".to_owned(),
                "artifact_prepare".to_owned(),
                "skill.weather".to_owned(),
            ],
            blocked_tool_names: BTreeMap::new(),
        }
    }

    #[test]
    fn final_visibility_includes_core_only_by_default() {
        let result = compute_final_tool_visibility(visibility_input());

        assert_eq!(
            result.visible_tools,
            vec!["exec_command".to_owned(), "request_tools".to_owned()]
        );
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn final_visibility_adds_preflight_optional_tools_when_available() {
        let mut input = visibility_input();
        input.preflight_visible_tools = vec!["memory_get".to_owned(), "memory_search".to_owned()];

        let result = compute_final_tool_visibility(input);

        assert_eq!(
            result.visible_tools,
            vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "memory_get".to_owned(),
                "memory_search".to_owned()
            ]
        );
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn final_visibility_preserves_current_turn_expansion() {
        let mut input = visibility_input();
        input.current_visible_tools = vec!["artifact_prepare".to_owned()];

        let result = compute_final_tool_visibility(input);

        assert_eq!(
            result.visible_tools,
            vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "artifact_prepare".to_owned()
            ]
        );
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn final_visibility_drops_unknown_unavailable_and_blocked_tools() {
        let mut input = visibility_input();
        input.preflight_visible_tools = vec![
            "memory_search".to_owned(),
            "missing_tool".to_owned(),
            "task_create".to_owned(),
            "memory_get".to_owned(),
        ];
        input
            .available_tool_names
            .retain(|name| name != "task_create");
        input.blocked_tool_names = BTreeMap::from([(
            "memory_get".to_owned(),
            "memory reads blocked by policy".to_owned(),
        )]);

        let result = compute_final_tool_visibility(input);

        assert_eq!(
            result.visible_tools,
            vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "memory_search".to_owned()
            ]
        );
        assert_eq!(result.diagnostics.len(), 3);
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ToolVisibilityDiagnosticCode::UnknownToolDropped
                && diagnostic.tool_name == "missing_tool"
                && diagnostic.source == ToolVisibilitySource::Preflight
        }));
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ToolVisibilityDiagnosticCode::UnavailableToolDropped
                && diagnostic.tool_name == "task_create"
        }));
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ToolVisibilityDiagnosticCode::PolicyBlockedToolDropped
                && diagnostic.tool_name == "memory_get"
        }));
    }

    #[test]
    fn final_visibility_preserves_dynamic_extensions_independent_of_preflight() {
        let mut input = visibility_input();
        input.dynamic_tool_names = vec!["skill.weather".to_owned()];

        let result = compute_final_tool_visibility(input);

        assert_eq!(
            result.visible_tools,
            vec![
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "skill.weather".to_owned()
            ]
        );
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn dynamic_extension_names_exclude_core_and_builtin_domain_tools() {
        let dynamic = materialized_dynamic_extension_tool_names(
            [
                "exec_command".to_owned(),
                "request_tools".to_owned(),
                "memory_search".to_owned(),
                "task_create".to_owned(),
                "artifact_prepare".to_owned(),
                "computer_use".to_owned(),
                "read_skill".to_owned(),
                "skill.weather".to_owned(),
                "mcp.browser.open".to_owned(),
            ],
            ["exec_command".to_owned(), "request_tools".to_owned()],
        );

        assert_eq!(
            dynamic,
            vec!["skill.weather".to_owned(), "mcp.browser.open".to_owned()]
        );
    }
}
