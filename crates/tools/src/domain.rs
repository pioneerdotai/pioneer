use serde::{Deserialize, Serialize};

pub const MEMORY_DOMAIN_TOOL_NAMES: &[&str] = &[
    "memory_search",
    "memory_list",
    "memory_get",
    "memory_remember",
    "memory_forget",
];

pub const TASK_DOMAIN_TOOL_NAMES: &[&str] = &[
    "task_create",
    "task_wait",
    "task_cancel",
    "task_update",
    "task_detach",
    "task_list",
    "task_get",
    "task_reschedule",
    "task_pause",
    "task_resume",
];

pub const ARTIFACT_DOMAIN_TOOL_NAMES: &[&str] = &["artifact_prepare", "artifact_register"];

pub const COMPUTER_USE_DOMAIN_TOOL_NAMES: &[&str] = &["computer_use"];

pub const REQUEST_TOOLS_DOMAIN_VALUES: &[&str] = &[
    BuiltinToolDomain::Memory.as_str(),
    BuiltinToolDomain::Task.as_str(),
    BuiltinToolDomain::Artifact.as_str(),
    BuiltinToolDomain::ComputerUse.as_str(),
];

pub const BUILTIN_TOOL_DOMAIN_MAP: [(BuiltinToolDomain, &'static [&'static str]); 4] = [
    (BuiltinToolDomain::Memory, MEMORY_DOMAIN_TOOL_NAMES),
    (BuiltinToolDomain::Task, TASK_DOMAIN_TOOL_NAMES),
    (BuiltinToolDomain::Artifact, ARTIFACT_DOMAIN_TOOL_NAMES),
    (
        BuiltinToolDomain::ComputerUse,
        COMPUTER_USE_DOMAIN_TOOL_NAMES,
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuiltinToolDomain {
    Memory,
    Task,
    Artifact,
    ComputerUse,
}

impl BuiltinToolDomain {
    pub const ALL: [Self; 4] = [Self::Memory, Self::Task, Self::Artifact, Self::ComputerUse];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Task => "task",
            Self::Artifact => "artifact",
            Self::ComputerUse => "computer_use",
        }
    }

    pub const fn tool_names(self) -> &'static [&'static str] {
        match self {
            Self::Memory => MEMORY_DOMAIN_TOOL_NAMES,
            Self::Task => TASK_DOMAIN_TOOL_NAMES,
            Self::Artifact => ARTIFACT_DOMAIN_TOOL_NAMES,
            Self::ComputerUse => COMPUTER_USE_DOMAIN_TOOL_NAMES,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "memory" => Some(Self::Memory),
            "task" => Some(Self::Task),
            "artifact" => Some(Self::Artifact),
            "computer_use" => Some(Self::ComputerUse),
            _ => None,
        }
    }
}

pub fn builtin_tool_domain_map() -> &'static [(BuiltinToolDomain, &'static [&'static str])] {
    &BUILTIN_TOOL_DOMAIN_MAP
}

pub fn builtin_tool_domain_names(domain: BuiltinToolDomain) -> &'static [&'static str] {
    domain.tool_names()
}

pub fn registered_domain_tool_names<F>(
    domain: BuiltinToolDomain,
    mut is_registered: F,
) -> Vec<&'static str>
where
    F: FnMut(&str) -> bool,
{
    domain
        .tool_names()
        .iter()
        .copied()
        .filter(|name| is_registered(name))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::REQUEST_TOOLS_TOOL_NAME;
    use std::collections::HashSet;

    #[test]
    fn domain_values_match_request_tools_schema_contract() {
        let values = BuiltinToolDomain::ALL
            .iter()
            .map(|domain| domain.as_str())
            .collect::<Vec<_>>();

        assert_eq!(values, REQUEST_TOOLS_DOMAIN_VALUES);
    }

    #[test]
    fn domain_map_contains_exact_builtin_tool_names() {
        assert_eq!(
            BuiltinToolDomain::Memory.tool_names(),
            [
                "memory_search",
                "memory_list",
                "memory_get",
                "memory_remember",
                "memory_forget",
            ]
        );
        assert_eq!(
            BuiltinToolDomain::Task.tool_names(),
            [
                "task_create",
                "task_wait",
                "task_cancel",
                "task_update",
                "task_detach",
                "task_list",
                "task_get",
                "task_reschedule",
                "task_pause",
                "task_resume",
            ]
        );
        assert_eq!(
            BuiltinToolDomain::Artifact.tool_names(),
            ["artifact_prepare", "artifact_register"]
        );
        assert_eq!(
            BuiltinToolDomain::ComputerUse.tool_names(),
            ["computer_use"]
        );
    }

    #[test]
    fn domain_map_excludes_dynamic_skill_mcp_and_control_tools() {
        let mapped = builtin_tool_domain_map()
            .iter()
            .flat_map(|(_, names)| names.iter().copied())
            .collect::<Vec<_>>();

        assert!(!mapped.contains(&REQUEST_TOOLS_TOOL_NAME));
        assert!(!mapped.contains(&"read_skill"));
        assert!(!mapped.iter().any(|name| name.starts_with("skill.")));
        assert!(!mapped.iter().any(|name| name.starts_with("mcp_")));
        assert!(!mapped.iter().any(|name| name.contains("dynamic")));
    }

    #[test]
    fn domain_map_names_are_unique() {
        let mut names = HashSet::new();

        for (_, tool_names) in builtin_tool_domain_map() {
            for name in *tool_names {
                assert!(names.insert(*name), "duplicate domain tool name: {name}");
            }
        }
    }

    #[test]
    fn domain_parser_accepts_only_requestable_domain_values() {
        for value in REQUEST_TOOLS_DOMAIN_VALUES {
            assert_eq!(
                BuiltinToolDomain::parse(value).map(BuiltinToolDomain::as_str),
                Some(*value)
            );
        }

        assert_eq!(BuiltinToolDomain::parse("task_create"), None);
        assert_eq!(BuiltinToolDomain::parse("mcp_server_tool"), None);
    }

    #[cfg(feature = "computer-use")]
    #[test]
    fn domain_map_matches_registered_computer_use_builtin_spec_when_feature_enabled() {
        let specs = crate::builtin_tool_specs();
        let actual = specs
            .iter()
            .map(|configured| configured.spec.name.as_str())
            .filter(|name| BuiltinToolDomain::ComputerUse.tool_names().contains(name))
            .collect::<Vec<_>>();

        assert_eq!(
            actual.as_slice(),
            BuiltinToolDomain::ComputerUse.tool_names()
        );
    }

    #[cfg(not(feature = "computer-use"))]
    #[test]
    fn domain_map_handles_computer_use_as_unavailable_without_feature() {
        let registered = crate::builtin_tool_specs()
            .into_iter()
            .map(|configured| configured.spec.name)
            .collect::<HashSet<_>>();

        let actual = registered_domain_tool_names(BuiltinToolDomain::ComputerUse, |name| {
            registered.contains(name)
        });
        let expected = if registered.contains("computer_use") {
            vec!["computer_use"]
        } else {
            Vec::new()
        };

        assert_eq!(actual, expected);
    }
}
