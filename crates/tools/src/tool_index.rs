use crate::domain::BuiltinToolDomain;
use crate::spec::{ConfiguredToolSpec, ToolIdempotencyMode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const PREFLIGHT_SUMMARY_MAX_CHARS: usize = 160;

pub const PREFLIGHT_CORE_TOOL_NAMES: &[&str] = &[
    "exec_command",
    "write_stdin",
    "read_file",
    "write_file",
    "list_dir",
    "grep_files",
    "apply_patch",
    "web_search",
    "web_fetch",
    "download_url",
    "read_skill",
    "request_tools",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreflightToolIndex {
    pub core_tools: Vec<String>,
    pub candidate_tools: Vec<PreflightCandidateToolDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreflightCandidateToolDescriptor {
    pub name: String,
    pub domain: BuiltinToolDomain,
    pub summary: String,
    pub mutation: bool,
}

pub fn build_preflight_tool_index<'a>(
    specs: impl IntoIterator<Item = &'a ConfiguredToolSpec>,
) -> PreflightToolIndex {
    let specs_by_name = specs
        .into_iter()
        .map(|configured| (configured.spec.name.as_str(), configured))
        .collect::<BTreeMap<_, _>>();

    let core_tools = PREFLIGHT_CORE_TOOL_NAMES
        .iter()
        .copied()
        .filter(|name| specs_by_name.contains_key(*name))
        .map(str::to_owned)
        .collect();

    let mut candidate_tools = Vec::new();
    for domain in BuiltinToolDomain::ALL {
        for tool_name in domain.tool_names() {
            let Some(configured) = specs_by_name.get(tool_name).copied() else {
                continue;
            };

            candidate_tools.push(PreflightCandidateToolDescriptor {
                name: (*tool_name).to_owned(),
                domain,
                summary: compact_tool_summary(configured.spec.description.as_str()),
                mutation: candidate_tool_mutation(domain, configured),
            });
        }
    }

    PreflightToolIndex {
        core_tools,
        candidate_tools,
    }
}

fn compact_tool_summary(description: &str) -> String {
    let normalized = description.split_whitespace().collect::<Vec<_>>().join(" ");
    let summary = normalized.trim();
    if summary.is_empty() {
        return "No summary available.".to_owned();
    }

    let first_sentence_end = summary
        .char_indices()
        .find_map(|(index, ch)| matches!(ch, '.' | '!' | '?').then_some(index + ch.len_utf8()));
    let first_sentence = first_sentence_end
        .map(|end| &summary[..end])
        .unwrap_or(summary)
        .trim();

    truncate_summary(first_sentence)
}

fn truncate_summary(summary: &str) -> String {
    if summary.chars().count() <= PREFLIGHT_SUMMARY_MAX_CHARS {
        return summary.to_owned();
    }

    let mut end = 0;
    for (index, _) in summary.char_indices() {
        if summary[..index].chars().count() > PREFLIGHT_SUMMARY_MAX_CHARS {
            break;
        }
        end = index;
    }
    let truncated = summary[..end]
        .rsplit_once(' ')
        .map(|(prefix, _)| prefix)
        .unwrap_or(&summary[..end])
        .trim_end_matches([',', ';', ':', '-'])
        .trim();

    format!("{truncated}...")
}

fn candidate_tool_mutation(domain: BuiltinToolDomain, configured: &ConfiguredToolSpec) -> bool {
    match configured.spec.recovery.idempotency_mode {
        ToolIdempotencyMode::Safe => matches!(
            domain,
            BuiltinToolDomain::Artifact | BuiltinToolDomain::ComputerUse
        ),
        ToolIdempotencyMode::None
        | ToolIdempotencyMode::RequiresKey
        | ToolIdempotencyMode::SessionBound => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic_unknown_output_policy;
    use crate::spec::{ExecutionClass, PayloadKind, ToolRecoveryMetadata, ToolSpec};
    use serde_json::json;

    fn configured_spec(
        name: &str,
        description: &str,
        idempotency_mode: ToolIdempotencyMode,
    ) -> ConfiguredToolSpec {
        ConfiguredToolSpec::new(
            ToolSpec::new(
                name,
                description,
                json!({
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" }
                    }
                }),
                PayloadKind::Function,
            )
            .with_recovery(ToolRecoveryMetadata {
                idempotency_mode,
                ..ToolRecoveryMetadata::default()
            }),
            ExecutionClass::Shared,
            dynamic_unknown_output_policy(),
        )
    }

    #[test]
    fn tool_index_contains_compact_concrete_builtin_domain_candidates() {
        let specs = vec![
            configured_spec(
                "exec_command",
                "Run commands.",
                ToolIdempotencyMode::SessionBound,
            ),
            configured_spec(
                "request_tools",
                "Request domains.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "memory_search",
                "Search durable memory for relevant facts. Additional schema guidance must not be copied.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "memory_remember",
                "Store or update durable memory.",
                ToolIdempotencyMode::RequiresKey,
            ),
            configured_spec(
                "artifact_register",
                "Register a file you created into the artifact store. Full schema omitted.",
                ToolIdempotencyMode::Safe,
            ),
        ];

        let index = build_preflight_tool_index(&specs);

        assert_eq!(
            index.core_tools,
            vec!["exec_command".to_owned(), "request_tools".to_owned()]
        );
        assert_eq!(
            index
                .candidate_tools
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["memory_search", "memory_remember", "artifact_register"]
        );

        let memory_search = &index.candidate_tools[0];
        assert_eq!(memory_search.domain, BuiltinToolDomain::Memory);
        assert_eq!(
            memory_search.summary,
            "Search durable memory for relevant facts."
        );
        assert!(!memory_search.mutation);

        let memory_remember = &index.candidate_tools[1];
        assert_eq!(memory_remember.domain, BuiltinToolDomain::Memory);
        assert!(memory_remember.mutation);

        let artifact_register = &index.candidate_tools[2];
        assert_eq!(artifact_register.domain, BuiltinToolDomain::Artifact);
        assert!(artifact_register.mutation);
    }

    #[test]
    fn tool_index_core_tools_include_default_visible_file_tools() {
        let specs = [
            "exec_command",
            "write_stdin",
            "read_file",
            "write_file",
            "list_dir",
            "grep_files",
            "apply_patch",
            "request_tools",
        ]
        .into_iter()
        .map(|name| configured_spec(name, "Core tool.", ToolIdempotencyMode::Safe))
        .collect::<Vec<_>>();

        let index = build_preflight_tool_index(&specs);

        for name in [
            "read_file",
            "write_file",
            "list_dir",
            "grep_files",
            "apply_patch",
        ] {
            assert!(
                index.core_tools.contains(&name.to_owned()),
                "core tools must include default-visible file tool `{name}`"
            );
        }
    }

    #[test]
    fn tool_index_omits_dynamic_extensions_discovery_tools_and_schemas() {
        let specs = vec![
            configured_spec(
                "read_skill",
                "Read active skill.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "tool_search",
                "Removed discovery.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "tool_suggest",
                "Removed suggestion.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "skill.test.echo",
                "Dynamic skill runtime tool.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "mcp.filesystem.read",
                "Dynamic MCP runtime tool.",
                ToolIdempotencyMode::Safe,
            ),
            configured_spec(
                "memory_get",
                "Get exact durable memory details by memory id.",
                ToolIdempotencyMode::Safe,
            ),
        ];

        let index = build_preflight_tool_index(&specs);
        let candidate_names = index
            .candidate_tools
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(index.core_tools, vec!["read_skill".to_owned()]);
        assert_eq!(candidate_names, vec!["memory_get"]);
        assert!(!candidate_names.contains(&"tool_search"));
        assert!(!candidate_names.contains(&"tool_suggest"));
        assert!(!candidate_names.contains(&"skill.test.echo"));
        assert!(!candidate_names.contains(&"mcp.filesystem.read"));

        let serialized = serde_json::to_string(&index).expect("tool index serializes");
        assert!(!serialized.contains("parameters"));
        assert!(!serialized.contains("properties"));
        assert!(!serialized.contains("jsonSchema"));
    }

    #[test]
    fn tool_index_absent_domains_do_not_emit_unavailable_candidates() {
        let specs = vec![configured_spec(
            "memory_search",
            "Search durable memory.",
            ToolIdempotencyMode::Safe,
        )];

        let index = build_preflight_tool_index(&specs);

        assert_eq!(index.candidate_tools.len(), 1);
        assert_eq!(index.candidate_tools[0].name, "memory_search");
        assert!(
            index
                .candidate_tools
                .iter()
                .all(|candidate| candidate.domain == BuiltinToolDomain::Memory)
        );
    }

    #[test]
    fn tool_index_summaries_are_bounded() {
        let specs = vec![configured_spec(
            "task_create",
            "Create a durable task or subagent with a deliberately long description that exceeds the compact preflight summary limit and must be truncated before it can drag hidden schema-scale prose into the preflight request.",
            ToolIdempotencyMode::RequiresKey,
        )];

        let index = build_preflight_tool_index(&specs);
        let summary = &index.candidate_tools[0].summary;

        assert!(summary.ends_with("..."));
        assert!(summary.chars().count() <= PREFLIGHT_SUMMARY_MAX_CHARS + 3);
    }
}
