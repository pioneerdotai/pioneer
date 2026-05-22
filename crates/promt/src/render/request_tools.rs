use crate::section::{PromptDynamicSectionId, PromptRuntimeSectionId, PromptRuntimeSectionInput};

pub const REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID: &str = "request_tools.hidden_domains";
pub const REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE: &str = "Hidden Tool Domains";

pub fn render_request_tools_hidden_domain_catalog_prompt() -> String {
    let mut lines = vec![
        "Some tool domains and their tools are hidden until requested. If you need a hidden domain and its tools are not currently visible, call request_tools.".to_owned(),
        String::new(),
        "Domains:".to_owned(),
    ];

    for (domain, tool_names) in pioneer_tools::builtin_tool_domain_map() {
        lines.push(format!("- {}: {}.", domain.as_str(), tool_names.join(", ")));
    }

    lines.join("\n")
}

pub fn request_tools_hidden_domain_catalog_section() -> PromptRuntimeSectionInput {
    PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::Dynamic(
            PromptDynamicSectionId::new(REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID)
                .expect("static request_tools prompt section id must be valid"),
        ),
        title: Some(REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE.to_owned()),
        content: render_request_tools_hidden_domain_catalog_prompt(),
        max_chars: None,
        truncated: false,
    }
}

pub fn runtime_sections_with_request_tools_catalog(
    runtime_sections: &[PromptRuntimeSectionInput],
    include_request_tools_catalog: bool,
) -> Vec<PromptRuntimeSectionInput> {
    let mut sections = runtime_sections.to_vec();

    if !include_request_tools_catalog {
        return sections;
    }

    let already_present = sections
        .iter()
        .any(|section| section.id.manifest_id() == REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID);

    if !already_present {
        sections.push(request_tools_hidden_domain_catalog_section());
    }

    sections
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_tools_hidden_domain_catalog_contains_exact_runtime_map() {
        let catalog = render_request_tools_hidden_domain_catalog_prompt();

        assert!(catalog.contains("call request_tools"));
        for (domain, tool_names) in pioneer_tools::builtin_tool_domain_map() {
            let expected = format!("- {}: {}.", domain.as_str(), tool_names.join(", "));
            assert!(
                catalog.contains(expected.as_str()),
                "catalog missing domain line `{expected}`"
            );
        }
    }

    #[test]
    fn request_tools_hidden_domain_catalog_domain_lines_cannot_drift_from_runtime_map() {
        let catalog = render_request_tools_hidden_domain_catalog_prompt();
        let domain_lines = catalog
            .lines()
            .filter(|line| line.starts_with("- "))
            .collect::<Vec<_>>();
        let expected = pioneer_tools::builtin_tool_domain_map()
            .iter()
            .map(|(domain, tool_names)| {
                format!("- {}: {}.", domain.as_str(), tool_names.join(", "))
            })
            .collect::<Vec<_>>();

        assert_eq!(domain_lines, expected);
    }

    #[test]
    fn request_tools_hidden_domain_catalog_is_schema_free() {
        let catalog = render_request_tools_hidden_domain_catalog_prompt();

        assert!(!catalog.contains("\"parameters\""));
        assert!(!catalog.contains("\"properties\""));
        assert!(!catalog.contains("\"additionalProperties\""));
        assert!(!catalog.contains("\"required\""));
        assert!(!catalog.contains("\"type\""));
    }

    #[test]
    fn request_tools_hidden_domain_catalog_token_guard_stays_compact() {
        let catalog = render_request_tools_hidden_domain_catalog_prompt();
        const HIDDEN_DOMAIN_CATALOG_CHAR_LIMIT: usize = 1_200;

        assert!(
            catalog.chars().count() <= HIDDEN_DOMAIN_CATALOG_CHAR_LIMIT,
            "request_tools catalog should list domain tool names only; full schemas belong in provider tool definitions after explicit visibility expansion"
        );
        assert!(catalog.contains("request_tools"));
        assert!(catalog.contains("memory_search"));
        assert!(catalog.contains("task_create"));
        assert!(catalog.contains("artifact_prepare"));
        assert!(catalog.contains("computer_use"));
    }

    #[test]
    fn request_tools_hidden_domain_catalog_section_has_stable_identity() {
        let section = request_tools_hidden_domain_catalog_section();

        assert_eq!(
            section.id.manifest_id(),
            REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID
        );
        assert_eq!(
            section.title.as_deref(),
            Some(REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE)
        );
        assert!(section.content.contains("call request_tools"));
    }

    #[test]
    fn runtime_sections_with_request_tools_catalog_respects_include_flag() {
        let sections = runtime_sections_with_request_tools_catalog(&[], false);

        assert!(sections.is_empty());
    }

    #[test]
    fn runtime_sections_with_request_tools_catalog_avoids_duplicate_section() {
        let existing = request_tools_hidden_domain_catalog_section();
        let sections = runtime_sections_with_request_tools_catalog(&[existing], true);

        assert_eq!(sections.len(), 1);
        assert_eq!(
            sections[0].id.manifest_id(),
            REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID
        );
    }
}
