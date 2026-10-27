use crate::section::{PromptDynamicSectionId, PromptRuntimeSectionId, PromptRuntimeSectionInput};

pub const REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID: &str = "request_tools.hidden_domains";
pub const REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE: &str = "Hidden Tool Domains";

pub fn render_request_tools_hidden_domain_catalog_prompt(computer_use_available: bool) -> String {
    let mut lines = vec![
        "For hidden tools, call request_tools to open their domain.".to_owned(),
        "Shared tools become visible when any listed domain opens.".to_owned(),
        String::new(),
        "Domains:".to_owned(),
    ];

    for (domain, tool_names) in pioneer_tools::builtin_tool_domain_map() {
        if *domain == pioneer_tools::BuiltinToolDomain::ComputerUse
            && (!computer_use_available || pioneer_tools::computer_use_tool_specs().is_empty())
        {
            continue;
        }
        lines.push(format!("- {}: {}.", domain.as_str(), tool_names.join(", ")));
    }

    lines.join("\n")
}

pub fn request_tools_hidden_domain_catalog_section(
    computer_use_available: bool,
) -> PromptRuntimeSectionInput {
    PromptRuntimeSectionInput {
        id: PromptRuntimeSectionId::Dynamic(
            PromptDynamicSectionId::new(REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID)
                .expect("static request_tools prompt section id must be valid"),
        ),
        title: Some(REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_TITLE.to_owned()),
        content: render_request_tools_hidden_domain_catalog_prompt(computer_use_available),
        max_chars: None,
        truncated: false,
    }
}

pub fn runtime_sections_with_request_tools_catalog(
    runtime_sections: &[PromptRuntimeSectionInput],
    include_request_tools_catalog: bool,
    computer_use_available: bool,
) -> Vec<PromptRuntimeSectionInput> {
    let mut sections = runtime_sections.to_vec();

    if !include_request_tools_catalog {
        return sections;
    }

    let already_present = sections
        .iter()
        .any(|section| section.id.manifest_id() == REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID);

    if !already_present {
        sections.push(request_tools_hidden_domain_catalog_section(
            computer_use_available,
        ));
    }

    sections
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_tools_hidden_domain_catalog_lists_build_available_domains() {
        for computer_use_available in [false, true] {
            let catalog = render_request_tools_hidden_domain_catalog_prompt(computer_use_available);
            assert!(catalog.contains("call request_tools"));
            for domain in ["threads", "memory", "task", "artifact"] {
                assert!(catalog.contains(&format!("- {domain}: ")));
            }
            let cua_available =
                computer_use_available && !pioneer_tools::computer_use_tool_specs().is_empty();
            assert_eq!(catalog.contains("- computer_use: "), cua_available);
            assert_eq!(catalog.contains("cua_type_text"), cua_available);
        }
    }

    #[test]
    fn request_tools_hidden_domain_catalog_is_schema_free() {
        let catalog = render_request_tools_hidden_domain_catalog_prompt(true);

        assert!(!catalog.contains("\"parameters\""));
        assert!(!catalog.contains("\"properties\""));
        assert!(!catalog.contains("\"additionalProperties\""));
        assert!(!catalog.contains("\"required\""));
        assert!(!catalog.contains("\"type\""));
    }

    #[test]
    fn request_tools_hidden_domain_catalog_token_guard_stays_compact() {
        let catalog = render_request_tools_hidden_domain_catalog_prompt(true);
        const HIDDEN_DOMAIN_CATALOG_CHAR_LIMIT: usize = 1_200;

        assert!(
            catalog.chars().count() <= HIDDEN_DOMAIN_CATALOG_CHAR_LIMIT,
            "request_tools catalog should list domain tool names only; full schemas belong in provider tool definitions after explicit visibility expansion"
        );
        assert!(catalog.contains("request_tools"));
        assert!(catalog.contains("memory_search"));
        assert!(catalog.contains("task_create"));
        assert!(catalog.contains("artifact_prepare"));
    }

    #[test]
    fn request_tools_hidden_domain_catalog_section_has_stable_identity() {
        let section = request_tools_hidden_domain_catalog_section(true);

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
        let sections = runtime_sections_with_request_tools_catalog(&[], false, true);

        assert!(sections.is_empty());
    }

    #[test]
    fn runtime_sections_with_request_tools_catalog_avoids_duplicate_section() {
        let existing = request_tools_hidden_domain_catalog_section(true);
        let sections = runtime_sections_with_request_tools_catalog(&[existing], true, true);

        assert_eq!(sections.len(), 1);
        assert_eq!(
            sections[0].id.manifest_id(),
            REQUEST_TOOLS_HIDDEN_DOMAIN_SECTION_ID
        );
    }
}
