use crate::section::PromptSection;

pub fn render_thread_ids(initiating_thread_id: Option<&str>, execution_thread_id: &str) -> String {
    let relation = match initiating_thread_id {
        Some(id) if id == execution_thread_id => "same as initiator",
        Some(_) => "internal child",
        None => "",
    };
    format!(
        "Initiating thread: {}\nExecution thread: {}{}",
        initiating_thread_id.unwrap_or("unavailable"),
        execution_thread_id,
        if relation.is_empty() {
            String::new()
        } else {
            format!(" ({relation})")
        },
    )
}

pub fn render_sections(sections: &[PromptSection]) -> String {
    let mut output = Vec::new();
    let mut previous = None;
    for section in sections.iter().filter(|section| match section.id {
        crate::section::PromptSectionId::SoulCore
        | crate::section::PromptSectionId::IdentityCore
        | crate::section::PromptSectionId::UserPersona
        | crate::section::PromptSectionId::AgentsMd => !section.content.is_empty(),
        _ => !section.content.trim().is_empty(),
    }) {
        let current = group(&section.id);
        if let Some(title) = current {
            if previous != current {
                output.push(format!("## {title}"));
            }
            output.push(grouped_section(section));
        } else {
            output.push(section.as_rendered_text());
        }
        previous = current;
    }
    output.join("\n\n")
}

fn group(id: &crate::section::PromptSectionId) -> Option<&'static str> {
    use crate::section::PromptSectionId::*;
    match id {
        AssistantSafety => Some("Safety and authority"),
        IdentityBase | SoulCore | IdentityCore | UserPersona => Some("Agent profile"),
        AgentsMd => Some("Project instructions"),
        ToolUsagePolicy
        | MemoryRecall
        | TaskOrchestrationPolicy
        | TasksPolicy
        | SubagentsPolicy
        | ThreadsPolicy
        | PioneerCliRuntimeInstructions => Some("Tools and capabilities"),
        Dynamic(id) if id.as_str() == crate::section::FILESYSTEM_CAPABILITY_RUNTIME_SECTION_ID => {
            Some("Tools and capabilities")
        }
        ArtifactOutputContract | HtmlPolicy => Some("Outputs"),
        Dynamic(id) if id.as_str() == "artifact_references" => Some("Outputs"),
        ToolRecoveryPolicy => Some("Error recovery"),
        _ => None,
    }
}

fn grouped_section(section: &PromptSection) -> String {
    use crate::section::PromptSectionId::*;
    let file = match section.id {
        SoulCore => Some("SOUL.md"),
        IdentityCore => Some("IDENTITY.md"),
        UserPersona => Some("USER.md"),
        AgentsMd => Some("AGENTS.md"),
        _ => None,
    };
    if let Some(name) = file {
        let mut body = section.content.as_str();
        // Only the bootstrap producer owns this exact prefix. AGENTS.md uses
        // its existing scoped wrapper, which remains part of its content.
        if !matches!(section.id, AgentsMd) {
            if let Some(path) = section.sources.first() {
                let prefix = format!("### {name} ({path})\n");
                if let Some(content) = body.strip_prefix(&prefix) {
                    body = content;
                }
            }
        }
        let source = if section.sources.is_empty() {
            String::new()
        } else {
            format!("Source: {}\n", section.sources.join(", "))
        };
        return format!("### {name}\n{source}\n{body}");
    }
    if matches!(
        section.id,
        IdentityBase | AssistantSafety | ToolRecoveryPolicy
    ) {
        return section.content.clone();
    }
    format!("### {}\n\n{}", section.title.trim(), section.content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::section::{PromptSectionId as Id, PromptStability};
    fn section(id: Id, content: &str) -> PromptSection {
        PromptSection {
            id,
            title: "Rule".into(),
            content: content.into(),
            sources: vec![],
            stability: PromptStability::Stable,
        }
    }
    #[test]
    fn grouped_projection_preserves_canonical_documents_and_markdown_literals() {
        // The renderer owns captions, never the document's Markdown or boundary
        // whitespace. All supported document IDs use the same literal contract.
        let body = "\n \t\r\n# SOUL.md\r\n\n## Rules\n\n- item\n\tcontinuation\n\n    # indented\n\n```markdown\n# literal\n```\n\n<pre>\n# literal HTML\n</pre>\n\nTrailing spaces  \t\r\n\n";
        for (id, name) in [
            (Id::SoulCore, "SOUL.md"),
            (Id::IdentityCore, "IDENTITY.md"),
            (Id::UserPersona, "USER.md"),
            (Id::AgentsMd, "AGENTS.md"),
        ] {
            let path = format!("/rules/{name}");
            let content = if id == Id::AgentsMd {
                body.to_owned()
            } else {
                format!("### {name} ({path})\n{body}")
            };
            let mut document = section(id.clone(), &content);
            document.sources.push(path.clone());
            let before = document.clone();
            let group = if id == Id::AgentsMd {
                "Project instructions"
            } else {
                "Agent profile"
            };
            assert_eq!(
                render_sections(std::slice::from_ref(&document)),
                format!("## {group}\n\n### {name}\nSource: {path}\n\n{body}")
            );
            assert_eq!(document, before);
        }

        let sections = vec![
            section(Id::AssistantSafety, "Keep privacy."),
            section(Id::IdentityBase, "You are Pioneer."),
            section(Id::SoulCore, "# SOUL.md\nKeep these exact words."),
            section(Id::ToolUsagePolicy, "Read the skill."),
            section(Id::ToolRecoveryPolicy, "Diagnose errors."),
        ];
        let before = sections.clone();
        let rendered = render_sections(&sections);
        assert_eq!(sections, before);
        assert_eq!(
            rendered,
            "## Safety and authority\n\nKeep privacy.\n\n## Agent profile\n\nYou are Pioneer.\n\n### SOUL.md\n\n# SOUL.md\nKeep these exact words.\n\n## Tools and capabilities\n\n### Rule\n\nRead the skill.\n\n## Error recovery\n\nDiagnose errors."
        );
        assert!(!rendered.contains("Soul Core"));
        assert!(!rendered.contains("Identity Core"));
        assert_eq!(rendered.matches("### SOUL.md\n").count(), 1);
        assert!(!rendered.contains("Project instructions"));
        assert!(!rendered.contains("## Outputs"));
        assert_eq!(rendered, render_sections(&sections));
        assert!(render_sections(&[]).is_empty());
    }
}
