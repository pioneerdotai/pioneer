use crate::section::PromptSection;

pub fn render_sections(sections: &[PromptSection]) -> String {
    sections
        .iter()
        .map(PromptSection::as_rendered_text)
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}
