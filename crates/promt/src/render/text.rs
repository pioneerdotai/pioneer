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
    sections
        .iter()
        .map(PromptSection::as_rendered_text)
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}
