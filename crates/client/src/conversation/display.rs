use pioneer_protocol::ToolDisplayPayload;

pub fn tool_display_text(display: &ToolDisplayPayload) -> Option<String> {
    match display {
        ToolDisplayPayload::Shell {
            aggregated_output,
            stdout,
            stderr,
            ..
        } => aggregated_output
            .clone()
            .or_else(|| stdout.clone())
            .or_else(|| stderr.clone()),
        ToolDisplayPayload::Summary(summary) => {
            let mut lines = Vec::new();
            if !summary.title.trim().is_empty() {
                lines.push(summary.title.clone());
            }
            lines.extend(
                summary
                    .lines
                    .iter()
                    .filter(|line| !line.trim().is_empty())
                    .cloned(),
            );
            (!lines.is_empty()).then(|| lines.join("\n"))
        }
        ToolDisplayPayload::Progress { stage, .. } => Some(stage.clone()),
        ToolDisplayPayload::Hidden => None,
    }
}
