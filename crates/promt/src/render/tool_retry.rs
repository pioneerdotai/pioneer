use crate::content;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolRetryInstructionKind {
    Retry,
    Exhausted,
}

pub fn render_tool_retry_instruction(
    kind: ToolRetryInstructionKind,
    fact_lines: &[String],
) -> Option<String> {
    if fact_lines.is_empty() {
        return None;
    }

    let instruction = match kind {
        ToolRetryInstructionKind::Retry => content::TOOL_RETRY_INSTRUCTION,
        ToolRetryInstructionKind::Exhausted => content::TOOL_RETRY_EXHAUSTED_INSTRUCTION,
    };

    let mut lines = Vec::new();
    lines.push(instruction.to_owned());
    lines.extend(fact_lines.iter().cloned());
    Some(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_fact_lines() -> Vec<String> {
        vec![
            "exec_command: class=ExecutionFailed episode_retry_budget=1/32 class_retry_budget=1/2 tool_retry_budget=1/16 signature_retry_budget=1/3 hint=retry with corrected command"
                .to_owned(),
        ]
    }

    #[test]
    fn renders_retry_prompt_from_facts() {
        let fact_lines = sample_fact_lines();
        let rendered = render_tool_retry_instruction(ToolRetryInstructionKind::Retry, &fact_lines)
            .expect("retry prompt should render");

        assert!(rendered.contains("Tool output indicates recoverable/partial failure"));
        for fact_line in fact_lines {
            assert!(rendered.contains(fact_line.as_str()));
        }
    }

    #[test]
    fn renders_exhausted_prompt_from_facts() {
        let mut fact_lines = vec!["exhaustion_reason=total_retry_rounds 32/32".to_owned()];
        fact_lines.extend(sample_fact_lines());
        let rendered =
            render_tool_retry_instruction(ToolRetryInstructionKind::Exhausted, &fact_lines)
                .expect("exhausted prompt should render");

        assert!(rendered.contains("Retry budget for recoverable tool failures is exhausted"));
        for fact_line in fact_lines {
            assert!(rendered.contains(fact_line.as_str()));
        }
    }
}
