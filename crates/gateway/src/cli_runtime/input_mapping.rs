use pioneer_cli_agent_runtime::codex_input::{
    CodexInputMappingRequest, CodexInputSource, CodexTurnInputMapping, map_codex_turn_input,
};
use pioneer_protocol::UserInput;

pub(crate) fn map_codex_turn_input_from_pioneer(
    input: &[UserInput],
) -> Result<CodexTurnInputMapping, pioneer_cli_agent_runtime::codex_input::CodexInputMappingError> {
    map_codex_turn_input(CodexInputMappingRequest {
        inputs: input
            .iter()
            .map(codex_text_input_source_from_pioneer)
            .collect(),
    })
}

fn codex_text_input_source_from_pioneer(input: &UserInput) -> CodexInputSource {
    match input {
        UserInput::Text { text, .. } => CodexInputSource::Text { text: text.clone() },
        _ => unreachable!("CLI runtime turn input must be text-only before input mapping"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_text_input_maps_from_pioneer_user_input() {
        let mapping = map_codex_turn_input_from_pioneer(&[UserInput::Text {
            text: "hello".to_owned(),
            text_elements: Vec::new(),
        }])
        .expect("text input should map");

        assert!(mapping.diagnostics.is_empty());
        assert_eq!(
            mapping.input,
            vec![
                pioneer_cli_agent_runtime::codex_input::CodexTurnInputItem::Text {
                    text: "hello".to_owned(),
                }
            ]
        );
    }
}
