use pioneer_protocol::ProviderFailureClass;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NoToolFinalAnswerRejection {
    Empty,
}

impl NoToolFinalAnswerRejection {
    pub(super) fn recovery_instruction(self) -> &'static str {
        "Your previous response was empty and was not accepted. Continue the current turn from the existing tool results in context. Do not restart completed work. If work remains, call the next required tool. If the task is complete, provide a non-empty final answer."
    }
    pub(super) fn provider_failure_class(self) -> ProviderFailureClass {
        ProviderFailureClass::EmptyResponse
    }
    pub(super) fn provider_code(self) -> &'static str {
        "empty_model_response"
    }
    pub(super) fn provider_message(self) -> &'static str {
        "model returned an empty response without tool calls"
    }
}

// Final text is user data; protocol authority belongs to typed adapter metadata.
pub(super) fn no_tool_final_answer_rejection(text: &str) -> Option<NoToolFinalAnswerRejection> {
    text.trim()
        .is_empty()
        .then_some(NoToolFinalAnswerRejection::Empty)
}

#[cfg(test)]
mod tests {
    use super::{NoToolFinalAnswerRejection, no_tool_final_answer_rejection};

    fn looks_like_raw_tool_call_markup(text: &str) -> bool {
        no_tool_final_answer_rejection(text).is_some()
    }
    fn looks_like_tool_schema_dump(text: &str) -> bool {
        no_tool_final_answer_rejection(text).is_some()
    }
    #[test]
    fn whitespace_only_final_is_rejected() {
        assert_eq!(
            no_tool_final_answer_rejection(" \n\t"),
            Some(NoToolFinalAnswerRejection::Empty)
        );
    }

    #[test]
    fn exact_protocol_fixture_is_valid_final_text() {
        assert!(!looks_like_raw_tool_call_markup(
            "][transport][<tool_call>\n\
             ][transport][<invoke name=\"exec_command\">][transport][<command>]\
             <item>bash && -lc && grep -nP '[\\xe2\\x80\\x94]' file.md</item>\
             </command></invoke>\n\
             </tool_call>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "<tool_call>\n<invoke name=\"read_file\"><arguments>{\"path\":\"/tmp/a.md\"}</arguments></invoke>\n</tool_call>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "< | | DSML | | tool_calls>\n\
             < | | DSML | | invoke name=\"exec_command\">\n\
             < | | DSML | | parameter name=\"command\" string=\"false\">[\"brew\",\"install\",\"mole\"]</| | DSML | | parameter>\n\
             < | | DSML | | parameter name=\"timeout_ms\" string=\"false\">120000</| | DSML | | parameter>\n\
             </| | DSML | | invoke>\n\
             </| | DSML | | tool_calls>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "<\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}tool_calls>\n\
             <\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}invoke name=\"web_search\">\n\
             <\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}parameter name=\"q\" string=\"true\">Austrian GP 2026 F1 schedule</\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}parameter>\n\
             <\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}parameter name=\"region\" string=\"true\">ru-ru</\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}parameter>\n\
             </\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}invoke>\n\
             </\u{FF5C}\u{FF5C}DSML\u{FF5C}\u{FF5C}tool_calls>"
        ));
    }

    #[test]
    fn raw_tool_call_markup_detector_ignores_explanatory_text() {
        assert!(!looks_like_raw_tool_call_markup(
            "The provider leaked this text: <tool_call><invoke name=\"exec_command\"></invoke></tool_call>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "Use XML tags like <tool_call> only in this documentation example."
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "<tool_call> is the literal string I found in the logs."
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "<arguments>{\"path\":\"/tmp/a.md\"}</arguments>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "Example:<toolcall><invoke name=\"exec_command\"></invoke></toolcall>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "How LLM tool calls work:\n<toolcall><invoke name=\"exec_command\"></invoke></toolcall>"
        ));
        assert!(!looks_like_raw_tool_call_markup(
            "Use DSML tags like < | | DSML | | tool_calls> only in documentation examples."
        ));
    }

    #[test]
    fn exact_tool_definitions_are_valid_final_text() {
        assert!(!looks_like_tool_schema_dump(
            r#"{"name":"apply_patch","description":"Apply a patch","parameters":{"type":"object","properties":{"patch":{"type":"string"}},"required":["patch"],"additionalProperties":false}}"#
        ));
        assert!(!looks_like_tool_schema_dump(
            r#"[{"name":"read_file","description":"Read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}]"#
        ));
        assert!(!looks_like_tool_schema_dump(
            r#"{"tools":[{"type":"function","function":{"name":"apply_patch","description":"Apply a patch","parameters":{"type":"object","properties":{"patch":{"type":"string"}},"required":["patch"]}}}]}"#
        ));
        assert!(!looks_like_tool_schema_dump(
            r#"```json
{"name":"web_fetch","description":"Fetch a URL","input_schema":{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}}
```"#
        ));
    }

    #[test]
    fn tool_schema_dump_detector_ignores_regular_json_answers() {
        assert!(!looks_like_tool_schema_dump(
            r#"{"status":"ok","path":"/tmp/report.json","summary":"created"}"#
        ));
        assert!(!looks_like_tool_schema_dump(
            r#"{"path":"/tmp/report.md","content":"hello"}"#
        ));
        assert!(!looks_like_tool_schema_dump(
            r#"{"name":"report","description":"weather report","parameters":{"city":"Moscow"}}"#
        ));
        assert!(!looks_like_tool_schema_dump(
            "Here is the JSON:\n{\"name\":\"apply_patch\",\"description\":\"Apply\",\"parameters\":{\"type\":\"object\",\"properties\":{}}}"
        ));
    }
}
