use pioneer_protocol::ProviderFailureClass;
use serde_json::Value as JsonValue;

const EMPTY_NO_TOOL_ROUND_RECOVERY_INSTRUCTION: &str = concat!(
    "Your previous response was empty and was not accepted. ",
    "Continue the current turn from the existing tool results in context. ",
    "Do not restart completed work. ",
    "If work remains, call the next required tool. ",
    "If the task is complete, provide a non-empty final answer."
);
const TOOL_SCHEMA_DUMP_NO_TOOL_ROUND_RECOVERY_INSTRUCTION: &str = concat!(
    "Your previous response reproduced tool schemas or tool definitions instead of answering ",
    "the user and was not accepted. Do not output tool schemas or the tool catalog. ",
    "Continue the current turn from the existing context. ",
    "If work remains, call the next required tool. ",
    "If the task is complete, provide a direct final answer."
);
const RAW_TOOL_CALL_MARKUP_NO_TOOL_ROUND_RECOVERY_INSTRUCTION: &str = concat!(
    "Your previous response output raw tool-call markup instead of making a tool call or ",
    "answering the user and was not accepted. Do not output provider tool-call protocol text. ",
    "Continue the current turn from the existing context. ",
    "If work remains, call the next required tool. ",
    "If the task is complete, provide a direct final answer."
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NoToolFinalAnswerRejection {
    Empty,
    ToolSchemaDump,
    RawToolCallMarkup,
}

impl NoToolFinalAnswerRejection {
    pub(super) fn recovery_instruction(self) -> &'static str {
        match self {
            Self::Empty => EMPTY_NO_TOOL_ROUND_RECOVERY_INSTRUCTION,
            Self::ToolSchemaDump => TOOL_SCHEMA_DUMP_NO_TOOL_ROUND_RECOVERY_INSTRUCTION,
            Self::RawToolCallMarkup => RAW_TOOL_CALL_MARKUP_NO_TOOL_ROUND_RECOVERY_INSTRUCTION,
        }
    }

    pub(super) fn provider_failure_class(self) -> ProviderFailureClass {
        match self {
            Self::Empty => ProviderFailureClass::EmptyResponse,
            Self::ToolSchemaDump | Self::RawToolCallMarkup => ProviderFailureClass::Unknown,
        }
    }

    pub(super) fn provider_code(self) -> &'static str {
        match self {
            Self::Empty => "empty_model_response",
            Self::ToolSchemaDump => "tool_schema_dump_response",
            Self::RawToolCallMarkup => "raw_tool_call_markup_response",
        }
    }

    pub(super) fn provider_message(self) -> &'static str {
        match self {
            Self::Empty => "model returned an empty response without tool calls",
            Self::ToolSchemaDump => {
                "model returned tool schema definitions instead of a final answer"
            }
            Self::RawToolCallMarkup => {
                "model returned raw tool-call markup instead of a final answer"
            }
        }
    }
}

pub(super) fn no_tool_final_answer_rejection(text: &str) -> Option<NoToolFinalAnswerRejection> {
    if text.trim().is_empty() {
        return Some(NoToolFinalAnswerRejection::Empty);
    }
    if looks_like_raw_tool_call_markup(text) {
        return Some(NoToolFinalAnswerRejection::RawToolCallMarkup);
    }
    if looks_like_tool_schema_dump(text) {
        return Some(NoToolFinalAnswerRejection::ToolSchemaDump);
    }
    None
}

fn looks_like_raw_tool_call_markup(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    if looks_like_sanitized_dsml_tool_call_markup(trimmed) {
        return true;
    }

    let lower = trimmed.to_ascii_lowercase();
    let Some(tool_call_start) = raw_tool_call_opening_tag_index(lower.as_str()) else {
        return false;
    };
    let prefix = &trimmed[..tool_call_start];
    if !raw_tool_call_markup_prefix_is_allowed(prefix) {
        return false;
    }

    let body = &lower[tool_call_start..];
    let has_tool_call_open = body.starts_with("<tool_call") || body.starts_with("<toolcall");
    let has_tool_call_close = body.contains("</tool_call>") || body.contains("</toolcall>");
    let has_invoke_tag = body.contains("<invoke name=") || body.contains("</invoke>");
    let has_tool_payload_tag = body.contains("<command>")
        || body.contains("</command>")
        || body.contains("<arguments>")
        || body.contains("</arguments>")
        || body.contains("<item>")
        || body.contains("</item>");

    has_tool_call_open && has_tool_call_close && (has_invoke_tag || has_tool_payload_tag)
}

fn looks_like_sanitized_dsml_tool_call_markup(text: &str) -> bool {
    let compact = text
        .chars()
        .filter_map(|ch| {
            if ch.is_whitespace() {
                None
            } else if ch == '\u{FF5C}' {
                Some('|')
            } else {
                Some(ch.to_ascii_lowercase())
            }
        })
        .collect::<String>();
    let Some(tool_calls_start) = compact.find("<||dsml||tool_calls") else {
        return false;
    };
    let body = &compact[tool_calls_start..];
    body.contains("</||dsml||tool_calls>")
        && (body.contains("<||dsml||invoke") || body.contains("</||dsml||invoke>"))
        && (body.contains("<||dsml||parameter") || body.contains("</||dsml||parameter>"))
}

fn raw_tool_call_opening_tag_index(lower: &str) -> Option<usize> {
    ["<tool_call", "<toolcall"]
        .into_iter()
        .filter_map(|needle| lower.find(needle))
        .min()
}

fn raw_tool_call_markup_prefix_is_allowed(prefix: &str) -> bool {
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return true;
    }
    prefix.len() <= 64
        && !prefix.chars().any(char::is_whitespace)
        && prefix
            .chars()
            .next()
            .is_some_and(|ch| !ch.is_alphanumeric())
        && prefix.chars().any(|ch| matches!(ch, '<' | '>' | '[' | ']'))
}

fn looks_like_tool_schema_dump(text: &str) -> bool {
    let Some(candidate) = standalone_json_candidate(text) else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<JsonValue>(candidate) else {
        return false;
    };
    json_value_looks_like_tool_schema_dump(&value)
}

fn standalone_json_candidate(text: &str) -> Option<&str> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Some(trimmed);
    }
    if !trimmed.starts_with("```") {
        return Some(trimmed);
    }

    let rest = trimmed.strip_prefix("```")?;
    let newline_index = rest.find('\n')?;
    let info_string = rest[..newline_index].trim();
    if !info_string.is_empty()
        && !matches!(
            info_string.split_whitespace().next(),
            Some("json") | Some("jsonc")
        )
    {
        return None;
    }
    let body_and_end = &rest[newline_index + 1..];
    let end_index = body_and_end.rfind("```")?;
    if !body_and_end[end_index + 3..].trim().is_empty() {
        return None;
    }
    Some(body_and_end[..end_index].trim())
}

fn json_value_looks_like_tool_schema_dump(value: &JsonValue) -> bool {
    match value {
        JsonValue::Array(items) => {
            !items.is_empty() && items.iter().all(json_value_looks_like_tool_definition)
        }
        JsonValue::Object(map) => {
            if let Some(tools) = map.get("tools").and_then(JsonValue::as_array) {
                return !tools.is_empty()
                    && tools.iter().all(json_value_looks_like_tool_definition);
            }
            json_value_looks_like_tool_definition(value)
        }
        _ => false,
    }
}

fn json_value_looks_like_tool_definition(value: &JsonValue) -> bool {
    let Some(map) = value.as_object() else {
        return false;
    };

    if map.get("type").and_then(JsonValue::as_str) == Some("function")
        && let Some(function) = map.get("function")
    {
        return json_value_looks_like_tool_definition(function);
    }

    let has_name = map
        .get("name")
        .and_then(JsonValue::as_str)
        .is_some_and(|name| !name.trim().is_empty());
    let has_description = map
        .get("description")
        .and_then(JsonValue::as_str)
        .is_some_and(|description| !description.trim().is_empty());
    let schema = map.get("parameters").or_else(|| map.get("input_schema"));

    has_name && has_description && schema.is_some_and(json_value_looks_like_json_schema_parameters)
}

fn json_value_looks_like_json_schema_parameters(value: &JsonValue) -> bool {
    let Some(map) = value.as_object() else {
        return false;
    };
    let object_typed = map.get("type").and_then(JsonValue::as_str) == Some("object");
    object_typed
        && (map.get("properties").is_some_and(JsonValue::is_object)
            || map.get("required").is_some_and(JsonValue::is_array)
            || map.contains_key("additionalProperties"))
}

#[cfg(test)]
mod tests {
    use super::{looks_like_raw_tool_call_markup, looks_like_tool_schema_dump};

    #[test]
    fn raw_tool_call_markup_detector_matches_provider_protocol_leaks() {
        assert!(looks_like_raw_tool_call_markup(
            "][transport][<tool_call>\n\
             ][transport][<invoke name=\"exec_command\">][transport][<command>]\
             <item>bash && -lc && grep -nP '[\\xe2\\x80\\x94]' file.md</item>\
             </command></invoke>\n\
             </tool_call>"
        ));
        assert!(looks_like_raw_tool_call_markup(
            "<tool_call>\n<invoke name=\"read_file\"><arguments>{\"path\":\"/tmp/a.md\"}</arguments></invoke>\n</tool_call>"
        ));
        assert!(looks_like_raw_tool_call_markup(
            "< | | DSML | | tool_calls>\n\
             < | | DSML | | invoke name=\"exec_command\">\n\
             < | | DSML | | parameter name=\"command\" string=\"false\">[\"brew\",\"install\",\"mole\"]</| | DSML | | parameter>\n\
             < | | DSML | | parameter name=\"timeout_ms\" string=\"false\">120000</| | DSML | | parameter>\n\
             </| | DSML | | invoke>\n\
             </| | DSML | | tool_calls>"
        ));
        assert!(looks_like_raw_tool_call_markup(
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
    fn tool_schema_dump_detector_matches_common_schema_shapes() {
        assert!(looks_like_tool_schema_dump(
            r#"{"name":"apply_patch","description":"Apply a patch","parameters":{"type":"object","properties":{"patch":{"type":"string"}},"required":["patch"],"additionalProperties":false}}"#
        ));
        assert!(looks_like_tool_schema_dump(
            r#"[{"name":"read_file","description":"Read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}]"#
        ));
        assert!(looks_like_tool_schema_dump(
            r#"{"tools":[{"type":"function","function":{"name":"apply_patch","description":"Apply a patch","parameters":{"type":"object","properties":{"patch":{"type":"string"}},"required":["patch"]}}}]}"#
        ));
        assert!(looks_like_tool_schema_dump(
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
