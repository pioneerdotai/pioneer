use crate::types::ProviderToolCall;
use serde_json::Value;

#[derive(Debug, Clone)]
pub(crate) struct EmbeddedToolPayload {
    pub text: String,
    pub reasoning_content: Option<String>,
    pub tool_calls: Vec<ProviderToolCall>,
}

pub(crate) fn parse_tool_calls(
    tool_calls: Option<&Value>,
    function_call: Option<&Value>,
) -> Vec<ProviderToolCall> {
    let mut parsed = tool_calls
        .map(parse_tool_calls_from_value)
        .unwrap_or_default();

    if parsed.is_empty() {
        if let Some(function_call) = function_call {
            if let Some(call) = parse_tool_call_value(function_call, 0) {
                parsed.push(call);
            }
        }
    }

    parsed
}

pub(crate) fn parse_embedded_tool_payload(content: &str) -> Option<EmbeddedToolPayload> {
    let value: Value = serde_json::from_str(content).ok()?;
    let object = value.as_object()?;

    let tool_calls = parse_tool_calls(object.get("tool_calls"), object.get("function_call"));
    if tool_calls.is_empty() {
        return None;
    }

    let text = object
        .get("content")
        .or_else(|| object.get("text"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();

    let reasoning_content = object
        .get("reasoning_content")
        .or_else(|| object.get("reasoning"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    Some(EmbeddedToolPayload {
        text,
        reasoning_content,
        tool_calls,
    })
}

fn parse_tool_calls_from_value(value: &Value) -> Vec<ProviderToolCall> {
    match value {
        Value::Array(items) => items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| parse_tool_call_value(item, index))
            .collect(),
        Value::String(encoded) => serde_json::from_str::<Value>(encoded)
            .ok()
            .map(|decoded| parse_tool_calls_from_value(&decoded))
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn parse_tool_call_value(value: &Value, index: usize) -> Option<ProviderToolCall> {
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("call_{}", index + 1));

    let (name, arguments_value) = if let Some(function) = value.get("function") {
        (
            function
                .get("name")
                .and_then(Value::as_str)
                .or_else(|| value.get("name").and_then(Value::as_str)),
            function
                .get("arguments")
                .or_else(|| value.get("arguments"))
                .or_else(|| value.get("parameters")),
        )
    } else {
        (
            value.get("name").and_then(Value::as_str),
            value.get("arguments").or_else(|| value.get("parameters")),
        )
    };

    let name = name?.to_owned();
    let arguments = normalize_arguments(arguments_value);

    Some(ProviderToolCall {
        id,
        name,
        arguments,
    })
}

fn normalize_arguments(arguments: Option<&Value>) -> String {
    match arguments {
        Some(Value::String(text)) => text.clone(),
        Some(other) => serde_json::to_string(other).unwrap_or_else(|_| "{}".to_owned()),
        None => "{}".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tool_calls_accepts_object_arguments() {
        let value = serde_json::json!([
            {
                "id": "call_1",
                "function": {
                    "name": "shell",
                    "arguments": {"command": "pwd"}
                }
            }
        ]);

        let parsed = parse_tool_calls(Some(&value), None);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "call_1");
        assert_eq!(parsed[0].name, "shell");
        assert_eq!(parsed[0].arguments, r#"{"command":"pwd"}"#);
    }

    #[test]
    fn parse_embedded_tool_payload_extracts_content_and_calls() {
        let content = r#"{
            "content": "running tool",
            "tool_calls": [{
                "id": "call_9",
                "name": "read_file",
                "arguments": "{\"path\":\"Cargo.toml\"}"
            }]
        }"#;

        let parsed = parse_embedded_tool_payload(content).expect("payload should parse");
        assert_eq!(parsed.text, "running tool");
        assert_eq!(parsed.tool_calls.len(), 1);
        assert_eq!(parsed.tool_calls[0].id, "call_9");
        assert_eq!(parsed.tool_calls[0].name, "read_file");
    }
}
