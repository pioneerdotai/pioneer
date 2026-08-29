use crate::types::ProviderToolCall;
use anyhow::{Context, Result, bail};
use serde_json::Value;

pub(crate) fn parse_tool_calls(
    tool_calls: Option<&Value>,
    function_call: Option<&Value>,
) -> Result<Vec<ProviderToolCall>> {
    let mut parsed = match tool_calls {
        Some(value) => parse_tool_calls_from_value(value)?,
        None => Vec::new(),
    };

    if parsed.is_empty() {
        if let Some(function_call) = function_call {
            parsed.push(parse_tool_call_value(function_call, 0)?);
        }
    }

    Ok(parsed)
}

fn parse_tool_calls_from_value(value: &Value) -> Result<Vec<ProviderToolCall>> {
    match value {
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, item)| parse_tool_call_value(item, index))
            .collect(),
        Value::String(encoded) => {
            let decoded = serde_json::from_str::<Value>(encoded)
                .context("provider tool_calls string is not valid JSON")?;
            parse_tool_calls_from_value(&decoded)
        }
        _ => bail!("provider tool_calls must be an array or encoded array"),
    }
}

fn parse_tool_call_value(value: &Value, index: usize) -> Result<ProviderToolCall> {
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

    let name = name
        .filter(|name| !name.trim().is_empty())
        .context("provider tool call is missing a non-empty function name")?
        .to_owned();
    let arguments = normalize_arguments(arguments_value)?;

    Ok(ProviderToolCall {
        id,
        name,
        arguments,
    })
}

fn normalize_arguments(arguments: Option<&Value>) -> Result<String> {
    match arguments {
        Some(Value::String(text)) => {
            serde_json::from_str::<Value>(text)
                .context("provider tool call arguments string is not valid JSON")?;
            Ok(text.clone())
        }
        Some(other) => serde_json::to_string(other)
            .context("provider tool call arguments cannot be serialized"),
        None => bail!("provider tool call is missing arguments"),
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

        let parsed = parse_tool_calls(Some(&value), None).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "call_1");
        assert_eq!(parsed[0].name, "shell");
        assert_eq!(parsed[0].arguments, r#"{"command":"pwd"}"#);
    }

    #[test]
    fn malformed_member_rejects_the_entire_parallel_round() {
        let value = serde_json::json!([
            {"id":"ok","function":{"name":"shell","arguments":"{}"}},
            {"id":"bad","function":{"arguments":"{}"}}
        ]);
        assert!(parse_tool_calls(Some(&value), None).is_err());
    }

    #[test]
    fn missing_and_invalid_arguments_are_not_coerced_to_empty_objects() {
        let missing = serde_json::json!([
            {"id":"bad","function":{"name":"shell"}}
        ]);
        let invalid = serde_json::json!([
            {"id":"bad","function":{"name":"shell","arguments":"{"}}
        ]);
        assert!(parse_tool_calls(Some(&missing), None).is_err());
        assert!(parse_tool_calls(Some(&invalid), None).is_err());
    }

    #[test]
    fn assistant_content_with_tool_like_json_remains_plain_text() {
        let content = r#"{
            "content": "fallback",
            "tool_calls": [{
                "id": "call_1",
                "name": "shell",
                "arguments": "{}"
            }]
        }"#;

        let value: Value = serde_json::from_str(content).expect("content is valid JSON");
        assert!(
            parse_tool_calls(None, None)
                .expect("missing native fields are not a tool call")
                .is_empty()
        );
        assert_eq!(value["content"], "fallback");
        assert!(value.get("tool_calls").is_some());
    }
}
