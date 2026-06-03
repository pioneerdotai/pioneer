use crate::ToolError;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolArgumentCoercion {
    pub path: String,
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolArgumentNormalization {
    pub arguments: JsonValue,
    pub coercions: Vec<ToolArgumentCoercion>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ExpectedShape {
    object: bool,
    array: bool,
    string: bool,
}

impl ExpectedShape {
    fn expects_composite(self) -> bool {
        self.object || self.array
    }

    fn is_ambiguous_with_string(self) -> bool {
        self.string && self.expects_composite()
    }

    fn target_label(self) -> &'static str {
        match (self.object, self.array) {
            (true, false) => "object",
            (false, true) => "array",
            (true, true) => "object_or_array",
            (false, false) => "value",
        }
    }
}

pub fn normalize_tool_arguments_from_schema(
    arguments: JsonValue,
    schema: &JsonValue,
) -> Result<ToolArgumentNormalization, ToolError> {
    let mut coercions = Vec::new();
    let normalized = normalize_value(arguments, schema, schema, "$", &mut coercions)?;
    Ok(ToolArgumentNormalization {
        arguments: normalized,
        coercions,
    })
}

pub fn normalize_tool_arguments_for_tool(
    tool_name: &str,
    arguments: JsonValue,
    schema: &JsonValue,
) -> Result<ToolArgumentNormalization, ToolError> {
    let mut normalized = normalize_tool_arguments_from_schema(arguments, schema)?;
    normalize_tool_specific_aliases(tool_name, &mut normalized)?;
    Ok(normalized)
}

fn normalize_tool_specific_aliases(
    tool_name: &str,
    normalization: &mut ToolArgumentNormalization,
) -> Result<(), ToolError> {
    if tool_name != "write_file" {
        return Ok(());
    }

    let Some(object) = normalization.arguments.as_object_mut() else {
        return Ok(());
    };

    move_alias_to_canonical(object, &mut normalization.coercions, "file_path", "path");
    move_alias_to_canonical(object, &mut normalization.coercions, "filename", "path");
    move_alias_to_canonical(object, &mut normalization.coercions, "contents", "content");
    move_alias_to_canonical(object, &mut normalization.coercions, "text", "content");
    validate_write_file_known_fields(object)?;
    Ok(())
}

fn move_alias_to_canonical(
    object: &mut JsonMap<String, JsonValue>,
    coercions: &mut Vec<ToolArgumentCoercion>,
    alias: &str,
    canonical: &str,
) {
    if object.contains_key(canonical) {
        return;
    }

    let Some(value) = object.remove(alias) else {
        return;
    };
    object.insert(canonical.to_owned(), value);
    coercions.push(ToolArgumentCoercion {
        path: "$".to_owned(),
        from: alias.to_owned(),
        to: canonical.to_owned(),
    });
}

fn validate_write_file_known_fields(object: &JsonMap<String, JsonValue>) -> Result<(), ToolError> {
    for key in object.keys() {
        if !matches!(
            key.as_str(),
            "path"
                | "content"
                | "create_dirs"
                | "overwrite"
                | "read_observation_id"
                | "expected_sha256"
                | "expected_mtime_ms"
        ) {
            return Err(ToolError::invalid_arguments(format!(
                "write_file unknown field `{key}`"
            )));
        }
    }

    Ok(())
}

fn normalize_value(
    value: JsonValue,
    schema: &JsonValue,
    root_schema: &JsonValue,
    path: &str,
    coercions: &mut Vec<ToolArgumentCoercion>,
) -> Result<JsonValue, ToolError> {
    let schema = resolve_schema(schema, root_schema).unwrap_or(schema);
    let shape = expected_shape(schema, root_schema);

    let value = match value {
        JsonValue::String(raw) if should_try_stringified_json(shape) => {
            parse_stringified_json_for_shape(raw.as_str(), shape, path, coercions)?
        }
        other => other,
    };

    match value {
        JsonValue::Object(mut object) => {
            normalize_object_fields(&mut object, schema, root_schema, path, coercions)?;
            Ok(JsonValue::Object(object))
        }
        JsonValue::Array(items) => {
            let item_schema = schema
                .get("items")
                .and_then(|items| resolve_schema(items, root_schema).or(Some(items)));
            let mut normalized_items = Vec::with_capacity(items.len());
            for (index, item) in items.into_iter().enumerate() {
                if let Some(item_schema) = item_schema {
                    normalized_items.push(normalize_value(
                        item,
                        item_schema,
                        root_schema,
                        &format!("{path}[{index}]"),
                        coercions,
                    )?);
                } else {
                    normalized_items.push(item);
                }
            }
            Ok(JsonValue::Array(normalized_items))
        }
        other => Ok(other),
    }
}

fn normalize_object_fields(
    object: &mut JsonMap<String, JsonValue>,
    schema: &JsonValue,
    root_schema: &JsonValue,
    path: &str,
    coercions: &mut Vec<ToolArgumentCoercion>,
) -> Result<(), ToolError> {
    let Some(properties) = schema.get("properties").and_then(JsonValue::as_object) else {
        return Ok(());
    };

    let keys = object.keys().cloned().collect::<Vec<_>>();
    for key in keys {
        let Some(property_schema) = properties.get(key.as_str()) else {
            continue;
        };
        let Some(value) = object.remove(key.as_str()) else {
            continue;
        };
        let child_path = if path == "$" {
            format!("$.{key}")
        } else {
            format!("{path}.{key}")
        };
        let normalized =
            normalize_value(value, property_schema, root_schema, &child_path, coercions)?;
        object.insert(key, normalized);
    }

    Ok(())
}

fn should_try_stringified_json(shape: ExpectedShape) -> bool {
    shape.expects_composite() && !shape.is_ambiguous_with_string()
}

fn parse_stringified_json_for_shape(
    raw: &str,
    shape: ExpectedShape,
    path: &str,
    coercions: &mut Vec<ToolArgumentCoercion>,
) -> Result<JsonValue, ToolError> {
    let trimmed = raw.trim();
    let parsed = serde_json::from_str::<JsonValue>(trimmed).map_err(|error| {
        ToolError::invalid_arguments(format!(
            "`{path}` must be a JSON {}, not a string. The string value was not valid JSON: {error}",
            shape.target_label()
        ))
    })?;

    let matches_shape = (shape.object && parsed.is_object()) || (shape.array && parsed.is_array());
    if !matches_shape {
        return Err(ToolError::invalid_arguments(format!(
            "`{path}` must be a JSON {}, not a stringified {}",
            shape.target_label(),
            json_value_kind(&parsed)
        )));
    }

    coercions.push(ToolArgumentCoercion {
        path: path.to_owned(),
        from: "stringified_json".to_owned(),
        to: shape.target_label().to_owned(),
    });
    Ok(parsed)
}

fn expected_shape(schema: &JsonValue, root_schema: &JsonValue) -> ExpectedShape {
    let schema = resolve_schema(schema, root_schema).unwrap_or(schema);
    let mut shape = ExpectedShape::default();

    if let Some(kind) = schema.get("type").and_then(JsonValue::as_str) {
        apply_type(&mut shape, kind);
    }
    if let Some(kinds) = schema.get("type").and_then(JsonValue::as_array) {
        for kind in kinds.iter().filter_map(JsonValue::as_str) {
            apply_type(&mut shape, kind);
        }
    }

    for composite_key in ["anyOf", "oneOf", "allOf"] {
        if let Some(options) = schema.get(composite_key).and_then(JsonValue::as_array) {
            for option in options {
                let option_shape = expected_shape(option, root_schema);
                shape.object |= option_shape.object;
                shape.array |= option_shape.array;
                shape.string |= option_shape.string;
            }
        }
    }

    if schema.get("properties").is_some() {
        shape.object = true;
    }
    if schema.get("items").is_some() {
        shape.array = true;
    }

    shape
}

fn apply_type(shape: &mut ExpectedShape, kind: &str) {
    match kind {
        "object" => shape.object = true,
        "array" => shape.array = true,
        "string" => shape.string = true,
        _ => {}
    }
}

fn resolve_schema<'a>(schema: &'a JsonValue, root_schema: &'a JsonValue) -> Option<&'a JsonValue> {
    let reference = schema.get("$ref").and_then(JsonValue::as_str)?;
    resolve_local_ref(root_schema, reference)
}

fn resolve_local_ref<'a>(root_schema: &'a JsonValue, reference: &str) -> Option<&'a JsonValue> {
    let pointer = reference.strip_prefix('#')?;
    root_schema.pointer(pointer)
}

fn json_value_kind(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn coerces_stringified_object_for_object_schema_field() {
        let schema = json!({
            "type": "object",
            "properties": {
                "trigger": {
                    "type": "object",
                    "properties": { "kind": { "type": "string" } }
                }
            }
        });
        let normalized = normalize_tool_arguments_from_schema(
            json!({ "trigger": "{\"kind\":\"cron\"}" }),
            &schema,
        )
        .expect("normalization should succeed");

        assert_eq!(normalized.arguments["trigger"]["kind"], "cron");
        assert_eq!(normalized.coercions.len(), 1);
        assert_eq!(normalized.coercions[0].path, "$.trigger");
    }

    #[test]
    fn rejects_stringified_scalar_for_object_schema_field() {
        let schema = json!({
            "type": "object",
            "properties": {
                "trigger": { "type": "object" }
            }
        });
        let error = normalize_tool_arguments_from_schema(json!({ "trigger": "\"cron\"" }), &schema)
            .expect_err("stringified scalar should be rejected");
        assert!(error.to_string().contains("must be a JSON object"));
    }

    #[test]
    fn leaves_ambiguous_string_or_object_schema_unchanged() {
        let schema = json!({
            "type": "object",
            "properties": {
                "value": {
                    "anyOf": [
                        { "type": "string" },
                        { "type": "object" }
                    ]
                }
            }
        });
        let normalized = normalize_tool_arguments_from_schema(
            json!({ "value": "{\"kind\":\"cron\"}" }),
            &schema,
        )
        .expect("ambiguous schema should not fail");
        assert_eq!(normalized.arguments["value"], "{\"kind\":\"cron\"}");
        assert!(normalized.coercions.is_empty());
    }

    fn write_file_schema() -> JsonValue {
        json!({
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" },
                "create_dirs": { "type": "boolean" },
                "overwrite": { "type": "boolean" },
                "read_observation_id": { "type": "string" },
                "expected_sha256": { "type": "string" },
                "expected_mtime_ms": { "type": "integer" }
            },
            "required": ["path", "content"],
            "additionalProperties": false
        })
    }

    #[test]
    fn write_file_aliases_normalize_to_canonical_arguments() {
        let normalized = normalize_tool_arguments_for_tool(
            "write_file",
            json!({
                "file_path": "docs/example.md",
                "contents": "hello"
            }),
            &write_file_schema(),
        )
        .expect("aliases should normalize");

        assert_eq!(normalized.arguments["path"], "docs/example.md");
        assert_eq!(normalized.arguments["content"], "hello");
        assert!(normalized.arguments.get("file_path").is_none());
        assert!(normalized.arguments.get("contents").is_none());
        assert_eq!(normalized.coercions.len(), 2);
    }

    #[test]
    fn write_file_filename_and_text_aliases_normalize_to_canonical_arguments() {
        let normalized = normalize_tool_arguments_for_tool(
            "write_file",
            json!({
                "filename": "notes.txt",
                "text": "hello"
            }),
            &write_file_schema(),
        )
        .expect("aliases should normalize");

        assert_eq!(normalized.arguments["path"], "notes.txt");
        assert_eq!(normalized.arguments["content"], "hello");
    }

    #[test]
    fn write_file_unknown_fields_are_rejected_after_alias_normalization() {
        let error = normalize_tool_arguments_for_tool(
            "write_file",
            json!({
                "file_path": "notes.txt",
                "contents": "hello",
                "mode": "append"
            }),
            &write_file_schema(),
        )
        .expect_err("unknown write_file fields should fail");

        assert!(error.to_string().contains("unknown field `mode`"));
    }

    #[test]
    fn write_file_canonical_field_does_not_hide_alias_extra_field() {
        let error = normalize_tool_arguments_for_tool(
            "write_file",
            json!({
                "path": "canonical.txt",
                "file_path": "alias.txt",
                "content": "hello"
            }),
            &write_file_schema(),
        )
        .expect_err("alias should remain unknown when canonical already exists");

        assert!(error.to_string().contains("unknown field `file_path`"));
    }

    #[test]
    fn write_file_alias_normalization_does_not_leak_to_other_tools() {
        let normalized = normalize_tool_arguments_for_tool(
            "other_tool",
            json!({
                "file_path": "notes.txt",
                "contents": "hello"
            }),
            &write_file_schema(),
        )
        .expect("other tools should not use write_file aliases");

        assert_eq!(normalized.arguments["file_path"], "notes.txt");
        assert_eq!(normalized.arguments["contents"], "hello");
        assert!(normalized.arguments.get("path").is_none());
        assert!(normalized.arguments.get("content").is_none());
    }
}
