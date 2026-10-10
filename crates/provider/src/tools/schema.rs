//! Bounded validation of the non-strict function parameter surface we render.
//! This is a vocabulary/shape gate, not a schema compiler or argument validator.
use serde_json::Value;
use std::io::{self, Write};

pub(super) const MAX_SCHEMA_BYTES: usize = 128 * 1024;
pub(super) const MAX_MANIFEST_BYTES: usize = 2 * 1024 * 1024;

pub(super) fn rejected() -> anyhow::Error {
    crate::failure::ProviderTransportFailure {
        classification: crate::ProviderFailureClassification::new(
            pioneer_protocol::ProviderFailureClass::UnsupportedParameter,
        ),
    }
    .into()
}

struct Counter(usize);
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_SCHEMA_BYTES)
            .ok_or_else(|| io::Error::other("schema byte limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn validate(provider: &str, schema: &Value) -> anyhow::Result<usize> {
    // Bound every JSON value (also annotations/enum data) before serializer recursion.
    let mut pending = vec![(schema, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if depth > 32 || nodes > 4096 {
            return Err(rejected());
        }
        match value {
            Value::Object(map) => {
                if pending.len() + map.len() + nodes > 4096 {
                    return Err(rejected());
                }
                pending.extend(map.values().map(|v| (v, depth + 1)));
            }
            Value::Array(array) => {
                if pending.len() + array.len() + nodes > 4096 {
                    return Err(rejected());
                }
                pending.extend(array.iter().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, schema).map_err(|_| rejected())?;
    if schema.get("type").is_some_and(|v| v != "object") {
        return Err(rejected());
    }
    validate_node(provider, schema, true)?;
    Ok(counter.0)
}

fn validate_node(provider: &str, node: &Value, root: bool) -> anyhow::Result<()> {
    let object = node.as_object().ok_or_else(rejected)?;
    for (key, value) in object {
        // Controlled v1 function JSON profile: standard bounds also present on
        // Gemini's documented Schema surface plus JSON additionalProperties.
        // Constructs outside that verified set remain unsupported, even though
        // a protobuf Value could physically carry them.
        if provider == "gemini"
            && !matches!(
                key.as_str(),
                "schemaVersion"
                    | "type"
                    | "title"
                    | "description"
                    | "format"
                    | "default"
                    | "enum"
                    | "properties"
                    | "required"
                    | "additionalProperties"
                    | "items"
                    | "anyOf"
                    | "minimum"
                    | "maximum"
                    | "minItems"
                    | "maxItems"
                    | "minLength"
                    | "maxLength"
                    | "pattern"
                    | "minProperties"
                    | "maxProperties"
            )
        {
            return Err(rejected());
        }
        match key.as_str() {
            // Existing native file-tool schema version is an owned annotation,
            // not an arbitrary extension or schema constraint. Preserve it.
            "schemaVersion"
                if root
                    && value.as_u64()
                        == Some(u64::from(
                            crate::file_tools::NATIVE_FILE_TOOL_SCHEMA_VERSION,
                        )) => {}
            "type" => {
                let valid = |v: &Value| {
                    v.as_str().is_some_and(|s| {
                        matches!(
                            s,
                            "object"
                                | "array"
                                | "string"
                                | "number"
                                | "integer"
                                | "boolean"
                                | "null"
                        )
                    })
                };
                if !valid(value)
                    && !value
                        .as_array()
                        .is_some_and(|a| !a.is_empty() && a.iter().all(valid))
                {
                    return Err(rejected());
                }
            }
            "title" | "description" | "format" | "pattern" => {
                if !value.is_string() {
                    return Err(rejected());
                }
            }
            "default" | "const" => {}
            "enum" | "examples" => {
                if !value.as_array().is_some_and(|a| !a.is_empty()) {
                    return Err(rejected());
                }
            }
            "properties" => {
                let properties = value.as_object().ok_or_else(rejected)?;
                for property in properties.values() {
                    validate_node(provider, property, false)?;
                }
            }
            "required" => {
                let entries = value.as_array().ok_or_else(rejected)?;
                let mut unique = std::collections::BTreeSet::new();
                for entry in entries {
                    let name = entry.as_str().ok_or_else(rejected)?;
                    if !unique.insert(name) {
                        return Err(rejected());
                    }
                }
            }
            "additionalProperties" => {
                if !value.is_boolean() {
                    validate_node(provider, value, false)?;
                }
            }
            "items" => validate_node(provider, value, false)?,
            "anyOf" | "oneOf" | "allOf" => {
                let entries = value
                    .as_array()
                    .filter(|a| !a.is_empty())
                    .ok_or_else(rejected)?;
                for entry in entries {
                    validate_node(provider, entry, false)?;
                }
            }
            "not" | "if" | "then" | "else" => {
                validate_node(provider, value, false)?;
            }
            "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" | "multipleOf" => {
                if !value.is_number()
                    || key == "multipleOf" && value.as_f64().is_none_or(|v| v <= 0.0)
                {
                    return Err(rejected());
                }
            }
            "minItems" | "maxItems" | "minLength" | "maxLength" | "minProperties"
            | "maxProperties" => {
                if value.as_u64().is_none() {
                    return Err(rejected());
                }
            }
            "uniqueItems" => {
                if !value.is_boolean() {
                    return Err(rejected());
                }
            }
            // Includes all reference vocabularies and extensions. Never silently
            // discard an unsupported nested constraint or treat it as Supported.
            _ => return Err(rejected()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn validates_nested_schema_positions_without_mistaking_data_for_keywords() {
        let schema = json!({"type":"object","properties":{"unknown-keyword":{"type":"array","items":{"type":"string","enum":["$ref"]},"minItems":1}},"additionalProperties":false});
        assert!(validate("openai", &schema).is_ok());
        assert!(
            validate(
                "openai",
                &crate::apply_patch_tool_schema(
                    crate::file_tools::NativePatchWireShape::JsonFunction
                )
            )
            .is_ok()
        );
        for bad in [
            json!({"type":"object","properties":{"x":{"unknown-keyword":true}}}),
            json!({"type":"object","properties":{"x":{"items":{"$ref":"https://invalid.example"}}}}),
            json!({"type":"object","properties":{"x":{"required":"x"}}}),
            json!({"type":"object","properties":{"x":{"minItems":-1}}}),
        ] {
            let error = validate("openai", &bad).unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<crate::failure::ProviderTransportFailure>()
                    .unwrap()
                    .classification
                    .class,
                pioneer_protocol::ProviderFailureClass::UnsupportedParameter
            );
        }
        let conditional = json!({"type":"object","allOf":[{"if":{"properties":{"action":{"const":"act"}}},"then":{"required":["session"]}}]});
        assert!(validate("anthropic", &conditional).is_ok());
        assert!(validate("gemini", &conditional).is_err());
    }
    #[test]
    fn schema_depth_cardinality_and_utf8_byte_limits_are_real() {
        let mut nested = json!({"type":"string"});
        for _ in 0..34 {
            nested = json!({"type":"object","properties":{"nested":nested}});
        }
        assert!(validate("openai", &nested).is_err());
        let wide = json!({"type":"object","enum":vec![0;4097]});
        assert!(validate("openai", &wide).is_err());
        let bytes = json!({"type":"object","description":"😀".repeat(33_000)});
        assert!(validate("openai", &bytes).is_err());
    }
}
