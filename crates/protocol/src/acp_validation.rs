//! Shared validation of standard ACP elicitation. Used by transport and client presentation.
use crate::acp::*;
fn value(value: &impl serde::Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|_| Error::invalid_params())
}
use serde_json::Value;

pub fn validate_request(request: &CreateElicitationRequest) -> Result<()> {
    match &request.mode {
        ElicitationMode::Form(form) => {
            let schema = value(&form.requested_schema)?;
            let properties = schema["properties"]
                .as_object()
                .ok_or_else(Error::invalid_params)?;
            if properties.len() > 128 {
                return Err(Error::invalid_params());
            }
            if let Some(required) = schema.get("required").and_then(Value::as_array) {
                for name in required {
                    if !name
                        .as_str()
                        .is_some_and(|name| properties.contains_key(name))
                    {
                        return Err(Error::invalid_params());
                    }
                }
            }
            for property in properties.values() {
                if !matches!(
                    property["type"].as_str(),
                    Some("string" | "number" | "integer" | "boolean" | "array")
                ) {
                    return Err(Error::invalid_params());
                }
                if let Some(pattern) = property.get("pattern").and_then(Value::as_str) {
                    validate_pattern(pattern, None)?;
                }
                if property["type"] == "array"
                    && !(property["items"].get("enum").is_some_and(Value::is_array)
                        || property["items"].get("anyOf").is_some_and(Value::is_array))
                {
                    return Err(Error::invalid_params());
                }
            }
        }
        ElicitationMode::Url(mode) => {
            let url = url::Url::parse(&mode.url).map_err(|_| Error::invalid_params())?;
            if !matches!(url.scheme(), "https" | "http")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.host_str().is_none()
            {
                return Err(Error::invalid_params());
            }
        }
        _ => return Err(Error::invalid_params()),
    }
    Ok(())
}

pub fn validate_response(
    request: &CreateElicitationRequest,
    response: &CreateElicitationResponse,
) -> Result<()> {
    let ElicitationAction::Accept(accept) = &response.action else {
        return if matches!(
            response.action,
            ElicitationAction::Cancel | ElicitationAction::Decline
        ) {
            Ok(())
        } else {
            Err(Error::invalid_params())
        };
    };
    match &request.mode {
        ElicitationMode::Url(_) => {
            // URL consent never transports credentials or claims completion.
            if accept
                .content
                .as_ref()
                .is_some_and(|content| !content.is_empty())
            {
                return Err(Error::invalid_params());
            }
        }
        ElicitationMode::Form(form) => {
            let schema = value(&form.requested_schema)?;
            let content = accept
                .content
                .as_ref()
                .map(value)
                .transpose()?
                .unwrap_or_else(|| serde_json::json!({}));
            let fields = content.as_object().ok_or_else(Error::invalid_params)?;
            let properties = schema["properties"]
                .as_object()
                .ok_or_else(Error::invalid_params)?;
            if let Some(required) = schema.get("required").and_then(Value::as_array)
                && required
                    .iter()
                    .any(|name| !name.as_str().is_some_and(|name| fields.contains_key(name)))
            {
                return Err(Error::invalid_params());
            }
            for (name, field) in fields {
                let property = properties.get(name).ok_or_else(Error::invalid_params)?;
                validate_field(property, field)?;
            }
        }
        _ => return Err(Error::invalid_params()),
    }
    Ok(())
}

/// Standard ECMA RegExp with bounded compilation, allocation and matching.
/// No agent-provided JavaScript is evaluated: patterns and input are arguments
/// to this fixed function; no loaders, host functions or IO are installed.
fn validate_pattern(pattern: &str, text: Option<&str>) -> Result<bool> {
    use rquickjs_core::{Context, Function, Runtime, context::intrinsic};
    if pattern.len() > 4096 || text.is_some_and(|text| text.len() > 64 * 1024) {
        return Err(Error::invalid_params());
    }
    let runtime = Runtime::new().map_err(|_| Error::invalid_params())?;
    runtime.set_memory_limit(4 * 1024 * 1024);
    runtime.set_max_stack_size(256 * 1024);
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(20);
    let mut remaining = 1024usize;
    runtime.set_interrupt_handler(Some(Box::new(move || {
        remaining = remaining.saturating_sub(1);
        remaining == 0 || std::time::Instant::now() >= deadline
    })));
    let context = Context::custom::<(
        intrinsic::Eval,
        intrinsic::RegExpCompiler,
        intrinsic::RegExp,
    )>(&runtime)
    .map_err(|_| Error::invalid_params())?;
    context.with(|context| {
        let validator: Function = context.eval(
            "(pattern, text) => { const expression = new RegExp(pattern, 'u'); return text == null || expression.test(text); }"
        ).map_err(|_| Error::invalid_params())?;
        validator.call((pattern, text)).map_err(|_| Error::invalid_params())
    })
}

fn validate_field(schema: &Value, field: &Value) -> Result<()> {
    let valid = match schema["type"].as_str() {
        Some("string") => {
            let text = field.as_str().ok_or_else(Error::invalid_params)?;
            let length = text.chars().count() as u64;
            if schema
                .get("minLength")
                .and_then(Value::as_u64)
                .is_some_and(|min| length < min)
                || schema
                    .get("maxLength")
                    .and_then(Value::as_u64)
                    .is_some_and(|max| length > max)
            {
                return Err(Error::invalid_params());
            }
            if let Some(pattern) = schema.get("pattern").and_then(Value::as_str)
                && !validate_pattern(pattern, Some(text))?
            {
                return Err(Error::invalid_params());
            }
            match schema.get("format").and_then(Value::as_str) {
                Some("uri") if url::Url::parse(text).is_err() => {
                    return Err(Error::invalid_params());
                }
                Some("date") if chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").is_err() => {
                    return Err(Error::invalid_params());
                }
                Some("date-time") if chrono::DateTime::parse_from_rfc3339(text).is_err() => {
                    return Err(Error::invalid_params());
                }
                // Email is an annotation; the form must still review the exact
                // user-provided value. Avoid inventing narrower mailbox rules.
                _ => {}
            }
            allowed_enum(schema, field)
        }
        Some("number" | "integer") => {
            let number = field.as_f64().ok_or_else(Error::invalid_params)?;
            number.is_finite()
                && (schema["type"] != "integer" || number.fract() == 0.0)
                && !schema
                    .get("minimum")
                    .and_then(Value::as_f64)
                    .is_some_and(|min| number < min)
                && !schema
                    .get("maximum")
                    .and_then(Value::as_f64)
                    .is_some_and(|max| number > max)
        }
        Some("boolean") => field.is_boolean(),
        Some("array") => {
            let items = field.as_array().ok_or_else(Error::invalid_params)?;
            if schema
                .get("minItems")
                .and_then(Value::as_u64)
                .is_some_and(|min| (items.len() as u64) < min)
                || schema
                    .get("maxItems")
                    .and_then(Value::as_u64)
                    .is_some_and(|max| (items.len() as u64) > max)
            {
                return Err(Error::invalid_params());
            }
            items
                .iter()
                .all(|item| item.is_string() && allowed_enum(&schema["items"], item))
                && items
                    .iter()
                    .enumerate()
                    .all(|(i, item)| !items[..i].contains(item))
        }
        _ => false,
    };
    if !valid {
        return Err(Error::invalid_params());
    }
    Ok(())
}

fn allowed_enum(schema: &Value, field: &Value) -> bool {
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values.contains(field);
    }
    if let Some(options) = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
    {
        return options
            .iter()
            .any(|option| option.get("const") == Some(field));
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn standard_patterns_support_lookahead_and_ascii_digit_semantics_with_budgets() {
        assert!(validate_pattern("^(?=.*[A-Z])[A-Za-z]+$", Some("Hello")).unwrap());
        assert!(!validate_pattern("^(?=.*[A-Z])[A-Za-z]+$", Some("hello")).unwrap());
        assert!(validate_pattern(r"^\d+$", Some("123")).unwrap());
        assert!(!validate_pattern(r"^\d+$", Some("١٢٣")).unwrap());
        assert!(!validate_pattern(r"^(?=.)\d+$", Some("١٢٣")).unwrap());
        assert!(validate_pattern(r"^\p{L}+$", Some("Привет")).unwrap());
        assert!(validate_pattern(&"a".repeat(4097), None).is_err());
        assert!(validate_pattern(r"^(a+)+$", Some(&format!("{}!", "a".repeat(4096)))).is_err());
    }

    #[test]
    fn form_enforces_required_types_bounds_and_enum() {
        let request = serde_json::from_value(json!({"mode":"form", "sessionId":"s", "message":"Choose", "requestedSchema":{
            "type":"object", "properties":{"n":{"type":"integer","minimum":1,"maximum":3}, "v":{"type":"string","enum":["opaque"]}}, "required":["n","v"]
        }})).unwrap();
        validate_request(&request).unwrap();
        for content in [
            json!({"n":0,"v":"opaque"}),
            json!({"n":2,"v":"guess"}),
            json!({"n":2}),
        ] {
            let response =
                serde_json::from_value(json!({"action":"accept","content":content})).unwrap();
            assert!(validate_response(&request, &response).is_err());
        }
        let response =
            serde_json::from_value(json!({"action":"accept","content":{"n":2,"v":"opaque"}}))
                .unwrap();
        assert!(validate_response(&request, &response).is_ok());
    }
    #[test]
    fn sdk_multiselect_items_have_no_type_and_use_any_of_for_titles() {
        for items in [
            json!({"enum":["opaque/a","opaque/b"]}),
            json!({"anyOf":[{"const":"opaque/a","title":"A"},{"const":"opaque/b","title":"B"}]}),
        ] {
            let request = serde_json::from_value(json!({"mode":"form","sessionId":"s","message":"Choose","requestedSchema":{"type":"object","properties":{"v":{"type":"array","items":items}},"required":["v"]}})).unwrap();
            validate_request(&request).unwrap();
            for (values, valid) in [
                (json!(["opaque/b"]), true),
                (json!(["guess"]), false),
                (json!(["opaque/b", "opaque/b"]), false),
            ] {
                let response =
                    serde_json::from_value(json!({"action":"accept","content":{"v":values}}))
                        .unwrap();
                assert_eq!(validate_response(&request, &response).is_ok(), valid);
            }
        }
    }
}
