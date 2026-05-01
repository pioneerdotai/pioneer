use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

pub const REDACTED_VALUE: &str = "<redacted>";

pub fn redact_string_map(source: &BTreeMap<String, String>) -> Value {
    let mut object = Map::new();
    for key in source.keys() {
        object.insert(key.clone(), json!(REDACTED_VALUE));
    }
    Value::Object(object)
}

pub fn redact_text(value: &str, secrets: &[String]) -> String {
    let mut redacted = value.to_owned();
    for secret in secrets {
        if secret.is_empty() {
            continue;
        }
        redacted = redacted.replace(secret, REDACTED_VALUE);
    }
    redacted
}

pub fn bounded_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }

    let mut tail = value.chars().rev().take(max_chars).collect::<Vec<_>>();
    tail.reverse();
    format!("...{}", tail.into_iter().collect::<String>())
}
