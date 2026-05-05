use crate::HookMetadataKey;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type HookMetadata = BTreeMap<HookMetadataKey, HookValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(untagged)]
pub enum HookValue {
    #[default]
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    List(Vec<HookValue>),
    Object(BTreeMap<HookMetadataKey, HookValue>),
}

impl From<bool> for HookValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for HookValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<f64> for HookValue {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<String> for HookValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for HookValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<Vec<HookValue>> for HookValue {
    fn from(value: Vec<HookValue>) -> Self {
        Self::List(value)
    }
}

impl From<BTreeMap<HookMetadataKey, HookValue>> for HookValue {
    fn from(value: BTreeMap<HookMetadataKey, HookValue>) -> Self {
        Self::Object(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hook_value_roundtrips_object() {
        let mut object = BTreeMap::new();
        object.insert(
            HookMetadataKey::new("execution_policy").expect("valid key"),
            HookValue::Text("deadline".to_owned()),
        );
        object.insert(
            HookMetadataKey::new("safe").expect("valid key"),
            HookValue::Bool(true),
        );

        let value = HookValue::Object(object);
        let encoded = serde_json::to_value(&value).expect("value serializes");
        assert_eq!(encoded["execution_policy"], "deadline");
        assert_eq!(encoded["safe"], true);

        let decoded: HookValue = serde_json::from_value(encoded).expect("value deserializes");
        assert_eq!(decoded, value);
    }
}
