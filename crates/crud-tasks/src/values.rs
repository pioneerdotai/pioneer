//! Timestamp and JSON column conversions for Task repositories.

use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};

pub use pioneer_crud_execution::time::{deadline_unix_seconds, unix_to_datetime};

pub fn optional_typed_json_to_db<T: Serialize>(value: &Option<T>) -> Result<Option<String>> {
    value
        .as_ref()
        .map(|value| serde_json::to_string(value).context("failed to serialize JSON column"))
        .transpose()
}

pub fn optional_json_value_to_db<T: Serialize>(
    value: &Option<T>,
) -> Result<Option<serde_json::Value>> {
    value
        .as_ref()
        .map(|value| serde_json::to_value(value).context("failed to encode PostgreSQL JSON column"))
        .transpose()
}

pub fn optional_typed_json_from_db<T: DeserializeOwned>(
    value: Option<String>,
) -> Result<Option<T>> {
    value
        .map(|value| serde_json::from_str(value.as_str()).context("failed to decode JSON column"))
        .transpose()
}

pub fn typed_json_to_db<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).context("failed to serialize JSON column")
}

pub fn optional_json_value_from_db<T: DeserializeOwned>(
    value: Option<serde_json::Value>,
) -> Result<Option<T>> {
    value.map(json_value_from_db).transpose()
}

pub fn json_value_from_db<T: DeserializeOwned>(value: serde_json::Value) -> Result<T> {
    serde_json::from_value(value).context("failed to decode PostgreSQL JSON column")
}
