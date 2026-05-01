use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::{Serialize, de::DeserializeOwned};

pub fn unix_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_opt(value, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

pub fn unix_ms_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_millis_opt(value)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

pub fn optional_typed_json_to_db<T: Serialize>(value: &Option<T>) -> Result<Option<String>> {
    value
        .as_ref()
        .map(|value| serde_json::to_string(value).context("failed to serialize JSON column"))
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

pub fn typed_json_from_db<T: DeserializeOwned>(value: String) -> Result<T> {
    serde_json::from_str(value.as_str()).context("failed to decode JSON column")
}
