//! Timestamp and JSON column conversions for Task repositories.

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::Serialize;

pub fn unix_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_opt(value, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

/// SQL eligibility is checked at whole Unix seconds. Never round a fractional
/// durable deadline down into an immediate, still-ineligible retry loop.
pub fn deadline_unix_seconds(value: DateTimeWithTimeZone) -> i64 {
    value
        .timestamp()
        .saturating_add(i64::from(value.timestamp_subsec_nanos() != 0))
}

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
