use chrono::{TimeZone, Utc};
use sea_orm::entity::prelude::DateTimeWithTimeZone;

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

pub fn utc_now() -> DateTimeWithTimeZone {
    Utc::now().fixed_offset()
}
