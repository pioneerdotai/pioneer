use chrono::{TimeZone, Utc};
use sea_orm::entity::prelude::DateTimeWithTimeZone;

pub fn unix_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_opt(value, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

/// Eligibility is checked at whole Unix seconds, so round a fractional
/// deadline up to the first second at which the row is eligible.
pub fn deadline_unix_seconds(value: DateTimeWithTimeZone) -> i64 {
    value
        .timestamp()
        .saturating_add(i64::from(value.timestamp_subsec_nanos() != 0))
}
