use chrono::{TimeZone, Utc};
use sea_orm::entity::prelude::DateTimeWithTimeZone;

pub fn unix_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_opt(value, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}
