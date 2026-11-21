use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};

pub use pioneer_crud_tasks::values::{
    deadline_unix_seconds, json_value_from_db, optional_json_value_from_db,
    optional_json_value_to_db, optional_typed_json_from_db, optional_typed_json_to_db,
    typed_json_to_db, unix_to_datetime,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::de::DeserializeOwned;

pub fn unix_ms_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_millis_opt(value)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

pub fn typed_json_from_db<T: DeserializeOwned>(value: String) -> Result<T> {
    serde_json::from_str(value.as_str()).context("failed to decode JSON column")
}

pub(crate) use pioneer_crud_conversations::turn_payload::PreflightRace;

pub(crate) async fn rollback_preserving_error(
    transaction: pioneer_sqlite::SqliteTransaction,
    error: anyhow::Error,
) -> anyhow::Error {
    let _ = transaction.rollback().await;
    error
}
