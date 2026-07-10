use pioneer_entity::{turn, turn_item};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde_json::json;

use crate::timeline_projection::TurnItemProjectionClassification;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ItemEventOrder {
    pub event_id: String,
    pub sequence: i64,
}

pub(crate) fn classification_metadata_json(
    classification: &TurnItemProjectionClassification,
) -> String {
    json!({
        "placement": classification.placement_str(),
        "classification": classification.classification_str(),
        "audit": classification.audit,
        "auditReason": classification.audit_reason,
    })
    .to_string()
}

pub(crate) fn datetime_millis(value: DateTimeWithTimeZone) -> i64 {
    value.timestamp_millis()
}

pub(crate) fn elapsed_ms(
    started_at: DateTimeWithTimeZone,
    completed_at: DateTimeWithTimeZone,
) -> i64 {
    completed_at
        .signed_duration_since(started_at)
        .num_milliseconds()
        .max(0)
}

pub(crate) fn work_item_order_key(
    item: &turn_item::Model,
    source_order: Option<&ItemEventOrder>,
) -> String {
    if let Some(source_order) = source_order {
        return format!("{:020}:{}", source_order.sequence.max(0), item.item_id);
    }
    format!(
        "z:{:020}:{}",
        datetime_millis(item.created_at).max(0),
        item.item_id
    )
}

pub(crate) fn turn_block_sort_key(turn: &turn::Model, rank: u16, suffix: &str) -> String {
    format!(
        "{:020}:{}:{:03}:{}",
        datetime_millis(turn.created_at).max(0),
        turn.id,
        rank,
        suffix
    )
}

pub fn user_block_id(turn_id: &str) -> String {
    format!("turn:{turn_id}:user")
}

pub fn work_block_id(turn_id: &str) -> String {
    format!("turn:{turn_id}:work")
}

pub fn assistant_block_id(turn_id: &str, item_id: &str) -> String {
    format!("turn:{turn_id}:assistant:{item_id}")
}

pub fn work_item_projection_id(turn_id: &str, item_id: &str) -> String {
    format!("turn:{turn_id}:work:{item_id}")
}

pub fn approval_block_id(turn_id: &str, request_id: &str) -> String {
    format!("turn:{turn_id}:approval:{request_id}")
}

pub(crate) fn turn_work_presentation(turn: &turn::Model, has_final: bool) -> &'static str {
    if has_final {
        "collapsed_after_final"
    } else if turn_is_terminal(turn) {
        "expanded_terminal_no_final"
    } else {
        "expanded_live"
    }
}

pub(crate) fn turn_work_state(
    turn: &turn::Model,
    pending_request_count: i64,
    has_running_item: bool,
    has_stale_running_item: bool,
) -> &'static str {
    match turn.status.as_str() {
        "completed" => return "completed",
        "failed" => return "failed",
        "interrupted" => return "interrupted",
        "blocked" => return "blocked",
        _ => {}
    }

    if pending_request_count > 0 {
        return "waiting_for_approval";
    }

    if has_stale_running_item {
        return "stalled";
    }

    if turn.status == "in_progress" && has_running_item {
        return "running";
    }

    if turn.status == "in_progress" {
        return "starting";
    }

    "running"
}

pub(crate) fn turn_is_terminal(turn: &turn::Model) -> bool {
    matches!(
        turn.status.as_str(),
        "completed" | "failed" | "interrupted" | "blocked"
    )
}

pub(crate) fn terminal_completed_at(turn: &turn::Model) -> Option<DateTimeWithTimeZone> {
    if turn_is_terminal(turn) {
        Some(turn.updated_at)
    } else {
        None
    }
}
