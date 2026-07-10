//! Stable timeline layout hash helpers.

use super::{
    labels::{display_name_from_attachment, format_elapsed_ms, timeline_entry_text},
    rows::{TimelineCoalescedToolsKind, TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind},
};
use crate::conversation::ConversationViewState;
use crate::security::security_summary_label;
use pioneer_protocol::TurnItem;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

pub fn timeline_rows_layout_hash(
    projection: &ConversationViewState,
    rows: &[TimelineRow],
    expanded: &HashSet<String>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rows.len().hash(&mut hasher);
    for row in rows {
        row.key.hash(&mut hasher);
        timeline_row_layout_hash(projection, row, expanded).hash(&mut hasher);
    }
    hasher.finish()
}

pub fn timeline_row_layout_hash(
    projection: &ConversationViewState,
    row: &TimelineRow,
    expanded: &HashSet<String>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.key.hash(&mut hasher);

    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => {
            1u8.hash(&mut hasher);
            group.anchor_entry_id.hash(&mut hasher);
            group.elapsed_ms.hash(&mut hasher);
            group.is_open.hash(&mut hasher);
        }
        TimelineRowKind::CoalescedTools(group) => {
            2u8.hash(&mut hasher);
            group.toggle_key.hash(&mut hasher);
            group.count.hash(&mut hasher);
            group.is_open.hash(&mut hasher);
            group.kind.hash(&mut hasher);
        }
        TimelineRowKind::RunningTurn(running_turn) => {
            3u8.hash(&mut hasher);
            running_turn.turn_id.hash(&mut hasher);
            running_turn.started_at_unix_ms.hash(&mut hasher);
            running_turn.state.hash(&mut hasher);
            running_turn.message.hash(&mut hasher);
            running_turn
                .permission_profile
                .as_ref()
                .map(|profile| profile.mode)
                .hash(&mut hasher);
            if let Some(summary) = &running_turn.security_summary {
                summary.permission_mode.hash(&mut hasher);
                summary.sandbox_mode.hash(&mut hasher);
                summary.filesystem_access.hash(&mut hasher);
                summary.network_mode.hash(&mut hasher);
                summary.execution_backend.hash(&mut hasher);
                summary.sandbox_backend.hash(&mut hasher);
                summary.enforcement.hash(&mut hasher);
                summary.diagnostics.len().hash(&mut hasher);
                for diagnostic in &summary.diagnostics {
                    diagnostic.capability.hash(&mut hasher);
                    diagnostic.status.hash(&mut hasher);
                    diagnostic.message.hash(&mut hasher);
                }
            }
        }
        TimelineRowKind::Item { timeline_index } => {
            0u8.hash(&mut hasher);
            timeline_index.hash(&mut hasher);

            if let Some(entry) = projection.timeline.get(*timeline_index) {
                entry.id.hash(&mut hasher);
                entry.turn_id.hash(&mut hasher);
                entry.item_id.hash(&mut hasher);
                entry.item_index.hash(&mut hasher);
                projection
                    .turn_permission_profile(entry.turn_id.as_str())
                    .map(|profile| profile.mode)
                    .hash(&mut hasher);

                if let Some(item_view) = projection.item_for_timeline_entry(entry) {
                    item_view.item_type.hash(&mut hasher);
                    item_view.status.hash(&mut hasher);
                    item_view.updated_at_unix_ms.hash(&mut hasher);

                    let text = timeline_entry_text(item_view);
                    let text_bytes = text.as_bytes();
                    text_bytes.len().hash(&mut hasher);
                    text_bytes
                        .first()
                        .copied()
                        .unwrap_or_default()
                        .hash(&mut hasher);
                    text_bytes
                        .last()
                        .copied()
                        .unwrap_or_default()
                        .hash(&mut hasher);

                    if let Some(markdown) = &item_view.partial_markdown {
                        markdown.blocks.len().hash(&mut hasher);
                    }
                    if let Some(markdown) = &item_view.final_markdown {
                        markdown.blocks.len().hash(&mut hasher);
                    }

                    if let TurnItem::UserMessage { attachments, .. } = &item_view.item {
                        attachments.len().hash(&mut hasher);
                        for attachment in attachments {
                            display_name_from_attachment(attachment).hash(&mut hasher);
                        }
                    }
                }

                expanded.contains(entry.id.as_str()).hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

pub fn timeline_row_text_len(projection: &ConversationViewState, row: &TimelineRow) -> usize {
    match &row.kind {
        TimelineRowKind::Item { timeline_index } => projection
            .timeline
            .get(*timeline_index)
            .and_then(|entry| projection.item_for_timeline_entry(entry))
            .map(|item_view| {
                let permission_len = projection
                    .turn_permission_profile(item_view.turn_id.as_str())
                    .map(|profile| {
                        crate::composer::permissions::turn_permission_mode_display(profile.mode)
                            .label
                            .len()
                    })
                    .unwrap_or_default();
                timeline_entry_text(item_view)
                    .len()
                    .saturating_add(user_message_attachments_text_len(&item_view.item))
                    .saturating_add(permission_len)
            })
            .unwrap_or_default(),
        TimelineRowKind::TurnWorkToggle(group) => {
            let mut len = TURN_WORK_GROUP_COMPLETED_TEXT_LEN_ESTIMATE;
            if let Some(elapsed_ms) = group.elapsed_ms {
                len = len.saturating_add(format_elapsed_ms(elapsed_ms).len());
            }
            len
        }
        TimelineRowKind::CoalescedTools(group) => coalesced_tools_text_len_estimate(group),
        TimelineRowKind::RunningTurn(running_turn) => {
            let permission_len = running_turn
                .permission_profile
                .as_ref()
                .map(|profile| {
                    crate::composer::permissions::turn_permission_mode_display(profile.mode)
                        .label
                        .len()
                })
                .unwrap_or_default();
            let security_len = running_turn
                .security_summary
                .as_ref()
                .map(security_summary_label)
                .map(str::len)
                .unwrap_or_default();
            RUNNING_TURN_TEXT_LEN_ESTIMATE
                .saturating_add(permission_len)
                .saturating_add(security_len)
        }
    }
}

const TURN_WORK_GROUP_COMPLETED_TEXT_LEN_ESTIMATE: usize = 9;
const RUNNING_TURN_TEXT_LEN_ESTIMATE: usize = 12;

fn user_message_attachments_text_len(item: &TurnItem) -> usize {
    let TurnItem::UserMessage { attachments, .. } = item else {
        return 0;
    };

    attachments
        .iter()
        .map(|attachment| {
            display_name_from_attachment(attachment)
                .len()
                .saturating_add(1)
        })
        .sum()
}

fn coalesced_tools_text_len_estimate(group: &TimelineCoalescedToolsRow) -> usize {
    let count_len = group.count.to_string().len();
    let label_len = match group.kind {
        TimelineCoalescedToolsKind::CompletedTaskTools => 21,
        TimelineCoalescedToolsKind::RepeatedTaskWait => 24,
    };
    count_len.saturating_add(1).saturating_add(label_len)
}

pub fn timeline_row_toggle_key(row: &TimelineRow) -> Option<&str> {
    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::CoalescedTools(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::Item { .. } | TimelineRowKind::RunningTurn(_) => None,
    }
}
