//! Stable timeline render fingerprint helpers.

use super::{
    labels::{display_name_from_attachment, format_elapsed_ms, timeline_entry_text},
    rows::{TimelineCoalescedToolsKind, TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind},
};
use crate::conversation::ConversationViewState;
use crate::security::security_summary_label;
use pioneer_protocol::TurnItem;
use serde::Serialize;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

pub fn timeline_rows_render_fingerprint(
    projection: &ConversationViewState,
    rows: &[TimelineRow],
    expanded: &HashSet<String>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    rows.len().hash(&mut hasher);
    for row in rows {
        row.key.hash(&mut hasher);
        timeline_row_render_fingerprint(projection, row, expanded).hash(&mut hasher);
    }
    hasher.finish()
}

pub fn timeline_row_render_fingerprint(
    projection: &ConversationViewState,
    row: &TimelineRow,
    expanded: &HashSet<String>,
) -> u64 {
    timeline_row_render_fingerprint_from_content(
        timeline_row_content_fingerprint(projection, row),
        projection,
        row,
        expanded,
    )
}

pub fn timeline_row_render_fingerprint_from_content(
    content_fingerprint: u64,
    projection: &ConversationViewState,
    row: &TimelineRow,
    expanded: &HashSet<String>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    content_fingerprint.hash(&mut hasher);
    timeline_row_is_expanded(projection, row, expanded).hash(&mut hasher);
    hasher.finish()
}

pub fn timeline_row_content_fingerprint(
    projection: &ConversationViewState,
    row: &TimelineRow,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.key.hash(&mut hasher);
    hash_serializable(&row.author, &mut hasher);
    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => {
            1u8.hash(&mut hasher);
            group.toggle_key.hash(&mut hasher);
            group.elapsed_ms.hash(&mut hasher);
            group.is_open.hash(&mut hasher);
            group.state.hash(&mut hasher);
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
            running_turn.state.hash(&mut hasher);
            running_turn.message.hash(&mut hasher);
            hash_serializable(&running_turn.route, &mut hasher);
            hash_serializable(&running_turn.agent_work_graph, &mut hasher);
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
        TimelineRowKind::Item { timeline_index }
        | TimelineRowKind::UserMessage { timeline_index, .. } => {
            0u8.hash(&mut hasher);

            if let TimelineRowKind::UserMessage { presentation, .. } = &row.kind {
                hash_serializable(presentation, &mut hasher);
            }

            if let Some(entry) = projection.timeline.get(*timeline_index) {
                entry.id.hash(&mut hasher);
                projection
                    .turn_permission_profile(entry.turn_id.as_str())
                    .map(|profile| profile.mode)
                    .hash(&mut hasher);

                if let Some(item_view) = projection.item_for_timeline_entry(entry) {
                    item_view.item_type.hash(&mut hasher);
                    item_view.status.hash(&mut hasher);
                    item_view.partial_text.hash(&mut hasher);
                    item_view.final_text.hash(&mut hasher);
                    hash_serializable(&item_view.partial_markdown, &mut hasher);
                    hash_serializable(&item_view.final_markdown, &mut hasher);
                    hash_turn_item_without_identity(&item_view.item, &mut hasher);
                    hash_serializable(&item_view.route, &mut hasher);
                    hash_serializable(&item_view.timeline_origin, &mut hasher);
                    hash_serializable(&item_view.opaque_meta, &mut hasher);
                }
            }
        }
    }

    hasher.finish()
}

pub fn render_fingerprint_hex(fingerprint: u64) -> String {
    format!("{fingerprint:016x}")
}

fn timeline_row_is_expanded(
    projection: &ConversationViewState,
    row: &TimelineRow,
    expanded: &HashSet<String>,
) -> bool {
    let Some(timeline_index) = timeline_row_index(row) else {
        return false;
    };

    projection
        .timeline
        .get(timeline_index)
        .is_some_and(|entry| expanded.contains(entry.id.as_str()))
}

fn hash_turn_item_without_identity(
    item: &TurnItem,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) {
    let Ok(mut value) = serde_json::to_value(item) else {
        return;
    };
    if let serde_json::Value::Object(fields) = &mut value {
        fields.remove("id");
    }
    hash_serializable(&value, hasher);
}

fn hash_serializable(
    value: &impl Serialize,
    hasher: &mut std::collections::hash_map::DefaultHasher,
) {
    if let Ok(bytes) = serde_json::to_vec(value) {
        bytes.hash(hasher);
    }
}

pub fn timeline_row_text_len(projection: &ConversationViewState, row: &TimelineRow) -> usize {
    match &row.kind {
        TimelineRowKind::Item { timeline_index }
        | TimelineRowKind::UserMessage { timeline_index, .. } => projection
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
            if let Some(state) = group.state.as_ref() {
                len = len.saturating_add(match state {
                    pioneer_protocol::TurnWorkState::Starting
                    | pioneer_protocol::TurnWorkState::Stalled => 6,
                    pioneer_protocol::TurnWorkState::Running
                    | pioneer_protocol::TurnWorkState::WaitingForApproval => 7,
                    pioneer_protocol::TurnWorkState::Failed => 6,
                    pioneer_protocol::TurnWorkState::Interrupted => 9,
                    pioneer_protocol::TurnWorkState::Blocked => 7,
                    pioneer_protocol::TurnWorkState::Completed => 9,
                });
            }
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

#[cfg(test)]
mod tests {
    use super::timeline_row_render_fingerprint;
    use crate::{
        conversation::{ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus},
        timeline::labels::RunningTurnDisplay,
        timeline::rows::{TimelineRow, TimelineRowKind},
    };
    use pioneer_protocol::{PersistedActorRef, TurnAuthorSnapshot, TurnItem};
    use std::collections::HashSet;

    #[test]
    fn item_render_fingerprint_is_stable_when_history_is_prepended() {
        let (projection_before, row_before) = projection_with_target_item(false);
        let (projection_after, row_after) = projection_with_target_item(true);
        let expanded = HashSet::new();

        assert_eq!(
            timeline_row_render_fingerprint(&projection_before, &row_before, &expanded),
            timeline_row_render_fingerprint(&projection_after, &row_after, &expanded)
        );
    }

    #[test]
    fn item_render_fingerprint_ignores_durable_identity_and_timestamp_reconciliation() {
        let (optimistic, optimistic_row) = projection_with_target_item(false);
        let (mut durable, durable_row) = projection_with_target_item(false);
        durable.timeline[0].item_id = "durable-item".to_owned();
        durable.items[0].id = "durable-item".to_owned();
        durable.items[0].updated_at_unix_ms = Some(2);
        durable.items[0].completed_at_unix_ms = Some(2);
        durable.items[0].item = TurnItem::UserMessage {
            id: "durable-item".to_owned(),
            text: "Target message".to_owned(),
            attachments: Vec::new(),
        };
        let expanded = HashSet::new();

        assert_eq!(
            timeline_row_render_fingerprint(&optimistic, &optimistic_row, &expanded),
            timeline_row_render_fingerprint(&durable, &durable_row, &expanded)
        );
    }

    #[test]
    fn item_render_fingerprint_tracks_exact_content() {
        let (projection, row) = projection_with_target_item(false);
        let mut changed = projection.clone();
        changed.items[0].partial_text = "Target massage".to_owned();
        changed.items[0].final_text = Some("Target massage".to_owned());
        changed.items[0].item = TurnItem::UserMessage {
            id: "target-item".to_owned(),
            text: "Target massage".to_owned(),
            attachments: Vec::new(),
        };
        let expanded = HashSet::new();

        assert_ne!(
            timeline_row_render_fingerprint(&projection, &row, &expanded),
            timeline_row_render_fingerprint(&changed, &row, &expanded)
        );
    }

    #[test]
    fn item_render_fingerprint_tracks_exact_author() {
        let (projection, row) = projection_with_target_item(false);
        let mut authored = row.clone();
        authored.author = Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::System,
            display_name: "System".to_owned(),
            nickname: "system".to_owned(),
            avatar_revision: None,
            agent: None,
        });
        let expanded = HashSet::new();

        let original = timeline_row_render_fingerprint(&projection, &row, &expanded);
        assert_ne!(
            original,
            timeline_row_render_fingerprint(&projection, &authored, &expanded)
        );
    }

    #[test]
    fn running_render_fingerprint_tracks_exact_author() {
        let projection = ConversationViewState::default();
        let row = TimelineRow {
            key: "running-target".to_owned(),
            author: None,
            kind: TimelineRowKind::RunningTurn(RunningTurnDisplay {
                turn_id: "target-turn".to_owned(),
                started_at_unix_ms: Some(1),
                state: Some(pioneer_protocol::TurnWorkState::Running),
                message: None,
                route: None,
                agent_work_graph: None,
                permission_profile: None,
                security_summary: None,
            }),
        };
        let mut authored = row.clone();
        authored.author = Some(TurnAuthorSnapshot {
            actor: PersistedActorRef::System,
            display_name: "System".to_owned(),
            nickname: "system".to_owned(),
            avatar_revision: None,
            agent: None,
        });
        let expanded = HashSet::new();
        let original = timeline_row_render_fingerprint(&projection, &row, &expanded);

        assert_ne!(
            original,
            timeline_row_render_fingerprint(
                &projection,
                &TimelineRow {
                    key: row.key.clone(),
                    author: authored.author,
                    kind: row.kind,
                },
                &expanded,
            )
        );
    }

    fn projection_with_target_item(prepend_item: bool) -> (ConversationViewState, TimelineRow) {
        let mut projection = ConversationViewState::default();
        if prepend_item {
            push_user_item(
                &mut projection,
                "older-entry",
                "older-item",
                "older-turn",
                "Older message",
            );
        }

        let timeline_index = projection.timeline.len();
        push_user_item(
            &mut projection,
            "target-entry",
            "target-item",
            "target-turn",
            "Target message",
        );

        (
            projection,
            TimelineRow {
                key: "target-entry".to_owned(),
                author: None,
                kind: TimelineRowKind::Item { timeline_index },
            },
        )
    }

    fn push_user_item(
        projection: &mut ConversationViewState,
        entry_id: &str,
        item_id: &str,
        turn_id: &str,
        text: &str,
    ) {
        let item_index = projection.items.len();
        projection.items.push(ItemView {
            id: item_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_type: "user_message".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(1),
            completed_at_unix_ms: Some(1),
            partial_text: text.to_owned(),
            final_text: Some(text.to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::UserMessage {
                id: item_id.to_owned(),
                text: text.to_owned(),
                attachments: Vec::new(),
            },
            route: None,
            timeline_origin: None,
            opaque_meta: None,
        });
        projection.timeline.push(TimelineEntry {
            id: entry_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            item_index,
        });
    }
}

pub fn timeline_row_toggle_key(row: &TimelineRow) -> Option<&str> {
    match &row.kind {
        TimelineRowKind::TurnWorkToggle(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::CoalescedTools(group) => Some(group.toggle_key.as_str()),
        TimelineRowKind::Item { .. }
        | TimelineRowKind::UserMessage { .. }
        | TimelineRowKind::RunningTurn(_) => None,
    }
}

fn timeline_row_index(row: &TimelineRow) -> Option<usize> {
    match &row.kind {
        TimelineRowKind::Item { timeline_index }
        | TimelineRowKind::UserMessage { timeline_index, .. } => Some(*timeline_index),
        TimelineRowKind::TurnWorkToggle(_)
        | TimelineRowKind::CoalescedTools(_)
        | TimelineRowKind::RunningTurn(_) => None,
    }
}
