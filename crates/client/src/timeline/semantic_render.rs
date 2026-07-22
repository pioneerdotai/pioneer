//! Shared semantic timeline render projection.
//!
//! This is UI-neutral: it converts semantic timeline rows into the same
//! `ConversationViewState` and `TimelineRow` DTOs used by platform renderers.

use crate::{
    conversation::{ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus},
    timeline::{
        labels::RunningTurnDisplay,
        rows::{TimelineRow, TimelineRowKind, TurnWorkGroupRow},
        semantic::{SemanticTimelineRow, SemanticTimelineRowId, SemanticTimelineRowKind},
    },
};
use pioneer_protocol::{
    AgentMessagePhase, SystemEventLevel, TimelineBlock, TimelineBlockKind, TurnItem, TurnItemType,
    TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus, TurnWorkPresentation, TurnWorkState,
};
use std::collections::HashMap;

pub const SEMANTIC_TURN_WORK_GROUP_PREFIX: &str = "semantic-turn-work-group::";

#[derive(Debug, Clone, Default)]
pub struct SemanticTimelineRenderModel {
    pub projection: ConversationViewState,
    pub rows: Vec<TimelineRow>,
    pub semantic_row_ids: HashMap<String, SemanticTimelineRowId>,
}

pub fn render_semantic_timeline_rows(
    semantic_rows: &[SemanticTimelineRow],
    mut projection: ConversationViewState,
) -> SemanticTimelineRenderModel {
    projection.items.clear();
    projection.timeline.clear();

    let live_work = live_work_by_turn(semantic_rows);
    let mut inserted_running_rows = std::collections::HashSet::<String>::new();
    let mut rows = Vec::<TimelineRow>::new();
    let mut semantic_row_ids = HashMap::<String, SemanticTimelineRowId>::new();

    for (index, row) in semantic_rows.iter().enumerate() {
        let rows_before = rows.len();
        push_semantic_row(&mut projection, &mut rows, row);
        for render_row in &rows[rows_before..] {
            semantic_row_ids.insert(render_row.key.clone(), row.id.clone());
        }

        let current_turn_id = semantic_row_turn_id(row);
        let next_turn_id = semantic_rows.get(index + 1).and_then(semantic_row_turn_id);
        if let Some(turn_id) = current_turn_id
            && next_turn_id != Some(turn_id)
            && let Some((started_at_unix_ms, state)) = live_work.get(turn_id).copied()
            && inserted_running_rows.insert(turn_id.to_owned())
        {
            let running_key = format!("semantic-running-turn::{turn_id}");
            rows.push(TimelineRow {
                key: running_key.clone(),
                kind: TimelineRowKind::RunningTurn(running_turn_display_for_projection(
                    &projection,
                    turn_id,
                    started_at_unix_ms,
                    Some(state),
                    None,
                )),
            });
            semantic_row_ids.insert(running_key, row.id.clone());
        }
    }

    SemanticTimelineRenderModel {
        projection,
        rows,
        semantic_row_ids,
    }
}

fn push_semantic_row(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    row: &SemanticTimelineRow,
) {
    match &row.kind {
        SemanticTimelineRowKind::UserBlock { block } => push_user_block(projection, rows, block),
        SemanticTimelineRowKind::WorkHeader {
            block,
            work,
            expanded,
            ..
        } => push_work_header(rows, block, work, *expanded),
        SemanticTimelineRowKind::WorkItem { item } => push_work_item(projection, rows, item),
        SemanticTimelineRowKind::AssistantMessage { block } => {
            push_assistant_block(projection, rows, block);
        }
        SemanticTimelineRowKind::PendingRequest { .. } => {}
        SemanticTimelineRowKind::TurnState { block } => push_turn_state(projection, rows, block),
    }
}

fn push_user_block(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
) {
    let TimelineBlockKind::UserMessage {
        item_id,
        text,
        attachments,
        ..
    } = &block.kind
    else {
        return;
    };
    let item_id = item_id.as_deref().unwrap_or(block.block_id.as_str());
    let turn_id = block.turn_id.as_deref().unwrap_or(block.block_id.as_str());
    let item = TurnItem::UserMessage {
        id: item_id.to_owned(),
        text: text.clone(),
        attachments: attachments.clone(),
    };
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: block.block_id.clone(),
            item_id: item_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_type: "user_message".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: block.started_at_unix_ms.or(block.updated_at_unix_ms),
            updated_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            completed_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            partial_text: text.clone(),
            final_text: Some(text.clone()),
            partial_markdown: None,
            final_markdown: None,
            item,
            opaque_meta: None,
        },
    );
}

fn push_assistant_block(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
) {
    let TimelineBlockKind::AssistantMessage {
        item_id,
        text,
        status,
        markdown,
    } = &block.kind
    else {
        return;
    };
    let turn_id = block.turn_id.as_deref().unwrap_or(block.block_id.as_str());
    let item = TurnItem::AgentMessage {
        id: item_id.clone(),
        text: text.clone(),
        phase: AgentMessagePhase::FinalAnswer,
        markdown: markdown.clone(),
        markdown_version: None,
    };
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: block.block_id.clone(),
            item_id: item_id.clone(),
            turn_id: turn_id.to_owned(),
            item_type: "agent_message".to_owned(),
            status: work_item_status(*status),
            started_at_unix_ms: block.started_at_unix_ms.or(block.updated_at_unix_ms),
            updated_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            completed_at_unix_ms: if is_terminal_work_status(*status) {
                block.updated_at_unix_ms.or(block.started_at_unix_ms)
            } else {
                None
            },
            partial_text: text.clone(),
            final_text: is_terminal_work_status(*status).then(|| text.clone()),
            partial_markdown: markdown.clone(),
            final_markdown: is_terminal_work_status(*status)
                .then(|| markdown.clone())
                .flatten(),
            item,
            opaque_meta: None,
        },
    );
}

fn push_work_header(
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
    work: &TurnWorkBlock,
    expanded: bool,
) {
    if work.presentation == TurnWorkPresentation::ExpandedLive || work.work_count == 0 {
        return;
    }
    let toggle_key = semantic_turn_work_toggle_key(work.turn_id.as_str());
    rows.push(TimelineRow {
        key: toggle_key.clone(),
        kind: TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow {
            toggle_key,
            anchor_entry_id: block.block_id.clone(),
            elapsed_ms: work.elapsed_ms,
            is_open: expanded,
        }),
    });
}

fn push_work_item(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    item: &TurnWorkItem,
) {
    let (text, markdown) = turn_item_text_and_markdown(&item.item);
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: item.work_item_id.clone(),
            item_id: item.item_id.clone(),
            turn_id: item.turn_id.clone(),
            item_type: turn_item_type_label(item.item_type).to_owned(),
            status: work_item_status(item.status),
            started_at_unix_ms: item.started_at_unix_ms.or(item.completed_at_unix_ms),
            updated_at_unix_ms: item.completed_at_unix_ms.or(item.started_at_unix_ms),
            completed_at_unix_ms: completed_at_for_status(item.status, item.completed_at_unix_ms),
            partial_text: text.clone(),
            final_text: is_terminal_work_status(item.status).then_some(text.clone()),
            partial_markdown: markdown.clone(),
            final_markdown: is_terminal_work_status(item.status)
                .then(|| markdown.clone())
                .flatten(),
            item: item.item.clone(),
            opaque_meta: item.metadata.clone(),
        },
    );
}

fn push_turn_state(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
) {
    let TimelineBlockKind::TurnState { state, message } = &block.kind else {
        return;
    };
    let Some(turn_id) = block.turn_id.as_deref() else {
        return;
    };
    if matches!(
        state,
        TurnWorkState::Starting | TurnWorkState::Running | TurnWorkState::Stalled
    ) {
        rows.push(TimelineRow {
            key: format!("semantic-turn-state::{turn_id}::{}", block.block_id),
            kind: TimelineRowKind::RunningTurn(running_turn_display_for_projection(
                projection,
                turn_id,
                block.started_at_unix_ms.or(block.updated_at_unix_ms),
                Some(*state),
                message.clone(),
            )),
        });
        return;
    }

    let Some((level, code, fallback_message)) = terminal_turn_state_event(*state) else {
        return;
    };
    let message = message
        .as_deref()
        .filter(|message| !message.trim().is_empty())
        .unwrap_or(fallback_message)
        .to_owned();
    let details = Some(serde_json::json!({ "error_message": message }));
    let item_id = format!("{}:event", block.block_id);
    let item = TurnItem::SystemEvent {
        id: item_id.clone(),
        level,
        message: message.clone(),
        code: Some(code.to_owned()),
        details: details.clone(),
    };
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: block.block_id.clone(),
            item_id,
            turn_id: turn_id.to_owned(),
            item_type: "system_event".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: block.started_at_unix_ms.or(block.updated_at_unix_ms),
            updated_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            completed_at_unix_ms: block.updated_at_unix_ms.or(block.started_at_unix_ms),
            partial_text: message.clone(),
            final_text: Some(message),
            partial_markdown: None,
            final_markdown: None,
            item,
            opaque_meta: details,
        },
    );
}

fn terminal_turn_state_event(
    state: TurnWorkState,
) -> Option<(SystemEventLevel, &'static str, &'static str)> {
    match state {
        TurnWorkState::Failed => Some((SystemEventLevel::Error, "turn_failed", "Turn failed")),
        TurnWorkState::Interrupted => Some((
            SystemEventLevel::Warning,
            "turn_cancelled",
            "Turn cancelled",
        )),
        TurnWorkState::Blocked => Some((SystemEventLevel::Warning, "turn_blocked", "Turn blocked")),
        _ => None,
    }
}

fn running_turn_display_for_projection(
    projection: &ConversationViewState,
    turn_id: &str,
    started_at_unix_ms: Option<i64>,
    state: Option<TurnWorkState>,
    message: Option<String>,
) -> RunningTurnDisplay {
    RunningTurnDisplay {
        turn_id: turn_id.to_owned(),
        started_at_unix_ms,
        state,
        message,
        permission_profile: projection.turn_permission_profile(turn_id).cloned(),
        security_summary: projection.turn_security_summary(turn_id).cloned(),
    }
}

struct ItemRowInput {
    entry_id: String,
    item_id: String,
    turn_id: String,
    item_type: String,
    status: TimelineEntryStatus,
    started_at_unix_ms: Option<i64>,
    updated_at_unix_ms: Option<i64>,
    completed_at_unix_ms: Option<i64>,
    partial_text: String,
    final_text: Option<String>,
    partial_markdown: Option<pioneer_protocol::MarkdownDocument>,
    final_markdown: Option<pioneer_protocol::MarkdownDocument>,
    item: TurnItem,
    opaque_meta: Option<serde_json::Value>,
}

fn push_item_row(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    input: ItemRowInput,
) {
    let item_index = projection.items.len();
    let timeline_index = projection.timeline.len();
    projection.items.push(ItemView {
        id: input.item_id.clone(),
        turn_id: input.turn_id.clone(),
        item_type: input.item_type,
        status: input.status,
        started_at_unix_ms: input.started_at_unix_ms,
        updated_at_unix_ms: input.updated_at_unix_ms,
        completed_at_unix_ms: input.completed_at_unix_ms,
        partial_text: input.partial_text,
        final_text: input.final_text,
        partial_markdown: input.partial_markdown,
        final_markdown: input.final_markdown,
        item: input.item,
        timeline_origin: None,
        opaque_meta: input.opaque_meta,
    });
    projection.timeline.push(TimelineEntry {
        id: input.entry_id.clone(),
        turn_id: input.turn_id,
        item_id: input.item_id,
        item_index,
    });
    rows.push(TimelineRow {
        key: input.entry_id,
        kind: TimelineRowKind::Item { timeline_index },
    });
}

fn live_work_by_turn(
    rows: &[SemanticTimelineRow],
) -> HashMap<String, (Option<i64>, TurnWorkState)> {
    rows.iter()
        .filter_map(|row| {
            let SemanticTimelineRowKind::WorkHeader { work, .. } = &row.kind else {
                return None;
            };
            if work.presentation != TurnWorkPresentation::ExpandedLive {
                return None;
            }
            if !matches!(
                work.state,
                TurnWorkState::Starting | TurnWorkState::Running | TurnWorkState::Stalled
            ) {
                return None;
            }
            Some((work.turn_id.clone(), (work.started_at_unix_ms, work.state)))
        })
        .collect()
}

fn semantic_row_turn_id(row: &SemanticTimelineRow) -> Option<&str> {
    match &row.kind {
        SemanticTimelineRowKind::WorkHeader { work, .. } => Some(work.turn_id.as_str()),
        SemanticTimelineRowKind::WorkItem { item } => Some(item.turn_id.as_str()),
        SemanticTimelineRowKind::UserBlock { block }
        | SemanticTimelineRowKind::AssistantMessage { block }
        | SemanticTimelineRowKind::PendingRequest { block }
        | SemanticTimelineRowKind::TurnState { block } => block.turn_id.as_deref(),
    }
}

fn semantic_turn_work_toggle_key(turn_id: &str) -> String {
    format!("{SEMANTIC_TURN_WORK_GROUP_PREFIX}{turn_id}")
}

fn work_item_status(status: TurnWorkItemStatus) -> TimelineEntryStatus {
    match status {
        TurnWorkItemStatus::Running => TimelineEntryStatus::Running,
        TurnWorkItemStatus::Completed => TimelineEntryStatus::Completed,
        TurnWorkItemStatus::Blocked => TimelineEntryStatus::Blocked,
        TurnWorkItemStatus::Failed => TimelineEntryStatus::Failed,
        TurnWorkItemStatus::Cancelled => TimelineEntryStatus::Cancelled,
    }
}

fn is_terminal_work_status(status: TurnWorkItemStatus) -> bool {
    !matches!(status, TurnWorkItemStatus::Running)
}

fn completed_at_for_status(
    status: TurnWorkItemStatus,
    completed_at_unix_ms: Option<i64>,
) -> Option<i64> {
    is_terminal_work_status(status)
        .then_some(completed_at_unix_ms)
        .flatten()
}

fn turn_item_type_label(item_type: TurnItemType) -> &'static str {
    match item_type {
        TurnItemType::UserMessage => "user_message",
        TurnItemType::AgentMessage => "agent_message",
        TurnItemType::Reasoning => "reasoning",
        TurnItemType::SystemEvent => "system_event",
        TurnItemType::Task => "task",
        TurnItemType::CommandExecution => "command_execution",
        TurnItemType::FileChange => "file_change",
        TurnItemType::WebSearch => "web_search",
        TurnItemType::WebFetch => "web_fetch",
        TurnItemType::Download => "download",
        TurnItemType::DynamicToolCall => "dynamic_tool_call",
    }
}

fn turn_item_text_and_markdown(
    item: &TurnItem,
) -> (String, Option<pioneer_protocol::MarkdownDocument>) {
    match item {
        TurnItem::UserMessage { text, .. } => (text.clone(), None),
        TurnItem::AgentMessage { text, markdown, .. } => (text.clone(), markdown.clone()),
        TurnItem::Reasoning {
            summary, content, ..
        } => {
            let text = if content.is_empty() {
                summary.join("\n")
            } else {
                content.join("\n")
            };
            (text, None)
        }
        TurnItem::SystemEvent { message, .. } => (message.clone(), None),
        _ => (String::new(), None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::ConversationViewState;
    use pioneer_protocol::{
        MarkdownDocument, TimelineCursor, Turn, TurnKind, TurnOrigin, TurnPermissionMode,
        TurnPermissionProfileSource, TurnStatus,
    };

    #[test]
    fn assistant_block_preserves_final_markdown_in_shared_projection() {
        let markdown = MarkdownDocument::from_plain_text("final markdown");
        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::AssistantMessage {
                block: assistant_block(Some(markdown.clone())),
            })],
            ConversationViewState::default(),
        );

        assert_eq!(model.rows.len(), 1);
        assert_eq!(model.projection.items.len(), 1);
        assert_eq!(
            model.projection.items[0].final_text.as_deref(),
            Some("final **markdown**")
        );
        assert_eq!(
            model.projection.items[0].partial_markdown,
            Some(markdown.clone())
        );
        assert_eq!(model.projection.items[0].final_markdown, Some(markdown));
    }

    #[test]
    fn work_header_toggle_is_emitted_for_collapsed_work() {
        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::WorkHeader {
                block: work_block(TurnWorkPresentation::CollapsedAfterFinal, 70_000),
                work: work_payload(TurnWorkPresentation::CollapsedAfterFinal, 70_000),
                expanded: false,
                loaded_range: None,
            })],
            ConversationViewState::default(),
        );

        assert_eq!(model.rows.len(), 1);
        assert!(matches!(
            &model.rows[0],
            TimelineRow {
                key,
                kind: TimelineRowKind::TurnWorkToggle(group),
            } if key == "semantic-turn-work-group::turn_a"
                && group.toggle_key == "semantic-turn-work-group::turn_a"
                && !group.is_open
        ));

        let live_model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::WorkHeader {
                block: work_block(TurnWorkPresentation::ExpandedLive, 70_000),
                work: work_payload(TurnWorkPresentation::ExpandedLive, 70_000),
                expanded: true,
                loaded_range: None,
            })],
            ConversationViewState::default(),
        );

        assert!(
            live_model
                .rows
                .iter()
                .all(|row| !matches!(row.kind, TimelineRowKind::TurnWorkToggle(_))),
            "expanded live work should render work rows/running row, not a collapsed toggle"
        );
    }

    #[test]
    fn every_unsuccessful_terminal_state_has_a_system_event() {
        for (state, expected_level, expected_code, expected_message) in [
            (
                TurnWorkState::Failed,
                SystemEventLevel::Error,
                "turn_failed",
                "Turn failed",
            ),
            (
                TurnWorkState::Interrupted,
                SystemEventLevel::Warning,
                "turn_cancelled",
                "Turn cancelled",
            ),
            (
                TurnWorkState::Blocked,
                SystemEventLevel::Warning,
                "turn_blocked",
                "Turn blocked",
            ),
        ] {
            assert_eq!(
                terminal_turn_state_event(state),
                Some((expected_level, expected_code, expected_message))
            );
        }
        assert_eq!(terminal_turn_state_event(TurnWorkState::Completed), None);
    }

    #[test]
    fn turn_state_rows_preserve_running_indicator_while_stalled() {
        for state in [
            TurnWorkState::Starting,
            TurnWorkState::Running,
            TurnWorkState::Stalled,
        ] {
            let model = render_semantic_timeline_rows(
                &[semantic_row(SemanticTimelineRowKind::TurnState {
                    block: turn_state_block(state),
                })],
                ConversationViewState::default(),
            );
            assert_eq!(model.rows.len(), 1, "state {state:?}");
            assert!(matches!(
                &model.rows[0],
                TimelineRow {
                    kind: TimelineRowKind::RunningTurn(display),
                    ..
                } if display.turn_id == "turn_a" && display.state == Some(state)
            ));
        }

        let terminal_model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::TurnState {
                block: turn_state_block(TurnWorkState::Completed),
            })],
            ConversationViewState::default(),
        );
        assert!(terminal_model.rows.is_empty());

        let failed_model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::TurnState {
                block: turn_state_block(TurnWorkState::Failed),
            })],
            ConversationViewState::default(),
        );
        assert!(matches!(
            &failed_model.rows[0],
            TimelineRow {
                kind: TimelineRowKind::Item { timeline_index: 0 },
                ..
            }
        ));
        assert!(matches!(
            &failed_model.projection.items[0].item,
            TurnItem::SystemEvent {
                level: SystemEventLevel::Error,
                message,
                code: Some(code),
                ..
            } if message == "Turn failed" && code == "turn_failed"
        ));
    }

    #[test]
    fn stalled_live_work_keeps_running_row_after_work_items() {
        let mut work = work_payload(TurnWorkPresentation::ExpandedLive, 1);
        work.state = TurnWorkState::Stalled;
        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::WorkHeader {
                block: work_block(TurnWorkPresentation::ExpandedLive, 1),
                work,
                expanded: true,
                loaded_range: None,
            })],
            ConversationViewState::default(),
        );
        assert!(model.rows.iter().any(|row| matches!(
            &row.kind,
            TimelineRowKind::RunningTurn(display)
                if display.turn_id == "turn_a"
                    && display.state == Some(TurnWorkState::Stalled)
        )));
    }

    #[test]
    fn running_turn_display_projects_permission_profile() {
        let permission_profile = pioneer_protocol::compile_turn_permission_profile(
            TurnPermissionMode::AutoAcceptEdits,
            TurnPermissionProfileSource::Composer,
        );
        let mut projection = ConversationViewState::default();
        projection.upsert_turn_snapshot_metadata(&Turn {
            id: "turn_a".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            error: None,
            prompt_manifest: None,
            permission_profile: permission_profile.clone(),
        });

        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::TurnState {
                block: turn_state_block(TurnWorkState::Running),
            })],
            projection,
        );

        assert!(matches!(
            &model.rows[0],
            TimelineRow {
                kind: TimelineRowKind::RunningTurn(display),
                ..
            } if display.permission_profile == Some(permission_profile)
        ));
    }

    #[test]
    fn running_turn_display_projects_security_summary() {
        let security_summary = crate::security::ClientTurnSecuritySummary::from_execution_snapshot(
            &pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access("/repo", 1),
        );
        let mut projection = ConversationViewState::default();
        projection.upsert_turn_security_summary("turn_a", security_summary.clone());

        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::TurnState {
                block: turn_state_block(TurnWorkState::Running),
            })],
            projection,
        );

        assert!(matches!(
            &model.rows[0],
            TimelineRow {
                kind: TimelineRowKind::RunningTurn(display),
                ..
            } if display.security_summary == Some(security_summary)
        ));
    }

    fn semantic_row(kind: SemanticTimelineRowKind) -> SemanticTimelineRow {
        SemanticTimelineRow {
            id: SemanticTimelineRowId::TopLevelBlock {
                block_id: "block".to_owned(),
            },
            kind,
        }
    }

    fn assistant_block(markdown: Option<MarkdownDocument>) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_assistant".to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: "003".to_owned(),
            started_at_unix_ms: Some(3),
            updated_at_unix_ms: Some(3),
            kind: TimelineBlockKind::AssistantMessage {
                item_id: "assistant_final".to_owned(),
                text: "final **markdown**".to_owned(),
                status: TurnWorkItemStatus::Completed,
                markdown,
            },
        }
    }

    fn work_block(presentation: TurnWorkPresentation, work_count: u64) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_work".to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: "002".to_owned(),
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            kind: TimelineBlockKind::TurnWork {
                work: work_payload(presentation, work_count),
            },
        }
    }

    fn work_payload(presentation: TurnWorkPresentation, work_count: u64) -> TurnWorkBlock {
        TurnWorkBlock {
            turn_id: "turn_a".to_owned(),
            presentation,
            state: TurnWorkState::Completed,
            started_at_unix_ms: Some(1),
            completed_at_unix_ms: Some(2),
            elapsed_ms: Some(1),
            work_count,
            visible_work_count: work_count,
            hidden_work_count: 0,
            has_more_before: false,
            has_more_after: work_count > 0,
            before_cursor: None,
            after_cursor: Some(TimelineCursor {
                value: "after-work".to_owned(),
            }),
            first_work_item_id: None,
            last_work_item_id: None,
        }
    }

    fn turn_state_block(state: TurnWorkState) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_state".to_owned(),
            turn_id: Some("turn_a".to_owned()),
            sort_key: "004".to_owned(),
            started_at_unix_ms: Some(4),
            updated_at_unix_ms: Some(4),
            kind: TimelineBlockKind::TurnState {
                state,
                message: None,
            },
        }
    }
}
