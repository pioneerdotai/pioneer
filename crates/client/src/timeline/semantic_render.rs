//! Shared semantic timeline render projection.
//!
//! This is UI-neutral: it converts semantic timeline rows into the same
//! `ConversationViewState` and `TimelineRow` DTOs used by platform renderers.

use crate::{
    conversation::{ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus},
    timeline::{
        labels::RunningTurnDisplay,
        render_fingerprint::timeline_row_content_fingerprint,
        rows::{
            TimelineRow, TimelineRowKind, TurnWorkGroupRow, UserMessagePresentation,
            timeline_reply_state,
        },
        semantic::{SemanticTimelineRow, SemanticTimelineRowId, SemanticTimelineRowKind},
    },
};
use pioneer_protocol::{
    AgentMessagePhase, SystemEventLevel, TaskStatus, TimelineBlock, TimelineBlockKind, TurnItem,
    TurnItemType, TurnWorkBlock, TurnWorkItem, TurnWorkItemStatus, TurnWorkPresentation,
    TurnWorkState,
};
use std::collections::HashMap;

pub const SEMANTIC_TURN_WORK_GROUP_PREFIX: &str = "semantic-turn-work-group::";
const MAX_REPLY_PREVIEW_CHARS: usize = 160;

#[derive(Debug, Clone, Default)]
pub struct SemanticTimelineRenderModel {
    pub projection: ConversationViewState,
    pub rows: Vec<TimelineRow>,
    pub semantic_row_ids: HashMap<String, SemanticTimelineRowId>,
    pub row_render_fingerprints: HashMap<String, u64>,
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
            if let TimelineRowKind::RunningTurn(display) = &render_row.kind {
                inserted_running_rows.insert(display.turn_id.clone());
            }
        }

        let current_turn_id = semantic_row_turn_id(row);
        let next_turn_id = semantic_rows.get(index + 1).and_then(semantic_row_turn_id);
        if let Some(turn_id) = current_turn_id
            && next_turn_id != Some(turn_id)
            && let Some((started_at_unix_ms, state, author, agent_work_graph)) =
                live_work.get(turn_id).cloned()
            && inserted_running_rows.insert(turn_id.to_owned())
        {
            let running_key = format!("semantic-running-turn::{turn_id}");
            rows.push(TimelineRow {
                key: running_key.clone(),
                author,
                kind: TimelineRowKind::RunningTurn(running_turn_display_for_projection(
                    &projection,
                    turn_id,
                    started_at_unix_ms,
                    Some(state),
                    None,
                    None,
                    agent_work_graph,
                )),
            });
            semantic_row_ids.insert(running_key, row.id.clone());
        }
    }

    for row in &mut rows {
        let TimelineRowKind::RunningTurn(display) = &mut row.kind else {
            continue;
        };
        if display.agent_work_graph.is_none() {
            display.agent_work_graph = live_work
                .get(display.turn_id.as_str())
                .and_then(|(_, _, _, graph)| graph.clone());
        }
    }

    let row_render_fingerprints = rows
        .iter()
        .map(|row| {
            (
                row.key.clone(),
                timeline_row_content_fingerprint(&projection, row),
            )
        })
        .collect();

    SemanticTimelineRenderModel {
        projection,
        rows,
        semantic_row_ids,
        row_render_fingerprints,
    }
}

fn push_semantic_row(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    row: &SemanticTimelineRow,
) {
    match &row.kind {
        SemanticTimelineRowKind::UserBlock { block } => {
            push_user_block(projection, rows, block, row.author.clone())
        }
        SemanticTimelineRowKind::WorkHeader {
            block,
            work,
            expanded,
            ..
        } => push_work_header(rows, block, work, *expanded, row.author.clone()),
        SemanticTimelineRowKind::WorkItem { item } => {
            push_work_item(projection, rows, item, row.author.clone())
        }
        SemanticTimelineRowKind::DetachedTaskRun { block } => {
            push_detached_task_run(projection, rows, block, row.author.clone());
        }
        SemanticTimelineRowKind::AssistantMessage { block } => {
            push_assistant_block(projection, rows, block, row.author.clone());
        }
        SemanticTimelineRowKind::PendingRequest { .. } => {}
        SemanticTimelineRowKind::TurnState { block } => {
            push_turn_state(projection, rows, block, row.author.clone())
        }
    }
}

fn push_user_block(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
    row_author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) {
    let TimelineBlockKind::UserMessage {
        item_id,
        text,
        attachments,
        mode,
        author: _,
        route,
        reply,
        mentions,
        revision,
        edited,
        deleted,
        ..
    } = &block.kind
    else {
        return;
    };
    let item_id = item_id.as_deref().unwrap_or(block.block_id.as_str());
    let turn_id = block.turn_id.as_deref().unwrap_or(block.block_id.as_str());
    let visible_text = (!*deleted).then(|| text.clone()).unwrap_or_default();
    let visible_attachments = (!*deleted).then(|| attachments.clone()).unwrap_or_default();
    let item = TurnItem::UserMessage {
        id: item_id.to_owned(),
        text: visible_text.clone(),
        attachments: visible_attachments.clone(),
    };
    let timeline_index = push_item_row(
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
            partial_text: visible_text.clone(),
            final_text: Some(visible_text),
            partial_markdown: None,
            final_markdown: None,
            item,
            author: row_author.clone(),
            route: route.clone(),
            opaque_meta: None,
        },
    );
    rows.last_mut().expect("user item row was appended").kind = TimelineRowKind::UserMessage {
        timeline_index,
        presentation: UserMessagePresentation {
            workspace_id: block.workspace_id.clone(),
            thread_id: block.thread_id.clone(),
            block_id: block.block_id.clone(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            mode: *mode,
            author: row_author,
            route: route.clone(),
            reply: reply.clone().map(bound_reply_summary),
            reply_state: reply.as_ref().map(timeline_reply_state),
            mentions: (!*deleted).then(|| mentions.clone()).unwrap_or_default(),
            attachments: visible_attachments,
            revision: *revision,
            edited: *edited,
            deleted: *deleted,
        },
    };
}

fn bound_reply_summary(
    mut reply: pioneer_protocol::TimelineReplySummary,
) -> pioneer_protocol::TimelineReplySummary {
    if let Some(text) = reply.text.as_mut() {
        let truncated = text
            .chars()
            .take(MAX_REPLY_PREVIEW_CHARS)
            .collect::<String>();
        *text = truncated;
    }
    reply
}

fn push_assistant_block(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) {
    let TimelineBlockKind::AssistantMessage {
        item_id,
        text,
        status,
        markdown,
        route,
        ..
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
            author,
            route: route.clone(),
            opaque_meta: None,
        },
    );
}

fn push_work_header(
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
    work: &TurnWorkBlock,
    expanded: bool,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) {
    if work.presentation == TurnWorkPresentation::ExpandedLive || work.work_count == 0 {
        return;
    }
    let toggle_key = semantic_turn_work_toggle_key(work.turn_id.as_str());
    rows.push(TimelineRow {
        key: toggle_key.clone(),
        author,
        kind: TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow {
            toggle_key,
            anchor_entry_id: block.block_id.clone(),
            elapsed_ms: work.elapsed_ms,
            is_open: expanded,
            state: Some(work.state.clone()),
        }),
    });
}

fn push_work_item(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    item: &TurnWorkItem,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
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
            author,
            route: None,
            opaque_meta: item.metadata.clone(),
        },
    );
}

fn push_detached_task_run(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) {
    let TimelineBlockKind::DetachedTaskRun { task, .. } = &block.kind else {
        return;
    };
    let turn_id = block.turn_id.as_deref().unwrap_or(block.block_id.as_str());
    let status = task_timeline_entry_status(task.status);
    let terminal = task.status.is_terminal();
    push_item_row(
        projection,
        rows,
        ItemRowInput {
            entry_id: block.block_id.clone(),
            item_id: task.id.clone(),
            turn_id: turn_id.to_owned(),
            item_type: "task".to_owned(),
            status,
            started_at_unix_ms: task
                .started_at
                .map(|started_at| started_at.saturating_mul(1_000))
                .or(block.started_at_unix_ms)
                .or(Some(task.created_at.saturating_mul(1_000))),
            updated_at_unix_ms: block.updated_at_unix_ms.or(Some(task.updated_at)),
            completed_at_unix_ms: terminal
                .then(|| block.updated_at_unix_ms.or(Some(task.updated_at)))
                .flatten(),
            partial_text: task.title.clone(),
            final_text: terminal.then(|| task.title.clone()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::Task { item: task.clone() },
            author,
            route: None,
            opaque_meta: Some(serde_json::json!({
                "attachment": "detached",
                "taskId": task.task_id,
                "runId": task.run_id,
            })),
        },
    );
}

fn push_turn_state(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    block: &TimelineBlock,
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
) {
    let TimelineBlockKind::TurnState {
        state,
        message,
        route,
        ..
    } = &block.kind
    else {
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
            author,
            kind: TimelineRowKind::RunningTurn(running_turn_display_for_projection(
                projection,
                turn_id,
                block.started_at_unix_ms.or(block.updated_at_unix_ms),
                Some(*state),
                message.clone(),
                route.clone(),
                None,
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
            author,
            route: route.clone(),
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
    route: Option<pioneer_protocol::SafeRouteProvenance>,
    agent_work_graph: Option<pioneer_protocol::AgentWorkGraphProjection>,
) -> RunningTurnDisplay {
    RunningTurnDisplay {
        turn_id: turn_id.to_owned(),
        started_at_unix_ms,
        state,
        message,
        route,
        agent_work_graph,
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
    author: Option<pioneer_protocol::TurnAuthorSnapshot>,
    route: Option<pioneer_protocol::SafeRouteProvenance>,
    opaque_meta: Option<serde_json::Value>,
}

fn push_item_row(
    projection: &mut ConversationViewState,
    rows: &mut Vec<TimelineRow>,
    input: ItemRowInput,
) -> usize {
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
        route: input.route,
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
        author: input.author,
        kind: TimelineRowKind::Item { timeline_index },
    });
    timeline_index
}

fn live_work_by_turn(
    rows: &[SemanticTimelineRow],
) -> HashMap<
    String,
    (
        Option<i64>,
        TurnWorkState,
        Option<pioneer_protocol::TurnAuthorSnapshot>,
        Option<pioneer_protocol::AgentWorkGraphProjection>,
    ),
> {
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
            Some((
                work.turn_id.clone(),
                (
                    work.started_at_unix_ms,
                    work.state,
                    row.author.clone(),
                    work.agent_work_graph.clone(),
                ),
            ))
        })
        .collect()
}

fn semantic_row_turn_id(row: &SemanticTimelineRow) -> Option<&str> {
    match &row.kind {
        SemanticTimelineRowKind::WorkHeader { work, .. } => Some(work.turn_id.as_str()),
        SemanticTimelineRowKind::WorkItem { item } => Some(item.turn_id.as_str()),
        SemanticTimelineRowKind::UserBlock { block }
        | SemanticTimelineRowKind::DetachedTaskRun { block }
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

fn task_timeline_entry_status(status: TaskStatus) -> TimelineEntryStatus {
    match status {
        TaskStatus::Completed => TimelineEntryStatus::Completed,
        TaskStatus::Blocked => TimelineEntryStatus::Blocked,
        TaskStatus::Failed => TimelineEntryStatus::Failed,
        TaskStatus::Cancelled => TimelineEntryStatus::Cancelled,
        TaskStatus::Draft
        | TaskStatus::Scheduled
        | TaskStatus::Queued
        | TaskStatus::Running
        | TaskStatus::Waiting
        | TaskStatus::WaitingReview => TimelineEntryStatus::Running,
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
        MarkdownDocument, PersistedActorRef, PrincipalId, TaskAttachmentMode, TaskExecutorKind,
        TaskTriggerKind, TaskTurnItem, ThreadMode, TimelineCursor, TimelineReplySummary, Turn,
        TurnAuthorSnapshot, TurnKind, TurnMention, TurnOrigin, TurnPermissionMode,
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
        assert_eq!(model.row_render_fingerprints.len(), 1);
        assert!(
            model
                .row_render_fingerprints
                .contains_key(model.rows[0].key.as_str())
        );
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
    fn user_block_projects_authoritative_collaboration_metadata_and_exact_refs() {
        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::UserBlock {
                block: user_message_block(false, "r".repeat(200)),
            })],
            ConversationViewState::default(),
        );

        let TimelineRowKind::UserMessage {
            timeline_index,
            presentation,
        } = &model.rows[0].kind
        else {
            panic!("expected authored user-message row");
        };
        assert_eq!(*timeline_index, 0);
        assert_eq!(presentation.workspace_id, "workspace_a");
        assert_eq!(presentation.thread_id, "thread_a");
        assert_eq!(presentation.block_id, "block_user");
        assert_eq!(presentation.turn_id, "turn_user");
        assert_eq!(presentation.item_id, "item_user");
        assert_eq!(presentation.mode, ThreadMode::Message);
        assert_eq!(presentation.author.as_ref().unwrap().display_name, "Alice");
        assert_eq!(presentation.mentions[0].nickname, "bob");
        assert_eq!(
            presentation
                .reply
                .as_ref()
                .unwrap()
                .text
                .as_ref()
                .unwrap()
                .chars()
                .count(),
            MAX_REPLY_PREVIEW_CHARS
        );
        assert_eq!(presentation.revision, 4);
        assert_eq!(
            presentation.reply_state,
            Some(crate::timeline::rows::TimelineReplyState::Available)
        );
        assert!(presentation.edited);
        assert!(!presentation.deleted);
    }

    #[test]
    fn deleted_user_block_redacts_body_attachments_and_mentions() {
        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::UserBlock {
                block: user_message_block(true, "reply".to_owned()),
            })],
            ConversationViewState::default(),
        );

        let TimelineRowKind::UserMessage { presentation, .. } = &model.rows[0].kind else {
            panic!("expected authored user-message row");
        };
        assert!(presentation.deleted);
        assert!(presentation.attachments.is_empty());
        assert!(presentation.mentions.is_empty());
        assert_eq!(model.projection.items[0].final_text.as_deref(), Some(""));
    }

    #[test]
    fn detached_task_run_renders_as_a_standalone_task_card() {
        let model = render_semantic_timeline_rows(
            &[semantic_row(SemanticTimelineRowKind::DetachedTaskRun {
                block: detached_task_run_block(TaskStatus::Running),
            })],
            ConversationViewState::default(),
        );

        assert_eq!(model.rows.len(), 1);
        assert!(matches!(
            &model.rows[0],
            TimelineRow {
                kind: TimelineRowKind::Item { timeline_index: 0 },
                ..
            }
        ));
        assert_eq!(model.projection.items.len(), 1);
        assert_eq!(
            model.projection.items[0].status,
            TimelineEntryStatus::Running
        );
        assert_eq!(model.projection.items[0].started_at_unix_ms, Some(2_000));
        assert!(matches!(
            &model.projection.items[0].item,
            TurnItem::Task { item }
                if item.attachment == TaskAttachmentMode::Detached
                    && item.progress_preview.as_deref() == Some("Collecting sources")
        ));
        assert!(
            model
                .rows
                .iter()
                .all(|row| !matches!(row.kind, TimelineRowKind::TurnWorkToggle(_)))
        );
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
                ..
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
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
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
            author: None,
            kind,
        }
    }

    fn user_message_block(deleted: bool, reply_text: String) -> TimelineBlock {
        let alice = PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("principal id");
        let bob = PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").expect("principal id");
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_user".to_owned(),
            turn_id: Some("turn_user".to_owned()),
            sort_key: "001".to_owned(),
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            kind: TimelineBlockKind::UserMessage {
                item_id: Some("item_user".to_owned()),
                inputs: Vec::new(),
                text: "hello @bob".to_owned(),
                attachments: Vec::new(),
                mode: ThreadMode::Message,
                author: Some(TurnAuthorSnapshot {
                    actor: PersistedActorRef::Principal(alice.clone()),
                    display_name: "Alice".to_owned(),
                    nickname: "alice".to_owned(),
                    avatar_revision: Some("avatar-4".to_owned()),
                    agent: None,
                }),
                route: None,
                reply: Some(TimelineReplySummary {
                    turn_id: "turn_parent".to_owned(),
                    author: None,
                    text: Some(reply_text),
                    deleted: false,
                }),
                mentions: vec![TurnMention {
                    principal_id: bob,
                    nickname: "bob".to_owned(),
                }],
                revision: 4,
                edited: true,
                deleted,
            },
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
                author: None,
                route: None,
            },
        }
    }

    fn detached_task_run_block(status: TaskStatus) -> TimelineBlock {
        TimelineBlock {
            workspace_id: "workspace_a".to_owned(),
            thread_id: "thread_a".to_owned(),
            block_id: "block_detached_task".to_owned(),
            turn_id: Some("task_turn_a".to_owned()),
            sort_key: "002".to_owned(),
            started_at_unix_ms: Some(2),
            updated_at_unix_ms: Some(3),
            kind: TimelineBlockKind::DetachedTaskRun {
                task: TaskTurnItem {
                    id: "task_anchor_a".to_owned(),
                    task_id: "task_a".to_owned(),
                    created_by_turn_id: None,
                    run_id: Some("run_a".to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Background analysis".to_owned(),
                    status,
                    attachment: TaskAttachmentMode::Detached,
                    trigger_kind: TaskTriggerKind::Immediate,
                    executor_kind: TaskExecutorKind::Agent,
                    child_thread_id: Some("child_a".to_owned()),
                    child_turn_id: Some("child_turn_a".to_owned()),
                    agent_role: None,
                    depth: 0,
                    max_depth: 3,
                    next_fire_at: None,
                    progress_preview: Some("Collecting sources".to_owned()),
                    result_preview: None,
                    error_preview: None,
                    started_at: (!matches!(
                        status,
                        TaskStatus::Draft | TaskStatus::Scheduled | TaskStatus::Queued
                    ))
                    .then_some(2),
                    created_at: 2,
                    updated_at: 3,
                },
                author: None,
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
            agent_work_graph: None,
            author: None,
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
                author: None,
                route: None,
            },
        }
    }
}
