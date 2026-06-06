pub(super) use pioneer_client::timeline::labels::coalesced_tools_label;
pub(super) use pioneer_client::timeline::layout_hash::{
    timeline_row_layout_hash, timeline_row_text_len, timeline_row_toggle_key,
    timeline_rows_layout_hash,
};
pub(crate) use pioneer_client::timeline::rows::{
    TimelineCoalescedToolsRow, TimelineRow, TimelineRowKind, TurnWorkGroupRow, build_timeline_rows,
};

#[cfg(test)]
pub(super) use pioneer_client::timeline::rows::timeline_turn_work_group_key;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::conversation::{
        Conversation, ConversationViewState, ItemView, TimelineEntry, TimelineEntryStatus,
        TurnPhase, TurnView,
    };
    use pioneer_protocol::{
        SystemEventLevel, TaskEvent, TaskEventPayload, TaskExecutorKind, TaskStatus,
        TaskTriggerKind, TaskTurnItem, ThreadHistoryEvent, ThreadHistoryEventPayload, TimelineItem,
        TimelineLane, TimelineOrigin, TimelineOriginKind, TimelinePayload, ToolCallStatus,
        ToolDisplayPayload, ToolOutputPolicySnapshot, ToolStoragePayload, Turn, TurnItem,
        TurnStatus, TurnTimelineResponse,
    };
    use std::collections::HashSet;

    fn timeline_entry(id: &str, turn_id: &str, item_id: &str, item_index: usize) -> TimelineEntry {
        TimelineEntry {
            id: id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            item_index,
        }
    }

    fn task_anchor_item(
        id: &str,
        task_id: &str,
        turn_id: &str,
        run_id: &str,
        trigger_kind: TaskTriggerKind,
    ) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_type: "task".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(2),
            completed_at_unix_ms: Some(2),
            partial_text: "Task".to_owned(),
            final_text: Some("Task".to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::Task {
                item: TaskTurnItem {
                    id: id.to_owned(),
                    task_id: task_id.to_owned(),
                    run_id: Some(run_id.to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Task".to_owned(),
                    status: TaskStatus::Completed,
                    trigger_kind,
                    executor_kind: TaskExecutorKind::Agent,
                    child_thread_id: None,
                    child_turn_id: None,
                    agent_role: None,
                    depth: 0,
                    max_depth: 3,
                    next_fire_at: None,
                    result_preview: None,
                    error_preview: None,
                    created_at: 1,
                    updated_at: 2,
                },
            },
            timeline_origin: None,
            opaque_meta: None,
        }
    }

    fn task_timeline_origin(task_id: &str, run_id: &str) -> pioneer_protocol::TimelineOrigin {
        pioneer_protocol::TimelineOrigin {
            kind: pioneer_protocol::TimelineOriginKind::ChildTurn,
            task_id: Some(task_id.to_owned()),
            run_id: Some(run_id.to_owned()),
            child_thread_id: Some("child_thread".to_owned()),
            child_turn_id: Some("child_turn".to_owned()),
            origin_event_id: None,
            origin_turn_item_id: None,
            origin_sequence: 1,
            occurred_at: 1,
            lane: pioneer_protocol::TimelineLane::ChildTool,
        }
    }

    fn task_child_tool(id: &str, task_id: &str, run_id: &str) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: run_id.to_owned(),
            item_type: "dynamic_tool_call".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(2),
            updated_at_unix_ms: Some(3),
            completed_at_unix_ms: Some(3),
            partial_text: "tool".to_owned(),
            final_text: Some("tool".to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::DynamicToolCall {
                id: id.to_owned(),
                tool_name: "read_file".to_owned(),
                arguments: serde_json::json!({"path": "file"}),
                status: ToolCallStatus::Completed,
                recovery_policy: None,
                output_policy: ToolOutputPolicySnapshot::for_tool_name("read_file"),
                display: ToolDisplayPayload::Hidden,
                storage: ToolStoragePayload::None,
                recovery: None,
                success: Some(true),
                outcome: None,
                observation: None,
            },
            timeline_origin: Some(task_timeline_origin(task_id, run_id)),
            opaque_meta: None,
        }
    }

    fn task_child_agent(id: &str, task_id: &str, run_id: &str) -> ItemView {
        ItemView {
            id: id.to_owned(),
            turn_id: run_id.to_owned(),
            item_type: "agent_message".to_owned(),
            status: TimelineEntryStatus::Completed,
            started_at_unix_ms: Some(3),
            updated_at_unix_ms: Some(4),
            completed_at_unix_ms: Some(4),
            partial_text: "Child result".to_owned(),
            final_text: Some("Child result".to_owned()),
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::AgentMessage {
                id: id.to_owned(),
                text: "Child result".to_owned(),
                markdown: None,
                markdown_version: None,
            },
            timeline_origin: Some(task_timeline_origin(task_id, run_id)),
            opaque_meta: None,
        }
    }

    fn visible_item_ids<'a>(
        projection: &'a ConversationViewState,
        rows: &[TimelineRow],
    ) -> Vec<&'a str> {
        rows.iter()
            .filter_map(|row| match row.kind {
                TimelineRowKind::Item { timeline_index } => projection
                    .timeline
                    .get(timeline_index)
                    .map(|entry| entry.item_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn completed_parent_agent_message_collapses_turn_work_without_turn_metadata() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_task_list", "turn_parent", "task_list_1", 1),
                timeline_entry("entry_task_cancel", "turn_parent", "task_cancel_1", 2),
                timeline_entry(
                    "entry_task_cancelled",
                    "turn_task_event",
                    "task_cancelled_1",
                    3,
                ),
                timeline_entry("entry_parent_agent", "turn_parent", "parent_agent_1", 4),
            ],
            items: vec![
                ItemView {
                    id: "user_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "user_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(1),
                    completed_at_unix_ms: Some(1),
                    partial_text: "Cancel this task".to_owned(),
                    final_text: Some("Cancel this task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Cancel this task".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_list_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "task_list".to_owned(),
                    final_text: Some("task_list".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "task_list_1".to_owned(),
                        tool_name: "task_list".to_owned(),
                        arguments: serde_json::json!({"ownerKind": "thread"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_cancel_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "task_cancel".to_owned(),
                    final_text: Some("task_cancel".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "task_cancel_1".to_owned(),
                        tool_name: "task_cancel".to_owned(),
                        arguments: serde_json::json!({"taskId": "task_1"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_cancel"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_cancelled_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "system_event".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(4),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Task cancelled".to_owned(),
                    final_text: Some("Task cancelled".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::SystemEvent {
                        id: "task_cancelled_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Task cancelled".to_owned(),
                        code: Some(pioneer_protocol::constants::events::TASK_CANCELLED.to_owned()),
                        details: None,
                    },
                    timeline_origin: Some(pioneer_protocol::TimelineOrigin {
                        kind: pioneer_protocol::TimelineOriginKind::TaskEvent,
                        task_id: Some("task_1".to_owned()),
                        run_id: None,
                        child_thread_id: None,
                        child_turn_id: None,
                        origin_event_id: Some("task_event_1".to_owned()),
                        origin_turn_item_id: None,
                        origin_sequence: 1,
                        occurred_at: 4,
                        lane: pioneer_protocol::TimelineLane::Task,
                    }),
                    opaque_meta: None,
                },
                ItemView {
                    id: "parent_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(5),
                    updated_at_unix_ms: Some(5),
                    completed_at_unix_ms: Some(5),
                    partial_text: "Task cancelled.".to_owned(),
                    final_text: Some("Task cancelled.".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "parent_agent_1".to_owned(),
                        text: "Task cancelled.".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
            ],
            turns: Vec::new(),
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["user_1", "parent_agent_1"]
        );
        assert!(collapsed_rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec![
                "user_1",
                "task_list_1",
                "task_cancel_1",
                "task_cancelled_1",
                "parent_agent_1"
            ]
        );
    }

    #[test]
    fn completed_parent_work_collapses_when_other_turn_items_are_interleaved() {
        let mut projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_user", "turn_parent", "user_1", 0),
                timeline_entry("entry_other_user", "turn_other", "other_user", 1),
                timeline_entry("entry_task_list", "turn_parent", "task_list_1", 2),
                timeline_entry("entry_other_agent", "turn_other", "other_agent", 3),
                timeline_entry("entry_parent_agent", "turn_parent", "parent_agent_1", 4),
            ],
            items: vec![
                ItemView {
                    id: "user_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "user_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(1),
                    completed_at_unix_ms: Some(1),
                    partial_text: "List tasks".to_owned(),
                    final_text: Some("List tasks".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "List tasks".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "other_user".to_owned(),
                    turn_id: "turn_other".to_owned(),
                    item_type: "user_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "Other".to_owned(),
                    final_text: Some("Other".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "other_user".to_owned(),
                        text: "Other".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_list_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "task_list".to_owned(),
                    final_text: Some("task_list".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "task_list_1".to_owned(),
                        tool_name: "task_list".to_owned(),
                        arguments: serde_json::json!({"ownerKind": "thread"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "other_agent".to_owned(),
                    turn_id: "turn_other".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(4),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Other done".to_owned(),
                    final_text: Some("Other done".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "other_agent".to_owned(),
                        text: "Other done".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "parent_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(5),
                    updated_at_unix_ms: Some(5),
                    completed_at_unix_ms: Some(5),
                    partial_text: "Done".to_owned(),
                    final_text: Some("Done".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "parent_agent_1".to_owned(),
                        text: "Done".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
            ],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &rows),
            vec!["user_1", "other_user", "other_agent", "parent_agent_1"]
        );
        assert!(rows.iter().any(|row| {
            matches!(
                row.kind,
                TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
            )
        }));

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_user")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec![
                "user_1",
                "task_list_1",
                "other_user",
                "other_agent",
                "parent_agent_1"
            ]
        );

        projection.timeline.swap(1, 2);
        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &rows),
            vec!["user_1", "other_user", "other_agent", "parent_agent_1"]
        );
    }

    #[test]
    fn hydrated_cancel_turn_with_composed_task_cancelled_event_collapses_work() {
        let workspace_id = "ws_000000000000000001";
        let thread_id = "thread_cancel";
        let turn_id = "turn_cancel";

        let task_list = TurnItem::DynamicToolCall {
            id: "task_list_call".to_owned(),
            tool_name: "task_list".to_owned(),
            arguments: serde_json::json!({"ownerKind": "thread"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("task_list"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::None,
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let task_cancel = TurnItem::DynamicToolCall {
            id: "task_cancel_call".to_owned(),
            tool_name: "task_cancel".to_owned(),
            arguments: serde_json::json!({"taskId": "task_1"}),
            status: ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("task_cancel"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::None,
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };

        let mut conversation = Conversation::new(thread_id);
        conversation.hydrate_history(&[
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 1,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::TurnStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        id: turn_id.to_owned(),
                        status: TurnStatus::InProgress,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        error: None,
                        prompt_manifest: None,
                    },
                    input: Vec::new(),
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 2,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::UserMessage {
                        id: "user_cancel".to_owned(),
                        text: "Cancel this task".to_owned(),
                        attachments: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 3,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 4,
                created_at: 1_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::UserMessage {
                        id: "user_cancel".to_owned(),
                        text: "Cancel this task".to_owned(),
                        attachments: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 5,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_1".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 6,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_list.clone(),
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 7,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_list,
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 8,
                created_at: 10_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_2".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 9,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_2".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 10,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_cancel.clone(),
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 11,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: task_cancel,
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 12,
                created_at: 17_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_3".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 13,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: "reasoning_3".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 14,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::ItemStarted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_final".to_owned(),
                        text: String::new(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 15,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::ItemCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: "agent_final".to_owned(),
                        text: "Task cancelled.".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
            ThreadHistoryEvent {
                turn_id: turn_id.to_owned(),
                sequence: 16,
                created_at: 23_000,
                payload: ThreadHistoryEventPayload::TurnCompleted {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn: Turn {
                        id: turn_id.to_owned(),
                        status: TurnStatus::Completed,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        error: None,
                        prompt_manifest: None,
                    },
                },
            },
        ]);

        conversation.apply_composed_turn_timeline(&TurnTimelineResponse {
            thread_id: thread_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
            turn_id: turn_id.to_owned(),
            last_sequence: 1,
            items: vec![TimelineItem {
                id: "task:task_1:1".to_owned(),
                origin: TimelineOrigin {
                    kind: TimelineOriginKind::TaskEvent,
                    task_id: Some("task_1".to_owned()),
                    run_id: None,
                    child_thread_id: None,
                    child_turn_id: None,
                    origin_event_id: Some("task_event_cancelled".to_owned()),
                    origin_turn_item_id: None,
                    origin_sequence: 1,
                    occurred_at: 17_000,
                    lane: TimelineLane::Task,
                },
                payload: TimelinePayload::TaskEvent {
                    event: TaskEvent {
                        id: "task_event_cancelled".to_owned(),
                        task_id: "task_1".to_owned(),
                        run_id: None,
                        thread_id: None,
                        turn_id: None,
                        sequence: 1,
                        event_type: pioneer_protocol::constants::events::TASK_CANCELLED.to_owned(),
                        idempotency_key: None,
                        payload: TaskEventPayload::TaskCancelled {
                            task_id: "task_1".to_owned(),
                            reason: Some("User requested cancellation".to_owned()),
                            completed_at: 17,
                        },
                        created_at: 17,
                    },
                },
            }],
        });

        let rows = build_timeline_rows(conversation.projection(), &HashSet::new());
        assert_eq!(
            visible_item_ids(conversation.projection(), &rows),
            vec!["user_cancel", "agent_final"]
        );
        assert!(rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));
    }

    #[test]
    fn completed_task_group_collapses_members_by_default() {
        let projection = ConversationViewState {
            timeline: vec![
                TimelineEntry {
                    id: "entry_task".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "task_anchor".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_task_event".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "task_event_1".to_owned(),
                    item_index: 1,
                },
                TimelineEntry {
                    id: "entry_child_tool".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "child_tool_1".to_owned(),
                    item_index: 2,
                },
            ],
            items: vec![
                ItemView {
                    id: "task_anchor".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor".to_owned(),
                            task_id: "task_1".to_owned(),
                            run_id: Some("run_1".to_owned()),
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task".to_owned(),
                            status: TaskStatus::Completed,
                            trigger_kind: TaskTriggerKind::Immediate,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: None,
                            child_turn_id: None,
                            agent_role: None,
                            depth: 0,
                            max_depth: 3,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 1,
                            updated_at: 3,
                        },
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_event_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "system_event".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "Task run started".to_owned(),
                    final_text: Some("Task run started".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::SystemEvent {
                        id: "task_event_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Task run started".to_owned(),
                        code: Some(
                            pioneer_protocol::constants::events::TASK_RUN_STARTED.to_owned(),
                        ),
                        details: None,
                    },
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
                },
                ItemView {
                    id: "child_tool_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "system_event".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "Child tool".to_owned(),
                    final_text: Some("Child tool".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::SystemEvent {
                        id: "child_tool_1".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "Child tool".to_owned(),
                        code: None,
                        details: None,
                    },
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
                },
            ],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(rows.len(), 2);
        assert!(matches!(
            rows[0].kind,
            TimelineRowKind::Item { timeline_index: 0 }
        ));
        assert!(matches!(rows[1].kind, TimelineRowKind::TurnWorkToggle(_)));
    }

    #[test]
    fn running_task_group_coalesces_older_completed_child_tools() {
        let mut timeline = Vec::new();
        let mut items = Vec::new();
        timeline.push(TimelineEntry {
            id: "entry_task".to_owned(),
            turn_id: "turn_parent".to_owned(),
            item_id: "task_anchor".to_owned(),
            item_index: 0,
        });
        items.push(ItemView {
            id: "task_anchor".to_owned(),
            turn_id: "turn_parent".to_owned(),
            item_type: "task".to_owned(),
            status: TimelineEntryStatus::Running,
            started_at_unix_ms: Some(1),
            updated_at_unix_ms: Some(1),
            completed_at_unix_ms: None,
            partial_text: "Task".to_owned(),
            final_text: None,
            partial_markdown: None,
            final_markdown: None,
            item: TurnItem::Task {
                item: TaskTurnItem {
                    id: "task_anchor".to_owned(),
                    task_id: "task_1".to_owned(),
                    run_id: Some("run_1".to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Task".to_owned(),
                    status: TaskStatus::Running,
                    trigger_kind: TaskTriggerKind::Immediate,
                    executor_kind: TaskExecutorKind::Agent,
                    child_thread_id: None,
                    child_turn_id: None,
                    agent_role: None,
                    depth: 0,
                    max_depth: 3,
                    next_fire_at: None,
                    result_preview: None,
                    error_preview: None,
                    created_at: 1,
                    updated_at: 1,
                },
            },
            timeline_origin: None,
            opaque_meta: None,
        });

        for index in 0..5 {
            let id = format!("tool_{index}");
            timeline.push(TimelineEntry {
                id: format!("entry_{id}"),
                turn_id: "turn_parent".to_owned(),
                item_id: id.clone(),
                item_index: index + 1,
            });
            items.push(ItemView {
                id: id.clone(),
                turn_id: "turn_parent".to_owned(),
                item_type: "dynamic_tool_call".to_owned(),
                status: TimelineEntryStatus::Completed,
                started_at_unix_ms: Some(2 + index as i64),
                updated_at_unix_ms: Some(2 + index as i64),
                completed_at_unix_ms: Some(2 + index as i64),
                partial_text: id.clone(),
                final_text: Some(id.clone()),
                partial_markdown: None,
                final_markdown: None,
                item: TurnItem::DynamicToolCall {
                    id: id.clone(),
                    tool_name: "grep_files".to_owned(),
                    arguments: serde_json::json!({"pattern": "Task"}),
                    status: ToolCallStatus::Completed,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("grep_files"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::None,
                    recovery: None,
                    success: Some(true),
                    outcome: None,
                    observation: None,
                },
                timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                opaque_meta: None,
            });
        }

        let projection = ConversationViewState {
            timeline,
            items,
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(rows.len(), 2);
        assert!(matches!(
            rows[0].kind,
            TimelineRowKind::Item { timeline_index: 0 }
        ));
        assert!(matches!(rows[1].kind, TimelineRowKind::TurnWorkToggle(_)));

        let rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task")]),
        );
        assert!(rows.iter().any(|row| {
            matches!(
                row.kind,
                TimelineRowKind::CoalescedTools(TimelineCoalescedToolsRow { count: 3, .. })
            )
        }));
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(row.kind, TimelineRowKind::Item { .. }))
                .count(),
            3
        );
    }

    #[test]
    fn recurring_task_runs_with_same_task_id_use_distinct_work_groups() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_task_run_1", "run_1", "task_anchor_run_1", 0),
                timeline_entry("entry_child_tool_run_1", "run_1", "child_tool_run_1", 1),
                timeline_entry("entry_child_agent_run_1", "run_1", "child_agent_run_1", 2),
                timeline_entry("entry_task_run_2", "run_2", "task_anchor_run_2", 0),
                timeline_entry("entry_child_tool_run_2", "run_2", "child_tool_run_2", 1),
                timeline_entry("entry_child_agent_run_2", "run_2", "child_agent_run_2", 2),
            ],
            items: vec![
                task_anchor_item(
                    "task_anchor_run_1",
                    "task_1",
                    "run_1",
                    "run_1",
                    TaskTriggerKind::Immediate,
                ),
                task_child_tool("child_tool_run_1", "task_1", "run_1"),
                task_child_agent("child_agent_run_1", "task_1", "run_1"),
                task_anchor_item(
                    "task_anchor_run_2",
                    "task_1",
                    "run_2",
                    "run_2",
                    TaskTriggerKind::Immediate,
                ),
                task_child_tool("child_tool_run_2", "task_1", "run_2"),
                task_child_agent("child_agent_run_2", "task_1", "run_2"),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["task_anchor_run_1", "task_anchor_run_2"]
        );
        assert_eq!(
            collapsed_rows
                .iter()
                .filter(|row| matches!(row.kind, TimelineRowKind::TurnWorkToggle(_)))
                .count(),
            2
        );

        let run_1_expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task_run_1")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &run_1_expanded_rows),
            vec![
                "task_anchor_run_1",
                "child_tool_run_1",
                "child_agent_run_1",
                "task_anchor_run_2",
            ]
        );
    }

    #[test]
    fn task_group_members_render_under_anchor_even_when_timeline_order_is_early() {
        let projection = ConversationViewState {
            timeline: vec![
                timeline_entry("entry_child_tool", "run_1", "child_tool_1", 1),
                timeline_entry("entry_task_anchor", "run_1", "task_anchor_run_1", 0),
                timeline_entry("entry_child_agent", "run_1", "child_agent_1", 2),
            ],
            items: vec![
                task_child_tool("child_tool_1", "task_1", "run_1"),
                task_anchor_item(
                    "task_anchor_run_1",
                    "task_1",
                    "run_1",
                    "run_1",
                    TaskTriggerKind::Interval,
                ),
                task_child_agent("child_agent_1", "task_1", "run_1"),
            ],
            ..ConversationViewState::default()
        };

        let collapsed_rows = build_timeline_rows(&projection, &HashSet::new());
        assert_eq!(
            visible_item_ids(&projection, &collapsed_rows),
            vec!["task_anchor_run_1"]
        );

        let expanded_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_task_anchor")]),
        );
        assert_eq!(
            visible_item_ids(&projection, &expanded_rows),
            vec!["task_anchor_run_1", "child_tool_1", "child_agent_1"]
        );
    }

    #[test]
    fn stale_scheduled_task_anchor_does_not_steal_later_run_group() {
        let mut stale_anchor = match task_anchor_item(
            "task_task_1",
            "task_1",
            "run_1",
            "run_2",
            TaskTriggerKind::Interval,
        )
        .item
        {
            TurnItem::Task { item } => item,
            _ => unreachable!(),
        };
        stale_anchor.id = "task_task_1".to_owned();
        stale_anchor.trigger_kind = TaskTriggerKind::Interval;
        let mut run_2_anchor = match task_anchor_item(
            "task_run_run_2",
            "task_1",
            "run_2",
            "run_2",
            TaskTriggerKind::Interval,
        )
        .item
        {
            TurnItem::Task { item } => item,
            _ => unreachable!(),
        };
        run_2_anchor.id = "task_run_run_2".to_owned();
        run_2_anchor.trigger_kind = TaskTriggerKind::Interval;

        let projection = ConversationViewState {
            timeline: vec![
                TimelineEntry {
                    id: "entry_stale_run_1_anchor".to_owned(),
                    turn_id: "run_1".to_owned(),
                    item_id: "task_task_1".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_child_tool_run_1".to_owned(),
                    turn_id: "run_1".to_owned(),
                    item_id: "child_tool_run_1".to_owned(),
                    item_index: 1,
                },
                TimelineEntry {
                    id: "entry_run_2_anchor".to_owned(),
                    turn_id: "run_2".to_owned(),
                    item_id: "task_run_run_2".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_child_tool_run_2".to_owned(),
                    turn_id: "run_2".to_owned(),
                    item_id: "child_tool_run_2".to_owned(),
                    item_index: 1,
                },
            ],
            items: vec![
                ItemView {
                    id: "task_task_1".to_owned(),
                    turn_id: "run_1".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(2),
                    completed_at_unix_ms: Some(2),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task { item: stale_anchor },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                task_child_tool("child_tool_run_1", "task_1", "run_1"),
                ItemView {
                    id: "task_run_run_2".to_owned(),
                    turn_id: "run_2".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task { item: run_2_anchor },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                task_child_tool("child_tool_run_2", "task_1", "run_2"),
            ],
            ..ConversationViewState::default()
        };

        let run_1_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_stale_run_1_anchor")]),
        );
        let run_1_visible = visible_item_ids(&projection, &run_1_rows);
        assert!(run_1_visible.contains(&"child_tool_run_1"));
        assert!(!run_1_visible.contains(&"child_tool_run_2"));

        let run_2_rows = build_timeline_rows(
            &projection,
            &HashSet::from([timeline_turn_work_group_key("entry_run_2_anchor")]),
        );
        let run_2_visible = visible_item_ids(&projection, &run_2_rows);
        assert!(!run_2_visible.contains(&"child_tool_run_1"));
        assert!(run_2_visible.contains(&"child_tool_run_2"));
        assert_eq!(
            run_2_rows
                .iter()
                .filter(|row| matches!(row.kind, TimelineRowKind::TurnWorkToggle(_)))
                .count(),
            2
        );
    }

    #[test]
    fn repeated_task_wait_rows_are_grouped() {
        let mut timeline = Vec::new();
        let mut items = Vec::new();
        for index in 0..3 {
            let id = format!("wait_{index}");
            timeline.push(TimelineEntry {
                id: format!("entry_{id}"),
                turn_id: "turn_parent".to_owned(),
                item_id: id.clone(),
                item_index: index,
            });
            items.push(ItemView {
                id: id.clone(),
                turn_id: "turn_parent".to_owned(),
                item_type: "dynamic_tool_call".to_owned(),
                status: TimelineEntryStatus::Completed,
                started_at_unix_ms: Some(index as i64),
                updated_at_unix_ms: Some(index as i64),
                completed_at_unix_ms: Some(index as i64),
                partial_text: "task_wait".to_owned(),
                final_text: Some("task_wait".to_owned()),
                partial_markdown: None,
                final_markdown: None,
                item: TurnItem::DynamicToolCall {
                    id,
                    tool_name: "task_wait".to_owned(),
                    arguments: serde_json::json!({"taskIds": ["task_1"]}),
                    status: ToolCallStatus::Completed,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::None,
                    recovery: None,
                    success: Some(true),
                    outcome: None,
                    observation: None,
                },
                timeline_origin: None,
                opaque_meta: None,
            });
        }

        let projection = ConversationViewState {
            timeline,
            items,
            ..ConversationViewState::default()
        };
        let rows = build_timeline_rows(&projection, &HashSet::new());

        assert!(rows.iter().any(|row| {
            matches!(
                row.kind,
                TimelineRowKind::CoalescedTools(TimelineCoalescedToolsRow { count: 2, .. })
            )
        }));
    }

    #[test]
    fn child_agent_message_does_not_end_parent_work_group() {
        let projection = ConversationViewState {
            timeline: vec![
                TimelineEntry {
                    id: "entry_user".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "user_1".to_owned(),
                    item_index: 0,
                },
                TimelineEntry {
                    id: "entry_task".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "task_anchor".to_owned(),
                    item_index: 1,
                },
                TimelineEntry {
                    id: "entry_child_agent".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "child_agent_1".to_owned(),
                    item_index: 2,
                },
                TimelineEntry {
                    id: "entry_child_tool".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "child_tool_1".to_owned(),
                    item_index: 3,
                },
                TimelineEntry {
                    id: "entry_parent_agent".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_id: "parent_agent_1".to_owned(),
                    item_index: 4,
                },
            ],
            items: vec![
                ItemView {
                    id: "user_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "user_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(1),
                    updated_at_unix_ms: Some(1),
                    completed_at_unix_ms: Some(1),
                    partial_text: "Run subagent".to_owned(),
                    final_text: Some("Run subagent".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::UserMessage {
                        id: "user_1".to_owned(),
                        text: "Run subagent".to_owned(),
                        attachments: Vec::new(),
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "task_anchor".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "task".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(2),
                    updated_at_unix_ms: Some(4),
                    completed_at_unix_ms: Some(4),
                    partial_text: "Task".to_owned(),
                    final_text: Some("Task".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor".to_owned(),
                            task_id: "task_1".to_owned(),
                            run_id: Some("run_1".to_owned()),
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task".to_owned(),
                            status: TaskStatus::Completed,
                            trigger_kind: TaskTriggerKind::Immediate,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: Some("child_thread_1".to_owned()),
                            child_turn_id: Some("child_turn_1".to_owned()),
                            agent_role: None,
                            depth: 0,
                            max_depth: 3,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 2,
                            updated_at: 4,
                        },
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
                ItemView {
                    id: "child_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "Child result".to_owned(),
                    final_text: Some("Child result".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "child_agent_1".to_owned(),
                        text: "Child result".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
                },
                ItemView {
                    id: "child_tool_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "dynamic_tool_call".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(3),
                    updated_at_unix_ms: Some(3),
                    completed_at_unix_ms: Some(3),
                    partial_text: "grep_files".to_owned(),
                    final_text: Some("grep_files".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::DynamicToolCall {
                        id: "child_tool_1".to_owned(),
                        tool_name: "grep_files".to_owned(),
                        arguments: serde_json::json!({"pattern": "Task"}),
                        status: ToolCallStatus::Completed,
                        recovery_policy: None,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("grep_files"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::None,
                        recovery: None,
                        success: Some(true),
                        outcome: None,
                        observation: None,
                    },
                    timeline_origin: Some(task_timeline_origin("task_1", "run_1")),
                    opaque_meta: None,
                },
                ItemView {
                    id: "parent_agent_1".to_owned(),
                    turn_id: "turn_parent".to_owned(),
                    item_type: "agent_message".to_owned(),
                    status: TimelineEntryStatus::Completed,
                    started_at_unix_ms: Some(5),
                    updated_at_unix_ms: Some(5),
                    completed_at_unix_ms: Some(5),
                    partial_text: "Parent final".to_owned(),
                    final_text: Some("Parent final".to_owned()),
                    partial_markdown: None,
                    final_markdown: None,
                    item: TurnItem::AgentMessage {
                        id: "parent_agent_1".to_owned(),
                        text: "Parent final".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                    timeline_origin: None,
                    opaque_meta: None,
                },
            ],
            turns: vec![TurnView {
                id: "turn_parent".to_owned(),
                phase: TurnPhase::Completed,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(5),
                error: None,
            }],
            ..ConversationViewState::default()
        };

        let rows = build_timeline_rows(&projection, &HashSet::new());
        let visible_item_ids = rows
            .iter()
            .filter_map(|row| match row.kind {
                TimelineRowKind::Item { timeline_index } => projection
                    .timeline
                    .get(timeline_index)
                    .map(|entry| entry.item_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(visible_item_ids, vec!["user_1", "parent_agent_1"]);
        assert!(rows.iter().any(|row| matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(TurnWorkGroupRow { .. })
        )));
    }
}
