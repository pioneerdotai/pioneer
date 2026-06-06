//! Thread history helpers.

use pioneer_protocol::{
    ThreadHistoryEventPayload, ThreadHistoryParams, ThreadHistoryResponse, TurnItem,
    TurnTimelineParams,
};

pub fn thread_history_params(
    thread_id: impl Into<String>,
    limit: Option<u32>,
) -> ThreadHistoryParams {
    ThreadHistoryParams {
        thread_id: thread_id.into(),
        limit,
    }
}

pub fn turn_ids_with_task_anchors(response: &ThreadHistoryResponse) -> Vec<String> {
    let mut turn_ids = Vec::new();
    for event in &response.events {
        let has_task_anchor = match &event.payload {
            ThreadHistoryEventPayload::ItemStarted { item, .. }
            | ThreadHistoryEventPayload::ItemCompleted { item, .. }
            | ThreadHistoryEventPayload::ItemUpdated { item, .. } => {
                matches!(item, TurnItem::Task { .. })
            }
            _ => false,
        };
        if has_task_anchor && !turn_ids.iter().any(|turn_id| turn_id == &event.turn_id) {
            turn_ids.push(event.turn_id.clone());
        }
    }
    turn_ids
}

pub fn composed_task_turn_timeline_params(
    response: &ThreadHistoryResponse,
) -> Vec<TurnTimelineParams> {
    turn_ids_with_task_anchors(response)
        .into_iter()
        .map(|turn_id| composed_task_turn_timeline_param(response.thread_id.clone(), turn_id))
        .collect()
}

pub fn composed_task_turn_timeline_param(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> TurnTimelineParams {
    TurnTimelineParams {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        compose_tasks: true,
        include_collapsed_task_events: false,
        max_child_items_per_task: Some(500),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SystemEventLevel, TaskExecutorKind, TaskStatus, TaskTriggerKind, TaskTurnItem,
        ThreadHistoryEvent, TurnItem,
    };

    fn event(turn_id: &str, item: TurnItem) -> ThreadHistoryEvent {
        ThreadHistoryEvent {
            turn_id: turn_id.to_owned(),
            sequence: 1,
            created_at: 1,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_a".to_owned(),
                thread_id: "thread_a".to_owned(),
                turn_id: turn_id.to_owned(),
                item,
            },
        }
    }

    #[test]
    fn history_turn_timeline_params_are_built_for_unique_task_anchor_turns() {
        let response = ThreadHistoryResponse {
            thread_id: "thread_a".to_owned(),
            workspace_id: "ws_a".to_owned(),
            events: vec![
                event(
                    "turn_1",
                    TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor_1".to_owned(),
                            task_id: "task_1".to_owned(),
                            run_id: None,
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task".to_owned(),
                            status: TaskStatus::Running,
                            trigger_kind: TaskTriggerKind::Manual,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: None,
                            child_turn_id: None,
                            agent_role: None,
                            depth: 0,
                            max_depth: 1,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 1,
                            updated_at: 1,
                        },
                    },
                ),
                event(
                    "turn_1",
                    TurnItem::Task {
                        item: TaskTurnItem {
                            id: "task_anchor_2".to_owned(),
                            task_id: "task_2".to_owned(),
                            run_id: None,
                            parent_task_id: None,
                            root_task_id: None,
                            title: "Task 2".to_owned(),
                            status: TaskStatus::Running,
                            trigger_kind: TaskTriggerKind::Manual,
                            executor_kind: TaskExecutorKind::Agent,
                            child_thread_id: None,
                            child_turn_id: None,
                            agent_role: None,
                            depth: 0,
                            max_depth: 1,
                            next_fire_at: None,
                            result_preview: None,
                            error_preview: None,
                            created_at: 1,
                            updated_at: 1,
                        },
                    },
                ),
                event(
                    "turn_2",
                    TurnItem::SystemEvent {
                        id: "system".to_owned(),
                        level: SystemEventLevel::Info,
                        message: "not a task".to_owned(),
                        code: None,
                        details: None,
                    },
                ),
            ],
        };

        let params = composed_task_turn_timeline_params(&response);

        assert_eq!(params.len(), 1);
        assert_eq!(params[0].thread_id, "thread_a");
        assert_eq!(params[0].turn_id, "turn_1");
        assert!(params[0].compose_tasks);
        assert!(!params[0].include_collapsed_task_events);
        assert_eq!(params[0].max_child_items_per_task, Some(500));
    }
}
