use super::events::EventKind;
use super::{Conversation, ConversationEvent, MAX_EVENT_LOG_LEN, TimelineEntryStatus, TurnPhase};
use pioneer_protocol::{
    ItemDeltaStream, RecoveryAction, RecoveryJobStatus, RecoveryTrigger, TaskEvent,
    TaskEventPayload, TaskExecutorKind, TaskStatus, TaskTriggerKind, TaskTurnItem,
    ThreadHistoryEvent, ThreadHistoryEventPayload, TimelineItem, TimelineLane, TimelineOrigin,
    TimelineOriginKind, TimelinePayload, ToolCallStatus, ToolDisplayPayload, ToolLoopBudgetAction,
    ToolLoopBudgetLimitKind, ToolMetadata, ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode,
    ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass, ToolRetryBudgetKind, ToolRetryBudgetUsage,
    ToolRetryErrorClass, ToolRetryExhaustionKind, ToolRetryResolution, ToolStoragePayload, Turn,
    TurnItem, TurnItemEvent, TurnItemEventPayload, TurnItemTimeoutReason, TurnItemType, TurnStatus,
    TurnTimelineResponse, UserInput,
};

const THREAD_ID: &str = "thr_000000000000000001";
const TURN_ID: &str = "turn_000000000000000001";
const PENDING_REQUEST_ID: &str = "req_000000000000000001";

fn pending_request_id(conversation: &Conversation) -> Option<&str> {
    conversation.state_machine.pending_request_id()
}

fn sample_tool_recovery_policy() -> ToolRecoveryPolicySnapshot {
    ToolRecoveryPolicySnapshot {
        retry_class: ToolRecoveryRetryClass::Network,
        idempotency_mode: ToolRecoveryIdempotencyMode::Safe,
        max_attempts: 3,
        can_resume: true,
        resolved_action: RecoveryAction::RetryWithBackoff,
        base_backoff_secs: 3,
        max_wall_clock_secs: 240,
        no_progress_limit: 3,
    }
}

fn sample_web_fetch_item(
    item_id: &str,
    status: pioneer_protocol::ToolCallStatus,
    recovery_policy: Option<ToolRecoveryPolicySnapshot>,
) -> TurnItem {
    TurnItem::WebFetch {
        id: item_id.to_owned(),
        tool_name: "web_fetch".to_owned(),
        arguments: serde_json::json!({ "url": "https://example.com/policy" }),
        status,
        recovery_policy,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(serde_json::json!({
                "url": "https://example.com/policy"
            })),
        },
        recovery: None,
        url: Some("https://example.com/policy".to_owned()),
        final_url: None,
        status_code: None,
        content_type: None,
        extract_mode: None,
        resolved_mode: None,
        bytes_received: None,
        elapsed_ms: None,
        truncated: None,
        title: None,
        word_count: None,
        links: Vec::new(),
        success: None,
        outcome: None,
        observation: None,
    }
}

#[test]
fn send_stays_blocked_until_terminal_event() {
    let mut conversation = Conversation::new(THREAD_ID);

    assert!(conversation.can_submit_message());

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
    });
    assert!(!conversation.can_submit_message());
    assert_eq!(pending_request_id(&conversation), Some(PENDING_REQUEST_ID));

    conversation.apply(ConversationEvent::LocalTurnStartAccepted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
    });
    assert!(!conversation.can_submit_message());

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            error: None,

            prompt_manifest: None,
        },
    });
    assert!(!conversation.can_submit_message());

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            error: None,

            prompt_manifest: None,
        },
    });

    assert!(!conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());
    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "completed");
}

#[test]
fn send_unlocks_only_on_terminal_failed_or_cancelled() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
    });

    conversation.apply(ConversationEvent::TurnFailed {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Failed,
            error: Some("network".to_owned()),

            prompt_manifest: None,
        },
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "failed");

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: "turn_000000000000000002".to_owned(),
        pending_request_id: "req_000000000000000002".to_owned(),
        user_text: "again".to_owned(),
    });
    assert!(!conversation.can_submit_message());

    conversation.apply(ConversationEvent::TurnFailed {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: "turn_000000000000000002".to_owned(),
            status: TurnStatus::Interrupted,
            error: Some("cancelled".to_owned()),

            prompt_manifest: None,
        },
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "cancelled");
    let current_turn = conversation
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == "turn_000000000000000002")
        .expect("turn should exist");
    assert_eq!(current_turn.phase, TurnPhase::Cancelled);
}

#[test]
fn cancel_request_locks_until_rejected_or_interrupted() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
        user_text: "hello".to_owned(),
    });
    conversation.apply(ConversationEvent::LocalTurnStartAccepted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        pending_request_id: PENDING_REQUEST_ID.to_owned(),
    });

    conversation.apply(ConversationEvent::LocalTurnCancelRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
    });

    assert!(!conversation.can_submit_message());
    assert!(conversation.is_cancelling_turn());
    assert_eq!(conversation.in_flight_turn_id(), Some(TURN_ID));
    assert!(conversation.projection().composer_locked);
    assert_eq!(conversation.projection().phase_label, "cancelling");

    conversation.apply(ConversationEvent::LocalTurnCancelRejected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        error: "gateway unavailable".to_owned(),
    });

    assert!(!conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "running");
    assert_eq!(
        conversation.projection().last_error.as_deref(),
        Some("gateway unavailable")
    );

    conversation.apply(ConversationEvent::LocalTurnCancelRequested {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
    });
    conversation.apply(ConversationEvent::TurnFailed {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Interrupted,
            error: Some("stopped by user".to_owned()),

            prompt_manifest: None,
        },
    });
    conversation.apply(ConversationEvent::LocalTurnCancelRejected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        error: "late error".to_owned(),
    });

    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "cancelled");
    assert_eq!(conversation.in_flight_turn_id(), None);
}

#[test]
fn item_delta_streams_text_until_completion() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_0000000000000000001".to_owned(),
            text: String::new(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_0000000000000000001".to_owned(),
        delta: "hel".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_0000000000000000001".to_owned(),
        delta: "lo".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    let streaming_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_0000000000000000001")
        .expect("item should exist");
    assert_eq!(streaming_item.partial_text, "hello");
    assert_eq!(streaming_item.status, TimelineEntryStatus::Running);

    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_0000000000000000001".to_owned(),
            text: "hello".to_owned(),
            markdown: None,
            markdown_version: None,
        },
    });

    let completed_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_0000000000000000001")
        .expect("item should exist");
    assert_eq!(completed_item.final_text.as_deref(), Some("hello"));
    assert_eq!(completed_item.status, TimelineEntryStatus::Completed);
}

#[test]
fn web_fetch_delta_hides_raw_content_but_keeps_progress_updates() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_web_fetch_1";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com" }),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com"
                })),
            },
            recovery: None,
            url: Some("https://example.com".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        },
    });

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        delta: "RAW_FETCHED_CONTENT_SHOULD_NOT_BE_VISIBLE".to_owned(),
        stream: Some(ItemDeltaStream::Generic),
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        delta: "fetching page metadata".to_owned(),
        stream: Some(ItemDeltaStream::ToolProgress),
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("web_fetch item should exist");
    assert!(
        !item
            .partial_text
            .contains("RAW_FETCHED_CONTENT_SHOULD_NOT_BE_VISIBLE")
    );
    assert!(
        item.partial_text
            .contains(t!("timeline.tool.progress", value = "fetching page metadata").as_ref())
    );
}

#[test]
fn web_fetch_completion_uses_summary_instead_of_output_blob() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_web_fetch_2";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com/page" }),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com/page"
                })),
            },
            recovery: None,
            url: Some("https://example.com/page".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        },
    });

    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com/page" }),
            status: pioneer_protocol::ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com/page",
                    "statusCode": 200
                })),
            },
            recovery: None,
            url: Some("https://example.com/page".to_owned()),
            final_url: Some("https://example.com/page".to_owned()),
            status_code: Some(200),
            content_type: Some("text/html".to_owned()),
            extract_mode: Some("readable".to_owned()),
            resolved_mode: Some("readable".to_owned()),
            bytes_received: Some(12345),
            elapsed_ms: Some(321),
            truncated: None,
            title: Some("Example Page".to_owned()),
            word_count: Some(555),
            links: Vec::new(),
            success: Some(true),
            outcome: None,
            observation: None,
        },
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("web_fetch item should exist");
    let final_text = item
        .final_text
        .as_deref()
        .expect("completed web_fetch should have final_text");
    assert!(final_text.contains(t!("timeline.tool.finished", tool_name = "web_fetch").as_ref()));
    assert!(
        final_text.contains(t!("timeline.tool.url", url = "https://example.com/page").as_ref())
    );
    assert!(!final_text.contains("VERY_LONG_OUTPUT_SHOULD_NOT_BE_VISIBLE"));
    assert!(!final_text.contains("VERY_LONG_CONTENT_SHOULD_NOT_BE_VISIBLE"));
}

#[test]
fn tool_item_projection_preserves_recovery_policy_without_timeline_noise() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_web_fetch_policy";
    let recovery_policy = sample_tool_recovery_policy();

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::WebFetch {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({ "url": "https://example.com/policy" }),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: Some(recovery_policy.clone()),
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({
                    "url": "https://example.com/policy"
                })),
            },
            recovery: None,
            url: Some("https://example.com/policy".to_owned()),
            final_url: None,
            status_code: None,
            content_type: None,
            extract_mode: None,
            resolved_mode: None,
            bytes_received: None,
            elapsed_ms: None,
            truncated: None,
            title: None,
            word_count: None,
            links: Vec::new(),
            success: None,
            outcome: None,
            observation: None,
        },
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("web_fetch item should exist");
    assert_eq!(item.item.recovery_policy(), Some(&recovery_policy));
    assert!(!item.partial_text.contains("max_attempts"));
    assert!(!item.partial_text.contains("retry_with_backoff"));
}

#[test]
fn history_hydration_preserves_tool_recovery_policy_snapshot() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_history_policy";
    let recovery_policy = sample_tool_recovery_policy();

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    error: None,
                    prompt_manifest: None,
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: sample_web_fetch_item(
                    item_id,
                    pioneer_protocol::ToolCallStatus::InProgress,
                    Some(recovery_policy.clone()),
                ),
            },
        },
    ]);

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("hydrated web_fetch item should exist");
    assert_eq!(item.item.recovery_policy(), Some(&recovery_policy));
}

#[test]
fn history_hydration_keeps_dynamic_model_only_body_out_of_desktop_state() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_dynamic_http_history";
    let dynamic_item = TurnItem::DynamicToolCall {
        id: item_id.to_owned(),
        tool_name: "skill.tests-http-skill.fetch-secret".to_owned(),
        arguments: serde_json::json!({}),
        status: pioneer_protocol::ToolCallStatus::Completed,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic_tool"),
        display: ToolDisplayPayload::Hidden,
        storage: ToolStoragePayload::Metadata {
            metadata: ToolMetadata::from_json(serde_json::json!({
                "url": "http://127.0.0.1/dynamic-secret",
                "statusCode": 200,
                "bodyHash": "sha256:test",
                "bodyBytes": 33
            })),
        },
        recovery: None,
        success: Some(true),
        outcome: None,
        observation: None,
    };

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    error: None,
                    prompt_manifest: None,
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::DynamicToolCall {
                    id: item_id.to_owned(),
                    tool_name: "skill.tests-http-skill.fetch-secret".to_owned(),
                    arguments: serde_json::json!({}),
                    status: pioneer_protocol::ToolCallStatus::InProgress,
                    recovery_policy: None,
                    output_policy: ToolOutputPolicySnapshot::for_tool_name("dynamic_tool"),
                    display: ToolDisplayPayload::Hidden,
                    storage: ToolStoragePayload::Metadata {
                        metadata: ToolMetadata::from_json(serde_json::json!({"state": "running"})),
                    },
                    recovery: None,
                    success: None,
                    outcome: None,
                    observation: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: dynamic_item,
            },
        },
    ]);

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("hydrated dynamic item should exist");
    let desktop_state = format!("{:?}", item);
    assert!(!desktop_state.contains("SECRET_DYNAMIC_HTTP_BODY_SENTINEL"));
    assert!(desktop_state.contains("bodyHash"));
}

#[test]
fn item_delta_without_started_item_does_not_break_view_state() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_unknown".to_owned(),
        delta: "noop".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });

    assert!(conversation.projection().timeline.is_empty());
    assert!(conversation.projection().items.is_empty());
}

#[test]
fn timeout_event_adds_recovery_context_to_running_item() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_tool_timeout_1";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::DynamicToolCall {
            id: item_id.to_owned(),
            tool_name: "web_fetch".to_owned(),
            arguments: serde_json::json!({}),
            status: pioneer_protocol::ToolCallStatus::InProgress,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
            display: ToolDisplayPayload::Hidden,
            storage: ToolStoragePayload::Metadata {
                metadata: ToolMetadata::from_json(serde_json::json!({})),
            },
            recovery: None,
            success: None,
            outcome: None,
            observation: None,
        },
    });

    conversation.apply(ConversationEvent::ItemTimeoutDetected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        item_type: TurnItemType::DynamicToolCall,
        attempt_number: 2,
        reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
        recovery_job_id: Some("job_timeout_1".to_owned()),
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("timed out item should exist");
    assert_eq!(item.status, TimelineEntryStatus::Running);
    assert!(
        item.partial_text
            .contains("[timeout] attempt #2 idle deadline exceeded")
    );
}

#[test]
fn timeout_event_without_recovery_job_marks_no_automatic_recovery() {
    let mut conversation = Conversation::new(THREAD_ID);
    let item_id = "item_reasoning_timeout_1";

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: item_id.to_owned(),
            summary: vec![],
            content: vec![],
        },
    });

    conversation.apply(ConversationEvent::ItemTimeoutDetected {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: item_id.to_owned(),
        item_type: TurnItemType::Reasoning,
        attempt_number: 1,
        reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
        recovery_job_id: None,
    });

    let item = conversation
        .projection()
        .items
        .iter()
        .find(|value| value.id == item_id)
        .expect("timed out item should exist");
    assert!(item.partial_text.contains("no automatic recovery"));
}

#[test]
fn history_hydration_restores_recovery_events_without_terminal_duplicate() {
    let item_id = "item_reasoning_recovery";
    let recovery_job_id = "job_reasoning_recovery";
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    error: None,
                    prompt_manifest: None,
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: item_id.to_owned(),
                    summary: vec![],
                    content: vec![],
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::ItemRecoveryOpened {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: item_id.to_owned(),
                item_type: TurnItemType::Reasoning,
                recovery_job_id: recovery_job_id.to_owned(),
                trigger: RecoveryTrigger::ProviderError,
                action: RecoveryAction::MarkFailed,
                attempt_number: 1,
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 1_300,
            payload: ThreadHistoryEventPayload::ItemRecoveryExhausted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: item_id.to_owned(),
                item_type: TurnItemType::Reasoning,
                recovery_job_id: recovery_job_id.to_owned(),
                attempt_number: 1,
                status: RecoveryJobStatus::Failed,
                error_message: "recovery policy marks this failure as terminal".to_owned(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 1_400,
            payload: ThreadHistoryEventPayload::TurnFailed {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Failed,
                    error: Some(format!(
                        "recovery failed for item `{item_id}`: recovery policy marks this failure as terminal"
                    )),
                    prompt_manifest: None,
                },
            },
        },
    ]);

    let system_events = conversation
        .projection()
        .items
        .iter()
        .filter(|item| item.item_type == "system_event")
        .map(|item| item.partial_text.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        system_events,
        vec![
            t!("timeline.system.recovery_opened").to_string(),
            t!("timeline.system.recovery_failed").to_string()
        ]
    );
    assert!(
        system_events
            .iter()
            .all(|text| !text.contains("recovery failed"))
    );
}

#[test]
fn foreign_thread_events_do_not_modify_local_projection() {
    let mut conversation = Conversation::new("thr_local");

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: "thr_foreign".to_owned(),
        turn: Turn {
            id: "turn_foreign".to_owned(),
            status: TurnStatus::InProgress,
            error: None,

            prompt_manifest: None,
        },
    });

    assert!(conversation.projection().timeline.is_empty());
    assert!(conversation.projection().turns.is_empty());
    assert!(conversation.can_submit_message());
}

#[test]
fn replay_style_sequence_restores_final_state() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            error: None,

            prompt_manifest: None,
        },
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "item_reasoning".to_owned(),
            summary: vec![],
            content: vec![],
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::Reasoning {
            id: "item_reasoning".to_owned(),
            summary: vec!["thinking done".to_owned()],
            content: vec![],
        },
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_answer".to_owned(),
            text: String::new(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemDelta {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_answer".to_owned(),
        delta: "final".to_owned(),
        stream: None,
        payload: None,
        markdown: None,
        markdown_version: None,
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "item_answer".to_owned(),
            text: "final".to_owned(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            error: None,

            prompt_manifest: None,
        },
    });
    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());

    let answer = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_answer")
        .expect("assistant answer should exist");
    assert_eq!(answer.final_text.as_deref(), Some("final"));
    assert!(conversation.can_submit_message());
    assert_eq!(conversation.status_label(), "completed");
}

#[test]
fn duplicate_turn_completed_event_is_ignored_after_terminal_completion() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            error: None,

            prompt_manifest: None,
        },
    });

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            error: None,

            prompt_manifest: None,
        },
    });
    assert_eq!(conversation.status_label(), "completing");
    assert!(conversation.tick());
    assert_eq!(conversation.status_label(), "completed");

    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::Completed,
            error: None,

            prompt_manifest: None,
        },
    });

    assert_eq!(conversation.status_label(), "completed");
    assert!(!conversation.tick());
    assert!(conversation.can_submit_message());
    let turn = conversation
        .projection()
        .turns
        .iter()
        .find(|turn| turn.id == TURN_ID)
        .expect("turn should exist");
    assert_eq!(turn.phase, TurnPhase::Completed);
}

#[test]
fn event_log_is_bounded() {
    let mut conversation = Conversation::new(THREAD_ID);

    for idx in 0..(MAX_EVENT_LOG_LEN + 100) {
        conversation.apply(ConversationEvent::ItemDelta {
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            item_id: format!("item_{idx}"),
            delta: "noop".to_owned(),
            stream: None,
            payload: None,
            markdown: None,
            markdown_version: None,
        });
    }

    assert_eq!(conversation.event_log.len(), MAX_EVENT_LOG_LEN);
    assert_eq!(
        conversation.event_log.front().map(|event| event.sequence),
        Some(101)
    );
}

fn tool_retry_budget_usage() -> Vec<ToolRetryBudgetUsage> {
    vec![ToolRetryBudgetUsage {
        kind: ToolRetryBudgetKind::Episode,
        used: 1,
        limit: 2,
    }]
}

#[test]
fn tool_retry_events_are_logged_without_recovery_projection() {
    let mut conversation = Conversation::new(THREAD_ID);

    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            error: None,
            prompt_manifest: None,
        },
    });
    conversation.apply(ConversationEvent::ItemToolRetryScheduled {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 1,
        error_class: ToolRetryErrorClass::Timeout,
        retry_hint: "retry with a smaller request".to_owned(),
        budgets: tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_timeout".to_owned(),
        reason: "recoverable_tool_output".to_owned(),
    });
    conversation.apply(ConversationEvent::ItemToolRetryResolved {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 2,
        resolution: ToolRetryResolution::Succeeded,
        budgets: tool_retry_budget_usage(),
        reason: "successful_tool_output".to_owned(),
    });
    conversation.apply(ConversationEvent::ItemToolRetryExhausted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_2".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 3,
        error_class: ToolRetryErrorClass::Timeout,
        exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
        budgets: tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_timeout".to_owned(),
        reason: "same_failure_signature".to_owned(),
    });
    conversation.apply(ConversationEvent::TurnToolLoopBudgetExceeded {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
        limit: 32,
        observed: 33,
        action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
        reason: "agent_rounds_exceeded".to_owned(),
    });

    let kinds = conversation
        .event_log
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert!(kinds.contains(&EventKind::ItemToolRetryScheduled));
    assert!(kinds.contains(&EventKind::ItemToolRetryResolved));
    assert!(kinds.contains(&EventKind::ItemToolRetryExhausted));
    assert!(kinds.contains(&EventKind::TurnToolLoopBudgetExceeded));
    assert_eq!(conversation.status_label(), "running");
    assert!(!conversation.can_submit_message());
    let retry_scheduled = t!(
        "timeline.system.tool_retry_scheduled_with_attempt",
        tool_name = "web_fetch",
        attempt = 1
    )
    .to_string();
    assert!(
        conversation
            .projection()
            .items
            .iter()
            .any(|item| item.partial_text.contains(retry_scheduled.as_str()))
    );
    assert!(
        conversation
            .projection()
            .items
            .iter()
            .all(|item| !item.partial_text.contains("recovery job"))
    );
}

#[test]
fn history_hydration_replays_tool_retry_events_like_live_events() {
    let mut live = Conversation::new(THREAD_ID);
    live.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: Turn {
            id: TURN_ID.to_owned(),
            status: TurnStatus::InProgress,
            error: None,
            prompt_manifest: None,
        },
    });
    live.apply(ConversationEvent::ItemToolRetryScheduled {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item_id: "item_web_fetch_retry".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 1,
        error_class: ToolRetryErrorClass::Timeout,
        retry_hint: "retry with a smaller request".to_owned(),
        budgets: tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_timeout".to_owned(),
        reason: "recoverable_tool_output".to_owned(),
    });
    live.apply(ConversationEvent::TurnToolLoopBudgetExceeded {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
        limit: 32,
        observed: 33,
        action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
        reason: "agent_rounds_exceeded".to_owned(),
    });

    let mut replay = Conversation::new(THREAD_ID);
    replay.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    error: None,
                    prompt_manifest: None,
                },
                input: Vec::new(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemToolRetryScheduled {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: "item_web_fetch_retry".to_owned(),
                item_type: TurnItemType::WebFetch,
                tool_retry_episode_id: "tool_retry_turn_1_1".to_owned(),
                tool_name: "web_fetch".to_owned(),
                attempt_number: 1,
                error_class: ToolRetryErrorClass::Timeout,
                retry_hint: "retry with a smaller request".to_owned(),
                budgets: tool_retry_budget_usage(),
                failure_signature_fingerprint: "sig_timeout".to_owned(),
                reason: "recoverable_tool_output".to_owned(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::TurnToolLoopBudgetExceeded {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
                limit: 32,
                observed: 33,
                action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
                reason: "agent_rounds_exceeded".to_owned(),
            },
        },
    ]);

    let live_kinds = live
        .event_log
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    let replay_kinds = replay
        .event_log
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(live_kinds, replay_kinds);
    assert_eq!(replay.status_label(), "running");
    assert_eq!(
        live.projection()
            .items
            .iter()
            .filter(|item| item.item_type == "system_event")
            .count(),
        replay
            .projection()
            .items
            .iter()
            .filter(|item| item.item_type == "system_event")
            .count()
    );
}

#[test]
fn hydrate_history_restores_all_items_and_thinking_duration() {
    let mut conversation = Conversation::new(THREAD_ID);
    let events = vec![
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    error: None,

                    prompt_manifest: None,
                },
                input: vec![UserInput::Text {
                    text: "history prompt".to_owned(),
                    text_elements: Vec::new(),
                }],
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 1_100,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::UserMessage {
                    id: "item_user_history".to_owned(),
                    text: "history prompt".to_owned(),
                    attachments: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 1_200,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::UserMessage {
                    id: "item_user_history".to_owned(),
                    text: "history prompt".to_owned(),
                    attachments: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 2_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_history".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 32_500,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_history".to_owned(),
                    summary: vec!["done".to_owned()],
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 6,
            created_at: 32_900,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::AgentMessage {
                    id: "item_answer_history".to_owned(),
                    text: String::new(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 7,
            created_at: 33_100,
            payload: ThreadHistoryEventPayload::ItemDelta {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: "item_answer_history".to_owned(),
                delta: "history ".to_owned(),
                stream: None,
                payload: None,
                markdown: None,
                markdown_version: None,
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 8,
            created_at: 33_200,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::AgentMessage {
                    id: "item_answer_history".to_owned(),
                    text: "history response".to_owned(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 9,
            created_at: 33_400,
            payload: ThreadHistoryEventPayload::TurnCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Completed,
                    error: None,

                    prompt_manifest: None,
                },
            },
        },
    ];

    conversation.hydrate_history(&events);

    let user_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_user_history")
        .expect("hydrated user entry should exist");
    let user_item = conversation
        .projection()
        .item_for_timeline_entry(user_entry)
        .expect("hydrated user timeline item should resolve");
    assert_eq!(user_item.partial_text, "history prompt");

    let reasoning_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_reasoning_history")
        .expect("hydrated reasoning item should exist");
    let reasoning_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_reasoning_history")
        .expect("hydrated reasoning view item should exist");
    let reasoning_timeline_item = conversation
        .projection()
        .item_for_timeline_entry(reasoning_entry)
        .expect("hydrated reasoning timeline item should resolve");
    assert!(
        matches!(reasoning_timeline_item.item, TurnItem::Reasoning { .. }),
        "timeline reasoning entry should keep typed payload"
    );
    assert_eq!(reasoning_item.started_at_unix_ms, Some(2_000));
    assert_eq!(reasoning_item.completed_at_unix_ms, Some(32_500));

    let answer_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_answer_history")
        .expect("hydrated assistant item should exist");
    let answer_item = conversation
        .projection()
        .item_for_timeline_entry(answer_entry)
        .expect("hydrated assistant timeline item should resolve");
    assert_eq!(answer_item.partial_text, "history response");

    assert_eq!(conversation.status_label(), "completed");
    assert!(conversation.can_submit_message());
}

#[test]
fn hydrate_history_preserves_reasoning_delta_text_when_completed_payload_is_empty() {
    let mut conversation = Conversation::new(THREAD_ID);
    let events = vec![
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::TurnStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::InProgress,
                    error: None,

                    prompt_manifest: None,
                },
                input: vec![UserInput::Text {
                    text: "reasoning test".to_owned(),
                    text_elements: Vec::new(),
                }],
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 2_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_delta_history".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 3_000,
            payload: ThreadHistoryEventPayload::ItemDelta {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item_id: "item_reasoning_delta_history".to_owned(),
                delta: "Reasoning chunk".to_owned(),
                stream: Some(ItemDeltaStream::Generic),
                payload: None,
                markdown: None,
                markdown_version: None,
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 4_000,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::Reasoning {
                    id: "item_reasoning_delta_history".to_owned(),
                    summary: Vec::new(),
                    content: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 5_000,
            payload: ThreadHistoryEventPayload::TurnCompleted {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Completed,
                    error: None,

                    prompt_manifest: None,
                },
            },
        },
    ];

    conversation.hydrate_history(&events);

    let reasoning_entry = conversation
        .projection()
        .timeline
        .iter()
        .find(|entry| entry.item_id == "item_reasoning_delta_history")
        .expect("hydrated reasoning timeline entry should exist");
    let reasoning_timeline_item = conversation
        .projection()
        .item_for_timeline_entry(reasoning_entry)
        .expect("reasoning timeline entry should resolve to item");
    assert!(
        reasoning_timeline_item
            .partial_text
            .contains("Reasoning chunk")
    );
    assert_eq!(
        reasoning_timeline_item.status,
        TimelineEntryStatus::Completed
    );

    let reasoning_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_reasoning_delta_history")
        .expect("hydrated reasoning item should exist");
    assert!(reasoning_item.partial_text.contains("Reasoning chunk"));
    assert_eq!(reasoning_item.status, TimelineEntryStatus::Completed);
}

#[test]
fn composed_turn_timeline_projects_task_events_and_child_tool_items() {
    let mut conversation = Conversation::new(THREAD_ID);
    conversation.hydrate_history(&[ThreadHistoryEvent {
        turn_id: TURN_ID.to_owned(),
        sequence: 1,
        created_at: 1_000,
        payload: ThreadHistoryEventPayload::ItemStarted {
            workspace_id: "ws_000000000000000001".to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            item: TurnItem::Task {
                item: TaskTurnItem {
                    id: "task_anchor_1".to_owned(),
                    task_id: "task_1".to_owned(),
                    run_id: Some("run_1".to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Investigate build".to_owned(),
                    status: TaskStatus::Running,
                    trigger_kind: TaskTriggerKind::Immediate,
                    executor_kind: TaskExecutorKind::Agent,
                    child_thread_id: Some("child_thread_1".to_owned()),
                    child_turn_id: Some("child_turn_1".to_owned()),
                    agent_role: None,
                    depth: 1,
                    max_depth: 3,
                    next_fire_at: None,
                    result_preview: None,
                    error_preview: None,
                    created_at: 1,
                    updated_at: 1,
                },
            },
        },
    }]);

    let child_tool = TurnItem::CommandExecution {
        id: "child_cmd_1".to_owned(),
        tool_name: "exec_command".to_owned(),
        arguments: serde_json::json!({ "cmd": "cargo test" }),
        status: ToolCallStatus::InProgress,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_tool_name("exec_command"),
        display: ToolDisplayPayload::default(),
        storage: ToolStoragePayload::default(),
        recovery: None,
        command: vec!["cargo".to_owned(), "test".to_owned()],
        cwd: Some("/workspace".to_owned()),
        success: None,
        outcome: None,
        observation: None,
    };
    let completed_child_tool = match child_tool.clone() {
        TurnItem::CommandExecution {
            id,
            tool_name,
            arguments,
            recovery_policy,
            output_policy,
            display,
            storage,
            recovery,
            command,
            cwd,
            outcome,
            observation,
            ..
        } => TurnItem::CommandExecution {
            id,
            tool_name,
            arguments,
            status: ToolCallStatus::Completed,
            recovery_policy,
            output_policy,
            display,
            storage,
            recovery,
            command,
            cwd,
            success: Some(true),
            outcome,
            observation,
        },
        _ => unreachable!("test child tool is command execution"),
    };

    conversation.apply_composed_turn_timeline(&TurnTimelineResponse {
        thread_id: THREAD_ID.to_owned(),
        workspace_id: "ws_000000000000000001".to_owned(),
        turn_id: TURN_ID.to_owned(),
        last_sequence: 3,
        items: vec![
            TimelineItem {
                id: "task:task_1:1".to_owned(),
                origin: TimelineOrigin {
                    kind: TimelineOriginKind::TaskEvent,
                    task_id: Some("task_1".to_owned()),
                    run_id: Some("run_1".to_owned()),
                    child_thread_id: None,
                    child_turn_id: None,
                    origin_event_id: Some("task_event_1".to_owned()),
                    origin_turn_item_id: None,
                    origin_sequence: 1,
                    occurred_at: 1,
                    lane: TimelineLane::Task,
                },
                payload: TimelinePayload::TaskEvent {
                    event: TaskEvent {
                        id: "task_event_1".to_owned(),
                        task_id: "task_1".to_owned(),
                        run_id: Some("run_1".to_owned()),
                        thread_id: None,
                        turn_id: None,
                        sequence: 1,
                        event_type: pioneer_protocol::constants::events::TASK_RUN_STARTED
                            .to_owned(),
                        payload: TaskEventPayload::RunStarted {
                            task_id: "task_1".to_owned(),
                            run_id: "run_1".to_owned(),
                            started_at: 1,
                        },
                        created_at: 1,
                    },
                },
            },
            TimelineItem {
                id: "child:child_cmd_1:2".to_owned(),
                origin: TimelineOrigin {
                    kind: TimelineOriginKind::ChildTurn,
                    task_id: Some("task_1".to_owned()),
                    run_id: Some("run_1".to_owned()),
                    child_thread_id: Some("child_thread_1".to_owned()),
                    child_turn_id: Some("child_turn_1".to_owned()),
                    origin_event_id: None,
                    origin_turn_item_id: Some("child_cmd_1".to_owned()),
                    origin_sequence: 2,
                    occurred_at: 2,
                    lane: TimelineLane::ChildTool,
                },
                payload: TimelinePayload::TurnItemEvent {
                    event: TurnItemEvent {
                        sequence: 2,
                        created_at: 2,
                        payload: TurnItemEventPayload::ItemStarted {
                            workspace_id: "ws_000000000000000001".to_owned(),
                            thread_id: "child_thread_1".to_owned(),
                            turn_id: "child_turn_1".to_owned(),
                            item: child_tool,
                        },
                    },
                },
            },
            TimelineItem {
                id: "child:child_cmd_1:3".to_owned(),
                origin: TimelineOrigin {
                    kind: TimelineOriginKind::ChildTurn,
                    task_id: Some("task_1".to_owned()),
                    run_id: Some("run_1".to_owned()),
                    child_thread_id: Some("child_thread_1".to_owned()),
                    child_turn_id: Some("child_turn_1".to_owned()),
                    origin_event_id: None,
                    origin_turn_item_id: Some("child_cmd_1".to_owned()),
                    origin_sequence: 3,
                    occurred_at: 3,
                    lane: TimelineLane::ChildTool,
                },
                payload: TimelinePayload::TurnItemEvent {
                    event: TurnItemEvent {
                        sequence: 3,
                        created_at: 3,
                        payload: TurnItemEventPayload::ItemCompleted {
                            workspace_id: "ws_000000000000000001".to_owned(),
                            thread_id: "child_thread_1".to_owned(),
                            turn_id: "child_turn_1".to_owned(),
                            item: completed_child_tool,
                        },
                    },
                },
            },
        ],
    });

    assert!(
        conversation
            .projection()
            .items
            .iter()
            .any(|item| item.id == "task_event_task_event_1"
                && item.partial_text.contains("Task run started"))
    );
    let child_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "child_cmd_1")
        .expect("child tool should project into parent timeline");
    assert_eq!(child_item.status, TimelineEntryStatus::Completed);
    assert!(
        conversation
            .projection()
            .timeline
            .iter()
            .any(|entry| entry.item_id == "child_cmd_1")
    );
}

#[test]
fn composed_child_retry_events_tag_synthetic_rows_with_task_meta() {
    let mut conversation = Conversation::new(THREAD_ID);

    let timeline = TurnTimelineResponse {
        thread_id: THREAD_ID.to_owned(),
        workspace_id: "ws_000000000000000001".to_owned(),
        turn_id: TURN_ID.to_owned(),
        last_sequence: 1,
        items: vec![TimelineItem {
            id: "child:tool_1:1".to_owned(),
            origin: TimelineOrigin {
                kind: TimelineOriginKind::ChildTurn,
                task_id: Some("task_1".to_owned()),
                run_id: Some("run_1".to_owned()),
                child_thread_id: Some("child_thread_1".to_owned()),
                child_turn_id: Some("child_turn_1".to_owned()),
                origin_event_id: None,
                origin_turn_item_id: Some("tool_1".to_owned()),
                origin_sequence: 1,
                occurred_at: 2,
                lane: TimelineLane::ChildTool,
            },
            payload: TimelinePayload::TurnItemEvent {
                event: TurnItemEvent {
                    sequence: 1,
                    created_at: 2,
                    payload: TurnItemEventPayload::ItemToolRetryScheduled {
                        workspace_id: "ws_000000000000000001".to_owned(),
                        thread_id: "child_thread_1".to_owned(),
                        turn_id: "child_turn_1".to_owned(),
                        item_id: "tool_1".to_owned(),
                        item_type: TurnItemType::DynamicToolCall,
                        tool_retry_episode_id: "retry_1".to_owned(),
                        tool_name: "grep_files".to_owned(),
                        attempt_number: 1,
                        error_class: ToolRetryErrorClass::ExecutionFailed,
                        retry_hint: "retry with corrected input".to_owned(),
                        budgets: tool_retry_budget_usage(),
                        failure_signature_fingerprint: "sig".to_owned(),
                        reason: "recoverable_tool_output".to_owned(),
                    },
                },
            },
        }],
    };
    conversation.apply_composed_turn_timeline(&timeline);
    conversation.apply_composed_turn_timeline(&timeline);

    let retry_events = conversation
        .projection()
        .items
        .iter()
        .filter(|item| {
            matches!(
                &item.item,
                TurnItem::SystemEvent {
                    code: Some(code),
                    ..
                } if code == "item_tool_retry_scheduled"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        retry_events.len(),
        1,
        "re-applying the same composed child event must be idempotent"
    );
    let retry_event = retry_events[0];
    let opaque_meta = retry_event
        .opaque_meta
        .as_ref()
        .expect("synthetic child system event should carry task metadata");
    assert_eq!(
        opaque_meta
            .get("timeline_group")
            .and_then(serde_json::Value::as_str),
        Some("task")
    );
    assert_eq!(
        opaque_meta
            .get("task_id")
            .and_then(serde_json::Value::as_str),
        Some("task_1")
    );
}

#[test]
fn composed_timeline_inserts_late_task_events_by_event_time() {
    let mut conversation = Conversation::new(THREAD_ID);
    let workspace_id = "ws_000000000000000001".to_owned();
    let task_anchor = TurnItem::Task {
        item: TaskTurnItem {
            id: "task_anchor_early".to_owned(),
            task_id: "task_early".to_owned(),
            run_id: Some("run_early".to_owned()),
            parent_task_id: None,
            root_task_id: None,
            title: "Early task".to_owned(),
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
            created_at: 2,
            updated_at: 4,
        },
    };

    conversation.hydrate_history(&[
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 1,
            created_at: 1_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: workspace_id.clone(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::UserMessage {
                    id: "user_late_timeline".to_owned(),
                    text: "run task".to_owned(),
                    attachments: Vec::new(),
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 2,
            created_at: 2_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: workspace_id.clone(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: task_anchor.clone(),
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 3,
            created_at: 5_000,
            payload: ThreadHistoryEventPayload::ItemStarted {
                workspace_id: workspace_id.clone(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::AgentMessage {
                    id: "agent_final_late_timeline".to_owned(),
                    text: "done".to_owned(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 4,
            created_at: 5_100,
            payload: ThreadHistoryEventPayload::ItemCompleted {
                workspace_id: workspace_id.clone(),
                thread_id: THREAD_ID.to_owned(),
                turn_id: TURN_ID.to_owned(),
                item: TurnItem::AgentMessage {
                    id: "agent_final_late_timeline".to_owned(),
                    text: "done".to_owned(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
        ThreadHistoryEvent {
            turn_id: TURN_ID.to_owned(),
            sequence: 5,
            created_at: 6_000,
            payload: ThreadHistoryEventPayload::TurnCompleted {
                workspace_id: workspace_id.clone(),
                thread_id: THREAD_ID.to_owned(),
                turn: Turn {
                    id: TURN_ID.to_owned(),
                    status: TurnStatus::Completed,
                    error: None,
                    prompt_manifest: None,
                },
            },
        },
    ]);

    conversation.apply_composed_turn_timeline(&TurnTimelineResponse {
        thread_id: THREAD_ID.to_owned(),
        workspace_id,
        turn_id: TURN_ID.to_owned(),
        last_sequence: 1,
        items: vec![TimelineItem {
            id: "task:task_early:1".to_owned(),
            origin: TimelineOrigin {
                kind: TimelineOriginKind::TaskEvent,
                task_id: Some("task_early".to_owned()),
                run_id: Some("run_early".to_owned()),
                child_thread_id: None,
                child_turn_id: None,
                origin_event_id: Some("task_event_early".to_owned()),
                origin_turn_item_id: None,
                origin_sequence: 1,
                occurred_at: 3_000,
                lane: TimelineLane::Task,
            },
            payload: TimelinePayload::TaskEvent {
                event: TaskEvent {
                    id: "task_event_early".to_owned(),
                    task_id: "task_early".to_owned(),
                    run_id: Some("run_early".to_owned()),
                    thread_id: None,
                    turn_id: None,
                    sequence: 1,
                    event_type: pioneer_protocol::constants::events::TASK_RUN_STARTED.to_owned(),
                    payload: TaskEventPayload::RunStarted {
                        task_id: "task_early".to_owned(),
                        run_id: "run_early".to_owned(),
                        started_at: 3,
                    },
                    created_at: 3,
                },
            },
        }],
    });

    let projection = conversation.projection();
    let task_event_index = projection
        .timeline
        .iter()
        .position(|entry| entry.item_id == "task_event_task_event_early")
        .expect("late-applied task event should be projected");
    let final_answer_index = projection
        .timeline
        .iter()
        .position(|entry| entry.item_id == "agent_final_late_timeline")
        .expect("final answer should be projected");

    assert!(
        task_event_index < final_answer_index,
        "task event created before the final answer must not be appended after it"
    );
}

#[test]
fn composed_task_event_uses_origin_task_group_for_metadata() {
    let mut conversation = Conversation::new(THREAD_ID);
    let workspace_id = "ws_000000000000000001".to_owned();
    let parent_task_id = "task_parent";
    let child_task_id = "task_child";

    conversation.hydrate_history(&[ThreadHistoryEvent {
        turn_id: TURN_ID.to_owned(),
        sequence: 1,
        created_at: 1_000,
        payload: ThreadHistoryEventPayload::ItemStarted {
            workspace_id: workspace_id.clone(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            item: TurnItem::Task {
                item: TaskTurnItem {
                    id: "task_anchor_parent".to_owned(),
                    task_id: parent_task_id.to_owned(),
                    run_id: Some("run_parent_1".to_owned()),
                    parent_task_id: None,
                    root_task_id: None,
                    title: "Parent task".to_owned(),
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
        },
    }]);

    conversation.apply_composed_turn_timeline(&TurnTimelineResponse {
        thread_id: THREAD_ID.to_owned(),
        workspace_id,
        turn_id: TURN_ID.to_owned(),
        last_sequence: 1,
        items: vec![TimelineItem {
            id: "task:task_child:1".to_owned(),
            origin: TimelineOrigin {
                kind: TimelineOriginKind::TaskEvent,
                task_id: Some(parent_task_id.to_owned()),
                run_id: Some("run_child_1".to_owned()),
                child_thread_id: Some("child_thread_1".to_owned()),
                child_turn_id: Some("child_turn_1".to_owned()),
                origin_event_id: Some("task_event_child_1".to_owned()),
                origin_turn_item_id: None,
                origin_sequence: 1,
                occurred_at: 2_000,
                lane: TimelineLane::Task,
            },
            payload: TimelinePayload::TaskEvent {
                event: TaskEvent {
                    id: "task_event_child_1".to_owned(),
                    task_id: child_task_id.to_owned(),
                    run_id: Some("run_child_1".to_owned()),
                    thread_id: Some("child_thread_1".to_owned()),
                    turn_id: Some("child_turn_1".to_owned()),
                    sequence: 1,
                    event_type: pioneer_protocol::constants::events::TASK_RUN_STARTED.to_owned(),
                    payload: TaskEventPayload::RunStarted {
                        task_id: child_task_id.to_owned(),
                        run_id: "run_child_1".to_owned(),
                        started_at: 2,
                    },
                    created_at: 2,
                },
            },
        }],
    });

    let task_event_item = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "task_event_task_event_child_1")
        .expect("task event should be projected");
    let meta = task_event_item
        .opaque_meta
        .as_ref()
        .expect("task event must carry task timeline metadata");
    assert_eq!(
        meta.get("task_id").and_then(serde_json::Value::as_str),
        Some(parent_task_id),
        "grouping should use composed origin.task_id (anchor), not source event.task_id"
    );

    let details = match &task_event_item.item {
        TurnItem::SystemEvent {
            details: Some(details),
            ..
        } => details,
        _ => panic!("projected item should be a system event with details"),
    };
    assert_eq!(
        details
            .get("source_task_id")
            .and_then(serde_json::Value::as_str),
        Some(child_task_id)
    );
}
