#[cfg(test)]
fn should_apply_thread_event(active_thread_id: Option<&str>, event_thread_id: &str) -> bool {
    active_thread_id == Some(event_thread_id)
}

#[cfg(test)]
fn should_apply_thread_event_optional(
    active_thread_id: Option<&str>,
    event_thread_id: Option<&str>,
) -> bool {
    let Some(event_thread_id) = event_thread_id else {
        return false;
    };
    should_apply_thread_event(active_thread_id, event_thread_id)
}

#[cfg(test)]
fn should_accept_local_thread_started(
    pending_thread_id: Option<&str>,
    started_thread_id: &str,
) -> bool {
    pending_thread_id == Some(started_thread_id)
}

use super::super::conversation::{Conversation, ConversationEvent, TimelineEntryStatus};
use super::thread_list::{
    TurnTimelineRefreshTransitionEvent, transition_turn_timeline_refresh_state,
};
use super::{
    build_remote_candidate_ws_connect_spec, build_ws_connect_spec,
    default_user_command_bin_dir_label, gateway_activation_is_noop,
    gateway_activation_requires_local_start, gateway_has_ready_ws_connection,
    is_transient_thread_start_error, normalize_workspace_id, should_apply_gateway_operation_result,
    should_apply_ws_event, should_refresh_workspace_bound_data, thread_start_retry_delay,
    turn_resume_retry_delay, warning_notification_messages,
};
use crate::app::root::GatewayConnectionState;
use crate::gateway::{
    GatewayEndpoint, GatewayEndpointKind, GatewayInstallWarning, GatewayRuntime, GatewayWsEvent,
};
use pioneer_protocol::{
    GatewayNotification, RecoveryAction, ToolCallStatus, ToolDisplayPayload, ToolLoopBudgetAction,
    ToolLoopBudgetLimitKind, ToolMetadata, ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode,
    ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass, ToolRetryBudgetKind, ToolRetryBudgetUsage,
    ToolRetryErrorClass, ToolRetryExhaustionKind, ToolRetryResolution, ToolStoragePayload, Turn,
    TurnItem, TurnItemEventPayload, TurnItemType, TurnStatus,
};
use std::time::Duration;

#[test]
fn ws_event_fencing_ignores_stale_connection_id() {
    let event = GatewayWsEvent::Connected {
        connection_id: 42,
        endpoint_id: "remote-42".to_owned(),
        endpoint_name: "remote".to_owned(),
        address: "0.0.0.0:17878".to_owned(),
    };

    assert!(should_apply_ws_event(Some(42), &event));
    assert!(!should_apply_ws_event(Some(7), &event));
    assert!(!should_apply_ws_event(None, &event));
}

#[test]
fn ws_connect_spec_uses_resolved_in_memory_auth_token() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    runtime
        .store_gateway_auth_token_for_tests("remote-123", "resolved-token")
        .expect("store test token");
    let endpoint = GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        auth_token_ref: Some("remote-123".to_owned()),
        workspace_id: None,
        service_name: None,
    };

    let spec = build_ws_connect_spec(&runtime, &endpoint).expect("build ws spec");

    assert_eq!(spec.auth_token.as_deref(), Some("resolved-token"));
}

#[test]
fn remote_ws_connect_spec_uses_remote_timeout_floor() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    let endpoint = GatewayEndpoint {
        id: "remote-123".to_owned(),
        name: "Remote".to_owned(),
        address: "127.0.0.1:22000".to_owned(),
        kind: GatewayEndpointKind::Remote,
        auth_token_ref: None,
        workspace_id: None,
        service_name: None,
    };

    let spec = build_ws_connect_spec(&runtime, &endpoint).expect("build remote ws spec");
    let candidate =
        build_remote_candidate_ws_connect_spec(&runtime, "Remote", "127.0.0.1:22000", "");

    assert_eq!(spec.timings.connect_timeout, Duration::from_millis(5_000));
    assert_eq!(
        candidate.timings.connect_timeout,
        Duration::from_millis(5_000)
    );
}

#[test]
fn local_ws_connect_spec_keeps_configured_timeout() {
    let runtime = GatewayRuntime::for_ws_spec_tests();
    let endpoint = GatewayEndpoint {
        id: "local".to_owned(),
        name: "Local".to_owned(),
        address: "0.0.0.0:17878".to_owned(),
        kind: GatewayEndpointKind::Local,
        auth_token_ref: None,
        workspace_id: None,
        service_name: Some("com.pioneer.gateway".to_owned()),
    };

    let spec = build_ws_connect_spec(&runtime, &endpoint).expect("build local ws spec");

    assert_eq!(spec.timings.connect_timeout, Duration::from_millis(300));
}

#[test]
fn operation_epoch_fencing_ignores_stale_async_results() {
    assert!(should_apply_gateway_operation_result(3, 3));
    assert!(!should_apply_gateway_operation_result(4, 3));
}

#[test]
fn gateway_activation_requires_local_start_only_for_local_endpoint() {
    assert!(gateway_activation_requires_local_start(Some(
        GatewayEndpointKind::Local
    )));
    assert!(!gateway_activation_requires_local_start(Some(
        GatewayEndpointKind::Remote
    )));
    assert!(!gateway_activation_requires_local_start(None));
}

#[test]
fn active_gateway_activation_is_noop_only_when_ws_is_ready() {
    assert!(gateway_activation_is_noop(
        Some("local"),
        "local",
        GatewayConnectionState::Connected,
        Some(7),
    ));
    assert!(!gateway_activation_is_noop(
        Some("local"),
        "local",
        GatewayConnectionState::Disconnected,
        None,
    ));
    assert!(!gateway_activation_is_noop(
        Some("local"),
        "remote",
        GatewayConnectionState::Connected,
        Some(7),
    ));
}

#[test]
fn ready_ws_connection_requires_connected_state_and_connection_id() {
    assert!(gateway_has_ready_ws_connection(
        GatewayConnectionState::Connected,
        Some(7),
    ));
    assert!(!gateway_has_ready_ws_connection(
        GatewayConnectionState::Connected,
        None,
    ));
    assert!(!gateway_has_ready_ws_connection(
        GatewayConnectionState::Reconnecting,
        Some(7),
    ));
}

#[test]
fn turn_timeline_refresh_singleflight_coalesces_dirty_requests() {
    let (state, first_generation) =
        transition_turn_timeline_refresh_state(None, TurnTimelineRefreshTransitionEvent::Request);
    assert_eq!(first_generation, Some(1));

    let (state, second_generation) =
        transition_turn_timeline_refresh_state(state, TurnTimelineRefreshTransitionEvent::Request);
    assert_eq!(
        second_generation, None,
        "second request should be coalesced while in-flight"
    );

    let (state, rerun_generation) = transition_turn_timeline_refresh_state(
        state,
        TurnTimelineRefreshTransitionEvent::Complete { generation: 1 },
    );
    assert_eq!(
        rerun_generation,
        Some(2),
        "dirty refresh should re-run exactly once"
    );

    let (state, next_generation) = transition_turn_timeline_refresh_state(
        state,
        TurnTimelineRefreshTransitionEvent::Complete { generation: 2 },
    );
    assert!(state.is_none());
    assert!(next_generation.is_none());
}

#[test]
fn turn_timeline_refresh_noop_complete_cleans_state() {
    let (state, first_generation) =
        transition_turn_timeline_refresh_state(None, TurnTimelineRefreshTransitionEvent::Request);
    assert_eq!(first_generation, Some(1));

    let (state, queued_generation) = transition_turn_timeline_refresh_state(
        state,
        TurnTimelineRefreshTransitionEvent::Complete { generation: 1 },
    );
    assert!(state.is_none());
    assert!(queued_generation.is_none());
}

#[test]
fn ws_event_fencing_handles_all_event_kinds() {
    let events = vec![
        GatewayWsEvent::Connecting {
            connection_id: 1,
            endpoint_id: "local".to_owned(),
            endpoint_name: "local".to_owned(),
            endpoint_kind: GatewayEndpointKind::Local,
        },
        GatewayWsEvent::Reconnecting {
            connection_id: 1,
            endpoint_id: "local".to_owned(),
            endpoint_name: "local".to_owned(),
            attempt: 2,
            delay_ms: 250,
            reason: "timeout".to_owned(),
        },
        GatewayWsEvent::Disconnected {
            connection_id: 1,
            endpoint_id: "local".to_owned(),
            endpoint_name: "local".to_owned(),
            endpoint_kind: GatewayEndpointKind::Local,
            address: "0.0.0.0:17878".to_owned(),
            reason: "closed".to_owned(),
        },
        GatewayWsEvent::ConnectFailed {
            connection_id: 1,
            endpoint_id: "local".to_owned(),
            endpoint_name: "local".to_owned(),
            endpoint_kind: GatewayEndpointKind::Local,
            address: "0.0.0.0:17878".to_owned(),
            error: "refused".to_owned(),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::ThreadStarted(
                pioneer_protocol::ThreadStartedNotification {
                    thread: pioneer_protocol::Thread {
                        id: "thr_123".to_owned(),
                        workspace_id: "ws_000000000000000001".to_owned(),
                        name: None,
                        preview: String::new(),
                        mode: pioneer_protocol::ThreadMode::Chat,
                        model: "gpt-5.4".to_owned(),
                        model_provider: "openai".to_owned(),
                        created_at: 0,
                        updated_at: 0,
                        status: pioneer_protocol::ThreadStatus::Idle,
                        origin_kind: pioneer_protocol::ThreadOriginKind::User,
                        sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
                        agent_nickname: None,
                        agent_role: None,
                        turns: Vec::new(),
                    },
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::TurnStarted(
                pioneer_protocol::TurnStartedNotification {
                    workspace_id: "ws_000000000000000001".to_owned(),
                    thread_id: "thr_123".to_owned(),
                    turn: pioneer_protocol::Turn {
                        id: "turn_123".to_owned(),
                        status: pioneer_protocol::TurnStatus::InProgress,
                        error: None,

                        prompt_manifest: None,
                    },
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::TurnCompleted(
                pioneer_protocol::TurnCompletedNotification {
                    workspace_id: "ws_000000000000000001".to_owned(),
                    thread_id: "thr_123".to_owned(),
                    turn: pioneer_protocol::Turn {
                        id: "turn_123".to_owned(),
                        status: pioneer_protocol::TurnStatus::Completed,
                        error: None,

                        prompt_manifest: None,
                    },
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::TurnFailed(
                pioneer_protocol::TurnFailedNotification {
                    workspace_id: "ws_000000000000000001".to_owned(),
                    thread_id: "thr_123".to_owned(),
                    turn: pioneer_protocol::Turn {
                        id: "turn_123".to_owned(),
                        status: pioneer_protocol::TurnStatus::Failed,
                        error: Some("failed".to_owned()),

                        prompt_manifest: None,
                    },
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::ItemStarted(
                pioneer_protocol::ItemStartedNotification {
                    workspace_id: "ws_000000000000000001".to_owned(),
                    thread_id: "thr_123".to_owned(),
                    turn_id: "turn_123".to_owned(),
                    item: pioneer_protocol::TurnItem::Reasoning {
                        id: "item_123".to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::ItemDelta(pioneer_protocol::ItemDeltaNotification {
                workspace_id: "ws_000000000000000001".to_owned(),
                thread_id: "thr_123".to_owned(),
                turn_id: "turn_123".to_owned(),
                item_id: "item_123".to_owned(),
                delta: "delta".to_owned(),
                stream: None,
                payload: None,
                markdown: None,
                markdown_version: None,
            }),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::ItemCompleted(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws_000000000000000001".to_owned(),
                    thread_id: "thr_123".to_owned(),
                    turn_id: "turn_123".to_owned(),
                    item: pioneer_protocol::TurnItem::AgentMessage {
                        id: "item_124".to_owned(),
                        text: "done".to_owned(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::Unknown(
                pioneer_protocol::UnknownGatewayNotification {
                    method: "item/new".to_owned(),
                    workspace_id: Some("ws_000000000000000001".to_owned()),
                    thread_id: Some("thr_123".to_owned()),
                    turn_id: Some("turn_123".to_owned()),
                    item_id: Some("item_125".to_owned()),
                    params: serde_json::json!({"foo":"bar"}),
                },
            ),
        },
        GatewayWsEvent::Notification {
            connection_id: 1,
            notification: GatewayNotification::ThreadClosed(
                pioneer_protocol::ThreadClosedNotification {
                    workspace_id: "ws_000000000000000001".to_owned(),
                    thread_id: "thr_123".to_owned(),
                },
            ),
        },
    ];

    for event in events {
        assert!(should_apply_ws_event(Some(1), &event));
        assert!(!should_apply_ws_event(Some(2), &event));
    }
}

#[test]
fn warning_notification_uses_friendly_path_message_for_path_update_warning() {
    let warnings = vec![GatewayInstallWarning {
        code: "path_update_skipped".to_owned(),
        message: "failed to update profile".to_owned(),
    }];

    let messages = warning_notification_messages(&warnings);
    assert_eq!(messages.len(), 1);
    assert!(messages[0].contains(default_user_command_bin_dir_label()));
    assert!(!messages[0].contains("failed to update profile"));
}

#[test]
fn warning_notification_keeps_one_message_per_warning() {
    let warnings = vec![
        GatewayInstallWarning {
            code: "path_update_skipped".to_owned(),
            message: "first".to_owned(),
        },
        GatewayInstallWarning {
            code: "path_update_skipped".to_owned(),
            message: "second".to_owned(),
        },
        GatewayInstallWarning {
            code: "other_warning".to_owned(),
            message: "third".to_owned(),
        },
    ];

    let messages = warning_notification_messages(&warnings);
    assert_eq!(messages.len(), 3);
    assert!(messages[0].contains(default_user_command_bin_dir_label()));
    assert!(messages[1].contains(default_user_command_bin_dir_label()));
    assert_eq!(messages[2], "third");
}

#[test]
fn thread_started_broadcast_does_not_activate_foreign_thread() {
    assert!(!should_accept_local_thread_started(None, "thr_foreign"));
    assert!(!should_accept_local_thread_started(
        Some("thr_local"),
        "thr_foreign"
    ));
}

#[test]
fn turn_and_item_events_apply_only_to_active_thread() {
    assert!(should_apply_thread_event(Some("thr_local"), "thr_local"));
    assert!(!should_apply_thread_event(Some("thr_local"), "thr_foreign"));
    assert!(!should_apply_thread_event(None, "thr_local"));

    assert!(should_apply_thread_event_optional(
        Some("thr_local"),
        Some("thr_local")
    ));
    assert!(!should_apply_thread_event_optional(
        Some("thr_local"),
        Some("thr_foreign")
    ));
    assert!(!should_apply_thread_event_optional(Some("thr_local"), None));
}

#[test]
fn thread_started_matches_local_active_thread() {
    assert!(should_accept_local_thread_started(
        Some("thr_local"),
        "thr_local"
    ));
}

#[test]
fn skills_changed_refreshes_only_for_active_workspace() {
    assert!(should_refresh_workspace_bound_data(
        Some("ws_000000000000000001"),
        "ws_000000000000000001"
    ));
    assert!(!should_refresh_workspace_bound_data(
        Some("ws_000000000000000001"),
        "ws_000000000000000002"
    ));
    assert!(!should_refresh_workspace_bound_data(
        None,
        "ws_000000000000000001"
    ));
}

#[test]
fn concurrent_clients_keep_local_active_thread_on_broadcasts() {
    let client_a_active = Some("thr_client_a".to_owned());
    let client_b_active = Some("thr_client_b".to_owned());

    for started_thread_id in ["thr_client_a", "thr_client_b", "thr_client_a"] {
        let _ = should_accept_local_thread_started(client_a_active.as_deref(), started_thread_id);
        let _ = should_accept_local_thread_started(client_b_active.as_deref(), started_thread_id);
    }

    assert_eq!(client_a_active.as_deref(), Some("thr_client_a"));
    assert_eq!(client_b_active.as_deref(), Some("thr_client_b"));
}

#[test]
fn thread_start_retry_delay_is_exponential_and_capped() {
    assert_eq!(thread_start_retry_delay(0), Duration::from_millis(500));
    assert_eq!(thread_start_retry_delay(1), Duration::from_millis(1_000));
    assert_eq!(thread_start_retry_delay(2), Duration::from_millis(2_000));
    assert_eq!(thread_start_retry_delay(3), Duration::from_millis(4_000));
    assert_eq!(thread_start_retry_delay(4), Duration::from_millis(5_000));
    assert_eq!(thread_start_retry_delay(20), Duration::from_millis(5_000));
}

#[test]
fn turn_resume_retry_delay_is_exponential_and_capped() {
    assert_eq!(turn_resume_retry_delay(0), Duration::from_millis(800));
    assert_eq!(turn_resume_retry_delay(1), Duration::from_millis(1_600));
    assert_eq!(turn_resume_retry_delay(2), Duration::from_millis(3_200));
    assert_eq!(turn_resume_retry_delay(3), Duration::from_millis(5_000));
    assert_eq!(turn_resume_retry_delay(20), Duration::from_millis(5_000));
}

#[test]
fn reconnect_replay_restores_final_turn_result() {
    let mut conversation = Conversation::new("thr_local");
    let recovery_policy = ToolRecoveryPolicySnapshot {
        retry_class: ToolRecoveryRetryClass::Network,
        idempotency_mode: ToolRecoveryIdempotencyMode::Safe,
        max_attempts: 3,
        can_resume: true,
        resolved_action: RecoveryAction::RetryWithBackoff,
        base_backoff_secs: 3,
        max_wall_clock_secs: 240,
        no_progress_limit: 3,
    };

    conversation.apply(ConversationEvent::LocalTurnStartRequested {
        thread_id: "thr_local".to_owned(),
        turn_id: "turn_local".to_owned(),
        pending_request_id: "req_local".to_owned(),
        user_text: "hello".to_owned(),
        attachments: Vec::new(),
    });
    assert!(!conversation.can_submit_message());

    let replay_events = vec![
        TurnItemEventPayload::ItemStarted {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item: TurnItem::Reasoning {
                id: "item_reasoning".to_owned(),
                summary: vec![],
                content: vec![],
            },
        },
        TurnItemEventPayload::ItemCompleted {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item: TurnItem::Reasoning {
                id: "item_reasoning".to_owned(),
                summary: vec!["done".to_owned()],
                content: vec![],
            },
        },
        TurnItemEventPayload::ItemStarted {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item: TurnItem::WebFetch {
                id: "item_fetch".to_owned(),
                tool_name: "web_fetch".to_owned(),
                arguments: serde_json::json!({"url": "https://example.com"}),
                status: ToolCallStatus::InProgress,
                recovery_policy: Some(recovery_policy.clone()),
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
        },
        TurnItemEventPayload::ItemStarted {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item: TurnItem::AgentMessage {
                id: "item_answer".to_owned(),
                text: String::new(),
                markdown: None,
                markdown_version: None,
            },
        },
        TurnItemEventPayload::ItemDelta {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item_id: "item_answer".to_owned(),
            delta: "hello".to_owned(),
            stream: None,
            payload: None,
            markdown: None,
            markdown_version: None,
        },
        TurnItemEventPayload::ItemCompleted {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item: TurnItem::AgentMessage {
                id: "item_answer".to_owned(),
                text: "hello".to_owned(),
                markdown: None,
                markdown_version: None,
            },
        },
    ];

    for payload in replay_events {
        match payload {
            TurnItemEventPayload::ItemStarted {
                workspace_id,
                thread_id,
                turn_id,
                item,
            } => {
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str(),
                ));
                assert_eq!(workspace_id, "ws_local");
                conversation.apply(ConversationEvent::ItemStarted {
                    thread_id,
                    turn_id,
                    item,
                });
            }
            TurnItemEventPayload::ItemDelta {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                delta,
                stream,
                payload,
                markdown,
                markdown_version,
            } => {
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str(),
                ));
                assert_eq!(workspace_id, "ws_local");
                conversation.apply(ConversationEvent::ItemDelta {
                    thread_id,
                    turn_id,
                    item_id,
                    delta,
                    stream,
                    payload,
                    markdown,
                    markdown_version,
                });
            }
            TurnItemEventPayload::ItemCompleted {
                workspace_id,
                thread_id,
                turn_id,
                item,
            } => {
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str(),
                ));
                assert_eq!(workspace_id, "ws_local");
                conversation.apply(ConversationEvent::ItemCompleted {
                    thread_id,
                    turn_id,
                    item,
                });
            }
            TurnItemEventPayload::ItemUpdated {
                workspace_id,
                thread_id,
                turn_id,
                item,
            } => {
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str(),
                ));
                assert_eq!(workspace_id, "ws_local");
                conversation.apply(ConversationEvent::ItemUpdated {
                    thread_id,
                    turn_id,
                    item,
                });
            }
            TurnItemEventPayload::ItemTimeoutDetected { .. }
            | TurnItemEventPayload::ItemRecoveryOpened { .. }
            | TurnItemEventPayload::ItemRecoveryAttached { .. }
            | TurnItemEventPayload::ItemRetryScheduled { .. }
            | TurnItemEventPayload::ItemRetryAttemptStarted { .. }
            | TurnItemEventPayload::ItemRecoverySucceeded { .. }
            | TurnItemEventPayload::ItemRecoveryExhausted { .. }
            | TurnItemEventPayload::ItemToolRetryScheduled { .. }
            | TurnItemEventPayload::ItemToolRetryResolved { .. }
            | TurnItemEventPayload::ItemToolRetryExhausted { .. }
            | TurnItemEventPayload::TurnToolLoopBudgetExceeded { .. } => {
                unreachable!("unexpected lifecycle replay event in basic turn resume test")
            }
        }
    }

    let completed_turn = Turn {
        id: "turn_local".to_owned(),
        status: TurnStatus::Completed,
        error: None,
        prompt_manifest: None,
    };
    conversation.apply(ConversationEvent::TurnCompleted {
        thread_id: "thr_local".to_owned(),
        turn: completed_turn,
    });
    assert_eq!(conversation.status_label(), "completing");
    assert!(!conversation.can_submit_message());

    assert!(conversation.tick());
    assert_eq!(conversation.status_label(), "completed");
    assert!(conversation.can_submit_message());

    let answer = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_answer")
        .expect("assistant answer should exist after replay");
    assert_eq!(answer.final_text.as_deref(), Some("hello"));
    assert_eq!(answer.status, TimelineEntryStatus::Completed);
    let fetch = conversation
        .projection()
        .items
        .iter()
        .find(|item| item.id == "item_fetch")
        .expect("turn/items replay should preserve tool item");
    assert_eq!(fetch.item.recovery_policy(), Some(&recovery_policy));
}

fn sample_tool_retry_budget_usage() -> Vec<ToolRetryBudgetUsage> {
    vec![ToolRetryBudgetUsage {
        kind: ToolRetryBudgetKind::Episode,
        used: 1,
        limit: 2,
    }]
}

#[test]
fn turn_items_replay_applies_tool_retry_lifecycle_like_live_events() {
    let mut live = Conversation::new("thr_local");
    let mut replay = Conversation::new("thr_local");
    let turn = Turn {
        id: "turn_local".to_owned(),
        status: TurnStatus::InProgress,
        error: None,
        prompt_manifest: None,
    };

    for conversation in [&mut live, &mut replay] {
        conversation.apply(ConversationEvent::TurnStarted {
            thread_id: "thr_local".to_owned(),
            turn: turn.clone(),
        });
    }

    live.apply(ConversationEvent::ItemToolRetryScheduled {
        thread_id: "thr_local".to_owned(),
        turn_id: "turn_local".to_owned(),
        item_id: "item_fetch".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_local_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 1,
        error_class: ToolRetryErrorClass::Timeout,
        retry_hint: "retry with a smaller request".to_owned(),
        budgets: sample_tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_fetch_timeout".to_owned(),
        reason: "recoverable_tool_output".to_owned(),
    });
    live.apply(ConversationEvent::ItemToolRetryResolved {
        thread_id: "thr_local".to_owned(),
        turn_id: "turn_local".to_owned(),
        item_id: "item_fetch".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_local_1".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 2,
        resolution: ToolRetryResolution::Succeeded,
        budgets: sample_tool_retry_budget_usage(),
        reason: "successful_tool_output".to_owned(),
    });
    live.apply(ConversationEvent::ItemToolRetryExhausted {
        thread_id: "thr_local".to_owned(),
        turn_id: "turn_local".to_owned(),
        item_id: "item_fetch".to_owned(),
        item_type: TurnItemType::WebFetch,
        tool_retry_episode_id: "tool_retry_turn_local_2".to_owned(),
        tool_name: "web_fetch".to_owned(),
        attempt_number: 3,
        error_class: ToolRetryErrorClass::Timeout,
        exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
        budgets: sample_tool_retry_budget_usage(),
        failure_signature_fingerprint: "sig_fetch_timeout".to_owned(),
        reason: "same_failure_signature".to_owned(),
    });
    live.apply(ConversationEvent::TurnToolLoopBudgetExceeded {
        thread_id: "thr_local".to_owned(),
        turn_id: "turn_local".to_owned(),
        limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
        limit: 512,
        observed: 512,
        action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
        reason: "max_agent_rounds_per_turn".to_owned(),
    });

    let replay_payloads = vec![
        TurnItemEventPayload::ItemToolRetryScheduled {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item_id: "item_fetch".to_owned(),
            item_type: TurnItemType::WebFetch,
            tool_retry_episode_id: "tool_retry_turn_local_1".to_owned(),
            tool_name: "web_fetch".to_owned(),
            attempt_number: 1,
            error_class: ToolRetryErrorClass::Timeout,
            retry_hint: "retry with a smaller request".to_owned(),
            budgets: sample_tool_retry_budget_usage(),
            failure_signature_fingerprint: "sig_fetch_timeout".to_owned(),
            reason: "recoverable_tool_output".to_owned(),
        },
        TurnItemEventPayload::ItemToolRetryResolved {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item_id: "item_fetch".to_owned(),
            item_type: TurnItemType::WebFetch,
            tool_retry_episode_id: "tool_retry_turn_local_1".to_owned(),
            tool_name: "web_fetch".to_owned(),
            attempt_number: 2,
            resolution: ToolRetryResolution::Succeeded,
            budgets: sample_tool_retry_budget_usage(),
            reason: "successful_tool_output".to_owned(),
        },
        TurnItemEventPayload::ItemToolRetryExhausted {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            item_id: "item_fetch".to_owned(),
            item_type: TurnItemType::WebFetch,
            tool_retry_episode_id: "tool_retry_turn_local_2".to_owned(),
            tool_name: "web_fetch".to_owned(),
            attempt_number: 3,
            error_class: ToolRetryErrorClass::Timeout,
            exhaustion_kind: ToolRetryExhaustionKind::FailureSignature,
            budgets: sample_tool_retry_budget_usage(),
            failure_signature_fingerprint: "sig_fetch_timeout".to_owned(),
            reason: "same_failure_signature".to_owned(),
        },
        TurnItemEventPayload::TurnToolLoopBudgetExceeded {
            workspace_id: "ws_local".to_owned(),
            thread_id: "thr_local".to_owned(),
            turn_id: "turn_local".to_owned(),
            limit_kind: ToolLoopBudgetLimitKind::AgentRounds,
            limit: 512,
            observed: 512,
            action: ToolLoopBudgetAction::RequestFinalNoToolsRound,
            reason: "max_agent_rounds_per_turn".to_owned(),
        },
    ];

    for payload in replay_payloads {
        match payload {
            TurnItemEventPayload::ItemToolRetryScheduled {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                error_class,
                retry_hint,
                budgets,
                failure_signature_fingerprint,
                reason,
            } => {
                assert_eq!(workspace_id, "ws_local");
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str()
                ));
                replay.apply(ConversationEvent::ItemToolRetryScheduled {
                    thread_id,
                    turn_id,
                    item_id,
                    item_type,
                    tool_retry_episode_id,
                    tool_name,
                    attempt_number,
                    error_class,
                    retry_hint,
                    budgets,
                    failure_signature_fingerprint,
                    reason,
                });
            }
            TurnItemEventPayload::ItemToolRetryResolved {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                resolution,
                budgets,
                reason,
            } => {
                assert_eq!(workspace_id, "ws_local");
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str()
                ));
                replay.apply(ConversationEvent::ItemToolRetryResolved {
                    thread_id,
                    turn_id,
                    item_id,
                    item_type,
                    tool_retry_episode_id,
                    tool_name,
                    attempt_number,
                    resolution,
                    budgets,
                    reason,
                });
            }
            TurnItemEventPayload::ItemToolRetryExhausted {
                workspace_id,
                thread_id,
                turn_id,
                item_id,
                item_type,
                tool_retry_episode_id,
                tool_name,
                attempt_number,
                error_class,
                exhaustion_kind,
                budgets,
                failure_signature_fingerprint,
                reason,
            } => {
                assert_eq!(workspace_id, "ws_local");
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str()
                ));
                replay.apply(ConversationEvent::ItemToolRetryExhausted {
                    thread_id,
                    turn_id,
                    item_id,
                    item_type,
                    tool_retry_episode_id,
                    tool_name,
                    attempt_number,
                    error_class,
                    exhaustion_kind,
                    budgets,
                    failure_signature_fingerprint,
                    reason,
                });
            }
            TurnItemEventPayload::TurnToolLoopBudgetExceeded {
                workspace_id,
                thread_id,
                turn_id,
                limit_kind,
                limit,
                observed,
                action,
                reason,
            } => {
                assert_eq!(workspace_id, "ws_local");
                assert!(should_apply_thread_event(
                    Some("thr_local"),
                    thread_id.as_str()
                ));
                replay.apply(ConversationEvent::TurnToolLoopBudgetExceeded {
                    thread_id,
                    turn_id,
                    limit_kind,
                    limit,
                    observed,
                    action,
                    reason,
                });
            }
            _ => unreachable!("test only builds tool retry lifecycle replay events"),
        }
    }

    let live_system_events = live
        .projection()
        .items
        .iter()
        .filter(|item| item.item_type == "system_event")
        .map(|item| item.partial_text.clone())
        .collect::<Vec<_>>();
    let replay_system_events = replay
        .projection()
        .items
        .iter()
        .filter(|item| item.item_type == "system_event")
        .map(|item| item.partial_text.clone())
        .collect::<Vec<_>>();
    assert_eq!(live.status_label(), replay.status_label());
    assert_eq!(live_system_events, replay_system_events);
    let retry_scheduled = t!(
        "timeline.system.tool_retry_scheduled_with_attempt",
        tool_name = "web_fetch",
        attempt = 1
    )
    .to_string();
    assert!(
        replay_system_events
            .iter()
            .any(|text| text.contains(retry_scheduled.as_str()))
    );
    assert!(
        replay_system_events
            .iter()
            .all(|text| !text.contains("recovery job"))
    );
}

#[test]
fn transient_thread_start_error_classifier_ignores_decode_errors() {
    assert!(is_transient_thread_start_error(&anyhow::anyhow!(
        "timed out waiting for `thread/start` response"
    )));
    assert!(!is_transient_thread_start_error(&anyhow::anyhow!(
        "failed to decode `thread/start` response payload"
    )));
}

#[test]
fn workspace_id_normalization_trims_and_rejects_empty_values() {
    assert_eq!(
        normalize_workspace_id(Some("  ws_trimmed  ".to_owned())).as_deref(),
        Some("ws_trimmed")
    );
    assert!(normalize_workspace_id(Some("   ".to_owned())).is_none());
    assert!(normalize_workspace_id(None).is_none());
}
