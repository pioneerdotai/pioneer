use pioneer_client::{
    conversation::{Conversation, ConversationEvent, TimelineEntryStatus},
    gateway::{
        registry::{GatewayRegistryConfig, default_registry, normalize_registry},
        timings::GatewayWsTimings,
        types::{GatewayEndpoint, GatewayEndpointKind},
    },
    threads::resume::{
        ThreadResumeCoordinator, TurnResumeQueueConnectionPlan, TurnResumeQueueItemPlan,
        apply_turn_resume_retry, begin_turn_resume_attempt, finish_turn_resume_attempt,
        plan_turn_resume_queue_connection, plan_turn_resume_queue_item, turn_resume_retry_delay,
    },
    timeline::rows::{TimelineRowKind, build_timeline_rows},
    transport::ws::{
        GatewayWsConnectSpec, GatewayWsEvent, rpc::build_ws_request, rpc::normalize_ws_url,
        should_apply_ws_event, worker,
    },
};
use pioneer_protocol::{
    GatewayNotification, JsonRpcNotification, ThreadHistoryResponse, Turn, TurnItem, TurnStatus,
};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

const WORKSPACE_ID: &str = "ws_phase29";
const THREAD_ID: &str = "thr_phase29";
const TURN_ID: &str = "turn_phase29";

fn turn(id: &str, status: TurnStatus) -> Turn {
    Turn {
        id: id.to_owned(),
        status,
        turn_kind: Default::default(),
        origin: Default::default(),
        error: None,
        prompt_manifest: None,
    }
}

fn remote_spec() -> GatewayWsConnectSpec {
    GatewayWsConnectSpec {
        endpoint_id: "remote-prod".to_owned(),
        endpoint_name: "Remote Prod".to_owned(),
        endpoint_kind: GatewayEndpointKind::Remote,
        address: "wss://gateway.example.com/socket".to_owned(),
        auth_token: Some("remote-token".to_owned()),
        timings: GatewayWsTimings::from_millis(100, 200, 300, 400, 5_000, 0).expect("timings"),
    }
}

#[test]
fn fixture_thread_history_replays_into_timeline_read_model() {
    let fixture: ThreadHistoryResponse =
        serde_json::from_str(include_str!("fixtures/phase29/thread_history_basic.json"))
            .expect("thread history fixture should decode");
    assert_eq!(fixture.thread_id, THREAD_ID);

    let mut conversation = Conversation::new(fixture.thread_id.as_str());
    conversation.hydrate_history(fixture.events.as_slice());

    assert_eq!(conversation.status_label(), "completed");
    assert!(conversation.can_submit_message());
    assert_eq!(conversation.projection().turns.len(), 1);
    assert_eq!(conversation.projection().items.len(), 2);
    assert_eq!(conversation.projection().timeline.len(), 2);

    let agent = conversation
        .projection()
        .item_by_id("agent_phase29")
        .expect("agent item should project");
    assert_eq!(agent.status, TimelineEntryStatus::Completed);
    assert_eq!(agent.partial_text, "Hello from the phase 29 fixture");
}

#[test]
fn fixture_gateway_notifications_map_known_and_unknown_events() {
    let notifications: Vec<JsonRpcNotification> =
        serde_json::from_str(include_str!("fixtures/phase29/gateway_notifications.json"))
            .expect("gateway notification fixture should decode");

    let mapped = notifications
        .into_iter()
        .filter_map(GatewayNotification::from_jsonrpc)
        .collect::<Vec<_>>();

    assert_eq!(mapped.len(), 3);
    assert!(matches!(
        mapped[0],
        GatewayNotification::WorkspaceChanged(_)
    ));
    assert!(matches!(mapped[1], GatewayNotification::TurnStarted(_)));
    match &mapped[2] {
        GatewayNotification::Unknown(notification) => {
            assert_eq!(notification.method, "turn/future_event");
            assert_eq!(notification.workspace_id.as_deref(), Some(WORKSPACE_ID));
            assert_eq!(notification.thread_id.as_deref(), Some(THREAD_ID));
            assert_eq!(notification.turn_id.as_deref(), Some(TURN_ID));
        }
        other => panic!("expected unknown future turn event, got {other:?}"),
    }
}

#[test]
fn large_timeline_projection_has_stable_row_build_time() {
    const WORK_ITEM_COUNT: usize = 900;

    let mut conversation = Conversation::new(THREAD_ID);
    conversation.apply(ConversationEvent::TurnStarted {
        thread_id: THREAD_ID.to_owned(),
        turn: turn(TURN_ID, TurnStatus::InProgress),
    });
    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::UserMessage {
            id: "user_phase29".to_owned(),
            text: "Run a large plan".to_owned(),
            attachments: Vec::new(),
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::UserMessage {
            id: "user_phase29".to_owned(),
            text: "Run a large plan".to_owned(),
            attachments: Vec::new(),
        },
    });

    for index in 0..WORK_ITEM_COUNT {
        let item = TurnItem::Reasoning {
            id: format!("reasoning_{index:04}"),
            summary: vec![format!("step {index}")],
            content: vec![format!("detail {index}")],
        };
        conversation.apply(ConversationEvent::ItemStarted {
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            item: item.clone(),
        });
        conversation.apply(ConversationEvent::ItemCompleted {
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            item,
        });
    }

    conversation.apply(ConversationEvent::ItemStarted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "agent_final".to_owned(),
            text: String::new(),
            markdown: None,
            markdown_version: None,
        },
    });
    conversation.apply(ConversationEvent::ItemCompleted {
        thread_id: THREAD_ID.to_owned(),
        turn_id: TURN_ID.to_owned(),
        item: TurnItem::AgentMessage {
            id: "agent_final".to_owned(),
            text: "Done".to_owned(),
            markdown: None,
            markdown_version: None,
        },
    });

    assert_eq!(conversation.projection().items.len(), WORK_ITEM_COUNT + 2);

    let started = Instant::now();
    let collapsed_rows = build_timeline_rows(conversation.projection(), &HashSet::new());
    let collapsed_elapsed = started.elapsed();

    assert!(
        collapsed_elapsed < Duration::from_secs(2),
        "large collapsed timeline row build took {collapsed_elapsed:?}"
    );
    assert!(collapsed_rows.iter().any(|row| {
        matches!(
            row.kind,
            TimelineRowKind::TurnWorkToggle(_) | TimelineRowKind::CoalescedTools(_)
        )
    }));

    let toggle_key = collapsed_rows
        .iter()
        .find_map(|row| match &row.kind {
            TimelineRowKind::TurnWorkToggle(group) => Some(group.toggle_key.clone()),
            _ => None,
        })
        .expect("large work group should expose a toggle");
    let mut expanded = HashSet::new();
    expanded.insert(toggle_key);

    let started = Instant::now();
    let expanded_rows = build_timeline_rows(conversation.projection(), &expanded);
    let expanded_elapsed = started.elapsed();

    assert!(
        expanded_elapsed < Duration::from_secs(2),
        "large expanded timeline row build took {expanded_elapsed:?}"
    );
    assert!(
        expanded_rows.len() > WORK_ITEM_COUNT,
        "expanded large timeline should expose grouped work rows"
    );
}

#[test]
fn reconnect_resume_chaos_filters_stale_ws_events_and_bounds_resume_retries() {
    let spec = remote_spec();

    let stale_event = worker::connected_event(10, &spec);
    let active_event = worker::connecting_event(11, &spec);
    assert!(!should_apply_ws_event(Some(11), &stale_event));
    assert!(should_apply_ws_event(Some(11), &active_event));

    let first_reconnect = worker::next_reconnect_plan(
        11,
        &spec,
        0,
        Duration::from_millis(400),
        "closed".to_owned(),
    );
    let second_reconnect = worker::next_reconnect_plan(
        11,
        &spec,
        first_reconnect.attempt,
        first_reconnect.next_backoff,
        "pong timeout".to_owned(),
    );

    assert_eq!(first_reconnect.attempt, 1);
    assert_eq!(first_reconnect.delay, Duration::from_millis(400));
    assert_eq!(second_reconnect.attempt, 2);
    assert_eq!(second_reconnect.delay, Duration::from_millis(800));
    assert!(matches!(
        second_reconnect.event,
        GatewayWsEvent::Reconnecting {
            connection_id: 11,
            attempt: 2,
            reason,
            ..
        } if reason == "pong timeout"
    ));

    assert_eq!(
        plan_turn_resume_queue_connection(None, true),
        TurnResumeQueueConnectionPlan::NotReady
    );
    assert_eq!(
        plan_turn_resume_queue_connection(Some(11), true),
        TurnResumeQueueConnectionPlan::Drive { connection_id: 11 }
    );

    let now = Instant::now();
    let mut resume = ThreadResumeCoordinator::default();
    begin_turn_resume_attempt(&mut resume);
    assert_eq!(
        plan_turn_resume_queue_item(resume.in_progress, true),
        TurnResumeQueueItemPlan::Skip
    );
    finish_turn_resume_attempt(&mut resume);

    let first_retry = apply_turn_resume_retry(Some(&mut resume), now);
    let second_retry = apply_turn_resume_retry(Some(&mut resume), now);
    let capped_retry = apply_turn_resume_retry(Some(&mut resume), now);

    assert_eq!(first_retry.attempt, 1);
    assert_eq!(first_retry.delay, turn_resume_retry_delay(0));
    assert_eq!(second_retry.attempt, 2);
    assert_eq!(second_retry.delay, turn_resume_retry_delay(1));
    assert!(capped_retry.delay <= Duration::from_secs(5));
}

#[test]
fn remote_gateway_attachment_smoke_builds_authenticated_ws_request() {
    let config = GatewayRegistryConfig {
        version: 1,
        local_gateway_id: "local".to_owned(),
        local_name: "Local Gateway".to_owned(),
        local_address: "127.0.0.1:17878".to_owned(),
        local_auth_token_ref: None,
        local_service_name: Some("com.pioneer.gateway".to_owned()),
    };
    let mut registry = default_registry(&config);
    registry.remotes.push(GatewayEndpoint {
        id: "remote-prod".to_owned(),
        name: "Remote Prod".to_owned(),
        address: "wss://gateway.example.com/socket".to_owned(),
        kind: GatewayEndpointKind::Local,
        auth_token_ref: Some("remote-prod".to_owned()),
        workspace_id: Some(WORKSPACE_ID.to_owned()),
        service_name: Some("must-be-cleared".to_owned()),
    });

    normalize_registry(
        &mut registry,
        &config,
        |endpoint_id| Ok(format!("secret:{endpoint_id}")),
        |index| format!("Remote Gateway {index}"),
    )
    .expect("registry should normalize");

    let remote = registry.remotes.first().expect("remote endpoint");
    assert_eq!(remote.kind, GatewayEndpointKind::Remote);
    assert_eq!(remote.service_name, None);
    assert_eq!(remote.auth_token_ref.as_deref(), Some("secret:remote-prod"));

    let ws_url = normalize_ws_url(remote.address.as_str());
    let request = build_ws_request(ws_url.as_str(), Some(" remote-token ")).expect("ws request");

    assert_eq!(
        request.uri().to_string(),
        "wss://gateway.example.com/socket"
    );
    assert_eq!(
        request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer remote-token")
    );
}
