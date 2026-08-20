use pioneer_client::{
    gateway::{
        endpoint::{GatewayBaseUrl, PIONEER_PROTOCOL_VERSION, PIONEER_PROTOCOL_VERSION_HEADER},
        registry::{
            GatewayLocalRegistryConfig, GatewayRegistryConfig, default_registry, normalize_registry,
        },
        timings::GatewayWsTimings,
        types::{GatewayEndpoint, GatewayEndpointKind},
    },
    threads::resume::{
        ThreadResumeCoordinator, TurnResumeQueueConnectionPlan, TurnResumeQueueItemPlan,
        apply_turn_resume_retry, begin_turn_resume_attempt, finish_turn_resume_attempt,
        plan_turn_resume_queue_connection, plan_turn_resume_queue_item, turn_resume_retry_delay,
    },
    timeline::semantic::{
        SemanticTimelineRequestHint, SemanticTimelineRowKind, SemanticTimelineState,
        TopLevelPageMergeMode, WorkPageMergeMode, apply_thread_timeline_page, apply_turn_work_page,
        expand_turn_work, flatten_semantic_timeline,
    },
    transport::ws::{
        GatewayWsConnectSpec, GatewayWsEvent, rpc::build_ws_request, should_apply_ws_event, worker,
    },
};
use pioneer_protocol::{
    GatewayNotification, JsonRpcNotification, ThreadTimelinePageResponse, TimelineBlock,
    TimelineBlockKind, TimelineCursor, TimelinePageInfo, TurnItem, TurnItemType, TurnWorkBlock,
    TurnWorkItem, TurnWorkItemStatus, TurnWorkPageResponse, TurnWorkPresentation, TurnWorkState,
};
use std::time::{Duration, Instant};

const WORKSPACE_ID: &str = "ws_phase29";
const THREAD_ID: &str = "thr_phase29";
const TURN_ID: &str = "turn_phase29";

fn remote_spec() -> GatewayWsConnectSpec {
    GatewayWsConnectSpec {
        endpoint_id: "remote-prod".to_owned(),
        endpoint_name: "Remote Prod".to_owned(),
        endpoint_kind: GatewayEndpointKind::Remote,
        gateway_base_url: GatewayBaseUrl::parse_presentation("https://gateway.example.com/pioneer")
            .unwrap(),
        auth_token: Some(pioneer_protocol::AuthSecretString::new("remote-token")),
        session: None,
        timings: GatewayWsTimings::from_millis(100, 200, 300, 400, 5_000, 0).expect("timings"),
    }
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
fn large_semantic_timeline_uses_stable_blocks_and_paged_work() {
    const TOTAL_WORK_ITEM_COUNT: u64 = 70_000;
    const LOADED_WORK_ITEM_COUNT: usize = 100;

    let mut state = SemanticTimelineState::default();

    let started = Instant::now();
    assert!(apply_thread_timeline_page(
        &mut state,
        ThreadTimelinePageResponse {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            projection_version: 1,
            blocks: vec![
                user_message_block("0001"),
                turn_work_block(
                    TurnWorkPresentation::CollapsedAfterFinal,
                    TOTAL_WORK_ITEM_COUNT
                ),
                assistant_message_block("0003"),
            ],
            page: page_info(None, None, false, false),
        },
        TopLevelPageMergeMode::Reset,
    ));
    let collapsed_rows =
        flatten_semantic_timeline(&state, THREAD_ID).expect("semantic rows should exist");
    let collapsed_elapsed = started.elapsed();

    assert!(
        collapsed_elapsed < Duration::from_secs(2),
        "large collapsed semantic timeline flatten took {collapsed_elapsed:?}"
    );
    assert_eq!(
        collapsed_rows.rows.len(),
        3,
        "top-level semantic timeline should stay block-sized even for huge work turns"
    );
    assert!(matches!(
        &collapsed_rows.rows[1].kind,
        SemanticTimelineRowKind::WorkHeader {
            expanded: false,
            work,
            ..
        } if work.work_count == TOTAL_WORK_ITEM_COUNT
    ));
    assert!(
        collapsed_rows
            .request_hints
            .iter()
            .all(|hint| { !matches!(hint, SemanticTimelineRequestHint::TurnWorkInitial { .. }) }),
        "collapsed final work must not eagerly request the full work range"
    );

    assert!(expand_turn_work(&mut state, THREAD_ID, TURN_ID));
    let expanded_without_items =
        flatten_semantic_timeline(&state, THREAD_ID).expect("expanded rows should exist");
    assert_eq!(expanded_without_items.rows.len(), 3);
    assert!(
        expanded_without_items.request_hints.iter().any(|hint| {
            matches!(hint, SemanticTimelineRequestHint::TurnWorkInitial { turn_id, .. } if turn_id == TURN_ID)
        }),
        "explicit expansion should request one bounded initial work page"
    );

    let started = Instant::now();
    assert!(apply_turn_work_page(
        &mut state,
        TurnWorkPageResponse {
            workspace_id: WORKSPACE_ID.to_owned(),
            thread_id: THREAD_ID.to_owned(),
            turn_id: TURN_ID.to_owned(),
            projection_version: 1,
            source_high_watermark: 100,
            projection_updated_at_unix_micros: 100,
            work: turn_work(
                TurnWorkPresentation::CollapsedAfterFinal,
                TOTAL_WORK_ITEM_COUNT
            ),
            items: (0..LOADED_WORK_ITEM_COUNT).map(work_item).collect(),
            page: page_info(None, Some(format!("{}:work:0100", THREAD_ID)), false, true,),
        },
        WorkPageMergeMode::Reset,
    ));
    let expanded_rows =
        flatten_semantic_timeline(&state, THREAD_ID).expect("expanded rows should exist");
    let expanded_elapsed = started.elapsed();

    assert!(
        expanded_elapsed < Duration::from_secs(2),
        "large expanded semantic timeline flatten took {expanded_elapsed:?}"
    );
    let visible_work_rows = expanded_rows
        .rows
        .iter()
        .filter(|row| matches!(row.kind, SemanticTimelineRowKind::WorkItem { .. }))
        .count();
    assert_eq!(visible_work_rows, LOADED_WORK_ITEM_COUNT);
    assert_eq!(expanded_rows.rows.len(), LOADED_WORK_ITEM_COUNT + 3);
    assert!(
        expanded_rows.request_hints.iter().any(|hint| {
            matches!(hint, SemanticTimelineRequestHint::TurnWorkAfter { turn_id, .. } if turn_id == TURN_ID)
        }),
        "loaded work page should expose a cursor hint instead of forcing full catchup"
    );
}

fn user_message_block(sort_key: &str) -> TimelineBlock {
    TimelineBlock {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        block_id: "block_user_phase29".to_owned(),
        turn_id: Some(TURN_ID.to_owned()),
        sort_key: sort_key.to_owned(),
        started_at_unix_ms: Some(1_000),
        updated_at_unix_ms: Some(1_000),
        kind: TimelineBlockKind::UserMessage {
            item_id: Some("user_phase29".to_owned()),
            inputs: Vec::new(),
            text: "Run a large plan".to_owned(),
            attachments: Vec::new(),
            mode: Default::default(),
            author: None,
            route: None,
            reply: None,
            mentions: Vec::new(),
            revision: 0,
            edited: false,
            deleted: false,
        },
    }
}

fn turn_work_block(presentation: TurnWorkPresentation, work_count: u64) -> TimelineBlock {
    TimelineBlock {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        block_id: "block_work_phase29".to_owned(),
        turn_id: Some(TURN_ID.to_owned()),
        sort_key: "0002".to_owned(),
        started_at_unix_ms: Some(1_001),
        updated_at_unix_ms: Some(1_500),
        kind: TimelineBlockKind::TurnWork {
            work: turn_work(presentation, work_count),
        },
    }
}

fn turn_work(presentation: TurnWorkPresentation, work_count: u64) -> TurnWorkBlock {
    TurnWorkBlock {
        turn_id: TURN_ID.to_owned(),
        presentation,
        state: TurnWorkState::Completed,
        agent_work_graph: None,
        started_at_unix_ms: Some(1_001),
        completed_at_unix_ms: Some(1_500),
        elapsed_ms: Some(499),
        work_count,
        visible_work_count: work_count,
        hidden_work_count: 0,
        has_more_before: false,
        has_more_after: work_count > 0,
        before_cursor: None,
        after_cursor: Some(TimelineCursor {
            value: format!("{THREAD_ID}:work:last"),
        }),
        first_work_item_id: Some("work_0000".to_owned()),
        last_work_item_id: Some(format!("work_{:04}", work_count.saturating_sub(1))),
    }
}

fn assistant_message_block(sort_key: &str) -> TimelineBlock {
    TimelineBlock {
        workspace_id: WORKSPACE_ID.to_owned(),
        thread_id: THREAD_ID.to_owned(),
        block_id: "block_assistant_phase29".to_owned(),
        turn_id: Some(TURN_ID.to_owned()),
        sort_key: sort_key.to_owned(),
        started_at_unix_ms: Some(1_501),
        updated_at_unix_ms: Some(1_600),
        kind: TimelineBlockKind::AssistantMessage {
            item_id: "agent_final".to_owned(),
            text: "Done".to_owned(),
            status: TurnWorkItemStatus::Completed,
            markdown: Some(pioneer_protocol::MarkdownDocument::from_plain_text("Done")),
            author: None,
            route: None,
        },
    }
}

fn work_item(index: usize) -> TurnWorkItem {
    TurnWorkItem {
        work_item_id: format!("work_{index:04}"),
        item_id: format!("reasoning_{index:04}"),
        turn_id: TURN_ID.to_owned(),
        order_key: format!("{index:08}"),
        source_sequence: index as i64,
        source_updated_at_unix_micros: index as i64,
        item_type: TurnItemType::Reasoning,
        status: TurnWorkItemStatus::Completed,
        started_at_unix_ms: Some(1_001 + index as i64),
        completed_at_unix_ms: Some(1_001 + index as i64),
        item: TurnItem::Reasoning {
            id: format!("reasoning_{index:04}"),
            summary: vec![format!("step {index}")],
            content: vec![format!("detail {index}")],
        },
        metadata: None,
    }
}

fn page_info(
    before_cursor: Option<String>,
    after_cursor: Option<String>,
    has_more_before: bool,
    has_more_after: bool,
) -> TimelinePageInfo {
    TimelinePageInfo {
        before_cursor: before_cursor.map(|value| TimelineCursor { value }),
        after_cursor: after_cursor.map(|value| TimelineCursor { value }),
        has_more_before,
        has_more_after,
    }
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
fn remote_gateway_session_access_smoke_builds_authenticated_ws_request() {
    let config = GatewayRegistryConfig {
        local: Some(GatewayLocalRegistryConfig {
            gateway_id: "local".to_owned(),
            name: "Local Gateway".to_owned(),
            gateway_base_url: GatewayBaseUrl::parse_presentation("127.0.0.1:17878").unwrap(),
            service_name: Some("com.pioneer.gateway".to_owned()),
        }),
    };
    let mut registry = default_registry(&config);
    registry.remotes.push(GatewayEndpoint {
        id: "remote-prod".to_owned(),
        name: "Remote Prod".to_owned(),
        gateway_base_url: GatewayBaseUrl::parse_presentation("https://gateway.example.com/pioneer")
            .unwrap(),
        kind: GatewayEndpointKind::Local,
        session_ref: Some("remote-prod".to_owned()),
        server_gateway_id: Some(
            pioneer_protocol::GatewayId::new("G00000000000000000001").expect("GatewayId"),
        ),
        workspace_id: Some(WORKSPACE_ID.to_owned()),
        service_name: Some("must-be-cleared".to_owned()),
    });

    normalize_registry(&mut registry, &config, |index| {
        format!("Remote Gateway {index}")
    })
    .expect("registry should normalize");

    let remote = registry.remotes.first().expect("remote endpoint");
    assert_eq!(remote.kind, GatewayEndpointKind::Remote);
    assert_eq!(remote.service_name, None);
    assert_eq!(remote.session_ref.as_deref(), Some("remote-prod"));

    let request = build_ws_request(&remote.gateway_base_url, Some(" short-lived-access "))
        .expect("ws request");

    assert_eq!(
        request.uri().to_string(),
        "wss://gateway.example.com/pioneer/"
    );
    assert_eq!(
        request
            .headers()
            .get(PIONEER_PROTOCOL_VERSION_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(PIONEER_PROTOCOL_VERSION)
    );
    assert_eq!(
        request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer short-lived-access")
    );
}
