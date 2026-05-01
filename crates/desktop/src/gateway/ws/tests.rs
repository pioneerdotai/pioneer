use super::{
    GatewayWsClient, GatewayWsConnectSpec, GatewayWsEvent, duration_to_millis_u64, next_backoff,
    normalize_ws_url, process_text_payload,
};
use crate::gateway::timings::GatewayWsTimings;
use crate::gateway::types::GatewayEndpointKind;
use futures_util::{SinkExt, StreamExt};
use pioneer_protocol::{
    GatewayNotification, ThreadStartParams, ThreadUnsubscribeStatus, TurnStartParams, TurnStatus,
    UserInput, constants::events, generate_id,
};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

const TEST_WORKSPACE_ID: &str = "ws_000000000000000001";
const THREAD_ID_LEN: usize = 21;

#[test]
fn normalize_ws_url_keeps_existing_scheme() {
    assert_eq!(
        normalize_ws_url("ws://127.0.0.1:8787"),
        "ws://127.0.0.1:8787"
    );
    assert_eq!(
        normalize_ws_url("wss://gateway.example.com/socket"),
        "wss://gateway.example.com/socket"
    );
}

#[test]
fn normalize_ws_url_adds_ws_scheme_when_missing() {
    assert_eq!(normalize_ws_url("127.0.0.1:8787"), "ws://127.0.0.1:8787");
    assert_eq!(
        normalize_ws_url(" gateway.example.com:443 "),
        "ws://gateway.example.com:443"
    );
}

#[test]
fn next_backoff_doubles_until_max() {
    let mut value = Duration::from_millis(100);
    let max = Duration::from_millis(750);

    value = next_backoff(value, max);
    assert_eq!(value, Duration::from_millis(200));
    value = next_backoff(value, max);
    assert_eq!(value, Duration::from_millis(400));
    value = next_backoff(value, max);
    assert_eq!(value, Duration::from_millis(750));
    value = next_backoff(value, max);
    assert_eq!(value, Duration::from_millis(750));
}

#[test]
fn process_text_payload_maps_unknown_agent_notifications() {
    let (event_tx, event_rx) = mpsc::channel();
    let mut pending_requests = HashMap::new();
    let mut pending_upload_chunks = HashMap::new();
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "item/tool.started",
        "params": {
            "thread_id": "thr_123",
            "turn_id": "turn_123",
            "name": "tool-call"
        }
    })
    .to_string();

    process_text_payload(
        &payload,
        17,
        &mut pending_requests,
        &mut pending_upload_chunks,
        &event_tx,
    );

    let event = event_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("expected websocket event");
    match event {
        GatewayWsEvent::Notification {
            connection_id,
            notification:
                GatewayNotification::Unknown(pioneer_protocol::UnknownGatewayNotification {
                    method,
                    thread_id,
                    turn_id,
                    ..
                }),
        } => {
            assert_eq!(connection_id, 17);
            assert_eq!(method, "item/tool.started");
            assert_eq!(thread_id.as_deref(), Some("thr_123"));
            assert_eq!(turn_id.as_deref(), Some("turn_123"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn process_text_payload_maps_thread_updated_notifications() {
    let (event_tx, event_rx) = mpsc::channel();
    let mut pending_requests = HashMap::new();
    let mut pending_upload_chunks = HashMap::new();
    let payload = json!({
        "jsonrpc": "2.0",
        "method": "thread/updated",
        "params": {
            "thread": {
                "workspace_id": "ws_123",
                "id": "thr_123",
                "name": "New title",
                "preview": "",
                "mode": "Chat",
                "model": "gpt-5.4",
                "model_provider": "openai",
                "created_at": 0,
                "updated_at": 0,
                "status": "Idle",
                "turns": []
            }
        }
    })
    .to_string();

    process_text_payload(
        &payload,
        17,
        &mut pending_requests,
        &mut pending_upload_chunks,
        &event_tx,
    );

    let event = event_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("expected websocket event");
    match event {
        GatewayWsEvent::Notification {
            connection_id,
            notification: GatewayNotification::ThreadUpdated(notification),
        } => {
            assert_eq!(connection_id, 17);
            assert_eq!(notification.thread.workspace_id, "ws_123");
            assert_eq!(notification.thread.id, "thr_123");
            assert_eq!(notification.thread.name.as_deref(), Some("New title"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn skill_upload_chunk_sends_binary_frame_and_receives_ack() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec("upload-chunk", "Upload Chunk", server.address.as_str());

    let _connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");
    let ack = sender
        .send_skill_upload_chunk(
            TEST_WORKSPACE_ID.to_owned(),
            "upload_00000000000001".to_owned(),
            0,
            b"hello".to_vec(),
        )
        .expect("upload chunk should be acked");

    assert_eq!(ack.upload_id, "upload_00000000000001");
    assert_eq!(ack.offset, 0);
    assert_eq!(ack.len, 5);
    assert_eq!(ack.received_bytes, 5);
    assert_eq!(ack.next_offset, 5);

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn connect_and_wait_reports_connected_event() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec("remote-1", "Remote 1", server.address.as_str());

    let connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");

    let event = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Connected {
                connection_id: id,
                endpoint_id,
                ..
            } if *id == connection_id && endpoint_id == "remote-1"
        )
    });

    assert!(matches!(event, GatewayWsEvent::Connected { .. }));
    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn reconnects_after_server_returns() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let address = server.address.clone();
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec("remote-reconnect", "Remote Reconnect", address.as_str());

    let connection_id = sender
        .connect_with_retry(spec)
        .expect("expected connect_with_retry to start");

    let _connected = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Connected {
                connection_id: id,
                ..
            } if *id == connection_id
        )
    });

    server.stop();

    let _reconnecting = wait_for_event(&client, Duration::from_secs(3), |event| {
        matches!(
            event,
            GatewayWsEvent::Reconnecting {
                connection_id: id,
                ..
            } if *id == connection_id
        )
    });

    let mut restarted = TestWsServer::spawn(address.as_str());

    let _connected_again = wait_for_event(&client, Duration::from_secs(3), |event| {
        matches!(
            event,
            GatewayWsEvent::Connected {
                connection_id: id,
                ..
            } if *id == connection_id
        )
    });

    let _ = sender.shutdown();
    restarted.stop();
}

#[test]
fn dropping_client_clone_does_not_shutdown_worker() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec("remote-clone", "Remote Clone", server.address.as_str());

    {
        let transient_clone = client.clone();
        drop(transient_clone);
    }

    let connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed after dropping clone");

    let event = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Connected {
                connection_id: id,
                endpoint_id,
                ..
            } if *id == connection_id && endpoint_id == "remote-clone"
        )
    });

    assert!(matches!(event, GatewayWsEvent::Connected { .. }));
    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn switch_during_reconnect_stops_old_connection_stream() {
    let unavailable_addr = reserve_unused_local_address();

    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let first_id = sender
        .connect_with_retry(connect_spec(
            "remote-old",
            "Remote Old",
            unavailable_addr.as_str(),
        ))
        .expect("expected first connect_with_retry to start");

    let _first_reconnecting = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Reconnecting {
                connection_id: id,
                ..
            } if *id == first_id
        )
    });

    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let second_id = sender
        .connect_and_wait(connect_spec(
            "remote-new",
            "Remote New",
            server.address.as_str(),
        ))
        .expect("expected second connect_and_wait to succeed");

    let _second_connected = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Connected {
                connection_id: id,
                ..
            } if *id == second_id
        )
    });

    let _ = client.drain_events();
    thread::sleep(Duration::from_millis(300));
    let stale_events = client
        .drain_events()
        .into_iter()
        .any(|event| event_connection_id(&event) == first_id);
    assert!(
        !stale_events,
        "old connection stream should stop after switching endpoint"
    );

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn thread_start_request_receives_response_and_started_notification() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-thread-start",
            "Remote Thread Start",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let response = sender
        .thread_start(thread_start_params())
        .expect("thread/start should succeed");
    assert_eq!(response.thread.id.chars().count(), 21);
    let response_thread_id = response.thread.id;

    let event = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Notification {
                connection_id: id,
                notification: GatewayNotification::ThreadStarted(notification),
            } if *id == connection_id && notification.thread.id == response_thread_id
        )
    });
    assert!(matches!(
        event,
        GatewayWsEvent::Notification {
            notification: GatewayNotification::ThreadStarted(_),
            ..
        }
    ));

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn thread_start_rejects_missing_workspace_id_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .thread_start(ThreadStartParams {
            thread_id: "thr_000000000000000001".to_owned(),
            workspace_id: "   ".to_owned(),
            name: None,
            model: None,
            model_provider: None,
            sandbox: None,
            mode: None,
            origin_kind: None,
            sidebar_visibility: None,
            agent_nickname: None,
            agent_role: None,
        })
        .expect_err("empty workspace_id must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("workspace_id"));

    let _ = sender.shutdown();
}

#[test]
fn turn_start_rejects_missing_turn_id_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .turn_start(TurnStartParams {
            thread_id: "thr_000000000000000001".to_owned(),
            turn_id: "   ".to_owned(),
            input: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
        })
        .expect_err("empty turn_id must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("turn_id"));

    let _ = sender.shutdown();
}

#[test]
fn turn_start_request_receives_response_and_started_notification() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-turn-start",
            "Remote Turn Start",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let thread_response = sender
        .thread_start(thread_start_params())
        .expect("thread/start should succeed");
    let thread_id = thread_response.thread.id;

    let _thread_started = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Notification {
                connection_id: id,
                notification: GatewayNotification::ThreadStarted(notification),
            } if *id == connection_id && notification.thread.id == thread_id
        )
    });

    let turn_response = sender
        .turn_start(TurnStartParams {
            thread_id: thread_id.clone(),
            turn_id: generate_id(THREAD_ID_LEN),
            input: vec![UserInput::Text {
                text: "Hello".to_owned(),
                text_elements: Vec::new(),
            }],
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
        })
        .expect("turn/start should succeed");
    assert_eq!(turn_response.turn.status, TurnStatus::InProgress);
    assert_eq!(turn_response.turn.id.chars().count(), 21);
    let turn_id = turn_response.turn.id;

    let event = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Notification {
                connection_id: id,
                notification: GatewayNotification::TurnStarted(notification),
            } if *id == connection_id
                && notification.thread_id == thread_id
                && notification.turn.id == turn_id
        )
    });
    assert!(matches!(
        event,
        GatewayWsEvent::Notification {
            notification: GatewayNotification::TurnStarted(_),
            ..
        }
    ));

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn thread_unsubscribe_request_receives_response_and_closed_notification() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-thread-unsubscribe",
            "Remote Thread Unsubscribe",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let response = sender
        .thread_unsubscribe("thr_test_thread_123456".to_owned())
        .expect("thread/unsubscribe should succeed");
    assert_eq!(response.status, ThreadUnsubscribeStatus::Unsubscribed);

    let event = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Notification {
                connection_id: id,
                notification: GatewayNotification::ThreadClosed(notification),
            } if *id == connection_id && notification.thread_id == "thr_test_thread_123456"
        )
    });
    assert!(matches!(
        event,
        GatewayWsEvent::Notification {
            notification: GatewayNotification::ThreadClosed(_),
            ..
        }
    ));

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn thread_start_retry_succeeds_without_reconnect_after_temporary_error() {
    let mut server = TemporaryThreadStartFailureWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-thread-retry",
            "Remote Thread Retry",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let first_result = sender.thread_start(thread_start_params());
    assert!(
        first_result.is_err(),
        "first thread/start should fail with temporary error"
    );

    let second_response = sender
        .thread_start(thread_start_params())
        .expect("second thread/start should succeed on same connection");
    assert_eq!(second_response.thread.id.chars().count(), 21);
    let second_thread_id = second_response.thread.id;

    let event = wait_for_event(&client, Duration::from_secs(2), |event| {
        matches!(
            event,
            GatewayWsEvent::Notification {
                connection_id: id,
                notification: GatewayNotification::ThreadStarted(notification),
            } if *id == connection_id && notification.thread.id == second_thread_id
        )
    });
    assert!(matches!(
        event,
        GatewayWsEvent::Notification {
            notification: GatewayNotification::ThreadStarted(_),
            ..
        }
    ));

    let reconnect_seen = client.drain_events().into_iter().any(|event| {
        matches!(
            event,
            GatewayWsEvent::Reconnecting {
                connection_id: id,
                ..
            } if id == connection_id
        )
    });
    assert!(
        !reconnect_seen,
        "temporary thread/start failure should not force websocket reconnect"
    );

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn concurrent_clients_receive_broadcast_thread_started_events() {
    let mut server = BroadcastThreadStartedWsServer::spawn("127.0.0.1:0");
    let client_a = GatewayWsClient::new();
    let client_b = GatewayWsClient::new();
    let sender_a = client_a.command_sender();
    let sender_b = client_b.command_sender();

    let connection_a = sender_a
        .connect_and_wait(connect_spec(
            "remote-a",
            "Remote A",
            server.address.as_str(),
        ))
        .expect("expected client A to connect");
    let connection_b = sender_b
        .connect_and_wait(connect_spec(
            "remote-b",
            "Remote B",
            server.address.as_str(),
        ))
        .expect("expected client B to connect");

    let sender_a_for_start = sender_a.clone();
    let sender_b_for_start = sender_b.clone();
    let start_a = thread::spawn(move || {
        sender_a_for_start
            .thread_start(thread_start_params())
            .expect("client A thread/start should succeed")
            .thread
            .id
    });
    let start_b = thread::spawn(move || {
        sender_b_for_start
            .thread_start(thread_start_params())
            .expect("client B thread/start should succeed")
            .thread
            .id
    });

    let local_thread_a = start_a
        .join()
        .expect("client A thread/start worker should join");
    let local_thread_b = start_b
        .join()
        .expect("client B thread/start worker should join");
    assert_ne!(local_thread_a, local_thread_b);

    let started_ids_a =
        wait_for_thread_started_ids(&client_a, connection_a, Duration::from_secs(3), 2);
    let started_ids_b =
        wait_for_thread_started_ids(&client_b, connection_b, Duration::from_secs(3), 2);

    assert!(started_ids_a.contains(local_thread_a.as_str()));
    assert!(started_ids_a.contains(local_thread_b.as_str()));
    assert!(started_ids_b.contains(local_thread_a.as_str()));
    assert!(started_ids_b.contains(local_thread_b.as_str()));

    let _ = sender_a.shutdown();
    let _ = sender_b.shutdown();
    server.stop();
}

fn connect_spec(endpoint_id: &str, endpoint_name: &str, address: &str) -> GatewayWsConnectSpec {
    GatewayWsConnectSpec {
        endpoint_id: endpoint_id.to_owned(),
        endpoint_name: endpoint_name.to_owned(),
        endpoint_kind: GatewayEndpointKind::Remote,
        address: address.to_owned(),
        auth_token: None,
        timings: GatewayWsTimings {
            connect_timeout: Duration::from_millis(600),
            ping_interval: Duration::from_millis(120),
            pong_timeout: Duration::from_millis(600),
            reconnect_initial: Duration::from_millis(80),
            reconnect_max: Duration::from_millis(250),
            reconnect_jitter_percent: 0,
        },
    }
}

fn thread_start_params() -> ThreadStartParams {
    ThreadStartParams {
        thread_id: generate_id(THREAD_ID_LEN),
        workspace_id: TEST_WORKSPACE_ID.to_owned(),
        name: None,
        model: None,
        model_provider: None,
        sandbox: None,
        mode: None,
        origin_kind: None,
        sidebar_visibility: None,
        agent_nickname: None,
        agent_role: None,
    }
}

fn reserve_unused_local_address() -> String {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("failed to reserve local test address");
    let address = listener
        .local_addr()
        .expect("failed to resolve reserved local test address")
        .to_string();
    drop(listener);
    address
}

fn wait_for_event(
    client: &GatewayWsClient,
    timeout: Duration,
    predicate: impl Fn(&GatewayWsEvent) -> bool,
) -> GatewayWsEvent {
    let deadline = Instant::now() + timeout;

    loop {
        for event in client.drain_events() {
            if predicate(&event) {
                return event;
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for websocket event"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_thread_started_ids(
    client: &GatewayWsClient,
    connection_id: u64,
    timeout: Duration,
    expected_count: usize,
) -> HashSet<String> {
    let deadline = Instant::now() + timeout;
    let mut thread_ids = HashSet::new();

    while thread_ids.len() < expected_count {
        for event in client.drain_events() {
            if let GatewayWsEvent::Notification {
                connection_id: event_connection_id,
                notification: GatewayNotification::ThreadStarted(notification),
            } = event
                && event_connection_id == connection_id
            {
                thread_ids.insert(notification.thread.id);
            }
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for thread/started notifications"
        );
        thread::sleep(Duration::from_millis(20));
    }

    thread_ids
}

fn event_connection_id(event: &GatewayWsEvent) -> u64 {
    match event {
        GatewayWsEvent::Connecting { connection_id, .. }
        | GatewayWsEvent::Connected { connection_id, .. }
        | GatewayWsEvent::Reconnecting { connection_id, .. }
        | GatewayWsEvent::Disconnected { connection_id, .. }
        | GatewayWsEvent::ConnectFailed { connection_id, .. }
        | GatewayWsEvent::Notification { connection_id, .. } => *connection_id,
    }
}

struct TestWsServer {
    address: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl TestWsServer {
    fn spawn(bind_addr: &str) -> Self {
        let (address_tx, address_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let bind_addr = bind_addr.to_owned();

        let join_handle = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for test ws server");
            runtime.block_on(async move {
                    let listener = bind_with_retry(bind_addr.as_str()).await;
                    let local_addr = listener
                        .local_addr()
                        .expect("failed to get local ws server addr");
                    address_tx
                        .send(local_addr)
                        .expect("failed to send ws server address");

                    let mut shutdown_rx = shutdown_rx;
                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => {
                                break;
                            }
                            accepted = listener.accept() => {
                                let Ok((stream, _)) = accepted else {
                                    break;
                                };
                                tokio::spawn(async move {
                                    let Ok(ws) = accept_async(stream).await else {
                                        return;
                                    };
                                    let (mut writer, mut reader) = ws.split();
                                    while let Some(payload) = reader.next().await {
                                        let Ok(message) = payload else {
                                            break;
                                        };
                                        match message {
                                            Message::Ping(payload) => {
                                                let _ = writer.send(Message::Pong(payload)).await;
                                            }
                                            Message::Binary(payload) => {
                                                if payload.len() < 8 || &payload[0..4] != b"PSU1" {
                                                    continue;
                                                }
                                                let header_len = u32::from_be_bytes([
                                                    payload[4], payload[5], payload[6], payload[7],
                                                ]) as usize;
                                                let header_start = 8usize;
                                                let header_end = header_start.saturating_add(header_len);
                                                if header_end > payload.len() {
                                                    continue;
                                                }
                                                let header = match serde_json::from_slice::<serde_json::Value>(&payload[header_start..header_end]) {
                                                    Ok(value) => value,
                                                    Err(_) => continue,
                                                };
                                                let Some(upload_id) = header
                                                    .get("upload_id")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };
                                                let offset = header
                                                    .get("offset")
                                                    .and_then(serde_json::Value::as_u64)
                                                    .unwrap_or(0);
                                                let chunk_len = u64::try_from(payload.len().saturating_sub(header_end))
                                                    .unwrap_or(u64::MAX);
                                                let ack = json!({
                                                    "jsonrpc": "2.0",
                                                    "method": events::SKILLS_UPLOAD_CHUNK_ACK,
                                                    "params": {
                                                        "upload_id": upload_id,
                                                        "offset": offset,
                                                        "len": chunk_len,
                                                        "received_bytes": offset.saturating_add(chunk_len),
                                                        "next_offset": offset.saturating_add(chunk_len)
                                                    }
                                                });
                                                let _ = writer
                                                    .send(Message::Text(ack.to_string().into()))
                                                    .await;
                                            }
                                            Message::Text(payload) => {
                                                let request = match serde_json::from_str::<serde_json::Value>(&payload) {
                                                    Ok(value) => value,
                                                    Err(_) => continue,
                                                };

                                                let Some(request_id) = request
                                                    .get("id")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };
                                                let Some(method) = request
                                                    .get("method")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };

                                                match method {
                                                    "thread/start" => {
                                                        let Some(thread_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("thread_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            let error_response = json!({
                                                                "jsonrpc": "2.0",
                                                                "id": request_id,
                                                                "error": {
                                                                    "code": -32602,
                                                                    "message": "thread_id is required"
                                                                }
                                                            });
                                                            let _ = writer
                                                                .send(Message::Text(error_response.to_string().into()))
                                                                .await;
                                                            continue;
                                                        };

                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "thread": {
                                                                    "id": thread_id,
                                                                    "workspace_id": "ws_000000000000000001",
                                                                    "name": null,
                                                                    "preview": "",
                                                                    "mode": "Chat",
                                                                    "model": "gpt-5.4",
                                                                    "model_provider": "openai",
                                                                    "created_at": 1_700_000_000i64,
                                                                    "updated_at": 1_700_000_000i64,
                                                                    "status": "Idle",
                                                                    "agent_nickname": null,
                                                                    "agent_role": null,
                                                                    "turns": []
                                                                },
                                                                "sandbox": {
                                                                    "mode": "FullAccess"
                                                                }
                                                            }
                                                        });
                                                        let notification = json!({
                                                            "jsonrpc": "2.0",
                                                            "method": "thread/started",
                                                            "params": {
                                                                "thread": {
                                                                    "id": thread_id,
                                                                    "workspace_id": "ws_000000000000000001",
                                                                    "name": null,
                                                                    "preview": "",
                                                                    "mode": "Chat",
                                                                    "model": "gpt-5.4",
                                                                    "model_provider": "openai",
                                                                    "created_at": 1_700_000_000i64,
                                                                    "updated_at": 1_700_000_000i64,
                                                                    "status": "Idle",
                                                                    "agent_nickname": null,
                                                                    "agent_role": null,
                                                                    "turns": []
                                                                }
                                                            }
                                                        });

                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                        let _ = writer
                                                            .send(Message::Text(notification.to_string().into()))
                                                            .await;
                                                    }
                                                    "turn/start" => {
                                                        let Some(thread_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("thread_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            let error_response = json!({
                                                                "jsonrpc": "2.0",
                                                                "id": request_id,
                                                                "error": {
                                                                    "code": -32602,
                                                                    "message": "thread_id is required"
                                                                }
                                                            });
                                                            let _ = writer
                                                                .send(Message::Text(error_response.to_string().into()))
                                                                .await;
                                                            continue;
                                                        };

                                                        let Some(turn_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("turn_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            let error_response = json!({
                                                                "jsonrpc": "2.0",
                                                                "id": request_id,
                                                                "error": {
                                                                    "code": -32602,
                                                                    "message": "turn_id is required"
                                                                }
                                                            });
                                                            let _ = writer
                                                                .send(Message::Text(error_response.to_string().into()))
                                                                .await;
                                                            continue;
                                                        };
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "turn": {
                                                                    "id": turn_id,
                                                                    "status": "InProgress",
                                                                    "error": null
                                                                }
                                                            }
                                                        });
                                                        let notification = json!({
                                                            "jsonrpc": "2.0",
                                                            "method": "turn/started",
                                                            "params": {
                                                                "workspace_id": "ws_000000000000000001",
                                                                "thread_id": thread_id,
                                                                "turn": {
                                                                    "id": turn_id,
                                                                    "status": "InProgress",
                                                                    "error": null
                                                                }
                                                            }
                                                        });

                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                        let _ = writer
                                                            .send(Message::Text(notification.to_string().into()))
                                                            .await;
                                                    }
                                                    "turn/get" => {
                                                        let Some(thread_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("thread_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            continue;
                                                        };
                                                        let Some(turn_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("turn_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            continue;
                                                        };

                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "thread_id": thread_id,
                                                                "workspace_id": "ws_000000000000000001",
                                                                "turn": {
                                                                    "id": turn_id,
                                                                    "status": "Completed",
                                                                    "error": null
                                                                }
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "turn/items" => {
                                                        let Some(thread_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("thread_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            continue;
                                                        };
                                                        let Some(turn_id) = request
                                                            .get("params")
                                                            .and_then(|value| value.get("turn_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .filter(|value| !value.trim().is_empty())
                                                        else {
                                                            continue;
                                                        };
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "thread_id": thread_id,
                                                                "workspace_id": "ws_000000000000000001",
                                                                "turn_id": turn_id,
                                                                "events": [],
                                                                "last_sequence": 0
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "thread/unsubscribe" => {
                                                        let thread_id = request
                                                            .get("params")
                                                            .and_then(|value| value.get("threadId"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .unwrap_or("thr_test_thread_123456");
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "status": "unsubscribed"
                                                            }
                                                        });
                                                        let notification = json!({
                                                            "jsonrpc": "2.0",
                                                            "method": "thread/closed",
                                                            "params": {
                                                                "workspaceId": "ws_000000000000000001",
                                                                "threadId": thread_id
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                        let _ = writer
                                                            .send(Message::Text(notification.to_string().into()))
                                                            .await;
                                                    }
                                                    _ => {}
                                                }
                                            }
                                            Message::Close(frame) => {
                                                let _ = writer.send(Message::Close(frame)).await;
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
        });

        let address = address_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("timed out waiting for test ws server address")
            .to_string();

        Self {
            address,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        }
    }

    fn stop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

struct BroadcastThreadStartedWsServer {
    address: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl BroadcastThreadStartedWsServer {
    fn spawn(bind_addr: &str) -> Self {
        let (address_tx, address_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let bind_addr = bind_addr.to_owned();

        let join_handle = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for broadcast ws server");
            runtime.block_on(async move {
                    let listener = bind_with_retry(bind_addr.as_str()).await;
                    let local_addr = listener
                        .local_addr()
                        .expect("failed to get local broadcast ws server addr");
                    address_tx
                        .send(local_addr)
                        .expect("failed to send broadcast ws server address");

                    let peers: Arc<Mutex<Vec<tokio_mpsc::UnboundedSender<Message>>>> =
                        Arc::new(Mutex::new(Vec::new()));
                    let thread_seq = Arc::new(AtomicUsize::new(0));

                    let mut shutdown_rx = shutdown_rx;
                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => {
                                break;
                            }
                            accepted = listener.accept() => {
                                let Ok((stream, _)) = accepted else {
                                    break;
                                };
                                let peers = Arc::clone(&peers);
                                let thread_seq = Arc::clone(&thread_seq);
                                tokio::spawn(async move {
                                    let Ok(ws) = accept_async(stream).await else {
                                        return;
                                    };
                                    let (mut writer, mut reader) = ws.split();
                                    let (writer_tx, mut writer_rx) = tokio_mpsc::unbounded_channel::<Message>();
                                    peers.lock().await.push(writer_tx.clone());

                                    let writer_task = tokio::spawn(async move {
                                        while let Some(message) = writer_rx.recv().await {
                                            if writer.send(message).await.is_err() {
                                                break;
                                            }
                                        }
                                    });

                                    while let Some(payload) = reader.next().await {
                                        let Ok(message) = payload else {
                                            break;
                                        };
                                        match message {
                                            Message::Ping(payload) => {
                                                let _ = writer_tx.send(Message::Pong(payload));
                                            }
                                            Message::Text(payload) => {
                                                let request = match serde_json::from_str::<serde_json::Value>(&payload) {
                                                    Ok(value) => value,
                                                    Err(_) => continue,
                                                };

                                                let Some(request_id) = request
                                                    .get("id")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };
                                                let Some(method) = request
                                                    .get("method")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };

                                                if method != "thread/start" {
                                                    continue;
                                                }

                                                if request
                                                    .get("params")
                                                    .and_then(|value| value.get("thread_id"))
                                                    .and_then(serde_json::Value::as_str)
                                                    .is_none_or(|value| value.trim().is_empty())
                                                {
                                                    let error_response = json!({
                                                        "jsonrpc": "2.0",
                                                        "id": request_id,
                                                        "error": {
                                                            "code": -32602,
                                                            "message": "thread_id is required"
                                                        }
                                                    });
                                                    let _ = writer_tx.send(Message::Text(error_response.to_string().into()));
                                                    continue;
                                                }

                                                let sequence = thread_seq.fetch_add(1, Ordering::Relaxed) + 1;
                                                let thread_id = format!("thr_broadcast_{sequence:02}");
                                                let thread_payload = json!({
                                                    "id": thread_id,
                                                    "workspace_id": "ws_000000000000000001",
                                                    "name": null,
                                                    "preview": "",
                                                    "mode": "Chat",
                                                    "model": "gpt-5.4",
                                                    "model_provider": "openai",
                                                    "created_at": 1_700_000_000i64,
                                                    "updated_at": 1_700_000_000i64,
                                                    "status": "Idle",
                                                    "agent_nickname": null,
                                                    "agent_role": null,
                                                    "turns": []
                                                });
                                                let response = json!({
                                                    "jsonrpc": "2.0",
                                                    "id": request_id,
                                                    "result": {
                                                        "thread": thread_payload,
                                                        "sandbox": {
                                                            "mode": "FullAccess"
                                                        }
                                                    }
                                                });
                                                let _ = writer_tx.send(Message::Text(response.to_string().into()));

                                                let notification = json!({
                                                    "jsonrpc": "2.0",
                                                    "method": "thread/started",
                                                    "params": {
                                                        "thread": thread_payload
                                                    }
                                                });
                                                let notification_text = notification.to_string();
                                                let peers_guard = peers.lock().await;
                                                for peer_tx in peers_guard.iter() {
                                                    let _ = peer_tx.send(Message::Text(notification_text.clone().into()));
                                                }
                                            }
                                            Message::Close(frame) => {
                                                let _ = writer_tx.send(Message::Close(frame));
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }

                                    writer_task.abort();
                                });
                            }
                        }
                    }
                });
        });

        let address = address_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("timed out waiting for broadcast ws server address")
            .to_string();

        Self {
            address,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        }
    }

    fn stop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

struct TemporaryThreadStartFailureWsServer {
    address: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl TemporaryThreadStartFailureWsServer {
    fn spawn(bind_addr: &str) -> Self {
        let (address_tx, address_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let bind_addr = bind_addr.to_owned();

        let join_handle = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for temporary failure ws server");
            runtime.block_on(async move {
                    let listener = bind_with_retry(bind_addr.as_str()).await;
                    let local_addr = listener
                        .local_addr()
                        .expect("failed to get local temporary failure ws server addr");
                    address_tx
                        .send(local_addr)
                        .expect("failed to send temporary failure ws server address");

                    let start_attempts = Arc::new(AtomicUsize::new(0));
                    let mut shutdown_rx = shutdown_rx;
                    loop {
                        tokio::select! {
                            _ = &mut shutdown_rx => {
                                break;
                            }
                            accepted = listener.accept() => {
                                let Ok((stream, _)) = accepted else {
                                    break;
                                };
                                let start_attempts = Arc::clone(&start_attempts);
                                tokio::spawn(async move {
                                    let Ok(ws) = accept_async(stream).await else {
                                        return;
                                    };
                                    let (mut writer, mut reader) = ws.split();
                                    while let Some(payload) = reader.next().await {
                                        let Ok(message) = payload else {
                                            break;
                                        };
                                        match message {
                                            Message::Ping(payload) => {
                                                let _ = writer.send(Message::Pong(payload)).await;
                                            }
                                            Message::Text(payload) => {
                                                let request = match serde_json::from_str::<serde_json::Value>(&payload) {
                                                    Ok(value) => value,
                                                    Err(_) => continue,
                                                };

                                                let Some(request_id) = request
                                                    .get("id")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };
                                                let Some(method) = request
                                                    .get("method")
                                                    .and_then(serde_json::Value::as_str)
                                                else {
                                                    continue;
                                                };

                                                if method != "thread/start" {
                                                    continue;
                                                }

                                                let Some(thread_id) = request
                                                    .get("params")
                                                    .and_then(|value| value.get("thread_id"))
                                                    .and_then(serde_json::Value::as_str)
                                                    .filter(|value| !value.trim().is_empty())
                                                else {
                                                    let error_response = json!({
                                                        "jsonrpc": "2.0",
                                                        "id": request_id,
                                                        "error": {
                                                            "code": -32602,
                                                            "message": "thread_id is required"
                                                        }
                                                    });
                                                    let _ = writer
                                                        .send(Message::Text(error_response.to_string().into()))
                                                        .await;
                                                    continue;
                                                };

                                                let attempt = start_attempts.fetch_add(1, Ordering::Relaxed);
                                                if attempt == 0 {
                                                    let error_response = json!({
                                                        "jsonrpc": "2.0",
                                                        "id": request_id,
                                                        "error": {
                                                            "code": -32000,
                                                            "message": "temporary thread/start backend error"
                                                        }
                                                    });
                                                    let _ = writer
                                                        .send(Message::Text(error_response.to_string().into()))
                                                        .await;
                                                    continue;
                                                }

                                                let response = json!({
                                                    "jsonrpc": "2.0",
                                                    "id": request_id,
                                                    "result": {
                                                        "thread": {
                                                            "id": thread_id,
                                                            "workspace_id": "ws_000000000000000001",
                                                            "name": null,
                                                            "preview": "",
                                                            "mode": "Chat",
                                                            "model": "gpt-5.4",
                                                            "model_provider": "openai",
                                                            "created_at": 1_700_000_000i64,
                                                            "updated_at": 1_700_000_000i64,
                                                            "status": "Idle",
                                                            "agent_nickname": null,
                                                            "agent_role": null,
                                                            "turns": []
                                                        },
                                                        "sandbox": {
                                                            "mode": "FullAccess"
                                                        }
                                                    }
                                                });
                                                let notification = json!({
                                                    "jsonrpc": "2.0",
                                                    "method": "thread/started",
                                                    "params": {
                                                        "thread": {
                                                            "id": thread_id,
                                                            "workspace_id": "ws_000000000000000001",
                                                            "name": null,
                                                            "preview": "",
                                                            "mode": "Chat",
                                                            "model": "gpt-5.4",
                                                            "model_provider": "openai",
                                                            "created_at": 1_700_000_000i64,
                                                            "updated_at": 1_700_000_000i64,
                                                            "status": "Idle",
                                                            "agent_nickname": null,
                                                            "agent_role": null,
                                                            "turns": []
                                                        }
                                                    }
                                                });

                                                let _ = writer
                                                    .send(Message::Text(response.to_string().into()))
                                                    .await;
                                                let _ = writer
                                                    .send(Message::Text(notification.to_string().into()))
                                                    .await;
                                            }
                                            Message::Close(frame) => {
                                                let _ = writer.send(Message::Close(frame)).await;
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                });
                            }
                        }
                    }
                });
        });

        let address = address_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("timed out waiting for temporary failure ws server address")
            .to_string();

        Self {
            address,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
        }
    }

    fn stop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(join_handle) = self.join_handle.take() {
            let _ = join_handle.join();
        }
    }
}

impl Drop for TestWsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for BroadcastThreadStartedWsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for TemporaryThreadStartFailureWsServer {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn bind_with_retry(bind_addr: &str) -> TcpListener {
    let mut last_error = None;
    for _ in 0..20 {
        match TcpListener::bind(bind_addr).await {
            Ok(listener) => return listener,
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }

    panic!(
        "failed to bind test websocket server to {bind_addr}: {}",
        last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_owned())
    );
}

#[test]
fn duration_to_millis_u64_handles_regular_values() {
    assert_eq!(duration_to_millis_u64(Duration::from_millis(123)), 123);
}
