use super::{
    DesktopGatewayWsCommandSenderExt, GatewayWsClient, GatewayWsConnectSpec, GatewayWsEvent,
};
use crate::gateway::timings::GatewayWsTimings;
use futures_util::{SinkExt, StreamExt};
use pioneer_client::artifacts::download::ArtifactDownloadRequest;
use pioneer_client::composer::{
    attachments::{ComposerAttachment, ComposerAttachmentKind, ComposerAttachmentUploadState},
    turn_prepare::PrepareComposerTurnRequest,
};
use pioneer_client::gateway::types::GatewayEndpointKind;
use pioneer_client::transport::ws::event_connection_id;
use pioneer_protocol::{
    AgentExecutionBackend, CLIAgentRuntimeKind, GatewayNotification, ThreadStartParams,
    ThreadUnsubscribeStatus, TurnCLIRuntimeOptions, TurnPermissionMode,
    TurnPermissionProfileSelection, TurnReasoningSelection, TurnStartParams, TurnStatus, UserInput,
    constants::events, generate_id,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
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
fn artifact_capabilities_request_receives_response() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec(
        "artifact-capabilities",
        "Artifact Capabilities",
        server.address.as_str(),
    );

    let _connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");
    let response = sender
        .artifact_capabilities(pioneer_protocol::ArtifactCapabilitiesParams {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
        })
        .expect("artifact capabilities");

    assert!(response.upload.required_for_local_paths);
    assert_eq!(response.upload.recommended_chunk_size_bytes, 3);

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn artifact_upload_chunk_sends_binary_frame_and_receives_ack() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec(
        "artifact-upload-chunk",
        "Artifact Upload Chunk",
        server.address.as_str(),
    );

    let _connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");
    let ack = sender
        .send_artifact_upload_chunk(
            TEST_WORKSPACE_ID.to_owned(),
            "artifact_upload_1".to_owned(),
            0,
            b"hello".to_vec(),
        )
        .expect("artifact upload chunk should be acked");

    assert_eq!(ack.workspace_id, TEST_WORKSPACE_ID);
    assert_eq!(ack.upload_id, "artifact_upload_1");
    assert_eq!(ack.offset, 0);
    assert_eq!(ack.len, 5);
    assert_eq!(ack.received_bytes, 5);
    assert_eq!(ack.next_offset, 5);

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn prepare_composer_turn_uploads_small_fixture_and_returns_artifact_input() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec(
        "artifact-upload-file",
        "Artifact Upload File",
        server.address.as_str(),
    );
    let temp = tempfile::tempdir().expect("temp dir");
    let file_path = temp.path().join("fixture.txt");
    std::fs::write(file_path.as_path(), b"hello").expect("write fixture");

    let _connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");
    let prepared = sender
        .prepare_composer_turn(PrepareComposerTurnRequest {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            thread_id: "thr_a".to_owned(),
            turn_id: "turn_a".to_owned(),
            endpoint_kind: Some(GatewayEndpointKind::Local),
            text: "hello".to_owned(),
            attachments: vec![ComposerAttachment {
                path: file_path.to_string_lossy().to_string(),
                file_name: "fixture.txt".to_owned(),
                kind: ComposerAttachmentKind::File,
                upload_state: ComposerAttachmentUploadState::Uploading,
            }],
            capabilities: Vec::new(),
            skill_selections: Vec::new(),
            skill_picker: Default::default(),
        })
        .expect("prepare composer turn");

    let artifact = prepared.attachments[0]
        .artifact
        .as_ref()
        .expect("uploaded artifact");
    assert_eq!(artifact.artifact_id, "artifact_upload_result");
    assert_eq!(artifact.display_name, "fixture.txt");
    assert_eq!(artifact.size_bytes, Some(5));
    assert!(matches!(prepared.input[1], UserInput::Artifact { .. }));

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn artifact_download_writes_verified_cache_file() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec(
        "artifact-download-file",
        "Artifact Download File",
        server.address.as_str(),
    );
    let temp = tempfile::tempdir().expect("temp dir");

    let _connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");
    let result = sender
        .download_artifact_to_cache_with_runtime_home(
            ArtifactDownloadRequest {
                gateway_profile_id: "remote/test".to_owned(),
                workspace_id: TEST_WORKSPACE_ID.to_owned(),
                artifact_id: "artifact_download_result".to_owned(),
                version_id: Some("artifact_download_version_1".to_owned()),
            },
            temp.path().to_path_buf(),
        )
        .expect("artifact download");

    assert_eq!(result.artifact.artifact_id, "artifact_download_result");
    assert_eq!(result.size_bytes, artifact_download_fixture().len() as u64);
    assert_eq!(
        std::fs::read(result.local_path.as_path()).expect("read downloaded file"),
        artifact_download_fixture()
    );
    assert_eq!(result.sha256, sha256_hex(artifact_download_fixture()));
    assert!(
        !result
            .local_path
            .as_path()
            .with_file_name("download.txt.part")
            .exists()
    );

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn artifact_download_interrupted_download_removes_part_file() {
    let mut server = TestWsServer::spawn("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();
    let spec = connect_spec(
        "artifact-download-corrupt",
        "Artifact Download Corrupt",
        server.address.as_str(),
    );
    let temp = tempfile::tempdir().expect("temp dir");

    let _connection_id = sender
        .connect_and_wait(spec)
        .expect("expected connect_and_wait to succeed");
    let error = sender
        .download_artifact_to_cache_with_runtime_home(
            ArtifactDownloadRequest {
                gateway_profile_id: "remote/test".to_owned(),
                workspace_id: TEST_WORKSPACE_ID.to_owned(),
                artifact_id: "artifact_download_corrupt".to_owned(),
                version_id: Some("artifact_download_version_1".to_owned()),
            },
            temp.path().to_path_buf(),
        )
        .expect_err("corrupt download should fail");

    assert!(error.to_string().contains("identity mismatch"));
    let part_path = temp
        .path()
        .join("downloads")
        .join("gateways")
        .join("remote_test")
        .join("workspaces")
        .join(TEST_WORKSPACE_ID)
        .join("artifacts")
        .join("artifact_download_corrupt")
        .join("artifact_download_version_1")
        .join("download.txt.part");
    assert!(!part_path.exists());

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
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: None,
            cli_runtime_options: None,
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
fn turn_start_request_sends_reasoning_effort() {
    let (mut server, requests) = TestWsServer::spawn_recording("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let _connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-turn-reasoning",
            "Remote Turn Reasoning",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let _response = sender
        .turn_start(TurnStartParams {
            thread_id: "thr_test_thread_123456".to_owned(),
            turn_id: "turn_test_reasoning".to_owned(),
            input: vec![UserInput::Text {
                text: "Hello".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: Some("gpt-5".to_owned()),
            model_provider: Some("openai".to_owned()),
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: Some(TurnReasoningSelection {
                effort: "high".to_owned(),
            }),
            permission_profile: None,
            cli_runtime_options: None,
        })
        .expect("turn/start should succeed");

    let request = wait_for_recorded_request(&requests, "turn/start", Duration::from_secs(2));
    assert_eq!(
        request
            .pointer("/params/reasoning/effort")
            .and_then(serde_json::Value::as_str),
        Some("high")
    );
    assert!(match request.pointer("/params/cli_runtime_options") {
        Some(value) => value.is_null(),
        None => true,
    });

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn turn_start_request_sends_cli_runtime_effort_options() {
    let (mut server, requests) = TestWsServer::spawn_recording("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let _connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-turn-cli-effort",
            "Remote Turn CLI Effort",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let _response = sender
        .turn_start(TurnStartParams {
            thread_id: "thr_test_thread_123456".to_owned(),
            turn_id: "turn_test_cli_effort".to_owned(),
            input: vec![UserInput::Text {
                text: "Hello".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: Some("gpt-5".to_owned()),
            model_provider: None,
            sandbox_policy: None,
            mode: None,
            execution_backend: Some(AgentExecutionBackend::CLIAgentRuntime {
                runtime_id: "codex".to_owned(),
                runtime_kind: CLIAgentRuntimeKind::Codex,
            }),
            reasoning: Some(TurnReasoningSelection {
                effort: "high".to_owned(),
            }),
            permission_profile: None,
            cli_runtime_options: Some(TurnCLIRuntimeOptions {
                sandbox: None,
                effort: Some("high".to_owned()),
                personality: None,
                summary: None,
                steer_if_active: None,
            }),
        })
        .expect("turn/start should succeed");

    let request = wait_for_recorded_request(&requests, "turn/start", Duration::from_secs(2));
    assert_eq!(
        request
            .pointer("/params/reasoning/effort")
            .and_then(serde_json::Value::as_str),
        Some("high")
    );
    assert_eq!(
        request
            .pointer("/params/cli_runtime_options/effort")
            .and_then(serde_json::Value::as_str),
        Some("high")
    );

    let _ = sender.shutdown();
    server.stop();
}

#[test]
fn turn_start_request_sends_permission_profile_selection() {
    let (mut server, requests) = TestWsServer::spawn_recording("127.0.0.1:0");
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let _connection_id = sender
        .connect_and_wait(connect_spec(
            "remote-turn-permissions",
            "Remote Turn Permissions",
            server.address.as_str(),
        ))
        .expect("expected connect_and_wait to succeed");

    let _response = sender
        .turn_start(TurnStartParams {
            thread_id: "thr_test_thread_123456".to_owned(),
            turn_id: "turn_test_permissions".to_owned(),
            input: vec![UserInput::Text {
                text: "Hello".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            model: Some("gpt-5".to_owned()),
            model_provider: Some("openai".to_owned()),
            sandbox_policy: None,
            mode: None,
            execution_backend: None,
            reasoning: None,
            permission_profile: Some(TurnPermissionProfileSelection {
                mode: TurnPermissionMode::Supervised,
            }),
            cli_runtime_options: None,
        })
        .expect("turn/start should succeed");

    let request = wait_for_recorded_request(&requests, "turn/start", Duration::from_secs(2));
    assert_eq!(
        request
            .pointer("/params/permission_profile/mode")
            .and_then(serde_json::Value::as_str),
        Some("supervised")
    );

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
        session: None,
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
        visibility: None,
        agent_nickname: None,
        agent_role: None,
    }
}

fn turn_permission_profile_json(request: &serde_json::Value) -> serde_json::Value {
    let mode = match request
        .pointer("/params/permission_profile/mode")
        .and_then(serde_json::Value::as_str)
    {
        Some("full_access") | None => TurnPermissionMode::FullAccess,
        Some("auto_accept_edits") => TurnPermissionMode::AutoAcceptEdits,
        Some("supervised") => TurnPermissionMode::Supervised,
        Some(other) => panic!("unexpected permission profile mode {other}"),
    };
    let source = if request.pointer("/params/permission_profile").is_some() {
        pioneer_protocol::TurnPermissionProfileSource::Composer
    } else {
        pioneer_protocol::TurnPermissionProfileSource::Defaulted
    };

    serde_json::to_value(pioneer_protocol::compile_turn_permission_profile(
        mode, source,
    ))
    .expect("permission profile should encode")
}

fn default_turn_permission_profile_json() -> serde_json::Value {
    serde_json::to_value(pioneer_protocol::default_turn_permission_profile_snapshot())
        .expect("permission profile should encode")
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

fn wait_for_recorded_request(
    requests: &mpsc::Receiver<serde_json::Value>,
    method: &str,
    timeout: Duration,
) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        assert!(now < deadline, "timed out waiting for recorded {method}");
        let remaining = deadline.saturating_duration_since(now);
        match requests.recv_timeout(remaining.min(Duration::from_millis(50))) {
            Ok(request) => {
                if request.get("method").and_then(serde_json::Value::as_str) == Some(method) {
                    return request;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("recorded request channel closed before {method}");
            }
        }
    }
}

fn artifact_download_fixture() -> &'static [u8] {
    b"hello artifact download"
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn artifact_download_frame(offset: u64, chunk: &[u8], final_chunk: bool) -> Vec<u8> {
    let header = json!({
        "workspace_id": TEST_WORKSPACE_ID,
        "download_id": "artifact_download_1",
        "artifact_id": "artifact_download_result",
        "version_id": "artifact_download_version_1",
        "offset": offset,
        "len": chunk.len() as u64,
        "total_size_bytes": artifact_download_fixture().len() as u64,
        "chunk_sha256": sha256_hex(chunk),
        "final_chunk": final_chunk
    });
    let header_bytes = serde_json::to_vec(&header).expect("encode download header");
    let mut frame = Vec::new();
    frame.extend_from_slice(b"ARTD");
    frame.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(header_bytes.as_slice());
    frame.extend_from_slice(chunk);
    frame
}

struct TestWsServer {
    address: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<thread::JoinHandle<()>>,
}

impl TestWsServer {
    fn spawn(bind_addr: &str) -> Self {
        Self::spawn_with_request_tx(bind_addr, None)
    }

    fn spawn_recording(bind_addr: &str) -> (Self, mpsc::Receiver<serde_json::Value>) {
        let (request_tx, request_rx) = mpsc::channel();
        (
            Self::spawn_with_request_tx(bind_addr, Some(request_tx)),
            request_rx,
        )
    }

    fn spawn_with_request_tx(
        bind_addr: &str,
        request_tx: Option<mpsc::Sender<serde_json::Value>>,
    ) -> Self {
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
                                let request_tx = request_tx.clone();
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
                                                if payload.len() < 8
                                                    || (&payload[0..4] != b"PSU1"
                                                        && &payload[0..4] != b"ARTU")
                                                {
                                                    continue;
                                                }
                                                let is_artifact = &payload[0..4] == b"ARTU";
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
                                                let workspace_id = header
                                                    .get("workspace_id")
                                                    .and_then(serde_json::Value::as_str)
                                                    .unwrap_or(TEST_WORKSPACE_ID);
                                                let chunk_len = u64::try_from(payload.len().saturating_sub(header_end))
                                                    .unwrap_or(u64::MAX);
                                                let ack = if is_artifact {
                                                    json!({
                                                        "jsonrpc": "2.0",
                                                        "method": events::ARTIFACT_UPLOAD_CHUNK_ACK,
                                                        "params": {
                                                            "workspace_id": workspace_id,
                                                            "upload_id": upload_id,
                                                            "offset": offset,
                                                            "len": chunk_len,
                                                            "received_bytes": offset.saturating_add(chunk_len),
                                                            "next_offset": offset.saturating_add(chunk_len)
                                                        }
                                                    })
                                                } else {
                                                    json!({
                                                        "jsonrpc": "2.0",
                                                        "method": events::SKILLS_UPLOAD_CHUNK_ACK,
                                                        "params": {
                                                            "upload_id": upload_id,
                                                            "offset": offset,
                                                            "len": chunk_len,
                                                            "received_bytes": offset.saturating_add(chunk_len),
                                                            "next_offset": offset.saturating_add(chunk_len)
                                                        }
                                                    })
                                                };
                                                let _ = writer
                                                    .send(Message::Text(ack.to_string().into()))
                                                    .await;
                                            }
                                            Message::Text(payload) => {
                                                let request = match serde_json::from_str::<serde_json::Value>(&payload) {
                                                    Ok(value) => value,
                                                    Err(_) => continue,
                                                };
                                                if let Some(request_tx) = request_tx.as_ref() {
                                                    let _ = request_tx.send(request.clone());
                                                }

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
                                                    "artifact/capabilities" => {
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "upload": {
                                                                    "required_for_local_paths": true,
                                                                    "recommended_chunk_size_bytes": 3,
                                                                    "max_chunk_size_bytes": 1024,
                                                                    "max_file_size_bytes": 1048576,
                                                                    "max_files_per_turn": 32
                                                                },
                                                                "download": {
                                                                    "recommended_chunk_size_bytes": 3,
                                                                    "max_chunk_size_bytes": 1024,
                                                                    "max_concurrent_downloads": 2
                                                                }
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "artifact/upload/start" => {
                                                        let params = request
                                                            .get("params")
                                                            .cloned()
                                                            .unwrap_or_else(|| json!({}));
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "upload_id": "artifact_upload_1",
                                                                "recommended_chunk_size_bytes": 3,
                                                                "max_chunk_size_bytes": 1024,
                                                                "max_size_bytes": 1048576,
                                                                "expires_at_unix": 1_700_000_000i64
                                                            }
                                                        });
                                                        let _ = params;
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "artifact/upload/finish" => {
                                                        let upload_id = request
                                                            .get("params")
                                                            .and_then(|value| value.get("upload_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .unwrap_or("artifact_upload_1");
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "upload_id": upload_id,
                                                                "artifact": {
                                                                    "artifact_id": "artifact_upload_result",
                                                                    "version_id": "artifact_version_1",
                                                                    "display_name": "fixture.txt",
                                                                    "kind": "text",
                                                                    "mime_type": "text/plain",
                                                                    "size_bytes": 5,
                                                                    "sha256": "00",
                                                                    "status": "ready",
                                                                    "preview": null
                                                                }
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "artifact/upload/abort" => {
                                                        let upload_id = request
                                                            .get("params")
                                                            .and_then(|value| value.get("upload_id"))
                                                            .and_then(serde_json::Value::as_str)
                                                            .unwrap_or("artifact_upload_1");
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "upload_id": upload_id,
                                                                "status": "aborted"
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "artifact/download/start" => {
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "download_id": "artifact_download_1",
                                                                "artifact": {
                                                                    "artifact_id": "artifact_download_result",
                                                                    "version_id": "artifact_download_version_1",
                                                                    "display_name": "download.txt",
                                                                    "kind": "text",
                                                                    "mime_type": "text/plain",
                                                                    "size_bytes": artifact_download_fixture().len(),
                                                                    "sha256": sha256_hex(artifact_download_fixture()),
                                                                    "status": "ready",
                                                                    "preview": null
                                                                },
                                                                "file_name": "download.txt",
                                                                "size_bytes": artifact_download_fixture().len(),
                                                                "sha256": sha256_hex(artifact_download_fixture()),
                                                                "recommended_chunk_size_bytes": 5,
                                                                "max_chunk_size_bytes": 8,
                                                                "expires_at_unix": 1_700_000_000i64
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "artifact/download/chunk" => {
                                                        let params = request
                                                            .get("params")
                                                            .cloned()
                                                            .unwrap_or_else(|| json!({}));
                                                        let offset = params
                                                            .get("offset")
                                                            .and_then(serde_json::Value::as_u64)
                                                            .unwrap_or(0);
                                                        let len = params
                                                            .get("len")
                                                            .and_then(serde_json::Value::as_u64)
                                                            .unwrap_or(0);
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "download_id": "artifact_download_1",
                                                                "offset": offset,
                                                                "len": len,
                                                                "queued": true
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;

                                                        let fixture = artifact_download_fixture();
                                                        let start = usize::try_from(offset).unwrap_or(usize::MAX);
                                                        let end = start.saturating_add(usize::try_from(len).unwrap_or(0));
                                                        if end <= fixture.len() {
                                                            let frame = artifact_download_frame(
                                                                offset,
                                                                &fixture[start..end],
                                                                end == fixture.len(),
                                                            );
                                                            let _ = writer
                                                                .send(Message::Binary(frame.into()))
                                                                .await;
                                                        }
                                                    }
                                                    "artifact/download/finish" => {
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "download_id": "artifact_download_1",
                                                                "status": "finished"
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
                                                    "artifact/download/abort" => {
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "download_id": "artifact_download_1",
                                                                "status": "aborted"
                                                            }
                                                        });
                                                        let _ = writer
                                                            .send(Message::Text(response.to_string().into()))
                                                            .await;
                                                    }
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
                                                        let permission_profile =
                                                            turn_permission_profile_json(&request);
                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "turn": {
                                                                    "id": turn_id,
                                                                    "status": "InProgress",
                                                                    "error": null,
                                                                    "permission_profile": permission_profile.clone()
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
                                                                    "error": null,
                                                                    "permission_profile": permission_profile
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
                                                        let permission_profile =
                                                            default_turn_permission_profile_json();

                                                        let response = json!({
                                                            "jsonrpc": "2.0",
                                                            "id": request_id,
                                                            "result": {
                                                                "thread_id": thread_id,
                                                                "workspace_id": "ws_000000000000000001",
                                                                "turn": {
                                                                    "id": turn_id,
                                                                    "status": "Completed",
                                                                    "error": null,
                                                                    "permission_profile": permission_profile
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
