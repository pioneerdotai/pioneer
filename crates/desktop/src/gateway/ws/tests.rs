use super::{
    GatewayWsClient, GatewayWsConnectSpec, GatewayWsEvent, duration_to_millis_u64,
    encode_artifact_upload_chunk_frame, next_backoff, normalize_ws_url, process_text_payload,
};
use crate::gateway::timings::GatewayWsTimings;
use futures_util::{SinkExt, StreamExt};
use pioneer_client::artifacts::download::ArtifactDownloadRequest;
use pioneer_client::composer::{
    attachments::{ComposerAttachment, ComposerAttachmentKind, ComposerAttachmentUploadState},
    turn_prepare::PrepareComposerTurnRequest,
};
use pioneer_client::gateway::types::GatewayEndpointKind;
use pioneer_client::rpc::PendingJsonRpcRequests;
use pioneer_protocol::{
    GatewayNotification, TaskAcceptParams, TaskCancelParams, TaskCancelScope, TaskGetResponse,
    TaskReviseParams, TaskStatus, TaskWaitResponse, ThreadStartParams, ThreadUnsubscribeStatus,
    TurnItem, TurnStartParams, TurnStatus, UserInput, WorkspaceChangeKind, WorkspaceCreateParams,
    WorkspaceSelectParams, WorkspaceUpdateParams, constants::events, generate_id,
};
use serde_json::json;
use sha2::{Digest, Sha256};
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

fn waiting_review_task_json() -> serde_json::Value {
    json!({
        "id": "task_review00000001",
        "workspaceId": TEST_WORKSPACE_ID,
        "ownerKind": "thread",
        "ownerId": "thread_parent000001",
        "createdByThreadId": "thread_parent000001",
        "createdByTurnId": "turn_parent0000001",
        "executorKind": "agent",
        "status": "waiting_review",
        "title": "Review child work",
        "goal": "Produce a result",
        "priority": 0,
        "revision": 1,
        "createdAt": 10,
        "updatedAt": 20
    })
}

fn pending_review_candidate_json() -> serde_json::Value {
    json!({
        "id": "candidate_review0001",
        "taskId": "task_review00000001",
        "runId": "run_review000000001",
        "taskRunTurnId": "run_turn_initial001",
        "threadId": "thread_child0000001",
        "turnId": "turn_child000000001",
        "round": 0,
        "status": "pending_review",
        "result": {
            "summary": "child result",
            "data": {
                "kind": "string",
                "value": "result body"
            }
        },
        "summary": "child result",
        "diagnostics": ["schema matched"],
        "createdAt": 20,
        "updatedAt": 20
    })
}

#[test]
fn normalize_ws_url_keeps_existing_scheme() {
    assert_eq!(normalize_ws_url("ws://0.0.0.0:17878"), "ws://0.0.0.0:17878");
    assert_eq!(
        normalize_ws_url("wss://gateway.example.com/socket"),
        "wss://gateway.example.com/socket"
    );
}

#[test]
fn normalize_ws_url_adds_ws_scheme_when_missing() {
    assert_eq!(normalize_ws_url("0.0.0.0:17878"), "ws://0.0.0.0:17878");
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
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();
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
        &mut pending_artifact_upload_chunks,
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
fn phase_12_process_text_payload_decodes_waiting_review_task_item() {
    let (event_tx, event_rx) = mpsc::channel();
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();
    let payload = json!({
        "jsonrpc": "2.0",
        "method": events::ITEM_COMPLETED,
        "params": {
            "workspace_id": TEST_WORKSPACE_ID,
            "thread_id": "thread_parent000001",
            "turn_id": "turn_parent0000001",
            "item": {
                "type": "task",
                "id": "task_item_review001",
                "taskId": "task_review00000001",
                "runId": "run_review000000001",
                "parentTaskId": null,
                "rootTaskId": null,
                "title": "Review child work",
                "status": "waiting_review",
                "triggerKind": "immediate",
                "executorKind": "agent",
                "childThreadId": "thread_child0000001",
                "childTurnId": "turn_child000000001",
                "agentRole": "worker",
                "depth": 0,
                "maxDepth": 3,
                "nextFireAt": null,
                "resultPreview": null,
                "errorPreview": null,
                "createdAt": 10,
                "updatedAt": 20
            }
        }
    })
    .to_string();

    process_text_payload(
        &payload,
        17,
        &mut pending_requests,
        &mut pending_upload_chunks,
        &mut pending_artifact_upload_chunks,
        &event_tx,
    );

    let event = event_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("expected websocket event");
    match event {
        GatewayWsEvent::Notification {
            connection_id,
            notification: GatewayNotification::ItemCompleted(notification),
        } => {
            assert_eq!(connection_id, 17);
            let TurnItem::Task { item } = notification.item else {
                panic!("expected task item");
            };
            assert_eq!(item.status, TaskStatus::WaitingReview);
            assert_eq!(item.child_thread_id.as_deref(), Some("thread_child0000001"));
            assert_eq!(item.child_turn_id.as_deref(), Some("turn_child000000001"));
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn phase_12_desktop_decodes_task_wait_review_required_payload() {
    let decoded: TaskWaitResponse = serde_json::from_value(json!({
        "completed": [],
        "failed": [],
        "cancelled": [],
        "reviewRequired": [{
            "item": {
                "task": waiting_review_task_json(),
                "childThreadId": "thread_child0000001",
                "childTurnId": "turn_child000000001"
            },
            "candidate": pending_review_candidate_json(),
            "reviewPolicy": {
                "mode": "user_approval",
                "maxRevisionRounds": 2,
                "requireExplicitAcceptance": true,
                "reviewers": [],
                "resolutionStrategy": "user_final"
            },
            "maxRevisionRounds": 2,
            "remainingRevisionRounds": 1,
            "allowedActions": ["task_accept", "task_revise", "task_cancel"]
        }],
        "pending": [],
        "nonWaitable": [],
        "timedOut": false,
        "totalCount": 1,
        "terminalCount": 0,
        "pendingCount": 0,
        "reviewRequiredCount": 1,
        "nonWaitableCount": 0,
        "mode": "all_terminal_or_review_required"
    }))
    .expect("desktop should decode review-required wait payload");

    assert_eq!(decoded.review_required_count, 1);
    assert_eq!(
        decoded.review_required[0]
            .review_policy
            .as_ref()
            .map(|policy| policy.mode),
        Some(pioneer_protocol::TaskAgentReviewMode::UserApproval)
    );
    assert_eq!(
        decoded.review_required[0].candidate.summary.as_deref(),
        Some("child result")
    );
}

#[test]
fn phase_12_desktop_decodes_candidate_and_review_history_payload() {
    let decoded: TaskGetResponse = serde_json::from_value(json!({
        "task": waiting_review_task_json(),
        "triggers": [],
        "runs": [],
        "agentSpecs": [],
        "dependencies": [],
        "writeLocks": [],
        "threadLineage": [],
        "taskRunThreadBindings": [],
        "taskRunTurns": [{
            "id": "run_turn_initial001",
            "taskId": "task_review00000001",
            "runId": "run_review000000001",
            "executionId": null,
            "threadId": "thread_child0000001",
            "turnId": "turn_child000000001",
            "kind": "initial",
            "round": 0,
            "sequence": 0,
            "status": "candidate_created",
            "createdAt": 10,
            "startedAt": 11,
            "completedAt": 20
        }, {
            "id": "run_turn_revision01",
            "taskId": "task_review00000001",
            "runId": "run_review000000001",
            "executionId": null,
            "threadId": "thread_child0000001",
            "turnId": "turn_child000000002",
            "kind": "revision",
            "round": 1,
            "sequence": 1,
            "status": "in_progress",
            "requestedByCandidateId": "candidate_review0001",
            "requestedByReviewEventId": "review_event0000001",
            "createdAt": 21,
            "startedAt": 22
        }],
        "resultCandidates": [pending_review_candidate_json()],
        "resultReviewEvents": [{
            "id": "review_event0000001",
            "candidateId": "candidate_review0001",
            "taskId": "task_review00000001",
            "runId": "run_review000000001",
            "taskRunTurnId": "run_turn_initial001",
            "reviewerKind": "review_agent",
            "reviewerThreadId": "thread_reviewer0001",
            "reviewerTurnId": "turn_reviewer00001",
            "eventKind": "advisory",
            "decision": "request_changes",
            "feedbackText": "tighten the result",
            "nextTaskRunTurnId": "run_turn_revision01",
            "createdAt": 21
        }]
    }))
    .expect("desktop should decode candidate and review history payload");

    assert_eq!(decoded.task_run_turns.len(), 2);
    assert_eq!(decoded.result_candidates[0].id, "candidate_review0001");
    assert_eq!(
        decoded.result_review_events[0].reviewer_kind,
        pioneer_protocol::TaskResultReviewerKind::ReviewAgent
    );
}

#[test]
fn process_text_payload_maps_thread_updated_notifications() {
    let (event_tx, event_rx) = mpsc::channel();
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();
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
        &mut pending_artifact_upload_chunks,
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
fn process_text_payload_maps_workspace_changed_notifications() {
    let (event_tx, event_rx) = mpsc::channel();
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();
    let payload = json!({
        "jsonrpc": "2.0",
        "method": events::WORKSPACE_CHANGED,
        "params": {
            "kind": "updated",
            "workspace": {
                "id": "ws_123",
                "name": "Renamed Workspace",
                "is_active": true,
                "is_current": false,
                "created_at": 1,
                "updated_at": 2
            }
        }
    })
    .to_string();

    process_text_payload(
        &payload,
        17,
        &mut pending_requests,
        &mut pending_upload_chunks,
        &mut pending_artifact_upload_chunks,
        &event_tx,
    );

    let event = event_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("expected websocket event");
    match event {
        GatewayWsEvent::Notification {
            connection_id,
            notification: GatewayNotification::WorkspaceChanged(notification),
        } => {
            assert_eq!(connection_id, 17);
            assert_eq!(notification.kind, WorkspaceChangeKind::Updated);
            assert_eq!(notification.workspace.id, "ws_123");
            assert_eq!(notification.workspace.name, "Renamed Workspace");
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
fn artifact_upload_frame_is_encoded_with_artu_magic() {
    let frame = encode_artifact_upload_chunk_frame(
        TEST_WORKSPACE_ID.to_owned(),
        "artifact_upload_1".to_owned(),
        0,
        b"hello",
    )
    .expect("encode artifact frame");

    assert_eq!(&frame[0..4], b"ARTU");
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
fn artifact_capabilities_rejects_missing_workspace_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .artifact_capabilities(pioneer_protocol::ArtifactCapabilitiesParams {
            workspace_id: String::new(),
        })
        .expect_err("missing workspace should be rejected");

    assert!(error.to_string().contains("workspace_id is required"));
    let _ = sender.shutdown();
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
fn artifact_upload_ack_routes_to_pending_sender() {
    let (event_tx, _event_rx) = mpsc::channel();
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();
    let (ack_tx, ack_rx) = mpsc::channel();
    pending_artifact_upload_chunks.insert("artifact_upload_1:0".to_owned(), ack_tx);
    let payload = json!({
        "jsonrpc": "2.0",
        "method": events::ARTIFACT_UPLOAD_CHUNK_ACK,
        "params": {
            "workspace_id": TEST_WORKSPACE_ID,
            "upload_id": "artifact_upload_1",
            "offset": 0,
            "len": 5,
            "received_bytes": 5,
            "next_offset": 5
        }
    })
    .to_string();

    process_text_payload(
        &payload,
        17,
        &mut pending_requests,
        &mut pending_upload_chunks,
        &mut pending_artifact_upload_chunks,
        &event_tx,
    );

    let ack = ack_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("expected artifact ack")
        .expect("ack should be ok");
    assert_eq!(ack.upload_id, "artifact_upload_1");
    assert!(pending_artifact_upload_chunks.is_empty());
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
fn phase_12_task_review_requests_reject_missing_ids_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let accept_error = sender
        .task_accept(TaskAcceptParams {
            task_id: " ".to_owned(),
            run_id: "run_review000000001".to_owned(),
            candidate_id: "candidate_review0001".to_owned(),
            reason: None,
        })
        .expect_err("empty task id must fail before JSON-RPC send");
    assert!(format!("{accept_error:#}").contains("task_id"));

    let revise_error = sender
        .task_revise(TaskReviseParams {
            task_id: "task_review00000001".to_owned(),
            run_id: " ".to_owned(),
            candidate_id: "candidate_review0001".to_owned(),
            feedback: "fix it".to_owned(),
            additional_instructions: Vec::new(),
        })
        .expect_err("empty run id must fail before JSON-RPC send");
    assert!(format!("{revise_error:#}").contains("run_id"));

    let cancel_error = sender
        .task_cancel(TaskCancelParams {
            task_id: " ".to_owned(),
            reason: None,
            scope: TaskCancelScope::AttachedSubtree,
        })
        .expect_err("empty task id must fail before JSON-RPC send");
    assert!(format!("{cancel_error:#}").contains("task_id"));

    let _ = sender.shutdown();
}

#[test]
fn phase_12_task_revise_rejects_blank_feedback_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .task_revise(TaskReviseParams {
            task_id: "task_review00000001".to_owned(),
            run_id: "run_review000000001".to_owned(),
            candidate_id: "candidate_review0001".to_owned(),
            feedback: "   ".to_owned(),
            additional_instructions: Vec::new(),
        })
        .expect_err("blank feedback must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("feedback"));

    let _ = sender.shutdown();
}

#[test]
fn workspace_create_rejects_missing_workspace_id_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .workspace_create(WorkspaceCreateParams {
            workspace_id: "   ".to_owned(),
            name: Some("Workspace".to_owned()),
            make_current: false,
        })
        .expect_err("empty workspace_id must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("workspace_id"));

    let _ = sender.shutdown();
}

#[test]
fn workspace_select_rejects_missing_workspace_id_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .workspace_select(WorkspaceSelectParams {
            workspace_id: "   ".to_owned(),
            make_current: true,
        })
        .expect_err("empty workspace_id must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("workspace_id"));

    let _ = sender.shutdown();
}

#[test]
fn workspace_update_rejects_missing_fields_before_request() {
    let client = GatewayWsClient::new();
    let sender = client.command_sender();

    let error = sender
        .workspace_update(WorkspaceUpdateParams {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            name: None,
        })
        .expect_err("missing update fields must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("at least one field"));

    let error = sender
        .workspace_update(WorkspaceUpdateParams {
            workspace_id: TEST_WORKSPACE_ID.to_owned(),
            name: Some("   ".to_owned()),
        })
        .expect_err("empty name must fail before JSON-RPC send");
    assert!(format!("{error:#}").contains("name"));

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
            capabilities: Vec::new(),
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
            capabilities: Vec::new(),
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
