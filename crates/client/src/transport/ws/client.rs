//! WebSocket client worker.

use super::{
    GatewayWsConnectSpec, GatewayWsEvent,
    decode::{
        fail_pending_artifact_download_chunks, fail_pending_artifact_upload_chunks,
        fail_pending_requests, fail_pending_upload_chunks, process_text_payload, upload_ack_key,
    },
    download::{
        ArtifactDownloadChunkPayload, artifact_download_chunk_key,
        process_artifact_download_binary_frame,
    },
    rpc::{build_ws_request, normalize_ws_url},
    worker::{
        WEBSOCKET_CLOSED_BY_PEER_MESSAGE, WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE,
        WEBSOCKET_PONG_TIMEOUT_MESSAGE, WEBSOCKET_STREAM_ENDED_MESSAGE, connect_failed_event,
        connected_event, connecting_event, disconnected_event, next_reconnect_plan,
        should_retry_after_connect_failure, websocket_ping_failed_message,
        websocket_pong_send_failed_message, websocket_read_failed_message,
        websocket_write_failed_message,
    },
};
use crate::rpc::PendingJsonRpcRequests;
use anyhow::{Context as _, Result};
use futures_util::{SinkExt, StreamExt};
use pioneer_protocol::{ArtifactUploadChunkAckNotification, SkillsUploadChunkAckNotification};
use serde_json::Value as JsonValue;
use std::{collections::HashMap, sync::mpsc::Sender, thread};
use tokio::{
    runtime::Runtime,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior, sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub enum GatewayWsCommand {
    Connect {
        connection_id: u64,
        spec: GatewayWsConnectSpec,
        initial_result_tx: Option<Sender<std::result::Result<(), String>>>,
        retry_initial_failure: bool,
    },
    Request {
        request_id: String,
        payload: String,
        response_tx: Sender<std::result::Result<JsonValue, String>>,
    },
    BinaryUploadChunk {
        upload_id: String,
        offset: u64,
        payload: Vec<u8>,
        response_tx: Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>,
    },
    ArtifactBinaryUploadChunk {
        upload_id: String,
        offset: u64,
        payload: Vec<u8>,
        response_tx: Sender<std::result::Result<ArtifactUploadChunkAckNotification, String>>,
    },
    VoiceBinaryChunk {
        payload: Vec<u8>,
    },
    ArtifactDownloadRegisterChunk {
        download_id: String,
        offset: u64,
        response_tx: Sender<std::result::Result<ArtifactDownloadChunkPayload, String>>,
        registered_tx: Sender<std::result::Result<(), String>>,
    },
    Disconnect,
    Shutdown,
}

enum ConnectionRpcCommand {
    Request {
        request_id: String,
        payload: String,
        response_tx: Sender<std::result::Result<JsonValue, String>>,
    },
    BinaryUploadChunk {
        upload_id: String,
        offset: u64,
        payload: Vec<u8>,
        response_tx: Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>,
    },
    ArtifactBinaryUploadChunk {
        upload_id: String,
        offset: u64,
        payload: Vec<u8>,
        response_tx: Sender<std::result::Result<ArtifactUploadChunkAckNotification, String>>,
    },
    VoiceBinaryChunk {
        payload: Vec<u8>,
    },
    ArtifactDownloadRegisterChunk {
        download_id: String,
        offset: u64,
        response_tx: Sender<std::result::Result<ArtifactDownloadChunkPayload, String>>,
        registered_tx: Sender<std::result::Result<(), String>>,
    },
}

struct ActiveConnectionTask {
    handle: JoinHandle<()>,
    rpc_tx: UnboundedSender<ConnectionRpcCommand>,
}

pub fn spawn_worker(
    command_rx: UnboundedReceiver<GatewayWsCommand>,
    event_tx: Sender<GatewayWsEvent>,
) {
    thread::spawn(move || {
        let runtime = Runtime::new().expect("failed to create tokio runtime for websocket worker");
        runtime.block_on(async move {
            run_worker(command_rx, event_tx).await;
        });
    });
}

pub fn connect_websocket_once(spec: &GatewayWsConnectSpec) -> Result<()> {
    let runtime = Runtime::new().context("failed to create tokio runtime for websocket connect")?;
    runtime.block_on(async {
        let stream = connect_websocket(spec).await?;
        drop(stream);
        Ok(())
    })
}

async fn run_worker(
    mut command_rx: UnboundedReceiver<GatewayWsCommand>,
    event_tx: Sender<GatewayWsEvent>,
) {
    let mut connection_task: Option<ActiveConnectionTask> = None;

    while let Some(command) = command_rx.recv().await {
        match command {
            GatewayWsCommand::Connect {
                connection_id,
                spec,
                initial_result_tx,
                retry_initial_failure,
            } => {
                abort_connection_task(&mut connection_task).await;
                let (rpc_tx, rpc_rx) = unbounded_channel();
                connection_task = Some(ActiveConnectionTask {
                    handle: tokio::spawn(run_connection_task(
                        connection_id,
                        spec,
                        initial_result_tx,
                        retry_initial_failure,
                        event_tx.clone(),
                        rpc_rx,
                    )),
                    rpc_tx,
                });
            }
            GatewayWsCommand::Request {
                request_id,
                payload,
                response_tx,
            } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    let _ = response_tx.send(Err("websocket is not connected".to_owned()));
                    continue;
                };

                let fallback_tx = response_tx.clone();
                if connection_task
                    .rpc_tx
                    .send(ConnectionRpcCommand::Request {
                        request_id,
                        payload,
                        response_tx,
                    })
                    .is_err()
                {
                    let _ = fallback_tx
                        .send(Err("websocket connection task is unavailable".to_owned()));
                }
            }
            GatewayWsCommand::BinaryUploadChunk {
                upload_id,
                offset,
                payload,
                response_tx,
            } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    let _ = response_tx.send(Err("websocket is not connected".to_owned()));
                    continue;
                };

                let fallback_tx = response_tx.clone();
                if connection_task
                    .rpc_tx
                    .send(ConnectionRpcCommand::BinaryUploadChunk {
                        upload_id,
                        offset,
                        payload,
                        response_tx,
                    })
                    .is_err()
                {
                    let _ = fallback_tx
                        .send(Err("websocket connection task is unavailable".to_owned()));
                }
            }
            GatewayWsCommand::ArtifactBinaryUploadChunk {
                upload_id,
                offset,
                payload,
                response_tx,
            } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    let _ = response_tx.send(Err("websocket is not connected".to_owned()));
                    continue;
                };

                let fallback_tx = response_tx.clone();
                if connection_task
                    .rpc_tx
                    .send(ConnectionRpcCommand::ArtifactBinaryUploadChunk {
                        upload_id,
                        offset,
                        payload,
                        response_tx,
                    })
                    .is_err()
                {
                    let _ = fallback_tx
                        .send(Err("websocket connection task is unavailable".to_owned()));
                }
            }
            GatewayWsCommand::VoiceBinaryChunk { payload } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    continue;
                };

                let _ = connection_task
                    .rpc_tx
                    .send(ConnectionRpcCommand::VoiceBinaryChunk { payload });
            }
            GatewayWsCommand::ArtifactDownloadRegisterChunk {
                download_id,
                offset,
                response_tx,
                registered_tx,
            } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    let _ = registered_tx.send(Err("websocket is not connected".to_owned()));
                    continue;
                };

                let fallback_tx = registered_tx.clone();
                if connection_task
                    .rpc_tx
                    .send(ConnectionRpcCommand::ArtifactDownloadRegisterChunk {
                        download_id,
                        offset,
                        response_tx,
                        registered_tx,
                    })
                    .is_err()
                {
                    let _ = fallback_tx
                        .send(Err("websocket connection task is unavailable".to_owned()));
                }
            }
            GatewayWsCommand::Disconnect => {
                abort_connection_task(&mut connection_task).await;
            }
            GatewayWsCommand::Shutdown => {
                break;
            }
        }
    }

    abort_connection_task(&mut connection_task).await;
}

async fn abort_connection_task(connection_task: &mut Option<ActiveConnectionTask>) {
    if let Some(connection_task) = connection_task.take() {
        connection_task.handle.abort();
        let _ = connection_task.handle.await;
    }
}

async fn run_connection_task(
    connection_id: u64,
    spec: GatewayWsConnectSpec,
    mut initial_result_tx: Option<Sender<std::result::Result<(), String>>>,
    retry_initial_failure: bool,
    event_tx: Sender<GatewayWsEvent>,
    mut rpc_rx: UnboundedReceiver<ConnectionRpcCommand>,
) {
    let mut has_connected = false;
    let mut attempt: u32 = 0;
    let mut backoff = spec.timings.reconnect_initial;

    loop {
        let _ = event_tx.send(connecting_event(connection_id, &spec));

        match connect_websocket(&spec).await {
            Ok(stream) => {
                has_connected = true;
                attempt = 0;
                backoff = spec.timings.reconnect_initial;

                if let Some(sender) = initial_result_tx.take() {
                    let _ = sender.send(Ok(()));
                }

                let _ = event_tx.send(connected_event(connection_id, &spec));

                let reason =
                    monitor_connection(stream, &spec, connection_id, &event_tx, &mut rpc_rx).await;

                let _ = event_tx.send(disconnected_event(connection_id, &spec, reason.clone()));

                let plan = next_reconnect_plan(connection_id, &spec, attempt, backoff, reason);
                attempt = plan.attempt;
                let delay = plan.delay;
                let _ = event_tx.send(plan.event);

                sleep(delay).await;
                backoff = plan.next_backoff;
            }
            Err(error) => {
                let message = format!("{error:#}");

                if let Some(sender) = initial_result_tx.take() {
                    let _ = sender.send(Err(message.clone()));

                    if !retry_initial_failure {
                        let _ = event_tx.send(connect_failed_event(connection_id, &spec, message));
                        return;
                    }
                }

                if !should_retry_after_connect_failure(has_connected, retry_initial_failure) {
                    return;
                }

                let plan = next_reconnect_plan(connection_id, &spec, attempt, backoff, message);
                attempt = plan.attempt;
                let delay = plan.delay;
                let _ = event_tx.send(plan.event);

                sleep(delay).await;
                backoff = plan.next_backoff;
            }
        }
    }
}

async fn connect_websocket(
    spec: &GatewayWsConnectSpec,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let ws_url = normalize_ws_url(spec.address.as_str());
    let request = build_ws_request(ws_url.as_str(), spec.auth_token.as_deref())?;
    let connect = timeout(spec.timings.connect_timeout, connect_async(request))
        .await
        .context("websocket connect timeout reached")?;

    let (stream, _) = connect.context("websocket handshake failed")?;
    Ok(stream)
}

async fn monitor_connection(
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    spec: &GatewayWsConnectSpec,
    connection_id: u64,
    event_tx: &Sender<GatewayWsEvent>,
    rpc_rx: &mut UnboundedReceiver<ConnectionRpcCommand>,
) -> String {
    let (mut writer, mut reader) = stream.split();
    let mut ping_interval = tokio::time::interval(spec.timings.ping_interval);
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_pong = Instant::now();
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();
    let mut pending_artifact_download_chunks = HashMap::new();

    let disconnect_reason = loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if Instant::now().duration_since(last_pong) > spec.timings.pong_timeout {
                    break WEBSOCKET_PONG_TIMEOUT_MESSAGE.to_owned();
                }

                if let Err(error) = writer.send(Message::Ping(Vec::new().into())).await {
                    break websocket_ping_failed_message(error);
                }
            }
            command = rpc_rx.recv() => {
                match command {
                    Some(ConnectionRpcCommand::Request { request_id, payload, response_tx }) => {
                        pending_requests.insert(request_id.clone(), response_tx);
                        if let Err(error) = writer.send(Message::Text(payload.into())).await {
                            let message = websocket_write_failed_message(error);
                            if let Some(response_tx) = pending_requests.remove(request_id.as_str()) {
                                let _ = response_tx.send(Err(message.clone()));
                            }
                            break message;
                        }
                    }
                    Some(ConnectionRpcCommand::BinaryUploadChunk { upload_id, offset, payload, response_tx }) => {
                        let ack_key = upload_ack_key(upload_id.as_str(), offset);
                        pending_upload_chunks.insert(ack_key.clone(), response_tx);
                        if let Err(error) = writer.send(Message::Binary(payload.into())).await {
                            if let Some(response_tx) = pending_upload_chunks.remove(&ack_key) {
                                let _ = response_tx.send(Err(format!("websocket binary write failed: {error}")));
                            }
                            break format!("websocket binary write failed: {error}");
                        }
                    }
                    Some(ConnectionRpcCommand::ArtifactBinaryUploadChunk { upload_id, offset, payload, response_tx }) => {
                        let ack_key = upload_ack_key(upload_id.as_str(), offset);
                        pending_artifact_upload_chunks.insert(ack_key.clone(), response_tx);
                        if let Err(error) = writer.send(Message::Binary(payload.into())).await {
                            if let Some(response_tx) = pending_artifact_upload_chunks.remove(&ack_key) {
                                let _ = response_tx.send(Err(format!("websocket binary write failed: {error}")));
                            }
                            break format!("websocket binary write failed: {error}");
                        }
                    }
                    Some(ConnectionRpcCommand::VoiceBinaryChunk { payload }) => {
                        if let Err(error) = writer.send(Message::Binary(payload.into())).await {
                            break format!("websocket voice binary write failed: {error}");
                        }
                    }
                    Some(ConnectionRpcCommand::ArtifactDownloadRegisterChunk { download_id, offset, response_tx, registered_tx }) => {
                        let key = artifact_download_chunk_key(download_id.as_str(), offset);
                        pending_artifact_download_chunks.insert(key, response_tx);
                        let _ = registered_tx.send(Ok(()));
                    }
                    None => {
                        break WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE.to_owned();
                    }
                }
            }
            payload = reader.next() => {
                match payload {
                    Some(Ok(Message::Pong(_))) => {
                        last_pong = Instant::now();
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if let Err(error) = writer.send(Message::Pong(payload)).await {
                            break websocket_pong_send_failed_message(error);
                        }
                        last_pong = Instant::now();
                    }
                    Some(Ok(Message::Close(frame))) => {
                        break frame
                            .map(|value| value.reason.to_string())
                            .unwrap_or_else(|| WEBSOCKET_CLOSED_BY_PEER_MESSAGE.to_owned());
                    }
                    Some(Ok(Message::Text(payload))) => {
                        process_text_payload(
                            payload.as_ref(),
                            connection_id,
                            &mut pending_requests,
                            &mut pending_upload_chunks,
                            &mut pending_artifact_upload_chunks,
                            event_tx,
                        );
                    }
                    Some(Ok(Message::Binary(payload))) => {
                        let _ = process_artifact_download_binary_frame(
                            payload.as_ref(),
                            &mut pending_artifact_download_chunks,
                        );
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => {
                        break websocket_read_failed_message(error);
                    }
                    None => {
                        break WEBSOCKET_STREAM_ENDED_MESSAGE.to_owned();
                    }
                }
            }
        }
    };

    fail_pending_requests(&mut pending_requests, disconnect_reason.as_str());
    fail_pending_upload_chunks(&mut pending_upload_chunks, disconnect_reason.as_str());
    fail_pending_artifact_upload_chunks(
        &mut pending_artifact_upload_chunks,
        disconnect_reason.as_str(),
    );
    fail_pending_artifact_download_chunks(
        &mut pending_artifact_download_chunks,
        disconnect_reason.as_str(),
    );
    disconnect_reason
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{timings::GatewayWsTimings, types::GatewayEndpointKind};
    use std::{net::TcpListener as StdTcpListener, sync::mpsc, time::Duration};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{
        accept_async, accept_hdr_async,
        tungstenite::{
            handshake::server::{ErrorResponse, Request, Response},
            http::{Response as HttpResponse, StatusCode},
        },
    };

    #[test]
    fn ws_client_worker_connects_and_emits_events() {
        let address = reserve_unused_local_address();
        let server = TestWsServer::start(address.clone());
        let (command_tx, command_rx) = unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel();
        spawn_worker(command_rx, event_tx);

        let (initial_tx, initial_rx) = mpsc::channel();
        command_tx
            .send(GatewayWsCommand::Connect {
                connection_id: 7,
                spec: connect_spec(address),
                initial_result_tx: Some(initial_tx),
                retry_initial_failure: false,
            })
            .expect("send connect");

        assert_eq!(
            initial_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("initial result"),
            Ok(())
        );

        let first = event_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("connecting event");
        let second = event_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("connected event");
        assert!(matches!(
            first,
            GatewayWsEvent::Connecting {
                connection_id: 7,
                ..
            }
        ));
        assert!(matches!(
            second,
            GatewayWsEvent::Connected {
                connection_id: 7,
                ..
            }
        ));

        command_tx
            .send(GatewayWsCommand::Shutdown)
            .expect("send shutdown");
        server.join();
    }

    #[test]
    fn connect_websocket_once_sends_bearer_token() {
        let address = reserve_unused_local_address();
        let server = TestAuthWsServer::start(address.clone(), "valid-token");
        let mut spec = connect_spec(address);
        spec.auth_token = Some(" valid-token ".to_owned());

        connect_websocket_once(&spec).expect("connect with bearer token");

        server.join();
    }

    #[test]
    fn connect_websocket_once_rejects_invalid_bearer_token() {
        let address = reserve_unused_local_address();
        let server = TestAuthWsServer::start(address.clone(), "valid-token");
        let mut spec = connect_spec(address);
        spec.auth_token = Some("invalid-token".to_owned());

        let error = connect_websocket_once(&spec).expect_err("invalid bearer token should fail");

        assert!(format!("{error:#}").contains("websocket handshake failed"));
        server.join();
    }

    fn connect_spec(address: String) -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: "local".to_owned(),
            endpoint_name: "Local".to_owned(),
            endpoint_kind: GatewayEndpointKind::Local,
            address,
            auth_token: None,
            timings: GatewayWsTimings::from_millis(500, 200, 1_000, 10, 50, 0).expect("timings"),
        }
    }

    fn reserve_unused_local_address() -> String {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve port");
        listener.local_addr().expect("local addr").to_string()
    }

    struct TestWsServer {
        handle: std::thread::JoinHandle<()>,
    }

    impl TestWsServer {
        fn start(address: String) -> Self {
            let (ready_tx, ready_rx) = mpsc::channel();
            let handle = std::thread::spawn(move || {
                let runtime = Runtime::new().expect("server runtime");
                runtime.block_on(async move {
                    let listener = match TcpListener::bind(address.as_str()).await {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!("bind server failed: {error}")));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));
                    let (stream, _) = listener.accept().await.expect("accept");
                    let mut websocket = accept_async(stream).await.expect("accept websocket");
                    let _ = timeout(Duration::from_millis(200), websocket.next()).await;
                });
            });
            ready_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("server readiness")
                .expect("server bind");
            Self { handle }
        }

        fn join(self) {
            let _ = self.handle.join();
        }
    }

    struct TestAuthWsServer {
        handle: std::thread::JoinHandle<()>,
    }

    impl TestAuthWsServer {
        fn start(address: String, expected_token: &'static str) -> Self {
            let (ready_tx, ready_rx) = mpsc::channel();
            let handle = std::thread::spawn(move || {
                let runtime = Runtime::new().expect("server runtime");
                runtime.block_on(async move {
                    let listener = match TcpListener::bind(address.as_str()).await {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_tx.send(Err(format!("bind server failed: {error}")));
                            return;
                        }
                    };
                    let _ = ready_tx.send(Ok(()));
                    let (stream, _) = listener.accept().await.expect("accept");
                    let callback = move |request: &Request, response: Response| {
                        let expected = format!("Bearer {expected_token}");
                        let authorization = request
                            .headers()
                            .get("authorization")
                            .and_then(|value| value.to_str().ok());

                        if authorization == Some(expected.as_str()) {
                            return Ok(response);
                        }

                        Err(unauthorized_response())
                    };
                    if let Ok(mut websocket) = accept_hdr_async(stream, callback).await {
                        let _ = timeout(Duration::from_millis(200), websocket.next()).await;
                    }
                });
            });
            ready_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("server readiness")
                .expect("server bind");
            Self { handle }
        }

        fn join(self) {
            let _ = self.handle.join();
        }
    }

    fn unauthorized_response() -> ErrorResponse {
        HttpResponse::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Some("unauthorized".to_owned()))
            .expect("unauthorized response")
    }
}
