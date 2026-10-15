//! WebSocket client worker.

use super::{
    GatewayWsConnectSpec, GatewayWsEvent,
    decode::{
        fail_pending_artifact_upload_chunks, fail_pending_requests, fail_pending_upload_chunks,
        process_text_payload, upload_ack_key,
    },
    rpc::build_ws_request,
    worker::{
        WEBSOCKET_CLOSED_BY_PEER_MESSAGE, WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE,
        WEBSOCKET_PONG_TIMEOUT_MESSAGE, WEBSOCKET_STREAM_ENDED_MESSAGE, connect_failed_event,
        connected_event, connecting_event, disconnect_reason_from_close, disconnected_event,
        next_reconnect_plan, should_retry_after_connect_failure, terminal_reason_from_disconnect,
        websocket_ping_failed_message, websocket_pong_send_failed_message,
        websocket_read_failed_message, websocket_write_failed_message,
    },
};
use crate::rpc::{JsonRpcResponseSender, PendingJsonRpcRequests};
use anyhow::{Context as _, Result};
use futures_util::{SinkExt, StreamExt};
use pioneer_protocol::{
    ArtifactUploadChunkAckNotification, AuthSecretString, GatewayNotification,
    SkillsUploadChunkAckNotification,
};
use std::time::{SystemTime, UNIX_EPOCH};
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
    Replace {
        connection_id: u64,
        spec: GatewayWsConnectSpec,
        result_tx: Sender<std::result::Result<(), String>>,
    },
    RenewAccess {
        connection_id: u64,
        spec: GatewayWsConnectSpec,
        refresh_generation: u64,
        deadline: Instant,
        response_tx: JsonRpcResponseSender,
    },
    Request {
        expected_connection: Option<u64>,
        request_id: String,
        payload: String,
        response_tx: JsonRpcResponseSender,
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
        expected_connection: Option<u64>,
        payload: Vec<u8>,
    },
    Disconnect,
    DisconnectConnection {
        connection_id: u64,
    },
    Shutdown,
}

enum ConnectionRpcCommand {
    RenewAccess {
        spec: GatewayWsConnectSpec,
        refresh_generation: u64,
        deadline: Instant,
        response_tx: JsonRpcResponseSender,
    },
    Request {
        request_id: String,
        payload: String,
        response_tx: JsonRpcResponseSender,
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
}

struct ActiveConnectionTask {
    connection_id: u64,
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
                    connection_id,
                    handle: tokio::spawn(run_connection_task(
                        connection_id,
                        spec,
                        initial_result_tx,
                        retry_initial_failure,
                        event_tx.clone(),
                        rpc_rx,
                        None,
                    )),
                    rpc_tx,
                });
            }
            GatewayWsCommand::Replace {
                connection_id,
                spec,
                result_tx,
            } => {
                if session_access_is_expired(&spec, unix_timestamp_secs()) {
                    let _ = result_tx.send(Err("access_refresh_required".to_owned()));
                    continue;
                }
                let _ = event_tx.send(connecting_event(connection_id, &spec));
                match connect_websocket(&spec).await {
                    Ok(stream) => {
                        abort_connection_task(&mut connection_task).await;
                        let (rpc_tx, rpc_rx) = unbounded_channel();
                        connection_task = Some(ActiveConnectionTask {
                            connection_id,
                            handle: tokio::spawn(run_connection_task(
                                connection_id,
                                spec,
                                None,
                                true,
                                event_tx.clone(),
                                rpc_rx,
                                Some(stream),
                            )),
                            rpc_tx,
                        });
                        let _ = result_tx.send(Ok(()));
                    }
                    Err(error) => {
                        let message = format!("{error:#}");
                        let _ = event_tx.send(connect_failed_event(
                            connection_id,
                            &spec,
                            message.clone(),
                        ));
                        let _ = result_tx.send(Err(message));
                    }
                }
            }
            GatewayWsCommand::RenewAccess {
                connection_id,
                spec,
                refresh_generation,
                deadline,
                response_tx,
            } => {
                let fallback = response_tx.clone();
                if let Some(task) = connection_task
                    .as_ref()
                    .filter(|task| task.connection_id == connection_id)
                {
                    if task
                        .rpc_tx
                        .send(ConnectionRpcCommand::RenewAccess {
                            spec,
                            refresh_generation,
                            deadline,
                            response_tx,
                        })
                        .is_ok()
                    {
                        continue;
                    }
                }
                let _ = fallback.send(Err(crate::rpc::JsonRpcResponseError::transport(
                    "connection renewal retired",
                )));
            }
            GatewayWsCommand::Request {
                expected_connection,
                request_id,
                payload,
                response_tx,
            } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(
                        "websocket is not connected",
                    )));
                    continue;
                };

                if expected_connection.is_some_and(|id| id != connection_task.connection_id) {
                    let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(
                        "websocket request connection retired",
                    )));
                    continue;
                }

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
                    let _ = fallback_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(
                        "websocket connection task is unavailable",
                    )));
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
            GatewayWsCommand::VoiceBinaryChunk {
                expected_connection,
                payload,
            } => {
                let Some(connection_task) = connection_task.as_mut() else {
                    continue;
                };

                if expected_connection.is_some_and(|id| id != connection_task.connection_id) {
                    continue;
                }
                let _ = connection_task
                    .rpc_tx
                    .send(ConnectionRpcCommand::VoiceBinaryChunk { payload });
            }
            GatewayWsCommand::Disconnect => {
                abort_connection_task(&mut connection_task).await;
            }
            GatewayWsCommand::DisconnectConnection { connection_id } => {
                if connection_task
                    .as_ref()
                    .is_some_and(|task| task.connection_id == connection_id)
                {
                    abort_connection_task(&mut connection_task).await;
                }
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
    mut spec: GatewayWsConnectSpec,
    mut initial_result_tx: Option<Sender<std::result::Result<(), String>>>,
    retry_initial_failure: bool,
    event_tx: Sender<GatewayWsEvent>,
    mut rpc_rx: UnboundedReceiver<ConnectionRpcCommand>,
    mut established_stream: Option<GatewayWebSocket>,
) {
    let mut has_connected = false;
    let mut attempt: u32 = 0;
    let mut backoff = spec.timings.reconnect_initial;

    loop {
        if session_access_is_expired(&spec, unix_timestamp_secs()) {
            let message = "access_refresh_required".to_owned();
            if let Some(sender) = initial_result_tx.take() {
                let _ = sender.send(Err(message.clone()));
            }
            let _ = event_tx.send(connect_failed_event(connection_id, &spec, message));
            return;
        }
        let connection = match established_stream.take() {
            Some(stream) => Ok(stream),
            None => {
                let _ = event_tx.send(connecting_event(connection_id, &spec));
                connect_websocket(&spec).await
            }
        };
        match connection {
            Ok(stream) => {
                has_connected = true;
                attempt = 0;
                backoff = spec.timings.reconnect_initial;

                if let Some(sender) = initial_result_tx.take() {
                    let _ = sender.send(Ok(()));
                }

                let _ = event_tx.send(connected_event(connection_id, &spec));

                let reason =
                    monitor_connection(stream, &mut spec, connection_id, &event_tx, &mut rpc_rx)
                        .await;

                let _ = event_tx.send(disconnected_event(connection_id, &spec, reason.clone()));

                if terminal_reason_from_disconnect(reason.as_str()).is_some() {
                    return;
                }
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

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn session_access_is_expired(spec: &GatewayWsConnectSpec, now_unix: u64) -> bool {
    spec.session
        .as_ref()
        .is_some_and(|session| now_unix >= session.access_expires_at_unix)
}

type GatewayWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_websocket(spec: &GatewayWsConnectSpec) -> Result<GatewayWebSocket> {
    let request = build_ws_request(
        &spec.gateway_base_url,
        spec.auth_token
            .as_ref()
            .map(AuthSecretString::expose_secret),
    )?;
    let connect = timeout(spec.timings.connect_timeout, connect_async(request))
        .await
        .context("websocket connect timeout reached")?;

    let (stream, _) = connect.context("websocket handshake failed")?;
    Ok(stream)
}

struct PendingAccessRenewal {
    request_id: String,
    spec: GatewayWsConnectSpec,
    response_tx: JsonRpcResponseSender,
    deadline: Instant,
}

fn same_session(left: &GatewayWsConnectSpec, right: &GatewayWsConnectSpec) -> bool {
    left.endpoint_id == right.endpoint_id
        && left.gateway_base_url == right.gateway_base_url
        && left
            .session
            .as_ref()
            .zip(right.session.as_ref())
            .is_some_and(|(a, b)| {
                a.server_gateway_id == b.server_gateway_id
                    && a.device_id == b.device_id
                    && a.session_id == b.session_id
            })
}

async fn monitor_connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: tokio_tungstenite::WebSocketStream<S>,
    spec: &mut GatewayWsConnectSpec,
    connection_id: u64,
    event_tx: &Sender<GatewayWsEvent>,
    rpc_rx: &mut UnboundedReceiver<ConnectionRpcCommand>,
) -> String {
    let (mut writer, mut reader) = stream.split();
    let mut ping_interval = tokio::time::interval(spec.timings.ping_interval);
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_pong = Instant::now();
    let mut pending_requests = PendingJsonRpcRequests::default();
    let mut renewal: Option<PendingAccessRenewal> = None;
    let mut pending_upload_chunks = HashMap::new();
    let mut pending_artifact_upload_chunks = HashMap::new();

    let disconnect_reason = loop {
        let renewal_deadline = renewal.as_ref().map_or_else(
            || Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT,
            |pending| pending.deadline,
        );
        tokio::select! {
            _ = tokio::time::sleep_until(renewal_deadline), if renewal.is_some() => {
                break "connection renewal timeout".to_owned();
            }
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
                    Some(ConnectionRpcCommand::RenewAccess { spec: next, refresh_generation, deadline, response_tx }) => {
                        if Instant::now() >= deadline || renewal.is_some() || !same_session(spec, &next) || session_access_is_expired(spec, unix_timestamp_secs()) {
                            let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport("connection renewal unavailable")));
                            continue;
                        }
                        let Some(token) = next.auth_token.clone() else {
                            let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::InvalidResponse));
                            continue;
                        };
                        let params = pioneer_protocol::AuthAccessRenewParams { access_token: token, refresh_generation };
                        let request = crate::rpc::build_json_rpc_request_payload(
                            pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW,
                            serde_json::to_value(params).expect("access renewal params"),
                        ).expect("access renewal request");
                        renewal = Some(PendingAccessRenewal { request_id: request.request_id,
                            spec: next, response_tx, deadline });
                        if writer.send(Message::Text(request.payload.into())).await.is_err() {
                            break "connection renewal write failed".to_owned();
                        }
                    }
                    Some(ConnectionRpcCommand::Request { request_id, payload, response_tx }) => {
                        pending_requests.insert(request_id.clone(), response_tx);
                        let startup_write = pioneer_observability::turn_startup::client_write(&payload, connection_id);
                        let write_result = writer.send(Message::Text(payload.into())).await;
                        drop(startup_write);
                        if let Err(error) = write_result {
                            let message = websocket_write_failed_message(error);
                            if let Some(response_tx) = pending_requests.remove(request_id.as_str()) {
                                let _ = response_tx.send(Err(
                                    crate::rpc::JsonRpcResponseError::transport(message.clone()),
                                ));
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
                            .map(|value| {
                                disconnect_reason_from_close(
                                    u16::from(value.code),
                                    value.reason.as_ref(),
                                )
                            })
                            .unwrap_or_else(|| WEBSOCKET_CLOSED_BY_PEER_MESSAGE.to_owned());
                    }
                    Some(Ok(Message::Text(payload))) => {
                        if let Some((id, result)) = serde_json::from_str::<serde_json::Value>(&payload)
                            .ok().as_ref().and_then(crate::rpc::decode_json_rpc_response_value)
                            && renewal.as_ref().is_some_and(|pending| pending.request_id == id)
                        {
                            let pending = renewal.take().expect("matched renewal");
                            let result = result.and_then(|value| {
                                let response: pioneer_protocol::AuthAccessRenewResponse = serde_json::from_value(value)
                                    .map_err(|_| crate::rpc::JsonRpcResponseError::InvalidResponse)?;
                                if Instant::now() >= pending.deadline || pending.spec.session.as_ref().is_none_or(|identity|
                                    identity.access_expires_at_unix != response.access_expires_at_unix)
                                    || response.access_expires_at_unix <= unix_timestamp_secs() {
                                    return Err(crate::rpc::JsonRpcResponseError::InvalidResponse);
                                }
                                // Reconnect reads this same spec, after server acknowledgement only.
                                *spec = pending.spec;
                                Ok(serde_json::Value::Null)
                            }).map_err(|error| {
                                // Do not let peer-supplied text enter lifecycle diagnostics.
                                let code = error.machine_code().filter(|code|
                                    crate::gateway::session_lifecycle::terminal_reason_from_auth_code(code).is_some()
                                    || crate::gateway::session_lifecycle::auth_code_requires_refresh(code)
                                    || matches!(*code, "temporarily_unavailable" | "auth_not_ready" | "auth_exchange_timeout" | "auth_method_not_allowed"));
                                match code {
                                    Some(code) => crate::rpc::JsonRpcResponseError::server(Some(-32040), code, Some(code.to_owned())),
                                    None => crate::rpc::JsonRpcResponseError::transport("connection renewal rejected"),
                                }
                            });
                            let failure = result.as_ref().err().map(|error| {
                                error.machine_code().filter(|code|
                                    crate::gateway::session_lifecycle::terminal_reason_from_auth_code(code).is_some()
                                    || crate::gateway::session_lifecycle::auth_code_requires_refresh(code))
                                    .unwrap_or("connection renewal rejected").to_owned()
                            });
                            let _ = pending.response_tx.send(result);
                            if let Some(reason) = failure { break reason; }
                            continue;
                        }
                        let notification = process_text_payload(
                            payload.as_ref(),
                            connection_id,
                            &mut pending_requests,
                            &mut pending_upload_chunks,
                            &mut pending_artifact_upload_chunks,
                            event_tx,
                        );
                        if let Some(GatewayNotification::AuthSessionRevoked(notification)) =
                            notification
                        {
                            break notification.reason.as_str().to_owned();
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
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
    pioneer_observability::turn_startup::connection_lost(connection_id);

    if let Some(pending) = renewal.take() {
        let _ = pending
            .response_tx
            .send(Err(crate::rpc::JsonRpcResponseError::transport(
                disconnect_reason.clone(),
            )));
    }
    fail_pending_requests(&mut pending_requests, disconnect_reason.as_str());
    fail_pending_upload_chunks(&mut pending_upload_chunks, disconnect_reason.as_str());
    fail_pending_artifact_upload_chunks(
        &mut pending_artifact_upload_chunks,
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
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn access_renewal_keeps_pending_rpc_and_voice_on_the_same_socket() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};
        let (client, server) = tokio::io::duplex(4096);
        let stream = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let mut peer = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut spec = connect_spec("https://synthetic.invalid".into());
        spec.auth_token = Some(AuthSecretString::new("old-synthetic-access"));
        spec.session = Some(crate::transport::ws::GatewayWsSessionIdentity {
            server_gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            session_id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
            device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
            access_expires_at_unix: unix_timestamp_secs() + 60,
            refresh_leeway_seconds: 10,
        });
        let mut next = spec.clone();
        next.auth_token = Some(AuthSecretString::new("next-synthetic-access"));
        next.session.as_mut().unwrap().access_expires_at_unix += 600;
        let expires = next.session.as_ref().unwrap().access_expires_at_unix;
        let (events, _event_rx) = mpsc::channel();
        let (commands, mut command_rx) = unbounded_channel();
        let task = tokio::spawn(async move {
            monitor_connection(stream, &mut spec, 7, &events, &mut command_rx).await;
            spec
        });
        let request = crate::rpc::build_json_rpc_request_payload(
            "synthetic/pending",
            serde_json::Value::Null,
        )
        .unwrap();
        let (rpc_tx, rpc_rx) = mpsc::channel();
        commands
            .send(ConnectionRpcCommand::Request {
                request_id: request.request_id.clone(),
                payload: request.payload,
                response_tx: rpc_tx,
            })
            .unwrap();
        while !matches!(peer.next().await.unwrap().unwrap(), Message::Text(_)) {}
        let (ack, ack_rx) = mpsc::channel();
        commands
            .send(ConnectionRpcCommand::RenewAccess {
                spec: next,
                refresh_generation: 2,
                deadline: Instant::now() + Duration::from_secs(2),
                response_tx: ack,
            })
            .unwrap();
        let renewal_id = loop {
            if let Message::Text(text) = peer.next().await.unwrap().unwrap() {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(
                    value["method"],
                    pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW
                );
                break value["id"].clone();
            }
        };
        peer.send(Message::Text(
            serde_json::json!({"jsonrpc":"2.0", "id":renewal_id,
            "result":{"access_expires_at_unix":expires}})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        tokio::task::spawn_blocking(move || {
            ack_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            rpc_rx.try_recv().is_err(),
            "renewal must retain the pending RPC"
        );
        commands
            .send(ConnectionRpcCommand::VoiceBinaryChunk {
                payload: vec![1, 2, 3],
            })
            .unwrap();
        loop {
            if let Message::Binary(bytes) = peer.next().await.unwrap().unwrap() {
                assert_eq!(bytes.as_ref(), &[1, 2, 3]);
                break;
            }
        }
        peer.send(Message::Text(
            serde_json::json!({"jsonrpc":"2.0", "id":request.request_id,
            "result":{"completed":true}})
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
        tokio::task::spawn_blocking(move || {
            rpc_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
        })
        .await
        .unwrap();
        peer.close(None).await.unwrap();
        let accepted = task.await.unwrap();
        assert_eq!(
            accepted.auth_token.unwrap().expose_secret(),
            "next-synthetic-access"
        );
        assert_eq!(accepted.session.unwrap().access_expires_at_unix, expires);
    }

    #[test]
    #[ignore = "opens a real listener; excluded from the hermetic test suite"]
    fn ws_client_worker_connects_and_emits_events() {
        let gateway_base_url = reserve_unused_local_address();
        let server = TestWsServer::start(gateway_base_url.clone());
        let (command_tx, command_rx) = unbounded_channel();
        let (event_tx, event_rx) = mpsc::channel();
        spawn_worker(command_rx, event_tx);

        let (initial_tx, initial_rx) = mpsc::channel();
        command_tx
            .send(GatewayWsCommand::Connect {
                connection_id: 7,
                spec: connect_spec(gateway_base_url),
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

    fn connect_spec(gateway_base_url: String) -> GatewayWsConnectSpec {
        GatewayWsConnectSpec {
            endpoint_id: "local".to_owned(),
            endpoint_name: "Local".to_owned(),
            endpoint_kind: GatewayEndpointKind::Local,
            gateway_base_url: crate::gateway::endpoint::GatewayBaseUrl::parse_presentation(
                gateway_base_url.as_str(),
            )
            .unwrap(),
            auth_token: None,
            session: None,
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
        fn start(gateway_base_url: String) -> Self {
            let (ready_tx, ready_rx) = mpsc::channel();
            let handle = std::thread::spawn(move || {
                let runtime = Runtime::new().expect("server runtime");
                runtime.block_on(async move {
                    let listener = match TcpListener::bind(gateway_base_url.as_str()).await {
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

    #[test]
    fn expired_session_spec_is_never_connected_or_retried() {
        let mut spec = connect_spec("http://127.0.0.1:17878".to_owned());
        spec.session = Some(crate::transport::ws::GatewayWsSessionIdentity {
            server_gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            session_id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
            device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
            access_expires_at_unix: 100,
            refresh_leeway_seconds: 10,
        });
        assert!(!session_access_is_expired(&spec, 99));
        assert!(session_access_is_expired(&spec, 100));
    }
}
