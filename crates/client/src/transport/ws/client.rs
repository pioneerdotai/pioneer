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
        websocket_read_failed_message, websocket_write_failed_message,
    },
};
use crate::rpc::{JsonRpcResponseSender, PendingJsonRpcRequests};
use anyhow::{Context as _, Result};
use futures_util::{SinkExt, StreamExt, future::Either};
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

use pioneer_observability::work::{Boundary, ExecutorRole, WorkItem, WorkKind};

/// Process-local event carrier; protocol/FFI event DTOs remain unchanged.
#[derive(Clone)]
pub struct GatewayWsEventSender {
    inner: Sender<WorkItem<GatewayWsEvent>>,
    queue: pioneer_observability::queues::QueueObserver,
}
impl From<Sender<WorkItem<GatewayWsEvent>>> for GatewayWsEventSender {
    fn from(inner: Sender<WorkItem<GatewayWsEvent>>) -> Self {
        Self {
            inner,
            queue: pioneer_observability::queues::QueueObserver::new(
                pioneer_observability::queues::QueueKind::ClientController,
                None,
                "Gateway event payload bytes unavailable without serialization",
            ),
        }
    }
}
impl GatewayWsEventSender {
    pub fn send(
        &self,
        event: GatewayWsEvent,
    ) -> std::result::Result<(), std::sync::mpsc::SendError<GatewayWsEvent>> {
        use pioneer_observability::queues::QueueBytes;
        self.inner
            .send(
                WorkItem::new(
                    event,
                    Boundary::EventLane,
                    WorkKind::Event,
                    ExecutorRole::ClientController,
                )
                .observed(self.queue.clone(), QueueBytes::Unsupported),
            )
            .map_err(|error| {
                if let Some((queue, bytes)) = &error.0.queue {
                    queue.rejected_send(*bytes);
                }
                std::sync::mpsc::SendError(error.0.value)
            })
    }
}
pub(super) struct WsSender<T> {
    inner: UnboundedSender<WorkItem<T>>,
    boundary: Boundary,
    queue: pioneer_observability::queues::QueueObserver,
}
impl<T> Clone for WsSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            boundary: self.boundary,
            queue: self.queue.clone(),
        }
    }
}
pub(super) trait WsQueued {
    fn observed_bytes(&self) -> pioneer_observability::queues::QueueBytes;
}
impl<T: WsQueued> WsSender<T> {
    pub(super) fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        let bytes = value.observed_bytes();
        self.inner
            .send(
                WorkItem::new(
                    value,
                    self.boundary,
                    if matches!(self.boundary, Boundary::ClientRpc) {
                        WorkKind::ClientRpc
                    } else {
                        WorkKind::ClientCommand
                    },
                    ExecutorRole::ClientWs,
                )
                .observed(self.queue.clone(), bytes),
            )
            .map_err(|error| {
                if let Some((queue, bytes)) = &error.0.queue {
                    queue.rejected_send(*bytes);
                }
                tokio::sync::mpsc::error::SendError(error.0.value)
            })
    }
}
impl<T> From<UnboundedSender<WorkItem<T>>> for WsSender<T> {
    fn from(inner: UnboundedSender<WorkItem<T>>) -> Self {
        Self {
            inner,
            boundary: Boundary::ClientCommand,
            queue: pioneer_observability::queues::QueueObserver::new(
                pioneer_observability::queues::QueueKind::ClientCommands,
                None,
                "command payload bytes; non-payload fields excluded",
            ),
        }
    }
}
pub(super) fn work_channel<T>(boundary: Boundary) -> (WsSender<T>, UnboundedReceiver<WorkItem<T>>) {
    let (inner, receiver) = unbounded_channel();
    (
        WsSender {
            inner,
            boundary,
            queue: pioneer_observability::queues::QueueObserver::new(
                if matches!(boundary, Boundary::ClientRpc) {
                    pioneer_observability::queues::QueueKind::ClientRpc
                } else {
                    pioneer_observability::queues::QueueKind::ClientCommands
                },
                None,
                "UTF-8 or binary payload bytes; other command fields excluded",
            ),
        },
        receiver,
    )
}

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

impl ConnectionRpcCommand {
    fn fail(self, reason: &str) {
        match self {
            Self::Request { response_tx, .. } | Self::RenewAccess { response_tx, .. } => {
                let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(reason)));
            }
            Self::BinaryUploadChunk { response_tx, .. } => {
                let _ = response_tx.send(Err(reason.to_owned()));
            }
            Self::ArtifactBinaryUploadChunk { response_tx, .. } => {
                let _ = response_tx.send(Err(reason.to_owned()));
            }
            Self::VoiceBinaryChunk { .. } => {}
        }
    }
}

fn fail_queued_connection_commands(
    rpc_rx: &mut UnboundedReceiver<WorkItem<ConnectionRpcCommand>>,
    reason: &str,
) {
    while let Ok(item) = rpc_rx.try_recv() {
        let mut queue = item.queue.as_ref().map(|(queue, _)| queue.processing());
        let (command, carrier) = item.received(
            Boundary::ClientRpc,
            WorkKind::ClientRpc,
            ExecutorRole::ClientWs,
        );
        carrier.run(|| command.fail(reason));
        if let Some(queue) = queue.as_mut() {
            queue.processed();
        }
    }
}

/// Also drains commands when the connection task is aborted during connect/backoff
/// or while a socket write is pending. Retained cancellation handles are not owners
/// of transport progress and must not keep these requests waiting after teardown.
struct ConnectionCommandReceiver(UnboundedReceiver<WorkItem<ConnectionRpcCommand>>);

impl Drop for ConnectionCommandReceiver {
    fn drop(&mut self) {
        self.0.close();
        fail_queued_connection_commands(&mut self.0, "websocket connection owner dropped");
    }
}

struct ActiveConnectionTask {
    connection_id: u64,
    handle: JoinHandle<()>,
    rpc_tx: WsSender<ConnectionRpcCommand>,
}

pub fn spawn_worker(
    command_rx: UnboundedReceiver<WorkItem<GatewayWsCommand>>,
    event_tx: GatewayWsEventSender,
) {
    let worker_work = pioneer_observability::work::WorkCarrier::child(
        Boundary::StdThread,
        WorkKind::WorkerLifecycle,
        pioneer_observability::work::Trigger::Lifecycle,
        ExecutorRole::ClientWs,
    );
    thread::spawn(move || {
        let runtime = Runtime::new().expect("failed to create tokio runtime for websocket worker");
        let _runtime_observer = pioneer_observability::runtime::register(
            runtime.handle(),
            pioneer_observability::runtime::RuntimeRole::ClientWs,
        );
        _runtime_observer.block_on(
            &runtime,
            worker_work.scope(async move {
                run_worker(command_rx, event_tx).await;
            }),
        );
    });
}

async fn run_worker(
    mut command_rx: UnboundedReceiver<WorkItem<GatewayWsCommand>>,
    event_tx: GatewayWsEventSender,
) {
    let mut connection_task: Option<ActiveConnectionTask> = None;

    while let Some(item) = pioneer_observability::live::waiting(
        pioneer_observability::live::WaitKind::QueueItem,
        command_rx.recv(),
    )
    .await
    {
        let mut queue = item.queue.as_ref().map(|(queue, _)| queue.processing());
        let (command, carrier) = item.received(
            Boundary::ClientCommand,
            WorkKind::ClientCommand,
            ExecutorRole::ClientWs,
        );
        let proceed = carrier
            .scope(handle_command(command, &mut connection_task, &event_tx))
            .await;
        if let Some(queue) = queue.as_mut() {
            queue.processed();
        }
        if !proceed {
            break;
        }
    }
    abort_connection_task(&mut connection_task).await;
}
async fn handle_command(
    command: GatewayWsCommand,
    connection_task: &mut Option<ActiveConnectionTask>,
    event_tx: &GatewayWsEventSender,
) -> bool {
    match command {
        GatewayWsCommand::Connect {
            connection_id,
            spec,
            initial_result_tx,
            retry_initial_failure,
        } => {
            abort_connection_task(connection_task).await;
            let (rpc_tx, rpc_rx) = work_channel(Boundary::ClientRpc);
            *connection_task = Some(ActiveConnectionTask {
                connection_id,
                handle: tokio::spawn(pioneer_observability::work::inherit(
                    Boundary::ClientRpc,
                    WorkKind::WorkerLifecycle,
                    ExecutorRole::ClientWs,
                    run_connection_task(
                        connection_id,
                        spec,
                        initial_result_tx,
                        retry_initial_failure,
                        event_tx.clone(),
                        ConnectionCommandReceiver(rpc_rx),
                        None,
                    ),
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
                return true;
            }
            let _ = event_tx.send(connecting_event(connection_id, &spec));
            match connect_websocket(&spec).await {
                Ok(stream) => {
                    abort_connection_task(connection_task).await;
                    let (rpc_tx, rpc_rx) = work_channel(Boundary::ClientRpc);
                    *connection_task = Some(ActiveConnectionTask {
                        connection_id,
                        handle: tokio::spawn(pioneer_observability::work::inherit(
                            Boundary::ClientRpc,
                            WorkKind::WorkerLifecycle,
                            ExecutorRole::ClientWs,
                            run_connection_task(
                                connection_id,
                                spec,
                                None,
                                true,
                                event_tx.clone(),
                                ConnectionCommandReceiver(rpc_rx),
                                Some(stream),
                            ),
                        )),
                        rpc_tx,
                    });
                    let _ = result_tx.send(Ok(()));
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    let _ =
                        event_tx.send(connect_failed_event(connection_id, &spec, message.clone()));
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
                    return true;
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
            if !response_tx.is_active() {
                response_tx.cancel();
                return true;
            }
            let Some(connection_task) = connection_task.as_mut() else {
                let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(
                    "websocket is not connected",
                )));
                return true;
            };

            if expected_connection.is_some_and(|id| id != connection_task.connection_id) {
                let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(
                    "websocket request connection retired",
                )));
                return true;
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
                return true;
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
                let _ =
                    fallback_tx.send(Err("websocket connection task is unavailable".to_owned()));
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
                return true;
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
                let _ =
                    fallback_tx.send(Err("websocket connection task is unavailable".to_owned()));
            }
        }
        GatewayWsCommand::VoiceBinaryChunk {
            expected_connection,
            payload,
        } => {
            let Some(connection_task) = connection_task.as_mut() else {
                return true;
            };

            if expected_connection.is_some_and(|id| id != connection_task.connection_id) {
                return true;
            }
            let _ = connection_task
                .rpc_tx
                .send(ConnectionRpcCommand::VoiceBinaryChunk { payload });
        }
        GatewayWsCommand::Disconnect => {
            abort_connection_task(connection_task).await;
        }
        GatewayWsCommand::DisconnectConnection { connection_id } => {
            if connection_task
                .as_ref()
                .is_some_and(|task| task.connection_id == connection_id)
            {
                abort_connection_task(connection_task).await;
            }
        }
        GatewayWsCommand::Shutdown => {
            return false;
        }
    }
    true
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
    event_tx: GatewayWsEventSender,
    mut rpc_rx: ConnectionCommandReceiver,
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
                pioneer_observability::live::waiting(
                    pioneer_observability::live::WaitKind::Network,
                    connect_websocket(&spec),
                )
                .await
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
                    monitor_connection(stream, &mut spec, connection_id, &event_tx, &mut rpc_rx.0)
                        .await;

                let _ = event_tx.send(disconnected_event(connection_id, &spec, reason.clone()));

                if terminal_reason_from_disconnect(reason.as_str()).is_some() {
                    return;
                }
                let plan = next_reconnect_plan(connection_id, &spec, attempt, backoff, reason);
                attempt = plan.attempt;
                let delay = plan.delay;
                let _ = event_tx.send(plan.event);

                pioneer_observability::live::retry(delay.as_millis().min(u64::MAX as u128) as u64);
                pioneer_observability::live::sleeping(delay, sleep(delay)).await;
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

                pioneer_observability::live::retry(delay.as_millis().min(u64::MAX as u128) as u64);
                pioneer_observability::live::sleeping(delay, sleep(delay)).await;
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

/// One waiting frame beyond the frame currently being written. This is the
/// smallest handoff that decouples admission from socket progress, not a tuned
/// throughput limit. Control has its own single slot so data cannot hide Pong.
const WRITER_HANDOFF_CAPACITY: usize = 1;

struct WriterFrame {
    message: Message,
    request: Option<JsonRpcResponseSender>,
    carrier: pioneer_observability::work::WorkCarrier,
}

impl WriterFrame {
    fn new(message: Message, request: Option<JsonRpcResponseSender>) -> Self {
        Self {
            message,
            request,
            carrier: pioneer_observability::work::WorkCarrier::child(
                Boundary::ClientRpc,
                WorkKind::ClientRpc,
                pioneer_observability::work::Trigger::Rpc,
                ExecutorRole::ClientWs,
            ),
        }
    }
}

async fn connection_writer<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut writer: futures_util::stream::SplitSink<tokio_tungstenite::WebSocketStream<S>, Message>,
    mut data: tokio::sync::mpsc::Receiver<WriterFrame>,
    mut control: tokio::sync::mpsc::Receiver<WriterFrame>,
    connection_id: u64,
) -> String {
    loop {
        let frame = tokio::select! {
            biased;
            frame = control.recv() => frame,
            frame = data.recv() => frame,
        };
        let Some(frame) = frame else {
            return "websocket writer channel closed".to_owned();
        };
        let WriterFrame {
            message,
            request,
            carrier,
        } = frame;
        let result = carrier
            .scope_result(async {
                if let Some(request) = request.as_ref() {
                    if !request.try_start_write() {
                        return Ok(());
                    }
                }
                let startup_write = match &message {
                    Message::Text(payload) if request.is_some() => {
                        pioneer_observability::turn_startup::client_write(payload, connection_id)
                    }
                    _ => None,
                };
                // Once claimed, finish this write even if the local request retires.
                // Dropping this future is allowed only together with both socket halves.
                let result = writer.send(message).await;
                drop(startup_write);
                result
            })
            .await;
        if let Err(error) = result {
            return websocket_write_failed_message(error);
        }
    }
}

struct ConnectionWaiters {
    connection_id: u64,
    requests: PendingJsonRpcRequests,
    renewal: Option<PendingAccessRenewal>,
    uploads: HashMap<String, Sender<std::result::Result<SkillsUploadChunkAckNotification, String>>>,
    artifact_uploads:
        HashMap<String, Sender<std::result::Result<ArtifactUploadChunkAckNotification, String>>>,
}

impl ConnectionWaiters {
    fn fail_all(&mut self, reason: &str) {
        if let Some(pending) = self.renewal.take() {
            let _ = pending
                .response_tx
                .send(Err(crate::rpc::JsonRpcResponseError::transport(reason)));
        }
        fail_pending_requests(&mut self.requests, reason);
        fail_pending_upload_chunks(&mut self.uploads, reason);
        fail_pending_artifact_upload_chunks(&mut self.artifact_uploads, reason);
    }
}

impl Drop for ConnectionWaiters {
    fn drop(&mut self) {
        self.fail_all("websocket connection owner dropped");
        pioneer_observability::turn_startup::connection_lost(self.connection_id);
    }
}

async fn monitor_connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: tokio_tungstenite::WebSocketStream<S>,
    spec: &mut GatewayWsConnectSpec,
    connection_id: u64,
    event_tx: &GatewayWsEventSender,
    rpc_rx: &mut UnboundedReceiver<WorkItem<ConnectionRpcCommand>>,
) -> String {
    let (writer, mut reader) = stream.split();
    let (data_tx, data_rx) = tokio::sync::mpsc::channel(WRITER_HANDOFF_CAPACITY);
    let (control_tx, control_rx) = tokio::sync::mpsc::channel(WRITER_HANDOFF_CAPACITY);
    let writer = connection_writer(writer, data_rx, control_rx, connection_id);
    tokio::pin!(writer);
    let mut ping_interval = tokio::time::interval(spec.timings.ping_interval);
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_pong = Instant::now();
    let mut waiters = ConnectionWaiters {
        connection_id,
        requests: PendingJsonRpcRequests::default(),
        renewal: None,
        uploads: HashMap::new(),
        artifact_uploads: HashMap::new(),
    };

    let mut prefer_commands = false;
    let disconnect_reason = loop {
        let renewal_deadline = waiters.renewal.as_ref().map_or_else(
            || Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT,
            |pending| pending.deadline,
        );
        tokio::select! {
            // Poll the writer before admission to avoid an artificial Full.
            // Alternate ready reads/commands below so neither can starve the other.
            biased;
            reason = &mut writer => { break reason; }
            _ = tokio::time::sleep_until(renewal_deadline), if waiters.renewal.is_some() => {
                break "connection renewal timeout".to_owned();
            }
            _ = ping_interval.tick() => {
                if Instant::now().duration_since(last_pong) > spec.timings.pong_timeout {
                    break WEBSOCKET_PONG_TIMEOUT_MESSAGE.to_owned();
                }

                if control_tx.try_send(WriterFrame::new(Message::Ping(Vec::new().into()), None)).is_err() {
                    break "websocket control writer handoff full or closed".to_owned();
                }
            }
            input = async {
                if prefer_commands {
                    tokio::select! {
                        biased;
                        item = rpc_rx.recv() => Either::Left(item),
                        payload = reader.next() => Either::Right(payload),
                    }
                } else {
                    tokio::select! {
                        biased;
                        payload = reader.next() => Either::Right(payload),
                        item = rpc_rx.recv() => Either::Left(item),
                    }
                }
            } => match input {
            Either::Right(payload) => {
                prefer_commands = true;
                let carrier=pioneer_observability::work::WorkCarrier::independent(Boundary::EventLane,WorkKind::Event,pioneer_observability::work::Trigger::Event,ExecutorRole::ClientWs);
                let failure=carrier.scope(async {
                match payload {
                    Some(Ok(Message::Pong(_))) => {
                        last_pong = Instant::now();
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if control_tx.try_send(WriterFrame::new(Message::Pong(payload), None)).is_err() {
                            return Some("websocket control writer handoff full or closed".to_owned());
                        }
                        last_pong = Instant::now();
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return Some(frame
                            .map(|value| {
                                disconnect_reason_from_close(
                                    u16::from(value.code),
                                    value.reason.as_ref(),
                                )
                            })
                            .unwrap_or_else(|| WEBSOCKET_CLOSED_BY_PEER_MESSAGE.to_owned()));
                    }
                    Some(Ok(Message::Text(payload))) => {
                        if let Some((id, result)) = serde_json::from_str::<serde_json::Value>(&payload)
                            .ok().as_ref().and_then(crate::rpc::decode_json_rpc_response_value)
                            && waiters.renewal.as_ref().is_some_and(|pending| pending.request_id == id)
                        {
                            let pending = waiters.renewal.take().expect("matched renewal");
                            let result = result.and_then(|value| {
                                let response: pioneer_protocol::AuthAccessRenewResponse = serde_json::from_value(value)
                                    .map_err(|_| crate::rpc::JsonRpcResponseError::InvalidResponse)?;
                                if !pending.response_tx.is_active() || Instant::now() >= pending.deadline || pending.spec.session.as_ref().is_none_or(|identity|
                                    identity.access_expires_at_unix != response.access_expires_at_unix)
                                    || response.access_expires_at_unix <= unix_timestamp_secs() {
                                    return Err(crate::rpc::JsonRpcResponseError::InvalidResponse);
                                }
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
                            let acknowledged = result.is_ok();
                            let delivered = pending.response_tx.send(result);
                            if let Some(reason) = failure { return Some(reason); }
                            if delivered.is_err() { return Some("connection renewal retired".to_owned()); }
                            if acknowledged {
                                // Reconnect reads this spec only after a live, acknowledged renewal.
                                *spec = pending.spec;
                            }
                            return None;
                        }
                        let notification = process_text_payload(
                            payload.as_ref(),
                            connection_id,
                            &mut waiters.requests,
                            &mut waiters.uploads,
                            &mut waiters.artifact_uploads,
                            event_tx,
                        );
                        if let Some(GatewayNotification::AuthSessionRevoked(notification)) =
                            notification
                        {
                            return Some(notification.reason.as_str().to_owned());
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {}
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => {
                        return Some(websocket_read_failed_message(error));
                    }
                    None => {
                        return Some(WEBSOCKET_STREAM_ENDED_MESSAGE.to_owned());
                    }
                }
                None
                }).await;
                if let Some(reason)=failure { break reason; }
            }
            Either::Left(item) => {
                prefer_commands = false;
                let Some(item)=item else {break WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE.to_owned();};
                let mut queue=item.queue.as_ref().map(|(queue,_)|queue.processing());
                let (command,carrier)=item.received(Boundary::ClientRpc,WorkKind::ClientRpc,ExecutorRole::ClientWs);
                let failure=carrier.scope(async {
                match Some(command) {
                    Some(ConnectionRpcCommand::RenewAccess { spec: next, refresh_generation, deadline, response_tx }) => {
                        if Instant::now() >= deadline || waiters.renewal.is_some() || !same_session(spec, &next) || session_access_is_expired(spec, unix_timestamp_secs()) {
                            let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport("connection renewal unavailable")));
                            return None;
                        }
                        let Some(token) = next.auth_token.clone() else {
                            let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::InvalidResponse));
                            return None;
                        };
                        let params = pioneer_protocol::AuthAccessRenewParams { access_token: token, refresh_generation };
                        let request = crate::rpc::build_json_rpc_request_payload(
                            pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW,
                            serde_json::to_value(params).expect("access renewal params"),
                        ).expect("access renewal request");
                        let frame = WriterFrame::new(Message::Text(request.payload.into()), Some(response_tx.clone()));
                        waiters.renewal = Some(PendingAccessRenewal { request_id: request.request_id,
                            spec: next, response_tx, deadline });
                        if data_tx.try_send(frame).is_err() {
                            return Some("connection renewal writer handoff full or closed".to_owned());
                        }
                    }
                    Some(ConnectionRpcCommand::Request { request_id, payload, response_tx }) => {
                        if !response_tx.is_active() { response_tx.cancel(); return None; }
                        waiters.requests.insert(request_id, response_tx.clone());
                        // Registration precedes admission, including a possible fast response.
                        if data_tx.try_send(WriterFrame::new(Message::Text(payload.into()), Some(response_tx.clone()))).is_err() {
                            let _ = response_tx.send(Err(crate::rpc::JsonRpcResponseError::transport(
                                "websocket writer handoff full or closed",
                            )));
                        }
                    }
                    Some(ConnectionRpcCommand::BinaryUploadChunk { upload_id, offset, payload, response_tx }) => {
                        let ack_key = upload_ack_key(upload_id.as_str(), offset);
                        waiters.uploads.insert(ack_key.clone(), response_tx);
                        if data_tx.try_send(WriterFrame::new(Message::Binary(payload.into()), None)).is_err() {
                            if let Some(response_tx) = waiters.uploads.remove(&ack_key) {
                                let _ = response_tx.send(Err("websocket writer handoff full or closed".to_owned()));
                            }
                        }
                    }
                    Some(ConnectionRpcCommand::ArtifactBinaryUploadChunk { upload_id, offset, payload, response_tx }) => {
                        let ack_key = upload_ack_key(upload_id.as_str(), offset);
                        waiters.artifact_uploads.insert(ack_key.clone(), response_tx);
                        if data_tx.try_send(WriterFrame::new(Message::Binary(payload.into()), None)).is_err() {
                            if let Some(response_tx) = waiters.artifact_uploads.remove(&ack_key) {
                                let _ = response_tx.send(Err("websocket writer handoff full or closed".to_owned()));
                            }
                        }
                    }
                    Some(ConnectionRpcCommand::VoiceBinaryChunk { payload }) => {
                        if data_tx.try_send(WriterFrame::new(Message::Binary(payload.into()), None)).is_err() {
                            return Some("websocket voice writer handoff full or closed".to_owned());
                        }
                    }
                    None => {
                        return Some(WEBSOCKET_COMMAND_CHANNEL_CLOSED_MESSAGE.to_owned());
                    }
                }
                None
                }).await;
                if let Some(queue)=queue.as_mut(){queue.processed();}
                if let Some(failure)=failure {break failure;}
            }
            }
        }
    };
    waiters.fail_all(&disconnect_reason);
    fail_queued_connection_commands(rpc_rx, &disconnect_reason);
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
        let events = GatewayWsEventSender::from(events);
        let (commands, mut command_rx) = work_channel(Boundary::ClientRpc);
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
                response_tx: JsonRpcResponseSender::new(
                    rpc_tx,
                    std::time::Instant::now() + crate::rpc::RPC_REQUEST_TIMEOUT,
                ),
            })
            .unwrap();
        while !matches!(peer.next().await.unwrap().unwrap(), Message::Text(_)) {}
        let (ack, ack_rx) = mpsc::channel();
        commands
            .send(ConnectionRpcCommand::RenewAccess {
                spec: next,
                refresh_generation: 2,
                deadline: Instant::now() + Duration::from_secs(2),
                response_tx: JsonRpcResponseSender::new(
                    ack,
                    std::time::Instant::now() + Duration::from_secs(2),
                ),
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
        pioneer_observability::work::spawn_blocking(move || {
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
        pioneer_observability::work::spawn_blocking(move || {
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
        let (command_tx, command_rx) = work_channel(Boundary::ClientCommand);
        let (event_tx, event_rx) = mpsc::channel();
        spawn_worker(command_rx, event_tx.into());

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
            .expect("connecting event")
            .value;
        let second = event_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("connected event")
            .value;
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

    pub(super) fn connect_spec(gateway_base_url: String) -> GatewayWsConnectSpec {
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

impl WsQueued for GatewayWsCommand {
    fn observed_bytes(&self) -> pioneer_observability::queues::QueueBytes {
        use pioneer_observability::queues::QueueBytes;
        match self {
            Self::Request { payload, .. } => QueueBytes::Known(payload.len() as u64),
            Self::BinaryUploadChunk { payload, .. }
            | Self::ArtifactBinaryUploadChunk { payload, .. }
            | Self::VoiceBinaryChunk { payload, .. } => QueueBytes::Known(payload.len() as u64),
            _ => QueueBytes::Unsupported,
        }
    }
}
impl WsQueued for ConnectionRpcCommand {
    fn observed_bytes(&self) -> pioneer_observability::queues::QueueBytes {
        use pioneer_observability::queues::QueueBytes;
        match self {
            Self::Request { payload, .. } => QueueBytes::Known(payload.len() as u64),
            Self::BinaryUploadChunk { payload, .. }
            | Self::ArtifactBinaryUploadChunk { payload, .. }
            | Self::VoiceBinaryChunk { payload, .. } => QueueBytes::Known(payload.len() as u64),
            _ => QueueBytes::Unsupported,
        }
    }
}

#[cfg(test)]
#[path = "client_lifecycle_tests.rs"]
mod lifecycle_tests;
