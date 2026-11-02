use crate::session::SessionOutbound;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{FutureExt, SinkExt, StreamExt};
use pioneer_config::AppConfig;
use pioneer_protocol::{
    AuthAccessExpiringNotification, AuthSessionTerminationReason, GatewayReadinessStatus,
    JsonRpcNotification, constants::events,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};

use super::http::{GatewayHttpState, gateway_router};
use super::restricted::{RestrictedExchangeOutcome, run as run_restricted_exchange};
use super::ws::admission::AdmittedConnection;
use crate::auth::{AuthenticatedSessionPrincipal, GatewayAuthService};
use crate::message::MessageProcessor;
use crate::session::SessionManager;

const ACCESS_CLOSE_GRACE: Duration = Duration::from_millis(250);

// Existing deferred budgets. Check projected bytes before retaining any frame.
const MAX_DEFERRED_FRAMES: usize = 64;
const MAX_DEFERRED_BYTES: usize = 4 * 1024 * 1024;
// Small connection budgets, not throughput tuning. Waits must never occupy Cancel.
const MAX_CANCEL_HANDLERS: usize = 1;
// Same window as DEFAULT_TASK_RESOURCES.max_concurrent_waits; the service's
// per-actor/workspace policy remains authoritative (and may be more restrictive).
const MAX_TASK_WAITERS: usize = 4;
// Independent resource reads share a small connection budget, separate from
// control/wait capacity. Saturation uses the existing deferred FIFO.
const MAX_READ_HANDLERS: usize = 4;

#[derive(Clone, Copy)]
enum ConnectionRequestClass {
    Cancel,
    TaskWait,
    Read,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IngressMode {
    Serial,
    ReadGroup,
}

// This is an execution allowlist, not an authorization classification. Each
// handler targets an explicit resource and has no selection/subscription/write
// effects. Everything else (including unknown requests and Binary) is a barrier.
fn independent_read_request(request: &pioneer_protocol::JsonRpcRequest) -> bool {
    use pioneer_protocol::constants::methods;
    matches!(
        request.method.as_str(),
        methods::THREAD_GET
            | methods::THREAD_TREE
            | methods::THREAD_TIMELINE_PAGE
            | methods::TURN_WORK_PAGE
            | methods::TURN_WORK_ITEMS_GET
    )
}

fn connection_request_class(
    request: &pioneer_protocol::JsonRpcRequest,
) -> Option<ConnectionRequestClass> {
    use pioneer_protocol::constants::methods;
    match request.method.as_str() {
        methods::TURN_CANCEL => Some(ConnectionRequestClass::Cancel),
        methods::TASK_WAIT => Some(ConnectionRequestClass::TaskWait),
        _ => None,
    }
}

/// Exact Cancel, task/wait and the explicit-resource read allowlist. Parsing
/// selects ownership, never authority. Serial work never overlaps a read group.
/// JoinSet aborts on Drop (including an unpolled/unwound pump); normal terminal
/// paths additionally drain joins before connection-owned resources are freed.
#[derive(Default)]
pub(crate) struct ConnectionHandlers {
    cancels: JoinSet<()>,
    waits: JoinSet<()>,
    reads: JoinSet<()>,
    closed: bool,
}

impl ConnectionHandlers {
    fn is_empty(&self) -> bool {
        self.cancels.is_empty() && self.waits.is_empty() && self.reads.is_empty()
    }

    fn reap(&mut self) -> Result<()> {
        while let Some(result) = self.cancels.try_join_next() {
            result?;
        }
        while let Some(result) = self.waits.try_join_next() {
            result?;
        }
        while let Some(result) = self.reads.try_join_next() {
            result?;
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<()> {
        tokio::select! {
            result = self.cancels.join_next(), if !self.cancels.is_empty() => { result.expect("active cancel")?; }
            result = self.waits.join_next(), if !self.waits.is_empty() => { result.expect("active wait")?; }
            result = self.reads.join_next(), if !self.reads.is_empty() => { result.expect("active read")?; }
        }
        Ok(())
    }

    fn admit<F>(&mut self, class: ConnectionRequestClass, processing: F) -> Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        anyhow::ensure!(!self.closed, "connection control admission closed");
        self.reap()?;
        let (tasks, capacity) = match class {
            ConnectionRequestClass::Cancel => (&mut self.cancels, MAX_CANCEL_HANDLERS),
            ConnectionRequestClass::TaskWait => (&mut self.waits, MAX_TASK_WAITERS),
            ConnectionRequestClass::Read => (&mut self.reads, MAX_READ_HANDLERS),
        };
        anyhow::ensure!(
            tasks.len() < capacity,
            "connection control capacity exhausted"
        );
        tasks.spawn(pioneer_observability::work::inherit(
            pioneer_observability::work::Boundary::MessageFreshTask,
            pioneer_observability::work::WorkKind::Message,
            pioneer_observability::work::ExecutorRole::Gateway,
            processing,
        ));
        Ok(())
    }

    pub(crate) async fn shutdown(&mut self) {
        self.closed = true;
        self.cancels.abort_all();
        self.waits.abort_all();
        self.reads.abort_all();
        while self.cancels.join_next().await.is_some() {}
        while self.waits.join_next().await.is_some() {}
        while self.reads.join_next().await.is_some() {}
    }
}

/// Only the connection pump publishes leases, after durable admission and principal CAS.
/// Publication and the final expiry check share the watch lock. A lease cannot be
/// published at or after the previous deadline, even if the timer task runs late.
#[derive(Clone)]
pub(crate) struct ConnectionAccessLease {
    pub(crate) principal: AuthenticatedSessionPrincipal,
    deadline: tokio::time::Instant,
}

impl ConnectionAccessLease {
    pub(crate) fn new(principal: AuthenticatedSessionPrincipal, lifetime: Duration) -> Self {
        Self {
            principal,
            deadline: tokio::time::Instant::now() + lifetime,
        }
    }

    fn publish(leases: &watch::Sender<Self>, mut next: Self) -> bool {
        leases.send_if_modified(|lease| {
            if tokio::time::Instant::now() >= lease.deadline {
                return false;
            }
            // An idempotent presentation of the same access credential must
            // retain its original deadline, including across wall-clock drift.
            if next.principal.access_expires_at_unix == lease.principal.access_expires_at_unix {
                next.deadline = lease.deadline;
            }
            *lease = next;
            true
        })
    }
}

type WriterCompletion = std::result::Result<Result<()>, tokio::task::JoinError>;

enum MessageProcessingOutcome {
    Completed,
    SessionTerminated(Option<AuthSessionTerminationReason>),
    ServerShutdown,
    AccessExpired,
    AccessLeaseStopped,
    WriterFinished(WriterCompletion),
}

#[derive(Debug)]
pub struct GatewayServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    join_handle: JoinHandle<Result<()>>,
    readiness: super::http::ReadinessState,
}

impl GatewayServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn set_readiness(&self, status: GatewayReadinessStatus) {
        self.readiness.set_status(status);
    }

    pub(crate) fn readiness_state(&self) -> super::http::ReadinessState {
        self.readiness.clone()
    }

    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.join_handle
            .await
            .context("gateway server join failed")??;
        Ok(())
    }
}

pub async fn spawn_server(
    config: AppConfig,
    auth: crate::auth::AuthAdmissionService,
    auth_service: Arc<GatewayAuthService>,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
) -> Result<GatewayServerHandle> {
    let addr = config.gateway.listen_addr.clone();
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind gateway server to {addr}"))?;
    let local_addr = listener
        .local_addr()
        .context("failed to read bound gateway server address")?;

    message_processor.set_gateway_listen_addr(local_addr);
    let state = GatewayHttpState::new(
        config,
        auth,
        auth_service,
        message_processor,
        session_manager,
    )
    .map_err(|error| anyhow::anyhow!("invalid Gateway HTTP configuration: {error:?}"))?;
    let app = gateway_router(state.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let readiness = state.readiness();
    let join_handle = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal(shutdown_rx, state))
        .await
        .context("Axum Gateway server failed")
    });

    Ok(GatewayServerHandle {
        local_addr,
        shutdown_tx,
        join_handle,
        readiness,
    })
}

async fn shutdown_signal(mut shutdown_rx: watch::Receiver<bool>, state: GatewayHttpState) {
    loop {
        if *shutdown_rx.borrow() || shutdown_rx.changed().await.is_err() {
            break;
        }
    }
    state.set_readiness(GatewayReadinessStatus::Starting);
    state.view_grants.begin_shutdown();
    state.http_streams.begin_shutdown();
    state.active_connections.cancel_all().await;
}

pub(super) async fn run_admitted_connection(
    ws: WebSocket,
    state: GatewayHttpState,
    admission: AdmittedConnection,
    mut server_cancellation: watch::Receiver<bool>,
) -> Result<()> {
    match admission {
        AdmittedConnection::Normal(principal) => {
            run_normal_connection(
                ws,
                state.config,
                principal,
                state.auth_service,
                state.message_processor,
                state.session_manager,
                server_cancellation,
            )
            .await
        }
        AdmittedConnection::Restricted { admission, permit } => {
            let deadline =
                Duration::from_secs(state.config.gateway.auth.auth_exchange_timeout_seconds);
            let result = tokio::select! {
                result = run_restricted_exchange(ws, admission, deadline, state.auth_service) => {
                    result.map(Some)
                }
                _ = wait_for_cancellation(&mut server_cancellation) => Ok(None),
            };
            match &result {
                Ok(Some(RestrictedExchangeOutcome::Succeeded)) => {
                    permit.record_success();
                    tracing::debug!(event = "websocket_auth", outcome = "restricted_succeeded",);
                }
                Ok(Some(RestrictedExchangeOutcome::RetryableFailure)) => {
                    tracing::debug!(
                        event = "websocket_auth",
                        outcome = "restricted_retryable_failure",
                    );
                }
                Ok(Some(RestrictedExchangeOutcome::Failed)) | Err(_) => {
                    permit.record_failure();
                    tracing::debug!(event = "websocket_auth", outcome = "restricted_failed",);
                }
                Ok(None) => {
                    tracing::debug!(event = "websocket_auth", outcome = "restricted_cancelled",)
                }
            }
            result.map(|_| ())
        }
    }
}

pub(crate) async fn run_normal_connection(
    ws: WebSocket,
    config: AppConfig,
    principal: Arc<AuthenticatedSessionPrincipal>,
    auth_service: Arc<GatewayAuthService>,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
    mut server_cancellation: watch::Receiver<bool>,
) -> Result<()> {
    let (mut ws_writer, mut ws_reader) = ws.split();
    let queue_capacity = config.gateway.outbound_queue_capacity.max(1);
    let (outbound_tx, mut outbound_rx) =
        mpsc::channel::<pioneer_observability::work::WorkItem<Message>>(queue_capacity);

    let outbound_tx = SessionOutbound::observed(outbound_tx);
    let connection_id = session_manager
        .register_outbound_connection(outbound_tx.clone(), principal)
        .await?;
    let connection_setup = async {
        let connection_context = session_manager.connection_context(connection_id).await?;
        if let Err(error) = auth_service
            .validate_session_lease(connection_context.principal())
            .await
        {
            anyhow::bail!(
                "auth session became invalid during connection registration ({})",
                error.code().as_str()
            );
        }
        let termination_rx = session_manager
            .connection_termination_receiver(connection_id)
            .await?;
        let now_since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?;
        let access_lifetime = remaining_access_duration(
            connection_context.principal().access_expires_at_unix,
            now_since_epoch,
        )?;
        Ok::<_, anyhow::Error>((connection_context, termination_rx, access_lifetime))
    }
    .await;
    let (mut connection_context, mut termination_rx, access_lifetime) = match connection_setup {
        Ok(setup) => setup,
        Err(error) => {
            session_manager.unregister_connection(connection_id).await;
            return Err(error);
        }
    };

    let (lease_cancel_tx, lease_cancel_rx) = watch::channel(false);
    let (access_expired_tx, mut access_expired_rx) = watch::channel(false);
    let (renewal_tx, renewal_rx) = watch::channel(ConnectionAccessLease::new(
        connection_context.principal().clone(),
        access_lifetime,
    ));
    let access_lease_outbound = outbound_tx.clone();
    let access_lease_leeway = config.gateway.auth.token_refresh_leeway_seconds;
    let access_lease_task = tokio::spawn(async move {
        enforce_access_lease(
            access_lease_outbound,
            access_lease_leeway,
            lease_cancel_rx,
            access_expired_tx,
            renewal_rx,
        )
        .await;
    });

    let mut writer_task = tokio::spawn(async move {
        while let Some(item) = outbound_rx.recv().await {
            let mut queue = item.queue.as_ref().map(|(q, _)| q.processing());
            let (message, work) = item.received(
                pioneer_observability::work::Boundary::EventLane,
                pioneer_observability::work::WorkKind::Event,
                pioneer_observability::work::ExecutorRole::Gateway,
            );
            let startup_output = if let Message::Text(text) = &message {
                if !pioneer_observability::turn_startup::active() {
                    None
                } else {
                    serde_json::from_str::<serde_json::Value>(text)
                        .ok()
                        .and_then(|v| {
                            let params = v.get("params")?;
                            let method = v.get("method")?.as_str()?;
                            let output = pioneer_observability::turn_startup::notification_output(
                                method, params,
                            );
                            let outcome = pioneer_observability::turn_startup::notification_outcome(
                                method, params,
                            );
                            if output.is_none() && outcome.is_none() {
                                return None;
                            }
                            Some((
                                pioneer_observability::turn_startup::request_key(params)?,
                                output,
                                outcome,
                            ))
                        })
                }
            } else {
                None
            };
            let startup_write = startup_output.as_ref().map(|(key, _, _)| {
                pioneer_observability::turn_startup::socket_write_started(key, connection_id);
                pioneer_observability::turn_startup::stage(
                    key,
                    pioneer_observability::turn_startup::Stage::SocketWrite,
                )
            });
            let terminal = matches!(message, Message::Close(_));
            work.scope_result(pioneer_observability::live::waiting(
                pioneer_observability::live::WaitKind::Network,
                ws_writer.send(message),
            ))
            .await
            .context("websocket write failed")?;
            if let Some(queue) = queue.as_mut() {
                queue.processed();
            }
            drop(startup_write);
            if let Some((key, output, outcome)) = startup_output {
                if output.is_some() {
                    pioneer_observability::turn_startup::sent_on_connection(&key, connection_id);
                }
                if let Some(outcome) = outcome {
                    pioneer_observability::turn_startup::finish_on_connection(
                        &key,
                        connection_id,
                        outcome,
                    );
                }
            }
            if terminal {
                return Ok::<(), anyhow::Error>(());
            }
        }
        ws_writer.close().await.context("websocket close failed")?;
        Ok::<(), anyhow::Error>(())
    });
    let mut writer_completion = None;
    let mut deferred_ingress = VecDeque::new();
    let mut handlers = ConnectionHandlers::default();
    let dispatch_owned = |context: crate::request_context::ConnectionContext, payload: String| {
        let processor = message_processor.clone();
        let auth = auth_service.clone();
        let sessions = session_manager.clone();
        let outbound = outbound_tx.clone();
        async move {
            // Use current access on this exact connection (renewal may have won
            // since admission). No old principal is treated as a lasting permit.
            let Ok(current) = sessions.connection_context(context.connection_id()).await else {
                return;
            };
            let old = context.principal();
            let new = current.principal();
            if old.gateway_id != new.gateway_id
                || old.principal_id != new.principal_id
                || old.device_id != new.device_id
                || old.session_id != new.session_id
            {
                return;
            }
            if let Some(reason) = normal_ingress_lease_failure(&auth, &current).await {
                send_close_bounded(&outbound, 4401, reason).await;
                sessions
                    .unregister_connection(current.connection_id())
                    .await;
                return;
            }
            processor.process_request(&current, &payload).await;
        }
    };
    let mut ordinary = None;
    let read_result = std::panic::AssertUnwindSafe(async {
        loop {
            let payload = tokio::select! {
                termination = termination_rx.changed() => {
                    let reason = termination
                        .is_ok()
                        .then(|| *termination_rx.borrow())
                        .flatten();
                    if let Some(reason) = reason {
                        send_close_bounded(&outbound_tx, 4403, reason.as_str()).await;
                    }
                    break;
                }
                _ = wait_for_cancellation(&mut server_cancellation) => {
                    send_close_bounded(&outbound_tx, 1012, "server_shutdown").await;
                    break;
                }
                changed = access_expired_rx.changed() => {
                    match changed {
                        Ok(()) if *access_expired_rx.borrow() => {
                            send_close_bounded(&outbound_tx, 4401, "access_expired").await;
                            break;
                        }
                        Ok(()) => continue,
                        Err(_) => {
                            return Err(anyhow::anyhow!(
                                "access lease task stopped before connection expiry"
                            ));
                        }
                    }
                }
                result = &mut writer_task => {
                    writer_completion = Some(result);
                    break;
                }
                completed = handlers.next(), if !handlers.is_empty() => {
                    completed?;
                    continue;
                }
                payload = async {
                    if let Some(message) = deferred_ingress.pop_front() { Some(Ok(message)) }
                    else { ws_reader.next().await }
                } => payload,
            };
            let Some(payload) = payload else {
                break;
            };
            let message = payload.context("websocket read failed")?;

            match message {
                Message::Text(payload) => {
                    // This transport control request deliberately bypasses domain dispatch;
                    // auth/refresh remains restricted to credential issuance.
                    let request = serde_json::from_str::<pioneer_protocol::JsonRpcRequest>(&payload).ok();
                    if let Some(class) = request.as_ref().and_then(connection_request_class) {
                        if handlers.admit(class, dispatch_owned(connection_context.clone(), payload.to_string())).is_err() {
                            send_close_bounded(&outbound_tx, 1013, "connection_control_overloaded").await;
                            break;
                        }
                        continue;
                    }
                    let independent_read = request.as_ref().is_some_and(independent_read_request);
                    let renewal = request.filter(|request| request.method == pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW);
                    let mut renewal_failed = false;
                    let mut ingress_alive = true;
                    let mut ordinary_failed = false;
                    let outcome = await_message_processing(
                        async {
                            if let Some(reason) = normal_ingress_lease_failure(&auth_service, &connection_context).await {
                                send_close_bounded(&outbound_tx, 4401, reason).await;
                                ingress_alive = false;
                                return;
                            }
                            if let Some(request) = renewal {
                                renewal_failed = !renew_normal_access(
                                    request,
                                    &auth_service,
                                    &session_manager,
                                    &mut connection_context,
                                    &renewal_tx,
                                    &outbound_tx,
                                )
                                .await;
                            } else {
                                let mode = if independent_read {
                                    if payload.len() > MAX_DEFERRED_BYTES {
                                        send_close_bounded(&outbound_tx, 1009, "ingress_message_too_large").await;
                                        ingress_alive = false;
                                        return;
                                    }
                                    // Return the head to the same bounded FIFO;
                                    // the read pump owns all read admission.
                                    deferred_ingress.push_front(Message::Text(payload));
                                    IngressMode::ReadGroup
                                } else {
                                    ordinary = Some(Arc::clone(&message_processor)
                                        .start_owned_request(
                                            connection_context.clone(),
                                            payload.to_string(),
                                        ));
                                    IngressMode::Serial
                                };
                                ingress_alive = process_with_access_renewals(
                                    async {
                                        if mode == IngressMode::Serial {
                                            if let Err(error) = ordinary.as_mut().expect("ordinary owner").join().await {
                                                ordinary_failed = true;
                                                tracing::warn!(error = %error, "ordinary RPC handler failed");
                                            }
                                        } else {
                                            std::future::pending::<()>().await;
                                        }
                                    },
                                    mode,
                                    &mut ws_reader,
                                    &mut deferred_ingress,
                                    &auth_service,
                                    &session_manager,
                                    &mut connection_context,
                                    &renewal_tx,
                                    &outbound_tx,
                                    &mut handlers,
                                    &dispatch_owned,
                                )
                                .await;
                            }
                        },
                        &mut termination_rx,
                        &mut server_cancellation,
                        &mut access_expired_rx,
                        &mut writer_task,
                    )
                    .await;
                    if let Some(ordinary) = ordinary.as_mut() {
                        ordinary.abort_and_join().await;
                    }
                    if !continue_after_message_processing(
                        outcome,
                        &outbound_tx,
                        &mut writer_completion,
                    )
                    .await?
                    {
                        break;
                    }
                    if ordinary_failed || !ingress_alive { break; }
                    if renewal_failed {
                        send_close_bounded(&outbound_tx, 1012, "access_renewal_unconfirmed").await;
                        break;
                    }
                }
                Message::Binary(payload) => {
                    let processing_context = connection_context.clone();
                    let mut ingress_alive = true;
                    let outcome = await_message_processing(
                        async {
                            if let Some(reason) = normal_ingress_lease_failure(&auth_service, &processing_context).await {
                                send_close_bounded(&outbound_tx, 4401, reason).await;
                                ingress_alive = false;
                                return;
                            }
                            ingress_alive = process_with_access_renewals(
                                message_processor
                                    .process_binary_frame(&processing_context, payload.as_ref()),
                                IngressMode::Serial,
                                &mut ws_reader,
                                &mut deferred_ingress,
                                &auth_service,
                                &session_manager,
                                &mut connection_context,
                                &renewal_tx,
                                &outbound_tx,
                                &mut handlers,
                                &dispatch_owned,
                            )
                            .await;
                        },
                        &mut termination_rx,
                        &mut server_cancellation,
                        &mut access_expired_rx,
                        &mut writer_task,
                    )
                    .await;
                    if !continue_after_message_processing(
                        outcome,
                        &outbound_tx,
                        &mut writer_completion,
                    )
                    .await?
                    {
                        break;
                    }
                    if !ingress_alive {
                        break;
                    }
                }
                // Axum automatically emits Pong for Ping and the close reply for peer Close.
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(_) => break,
            }
        }
        Ok(())
    }).catch_unwind().await;

    if let Some(ordinary) = ordinary.as_mut() {
        ordinary.abort_and_join().await;
    }
    // Stop admission and join every offloaded handler before unregister/cleanup.
    handlers.shutdown().await;
    let _ = lease_cancel_tx.send(true);
    session_manager.unregister_connection(connection_id).await;
    message_processor.connection_closed(connection_id).await;
    pioneer_observability::turn_startup::connection_lost(connection_id);
    drop(outbound_tx);
    let writer_result = match writer_completion {
        Some(result) => result
            .context("writer task join failed")
            .and_then(|result| result),
        None => match tokio::time::timeout(ACCESS_CLOSE_GRACE, &mut writer_task).await {
            Ok(result) => result
                .context("writer task join failed")
                .and_then(|result| result),
            Err(_) => {
                writer_task.abort();
                let _ = writer_task.await;
                Ok(())
            }
        },
    };
    // Even writer failure must join the lease owner before returning its error.
    let lease_result = access_lease_task
        .await
        .context("access lease task join failed");
    writer_result?;
    lease_result?;

    match read_result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn await_message_processing<F>(
    processing: F,
    termination_rx: &mut watch::Receiver<Option<AuthSessionTerminationReason>>,
    server_cancellation: &mut watch::Receiver<bool>,
    access_expired_rx: &mut watch::Receiver<bool>,
    writer_task: &mut JoinHandle<Result<()>>,
) -> MessageProcessingOutcome
where
    F: Future<Output = ()>,
{
    tokio::pin!(processing);
    loop {
        tokio::select! {
            () = &mut processing => return MessageProcessingOutcome::Completed,
            changed = termination_rx.changed() => {
                let reason = changed
                    .is_ok()
                    .then(|| *termination_rx.borrow())
                    .flatten();
                return MessageProcessingOutcome::SessionTerminated(reason);
            }
            _ = wait_for_cancellation(server_cancellation) => {
                return MessageProcessingOutcome::ServerShutdown;
            }
            changed = access_expired_rx.changed() => {
                match changed {
                    Ok(()) if *access_expired_rx.borrow() => {
                        return MessageProcessingOutcome::AccessExpired;
                    }
                    Ok(()) => continue,
                    Err(_) => return MessageProcessingOutcome::AccessLeaseStopped,
                }
            }
            result = &mut *writer_task => {
                return MessageProcessingOutcome::WriterFinished(result);
            }
        }
    }
}

async fn continue_after_message_processing(
    outcome: MessageProcessingOutcome,
    outbound_tx: &SessionOutbound,
    writer_completion: &mut Option<WriterCompletion>,
) -> Result<bool> {
    match outcome {
        MessageProcessingOutcome::Completed => Ok(true),
        MessageProcessingOutcome::SessionTerminated(reason) => {
            if let Some(reason) = reason {
                send_close_bounded(outbound_tx, 4403, reason.as_str()).await;
            }
            Ok(false)
        }
        MessageProcessingOutcome::ServerShutdown => {
            send_close_bounded(outbound_tx, 1012, "server_shutdown").await;
            Ok(false)
        }
        MessageProcessingOutcome::AccessExpired => {
            send_close_bounded(outbound_tx, 4401, "access_expired").await;
            Ok(false)
        }
        MessageProcessingOutcome::AccessLeaseStopped => Err(anyhow::anyhow!(
            "access lease task stopped before connection expiry"
        )),
        MessageProcessingOutcome::WriterFinished(result) => {
            *writer_completion = Some(result);
            Ok(false)
        }
    }
}

/// Poll serial work or a read group and connection-owned control together. Only
/// the FIFO's read prefix can join the group: a serial frame closes admission
/// until the whole group drains. A full FIFO never disables reading: the first
/// frame that cannot fit closes the socket. No context is captured for deferred
/// work until execution, after every preceding selection/mutation has completed.
pub(crate) async fn process_with_access_renewals<F, D, C>(
    processing: F,
    mode: IngressMode,
    reader: &mut (impl futures_util::Stream<Item = std::result::Result<Message, axum::Error>> + Unpin),
    deferred: &mut VecDeque<Message>,
    auth: &GatewayAuthService,
    sessions: &SessionManager,
    connection: &mut crate::request_context::ConnectionContext,
    renewal_tx: &watch::Sender<ConnectionAccessLease>,
    outbound: &SessionOutbound,
    handlers: &mut ConnectionHandlers,
    dispatch: &D,
) -> bool
where
    F: Future<Output = ()>,
    D: Fn(crate::request_context::ConnectionContext, String) -> C,
    C: Future<Output = ()> + Send + 'static,
{
    let mut deferred_bytes: usize = deferred
        .iter()
        .map(|message| match message {
            Message::Text(payload) => payload.len(),
            Message::Binary(payload) => payload.len(),
            _ => 0,
        })
        .sum();
    tokio::pin!(processing);
    loop {
        if handlers.reap().is_err() {
            return false;
        }
        if mode == IngressMode::ReadGroup {
            while handlers.reads.len() < MAX_READ_HANDLERS {
                let Some(Message::Text(payload)) = deferred.front() else {
                    break;
                };
                if !serde_json::from_str::<pioneer_protocol::JsonRpcRequest>(payload)
                    .is_ok_and(|request| independent_read_request(&request))
                {
                    break;
                }
                let Some(Message::Text(payload)) = deferred.pop_front() else {
                    unreachable!()
                };
                deferred_bytes -= payload.len();
                if handlers
                    .admit(
                        ConnectionRequestClass::Read,
                        dispatch(connection.clone(), payload.to_string()),
                    )
                    .is_err()
                {
                    return false;
                }
            }
            if handlers.reads.is_empty() {
                return true;
            }
        }
        tokio::select! {
            // A ready ordinary completion cannot be starved by control traffic.
            biased;
            () = &mut processing => return true,
            completed = handlers.next(), if !handlers.is_empty() => {
                if completed.is_err() { return false; }
            }
            incoming = reader.next() => {
                let message = match incoming {
                    Some(Ok(message)) => message,
                    _ => return false,
                };
                let bytes = match &message {
                    Message::Text(payload) => payload.len(),
                    Message::Binary(payload) => payload.len(),
                    Message::Ping(_) | Message::Pong(_) => continue,
                    Message::Close(_) => return false,
                };
                // Also bound a single new payload before JSON parsing/cloning.
                // The WebSocket parser already caps acquisition at 16 MiB.
                if bytes > MAX_DEFERRED_BYTES {
                    send_close_bounded(outbound, 1009, "ingress_message_too_large").await;
                    return false;
                }
                if let Message::Text(payload) = &message {
                    if let Ok(request) = serde_json::from_str::<pioneer_protocol::JsonRpcRequest>(payload) {
                        if request.method == pioneer_protocol::constants::methods::AUTH_ACCESS_RENEW {
                            let renewal = renew_normal_access(request, auth, sessions, connection, renewal_tx, outbound);
                            tokio::pin!(renewal);
                            let accepted = tokio::select! {
                                accepted = &mut renewal => accepted,
                                () = &mut processing => {
                                    let accepted = renewal.await;
                                    if !accepted { send_close_bounded(outbound, 1012, "access_renewal_unconfirmed").await; }
                                    return accepted;
                                }
                            };
                            if !accepted {
                                send_close_bounded(outbound, 1012, "access_renewal_unconfirmed").await;
                                return false;
                            }
                            continue;
                        }
                        if let Some(class) = connection_request_class(&request) {
                            if handlers.admit(class, dispatch(connection.clone(), payload.to_string())).is_err() {
                                send_close_bounded(outbound, 1013, "connection_control_overloaded").await;
                                return false;
                            }
                            continue;
                        }
                        if mode == IngressMode::ReadGroup
                            && deferred.is_empty()
                            && independent_read_request(&request)
                            && handlers.reads.len() < MAX_READ_HANDLERS {
                            if handlers.admit(ConnectionRequestClass::Read,
                                dispatch(connection.clone(), payload.to_string())).is_err() {
                                return false;
                            }
                            continue;
                        }
                    }
                }
                if deferred.len() >= MAX_DEFERRED_FRAMES
                    || bytes > MAX_DEFERRED_BYTES.saturating_sub(deferred_bytes) {
                    send_close_bounded(outbound, 1013, "ordinary_ingress_overloaded").await;
                    return false;
                }
                deferred_bytes += bytes;
                deferred.push_back(message);
            }
        }
    }
}

async fn renew_normal_access(
    request: pioneer_protocol::JsonRpcRequest,
    auth: &GatewayAuthService,
    sessions: &SessionManager,
    connection: &mut crate::request_context::ConnectionContext,
    renewal_tx: &watch::Sender<ConnectionAccessLease>,
    outbound: &SessionOutbound,
) -> bool {
    let result = async {
        if request.jsonrpc != pioneer_protocol::JSONRPC_VERSION {
            return Err(crate::auth::AuthError::new(
                crate::auth::AuthErrorCode::InvalidCredential,
            ));
        }
        let params = serde_json::from_value::<pioneer_protocol::AuthAccessRenewParams>(
            request.params.unwrap_or(serde_json::Value::Null),
        )
        .map_err(|_| crate::auth::AuthError::new(crate::auth::AuthErrorCode::InvalidCredential))?;
        let next = auth
            .renew_connection_access(connection.principal(), &params)
            .await?;
        let updated = sessions
            .renew_connection_principal(connection.connection_id(), connection.principal(), next)
            .await
            .map_err(|error| crate::auth::AuthError::new(error.auth_code()))?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
            crate::auth::AuthError::new(crate::auth::AuthErrorCode::TemporarilyUnavailable)
        })?;
        let remaining = remaining_access_duration(updated.principal().access_expires_at_unix, now)
            .map_err(|_| {
                crate::auth::AuthError::new(crate::auth::AuthErrorCode::CredentialExpired)
            })?;
        let next = ConnectionAccessLease::new(updated.principal().clone(), remaining);
        let accepted = ConnectionAccessLease::publish(renewal_tx, next);
        if !accepted {
            return Err(crate::auth::AuthError::new(
                crate::auth::AuthErrorCode::CredentialExpired,
            ));
        }
        *connection = updated;
        Ok::<_, crate::auth::AuthError>(pioneer_protocol::AuthAccessRenewResponse {
            access_expires_at_unix: connection.principal().access_expires_at_unix,
        })
    }
    .await;
    let accepted = result.is_ok();
    let response = match result {
        Ok(response) => pioneer_protocol::JsonRpcResponse::from_result(request.id, &response)
            .ok()
            .and_then(|response| serde_json::to_string(&response).ok()),
        Err(error) => serde_json::to_string(&pioneer_protocol::JsonRpcErrorResponse {
            jsonrpc: pioneer_protocol::JSONRPC_VERSION.to_owned(),
            id: Some(request.id),
            error: pioneer_protocol::JsonRpcError {
                code: -32040,
                message: error.code().as_str().to_owned(),
                data: Some(serde_json::json!({"code": error.code().as_str()})),
            },
        })
        .ok(),
    };
    if let Some(response) = response {
        // No database resources are retained across this bounded send.
        if !matches!(
            tokio::time::timeout(
                ACCESS_CLOSE_GRACE,
                outbound.send(Message::Text(response.into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return false;
        }
    } else {
        return false;
    }
    accepted
}

async fn enforce_access_lease(
    outbound_tx: SessionOutbound,
    refresh_leeway_seconds: u64,
    mut cancellation_rx: watch::Receiver<bool>,
    access_expired_tx: watch::Sender<bool>,
    mut renewal_rx: watch::Receiver<ConnectionAccessLease>,
) {
    let mut warned_deadline = None;
    loop {
        let lease = renewal_rx.borrow_and_update().clone();
        let now = tokio::time::Instant::now();
        let warning = now
            + remaining_access_warning_duration(
                lease.deadline.saturating_duration_since(now),
                refresh_leeway_seconds,
            );
        tokio::select! {
            biased;
            _ = wait_for_cancellation(&mut cancellation_rx) => return,
            _ = tokio::time::sleep_until(lease.deadline) => {
                // The publisher rejects an already expired old lease under the
                // same watch lock. Observe a timely publication before expiring,
                // even when both the old timer and changed() are ready.
                let current = renewal_rx.borrow_and_update();
                if current.deadline != lease.deadline { continue; }
                let _ = access_expired_tx.send(true);
                tracing::info!(event = "auth_access_expired", outcome = "connection_closed", reason_code = "access_expired");
                return;
            }
            changed = renewal_rx.changed() => {
                if changed.is_err() { return; }
            }
            _ = tokio::time::sleep_until(warning), if warned_deadline != Some(lease.deadline) => {
                warned_deadline = Some(lease.deadline);
                if let Ok(notification) = JsonRpcNotification::from_params(
                    events::AUTH_ACCESS_EXPIRING,
                    &AuthAccessExpiringNotification {
                        session_id: lease.principal.session_id.clone(),
                        access_expires_at_unix: lease.principal.access_expires_at_unix,
                    },
                ).and_then(|notification| serde_json::to_string(&notification)) {
                    if matches!(outbound_tx.try_send(Message::Text(notification.into())),
                        Err(mpsc::error::TrySendError::Full(_))) {
                        record_outbound_queue_saturation("access_expiry_warning");
                    }
                }
            }
        }
    }
}

async fn wait_for_cancellation(cancellation_rx: &mut watch::Receiver<bool>) {
    loop {
        if *cancellation_rx.borrow() || cancellation_rx.changed().await.is_err() {
            return;
        }
    }
}

fn remaining_access_duration(expires_at_unix: u64, now_since_epoch: Duration) -> Result<Duration> {
    Duration::from_secs(expires_at_unix)
        .checked_sub(now_since_epoch)
        .filter(|duration| !duration.is_zero())
        .context("access credential is already expired")
}

fn remaining_access_warning_duration(access_lifetime: Duration, leeway_seconds: u64) -> Duration {
    access_lifetime.saturating_sub(Duration::from_secs(leeway_seconds))
}

async fn normal_ingress_lease_failure(
    auth_service: &GatewayAuthService,
    connection: &crate::request_context::ConnectionContext,
) -> Option<&'static str> {
    auth_service
        .validate_session_lease(connection.principal())
        .await
        .err()
        .map(|error| match error.code() {
            crate::auth::AuthErrorCode::CredentialExpired => "access_expired",
            code => code.as_str(),
        })
}

async fn send_close_bounded(sender: &SessionOutbound, code: u16, reason: &'static str) {
    if sender.capacity() == 0 {
        record_outbound_queue_saturation("close_waited");
    }
    let _ = tokio::time::timeout(
        ACCESS_CLOSE_GRACE,
        sender.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}

fn record_outbound_queue_saturation(reason_code: &'static str) {
    tracing::debug!(
        event = "websocket_outbound_queue",
        outcome = "saturated",
        reason_code,
    );
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;

    use super::*;

    struct DropNotifier(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn control_owner_joins_unpolled_children_and_reaps_panic_before_retirement() {
        let mut owner = ConnectionHandlers::default();
        let mut drops = Vec::new();
        for class in [
            ConnectionRequestClass::Cancel,
            ConnectionRequestClass::TaskWait,
            ConnectionRequestClass::Read,
        ] {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let notifier = DropNotifier(Some(tx));
            owner
                .admit(class, async move {
                    let _notifier = notifier;
                    std::future::pending::<()>().await;
                })
                .unwrap();
            drops.push(rx);
        }
        owner.shutdown().await;
        for dropped in drops {
            dropped.await.unwrap();
        }
        assert!(owner.is_empty());
        assert!(
            owner
                .admit(ConnectionRequestClass::Cancel, async {})
                .is_err()
        );

        for class in [
            ConnectionRequestClass::TaskWait,
            ConnectionRequestClass::Read,
        ] {
            let mut owner = ConnectionHandlers::default();
            owner
                .admit(class, async { panic!("synthetic handler panic") })
                .unwrap();
            assert!(owner.next().await.is_err());
            assert!(owner.is_empty());
            owner.shutdown().await;
        }
    }

    #[tokio::test]
    async fn read_group_drain_observes_terminal_signals_and_joins_children() {
        for reason in ["revoke", "expiry", "shutdown", "writer"] {
            let (termination_tx, mut termination_rx) = watch::channel(None);
            let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
            let (expired_tx, mut expired_rx) = watch::channel(false);
            let mut writer = tokio::spawn(async move {
                if reason == "writer" {
                    anyhow::bail!("synthetic writer failure");
                }
                std::future::pending::<Result<()>>().await
            });
            let (started, entered) = tokio::sync::oneshot::channel();
            let (drop_tx, dropped) = tokio::sync::oneshot::channel();
            let mut owner = ConnectionHandlers::default();
            owner
                .admit(ConnectionRequestClass::Read, async move {
                    let _drop = DropNotifier(Some(drop_tx));
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                })
                .unwrap();
            entered.await.unwrap();
            match reason {
                "revoke" => {
                    termination_tx
                        .send(Some(AuthSessionTerminationReason::SessionRevoked))
                        .unwrap();
                }
                "expiry" => {
                    expired_tx.send(true).unwrap();
                }
                "shutdown" => {
                    shutdown_tx.send(true).unwrap();
                }
                "writer" => {}
                _ => unreachable!(),
            }
            let outcome = await_message_processing(
                async {
                    owner.next().await.unwrap();
                },
                &mut termination_rx,
                &mut shutdown_rx,
                &mut expired_rx,
                &mut writer,
            )
            .await;
            match (reason, outcome) {
                (
                    "revoke",
                    MessageProcessingOutcome::SessionTerminated(Some(
                        AuthSessionTerminationReason::SessionRevoked,
                    )),
                )
                | ("expiry", MessageProcessingOutcome::AccessExpired)
                | ("shutdown", MessageProcessingOutcome::ServerShutdown)
                | ("writer", MessageProcessingOutcome::WriterFinished(Ok(Err(_)))) => {}
                _ => panic!("wrong read-drain terminal outcome: {reason}"),
            }
            owner.shutdown().await;
            dropped.await.unwrap();
            assert!(owner.is_empty());
            if reason != "writer" {
                writer.abort();
                let _ = writer.await;
            }
        }
    }

    #[test]
    fn access_deadline_uses_exact_expiry_boundary() {
        assert_eq!(
            remaining_access_duration(101, Duration::from_millis(100_250)).unwrap(),
            Duration::from_millis(750)
        );
        assert!(remaining_access_duration(100, Duration::from_secs(100)).is_err());
        assert!(remaining_access_duration(99, Duration::from_secs(100)).is_err());
    }

    #[test]
    fn access_warning_never_moves_expiry_deadline() {
        assert_eq!(
            remaining_access_warning_duration(Duration::from_secs(30), 10),
            Duration::from_secs(20)
        );
        assert_eq!(
            remaining_access_warning_duration(Duration::from_secs(5), 10),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn outbound_backpressure_is_bounded_and_peer_drop_is_terminal() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .send(Message::Text("first".into()))
            .await
            .expect("prime bounded queue");

        let pending = sender.send(Message::Text("second".into()));
        tokio::pin!(pending);
        assert!(pending.as_mut().now_or_never().is_none());
        assert!(matches!(receiver.recv().await, Some(Message::Text(_))));
        pending
            .await
            .expect("writer progress releases backpressure");

        drop(receiver);
        assert!(sender.send(Message::Close(None)).await.is_err());
    }

    #[tokio::test]
    async fn cancellation_wait_is_deterministic_for_signal_and_owner_drop() {
        let (signal_tx, mut signal_rx) = watch::channel(false);
        signal_tx.send(true).unwrap();
        wait_for_cancellation(&mut signal_rx).await;

        let (owner_tx, mut owner_rx) = watch::channel(false);
        drop(owner_tx);
        wait_for_cancellation(&mut owner_rx).await;
    }

    #[tokio::test]
    async fn access_lease_cancellation_releases_task_without_wall_clock_wait() {
        let principal = AuthenticatedSessionPrincipal {
            gateway_id: pioneer_protocol::GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: pioneer_protocol::PrincipalId::new("P00000000000000000001").unwrap(),
            kind: pioneer_protocol::PrincipalKind::Superuser,
            role_key: None,
            device_id: pioneer_protocol::DeviceId::new("D00000000000000000001").unwrap(),
            session_id: pioneer_protocol::AuthSessionId::new("S00000000000000000001").unwrap(),
            access_jti: "J00000000000000000001".to_owned(),
            access_expires_at_unix: 10_000,
        };
        let (outbound_tx, mut outbound_rx) = mpsc::channel(1);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (expired_tx, expired_rx) = watch::channel(false);
        let (_renewal_tx, renewal_rx) = watch::channel(ConnectionAccessLease::new(
            principal.clone(),
            Duration::from_secs(60),
        ));
        let task = tokio::spawn(enforce_access_lease(
            outbound_tx.into(),
            30,
            cancel_rx,
            expired_tx,
            renewal_rx,
        ));

        cancel_tx.send(true).unwrap();
        task.await.unwrap();
        assert!(!*expired_rx.borrow());
        assert!(outbound_rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn ready_old_expiry_observes_timely_renewal_but_rejects_late_publication() {
        for timely in [true, false] {
            let mut principal =
                (*crate::session::test_support::authenticated_test_superuser()).clone();
            principal.access_expires_at_unix = 10_000;
            let old = ConnectionAccessLease::new(principal.clone(), Duration::from_secs(1));
            let (renewal_tx, renewal_rx) = watch::channel(old);
            let (outbound_tx, _outbound_rx) = mpsc::channel(4);
            let (cancel_tx, cancel_rx) = watch::channel(false);
            let (expired_tx, expired_rx) = watch::channel(false);
            let task =
                enforce_access_lease(outbound_tx.into(), 0, cancel_rx, expired_tx, renewal_rx);
            tokio::pin!(task);
            assert!(
                task.as_mut().now_or_never().is_none(),
                "park on the old deadline"
            );
            tokio::time::advance(Duration::from_millis(if timely { 500 } else { 1_000 })).await;
            let old_deadline = renewal_tx.borrow().deadline;
            assert_eq!(
                ConnectionAccessLease::publish(
                    &renewal_tx,
                    ConnectionAccessLease::new(principal.clone(), Duration::from_secs(60)),
                ),
                timely
            );
            assert_eq!(
                renewal_tx.borrow().deadline,
                old_deadline,
                "same credential cannot move expiry"
            );
            principal.access_expires_at_unix += 60;
            let accepted = ConnectionAccessLease::publish(
                &renewal_tx,
                ConnectionAccessLease::new(principal, Duration::from_secs(60)),
            );
            assert_eq!(accepted, timely);
            if timely {
                // Keep the lease future unpolled until BOTH its old timer and
                // the accepted publication are ready.
                tokio::time::advance(Duration::from_millis(500)).await;
                assert!(task.as_mut().now_or_never().is_none());
                assert!(!*expired_rx.borrow());
                cancel_tx.send(true).unwrap();
                task.await;
            } else {
                assert!(task.as_mut().now_or_never().is_some());
                assert!(*expired_rx.borrow());
            }
        }
    }

    #[tokio::test]
    async fn server_shutdown_aborts_in_flight_owned_message_task() {
        let (_termination_tx, mut termination_rx) = watch::channel(None);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_expired_tx, mut expired_rx) = watch::channel(false);
        let mut writer_task = tokio::spawn(std::future::pending::<Result<()>>());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();

        let processing = async move {
            crate::message::message_fresh_task(async move {
                let _drop_notifier = DropNotifier(Some(dropped_tx));
                started_tx.send(()).expect("report message task start");
                std::future::pending::<()>().await;
            })
            .await
            .expect("owned message task should remain pending until shutdown");
        };
        let shutdown_task = tokio::spawn(async move {
            started_rx.await.expect("observe message task start");
            shutdown_tx.send(true).expect("signal server shutdown");
        });

        let outcome = await_message_processing(
            processing,
            &mut termination_rx,
            &mut shutdown_rx,
            &mut expired_rx,
            &mut writer_task,
        )
        .await;

        assert!(matches!(outcome, MessageProcessingOutcome::ServerShutdown));
        shutdown_task.await.expect("join shutdown signal task");
        tokio::time::timeout(Duration::from_secs(5), dropped_rx)
            .await
            .expect("owned message task should be aborted promptly")
            .expect("observe owned message future drop");
        writer_task.abort();
        let _ = writer_task.await;
    }
}
