use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use pioneer_config::AppConfig;
use pioneer_protocol::{
    AuthAccessExpiringNotification, AuthSessionTerminationReason, JsonRpcNotification,
    constants::events,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::http::{GatewayHttpState, gateway_router};
use super::restricted::{RestrictedExchangeOutcome, run as run_restricted_exchange};
use super::ws::admission::AdmittedConnection;
use crate::auth::{AuthenticatedSessionPrincipal, GatewayAuthService};
use crate::message::MessageProcessor;
use crate::session::SessionManager;

const ACCESS_CLOSE_GRACE: Duration = Duration::from_millis(250);

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
}

impl GatewayServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
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
    state.set_ready(true);
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
    })
}

async fn shutdown_signal(mut shutdown_rx: watch::Receiver<bool>, state: GatewayHttpState) {
    loop {
        if *shutdown_rx.borrow() || shutdown_rx.changed().await.is_err() {
            break;
        }
    }
    state.set_ready(false);
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

async fn run_normal_connection(
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
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(queue_capacity);

    let connection_id = session_manager
        .register_connection(outbound_tx.clone(), principal)
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
        let access_warning_delay = remaining_access_warning_duration(
            access_lifetime,
            config.gateway.auth.token_refresh_leeway_seconds,
        );
        Ok::<_, anyhow::Error>((
            connection_context,
            termination_rx,
            access_lifetime,
            access_warning_delay,
        ))
    }
    .await;
    let (connection_context, mut termination_rx, access_lifetime, access_warning_delay) =
        match connection_setup {
            Ok(setup) => setup,
            Err(error) => {
                session_manager.unregister_connection(connection_id).await;
                return Err(error);
            }
        };

    let (lease_cancel_tx, lease_cancel_rx) = watch::channel(false);
    let (access_expired_tx, mut access_expired_rx) = watch::channel(false);
    let access_lease_principal = connection_context.principal().clone();
    let access_lease_outbound = outbound_tx.clone();
    let access_lease_task = tokio::spawn(async move {
        enforce_access_lease(
            access_lease_outbound,
            access_lease_principal,
            access_lifetime,
            access_warning_delay,
            lease_cancel_rx,
            access_expired_tx,
        )
        .await;
    });

    let mut writer_task = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
            let terminal = matches!(message, Message::Close(_));
            ws_writer
                .send(message)
                .await
                .context("websocket write failed")?;
            if terminal {
                return Ok::<(), anyhow::Error>(());
            }
        }
        ws_writer.close().await.context("websocket close failed")?;
        Ok::<(), anyhow::Error>(())
    });
    let mut writer_completion = None;
    let read_result = async {
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
                payload = ws_reader.next() => payload,
            };
            let Some(payload) = payload else {
                break;
            };
            let message = payload.context("websocket read failed")?;

            match message {
                Message::Text(payload) => {
                    if let Some(reason) =
                        normal_ingress_lease_failure(&auth_service, &connection_context).await
                    {
                        send_close_bounded(&outbound_tx, 4401, reason).await;
                        break;
                    }
                    let outcome = await_message_processing(
                        message_processor.process_request(&connection_context, payload.as_ref()),
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
                }
                Message::Binary(payload) => {
                    if let Some(reason) =
                        normal_ingress_lease_failure(&auth_service, &connection_context).await
                    {
                        send_close_bounded(&outbound_tx, 4401, reason).await;
                        break;
                    }
                    let outcome = await_message_processing(
                        message_processor
                            .process_binary_frame(&connection_context, payload.as_ref()),
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
                }
                // Axum automatically emits Pong for Ping and the close reply for peer Close.
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(_) => break,
            }
        }
        Ok(())
    }
    .await;

    let _ = lease_cancel_tx.send(true);
    session_manager.unregister_connection(connection_id).await;
    message_processor.connection_closed(connection_id).await;
    drop(outbound_tx);
    match writer_completion {
        Some(result) => result.context("writer task join failed")??,
        None => match tokio::time::timeout(ACCESS_CLOSE_GRACE, &mut writer_task).await {
            Ok(result) => result.context("writer task join failed")??,
            Err(_) => {
                writer_task.abort();
                let _ = writer_task.await;
            }
        },
    }
    access_lease_task
        .await
        .context("access lease task join failed")?;

    read_result
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
    outbound_tx: &mpsc::Sender<Message>,
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

async fn enforce_access_lease(
    outbound_tx: mpsc::Sender<Message>,
    principal: AuthenticatedSessionPrincipal,
    access_lifetime: Duration,
    access_warning_delay: Duration,
    mut cancellation_rx: watch::Receiver<bool>,
    access_expired_tx: watch::Sender<bool>,
) {
    let lease_started_at = tokio::time::Instant::now();
    let expiry_deadline = lease_started_at + access_lifetime;

    if access_warning_delay < access_lifetime {
        let warning_deadline = lease_started_at + access_warning_delay;
        tokio::select! {
            _ = tokio::time::sleep_until(warning_deadline) => {
                if let Ok(notification) = JsonRpcNotification::from_params(
                    events::AUTH_ACCESS_EXPIRING,
                    &AuthAccessExpiringNotification {
                        session_id: principal.session_id.clone(),
                        access_expires_at_unix: principal.access_expires_at_unix,
                    },
                )
                .and_then(|notification| serde_json::to_string(&notification))
                {
                    if matches!(
                        outbound_tx.try_send(Message::Text(notification.into())),
                        Err(mpsc::error::TrySendError::Full(_)),
                    ) {
                        record_outbound_queue_saturation("access_expiry_warning");
                    }
                }
            }
            _ = wait_for_cancellation(&mut cancellation_rx) => return,
        }
    }

    tokio::select! {
        _ = tokio::time::sleep_until(expiry_deadline) => {}
        _ = wait_for_cancellation(&mut cancellation_rx) => return,
    }

    let _ = access_expired_tx.send(true);
    tracing::info!(
        event = "auth_access_expired",
        gateway_id = %principal.gateway_id,
        principal_id = %principal.principal_id,
        auth_session_id = %principal.session_id,
        outcome = "connection_closed",
        reason_code = "access_expired",
    );
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

async fn send_close_bounded(sender: &mpsc::Sender<Message>, code: u16, reason: &'static str) {
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
        let task = tokio::spawn(enforce_access_lease(
            outbound_tx,
            principal,
            Duration::from_secs(60),
            Duration::from_secs(30),
            cancel_rx,
            expired_tx,
        ));

        cancel_tx.send(true).unwrap();
        task.await.unwrap();
        assert!(!*expired_rx.borrow());
        assert!(outbound_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn server_shutdown_interrupts_in_flight_message_processing() {
        let (_termination_tx, mut termination_rx) = watch::channel(None);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let (_expired_tx, mut expired_rx) = watch::channel(false);
        let mut writer_task = tokio::spawn(std::future::pending::<Result<()>>());

        shutdown_tx.send(true).unwrap();
        let outcome = await_message_processing(
            std::future::pending(),
            &mut termination_rx,
            &mut shutdown_rx,
            &mut expired_rx,
            &mut writer_task,
        )
        .await;

        assert!(matches!(outcome, MessageProcessingOutcome::ServerShutdown));
        writer_task.abort();
        let _ = writer_task.await;
    }
}
