use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::{IpAddr, Shutdown, SocketAddr};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};
use tracing::debug;

use pioneer_config::AppConfig;
use pioneer_protocol::{AuthAccessExpiringNotification, JsonRpcNotification, constants::events};

use super::restricted::{RestrictedExchangeOutcome, run as run_restricted_exchange};
use crate::auth::AuthenticatedSessionPrincipal;
use crate::auth::{
    AuthAdmissionService, AuthError, CapturedAdmission, GatewayAuthService, RestrictedAdmission,
};
use crate::message::MessageProcessor;
use crate::session::SessionManager;

const MAX_AUTH_IN_FLIGHT_PER_ADDRESS: usize = 8;
const MAX_AUTH_IN_FLIGHT_GLOBAL: usize = 512;
const MAX_AUTH_TRACKED_ADDRESSES: usize = 1_024;
const MAX_AUTH_FAILURE_PENALTY_STEPS: u32 = 10;
const AUTH_FAILURE_PENALTY_STEP: Duration = Duration::from_millis(25);
const AUTH_FAILURE_STATE_TTL: Duration = Duration::from_secs(10 * 60);
const ACCESS_CLOSE_GRACE: Duration = Duration::from_millis(250);

#[derive(Default)]
struct AuthAbuseState {
    addresses: HashMap<IpAddr, AuthAddressState>,
    total_in_flight: usize,
    gateway_failed_attempts: u32,
    gateway_last_failure: Option<Instant>,
}

struct AuthAddressState {
    in_flight: usize,
    failed_attempts: u32,
    last_touched: Instant,
}

#[derive(Default)]
struct AuthAbuseLimiter {
    state: Mutex<AuthAbuseState>,
}

struct AuthAttemptPermit {
    limiter: Arc<AuthAbuseLimiter>,
    address: IpAddr,
    penalty: Duration,
}

impl AuthAbuseLimiter {
    fn try_acquire(self: &Arc<Self>, address: IpAddr) -> Option<AuthAttemptPermit> {
        let now = Instant::now();
        let mut state = self.state.lock().ok()?;
        state.addresses.retain(|_, entry| {
            entry.in_flight > 0
                || now.saturating_duration_since(entry.last_touched) < AUTH_FAILURE_STATE_TTL
        });
        if state.gateway_last_failure.is_some_and(|last_failure| {
            now.saturating_duration_since(last_failure) >= AUTH_FAILURE_STATE_TTL
        }) {
            state.gateway_failed_attempts = 0;
            state.gateway_last_failure = None;
        }
        if state.total_in_flight >= MAX_AUTH_IN_FLIGHT_GLOBAL {
            return None;
        }
        if !state.addresses.contains_key(&address)
            && state.addresses.len() >= MAX_AUTH_TRACKED_ADDRESSES
        {
            let oldest_idle = state
                .addresses
                .iter()
                .filter(|(_, entry)| entry.in_flight == 0)
                .min_by_key(|(_, entry)| entry.last_touched)
                .map(|(address, _)| *address);
            if let Some(oldest_idle) = oldest_idle {
                state.addresses.remove(&oldest_idle);
            } else {
                return None;
            }
        }
        let address_penalty_steps = {
            let entry = state.addresses.entry(address).or_insert(AuthAddressState {
                in_flight: 0,
                failed_attempts: 0,
                last_touched: now,
            });
            if entry.in_flight >= MAX_AUTH_IN_FLIGHT_PER_ADDRESS {
                return None;
            }
            let penalty_steps = entry.failed_attempts.min(MAX_AUTH_FAILURE_PENALTY_STEPS);
            entry.in_flight += 1;
            entry.last_touched = now;
            penalty_steps
        };
        let penalty_steps = address_penalty_steps
            .max(state.gateway_failed_attempts)
            .min(MAX_AUTH_FAILURE_PENALTY_STEPS);
        state.total_in_flight += 1;
        Some(AuthAttemptPermit {
            limiter: self.clone(),
            address,
            penalty: AUTH_FAILURE_PENALTY_STEP.saturating_mul(penalty_steps),
        })
    }
}

impl AuthAttemptPermit {
    fn penalty(&self) -> Duration {
        self.penalty
    }

    fn record_success(&self) {
        if let Ok(mut state) = self.limiter.state.lock()
            && let Some(entry) = state.addresses.get_mut(&self.address)
        {
            entry.failed_attempts = 0;
            entry.last_touched = Instant::now();
        }
    }

    fn record_failure(&self) {
        if let Ok(mut state) = self.limiter.state.lock() {
            let now = Instant::now();
            if let Some(entry) = state.addresses.get_mut(&self.address) {
                entry.failed_attempts = entry
                    .failed_attempts
                    .saturating_add(1)
                    .min(MAX_AUTH_FAILURE_PENALTY_STEPS);
                entry.last_touched = now;
            }
            state.gateway_failed_attempts = state
                .gateway_failed_attempts
                .saturating_add(1)
                .min(MAX_AUTH_FAILURE_PENALTY_STEPS);
            state.gateway_last_failure = Some(now);
        }
    }
}

impl Drop for AuthAttemptPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.limiter.state.lock() {
            if let Some(entry) = state.addresses.get_mut(&self.address) {
                entry.in_flight = entry.in_flight.saturating_sub(1);
                entry.last_touched = Instant::now();
            }
            state.total_in_flight = state.total_in_flight.saturating_sub(1);
        }
    }
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
    auth: AuthAdmissionService,
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

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let join_handle = tokio::spawn(async move {
        run_accept_loop(
            listener,
            shutdown_rx,
            config,
            auth,
            auth_service,
            message_processor,
            session_manager,
        )
        .await
    });

    Ok(GatewayServerHandle {
        local_addr,
        shutdown_tx,
        join_handle,
    })
}

async fn run_accept_loop(
    listener: TcpListener,
    mut shutdown_rx: watch::Receiver<bool>,
    config: AppConfig,
    auth: AuthAdmissionService,
    auth_service: Arc<GatewayAuthService>,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
) -> Result<()> {
    let mut connection_tasks = JoinSet::new();
    let auth_abuse_limiter = Arc::new(AuthAbuseLimiter::default());

    loop {
        let config = config.clone();
        let auth = auth.clone();
        let auth_service = auth_service.clone();
        let message_processor = message_processor.clone();
        let session_manager = session_manager.clone();
        let auth_abuse_limiter = auth_abuse_limiter.clone();

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, peer_addr)) => {
                        connection_tasks.spawn(async move {
                            let _ = handle_connection(
                                stream,
                                peer_addr,
                                config,
                                auth,
                                auth_service,
                                message_processor,
                                session_manager,
                                auth_abuse_limiter,
                            )
                            .await;
                        });
                    }
                    Err(error) => {
                        return Err(anyhow::anyhow!("gateway accept failed: {error}"));
                    }
                }
            }
        }
    }

    connection_tasks.abort_all();
    while connection_tasks.join_next().await.is_some() {}

    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    config: AppConfig,
    auth: AuthAdmissionService,
    auth_service: Arc<GatewayAuthService>,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
    auth_abuse_limiter: Arc<AuthAbuseLimiter>,
) -> Result<()> {
    let (stream, shutdown_handle) = duplicate_shutdown_handle(stream)?;
    let auth_attempt = auth_abuse_limiter
        .try_acquire(peer_addr.ip())
        .context("Gateway authentication concurrency limit reached")?;
    if !auth_attempt.penalty().is_zero() {
        tokio::time::sleep(auth_attempt.penalty()).await;
    }

    let admission_capture = Arc::new(OnceLock::new());
    let callback_capture = admission_capture.clone();
    let handshake_deadline = Duration::from_secs(config.gateway.auth.auth_exchange_timeout_seconds);
    let ws = match tokio::time::timeout(
        handshake_deadline,
        accept_hdr_async(stream, move |request: &Request, response: Response| {
            capture_admission(auth.capture_request(request), callback_capture.as_ref())?;
            Ok(response)
        }),
    )
    .await
    {
        Ok(Ok(ws)) => ws,
        Ok(Err(error)) => {
            auth_attempt.record_failure();
            return Err(error).context("websocket handshake failed");
        }
        Err(_) => {
            auth_attempt.record_failure();
            anyhow::bail!("websocket authentication handshake timed out");
        }
    };
    let admission = match read_captured_admission(admission_capture) {
        Ok(admission) => admission,
        Err(error) => {
            auth_attempt.record_failure();
            return Err(error);
        }
    };

    match admission {
        CapturedAdmission::Access(credential) => {
            match auth_service.authenticate_access(credential).await {
                Ok(principal) => {
                    auth_attempt.record_success();
                    drop(auth_attempt);
                    run_normal_connection(
                        ws,
                        shutdown_handle,
                        config,
                        principal,
                        auth_service,
                        message_processor,
                        session_manager,
                    )
                    .await
                }
                Err(error) => {
                    auth_attempt.record_failure();
                    close_with_auth_reason(ws, 4401, error.code().as_str()).await
                }
            }
        }
        CapturedAdmission::Restricted(restricted) => {
            drop(shutdown_handle);
            let deadline = Duration::from_secs(config.gateway.auth.auth_exchange_timeout_seconds);
            let result = handle_restricted_admission(ws, restricted, deadline, auth_service).await;
            match result {
                Ok(RestrictedExchangeOutcome::Succeeded) => auth_attempt.record_success(),
                Ok(RestrictedExchangeOutcome::Failed) | Err(_) => auth_attempt.record_failure(),
            }
            result.map(|_| ())
        }
    }
}

async fn run_normal_connection(
    ws: WebSocketStream<TcpStream>,
    shutdown_handle: std::net::TcpStream,
    config: AppConfig,
    principal: Arc<AuthenticatedSessionPrincipal>,
    auth_service: Arc<GatewayAuthService>,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
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
            shutdown_handle,
            access_lease_outbound,
            access_lease_principal,
            connection_id,
            access_lifetime,
            access_warning_delay,
            lease_cancel_rx,
            access_expired_tx,
        )
        .await;
    });

    let mut writer_task = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
            ws_writer
                .send(message)
                .await
                .context("websocket write failed")?;
        }
        ws_writer.close().await.context("websocket close failed")?;
        Ok::<(), anyhow::Error>(())
    });

    let read_result = async {
        loop {
            let payload = tokio::select! {
                termination = termination_rx.changed() => {
                    let _ = termination;
                    break;
                }
                changed = access_expired_rx.changed() => {
                    match changed {
                        Ok(()) if *access_expired_rx.borrow() => break,
                        Ok(()) => continue,
                        Err(_) => {
                            return Err(anyhow::anyhow!(
                                "access lease task stopped before connection expiry"
                            ));
                        }
                    }
                }
                payload = ws_reader.next() => payload,
            };
            let Some(payload) = payload else {
                break;
            };
            let message = match payload {
                Ok(value) => value,
                Err(error) => {
                    return Err(anyhow::anyhow!("websocket read failed: {error}"));
                }
            };

            match message {
                Message::Text(payload) => {
                    if let Some(reason) =
                        normal_ingress_lease_failure(&auth_service, &connection_context).await
                    {
                        send_outbound(
                            &outbound_tx,
                            Message::Close(Some(CloseFrame {
                                code: CloseCode::from(4401),
                                reason: reason.into(),
                            })),
                        )
                        .await?;
                        break;
                    }
                    message_processor
                        .process_request(&connection_context, payload.as_ref())
                        .await;
                }
                Message::Binary(payload) => {
                    if let Some(reason) =
                        normal_ingress_lease_failure(&auth_service, &connection_context).await
                    {
                        send_outbound(
                            &outbound_tx,
                            Message::Close(Some(CloseFrame {
                                code: CloseCode::from(4401),
                                reason: reason.into(),
                            })),
                        )
                        .await?;
                        break;
                    }
                    message_processor
                        .process_binary_frame(&connection_context, payload.as_ref())
                        .await;
                }
                Message::Ping(payload) => {
                    send_outbound(&outbound_tx, Message::Pong(payload)).await?;
                }
                Message::Pong(_) => {}
                Message::Close(frame) => {
                    send_outbound(&outbound_tx, Message::Close(frame)).await?;
                    break;
                }
                Message::Frame(_) => {}
            }
        }
        Ok(())
    }
    .await;

    let _ = lease_cancel_tx.send(true);
    session_manager.unregister_connection(connection_id).await;
    message_processor.connection_closed(connection_id).await;
    drop(outbound_tx);
    match tokio::time::timeout(ACCESS_CLOSE_GRACE, &mut writer_task).await {
        Ok(result) => result.context("writer task join failed")??,
        Err(_) => {
            writer_task.abort();
            let _ = writer_task.await;
        }
    }
    access_lease_task
        .await
        .context("access lease task join failed")?;

    read_result
}

async fn enforce_access_lease(
    shutdown_handle: std::net::TcpStream,
    outbound_tx: mpsc::Sender<Message>,
    principal: AuthenticatedSessionPrincipal,
    connection_id: u64,
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
                    let _ = outbound_tx.try_send(Message::Text(notification.into()));
                }
            }
            _ = wait_for_lease_cancellation(&mut cancellation_rx) => return,
        }
    }

    tokio::select! {
        _ = tokio::time::sleep_until(expiry_deadline) => {}
        _ = wait_for_lease_cancellation(&mut cancellation_rx) => return,
    }

    let _ = access_expired_tx.send(true);
    tracing::info!(
        event = "auth_access_expired",
        gateway_id = %principal.gateway_id,
        principal_id = %principal.principal_id,
        device_id = %principal.device_id,
        auth_session_id = %principal.session_id,
        connection_id,
        outcome = "connection_closed",
        reason = "access_expired",
    );

    let close_queued = outbound_tx
        .try_send(Message::Close(Some(CloseFrame {
            code: CloseCode::from(4401),
            reason: "access_expired".into(),
        })))
        .is_ok();
    if close_queued {
        tokio::time::sleep(ACCESS_CLOSE_GRACE).await;
    }
    let _ = shutdown_handle.shutdown(Shutdown::Both);
}

async fn wait_for_lease_cancellation(cancellation_rx: &mut watch::Receiver<bool>) {
    loop {
        if *cancellation_rx.borrow() {
            return;
        }
        if cancellation_rx.changed().await.is_err() {
            return;
        }
    }
}

fn duplicate_shutdown_handle(stream: TcpStream) -> Result<(TcpStream, std::net::TcpStream)> {
    let stream = stream
        .into_std()
        .context("failed to convert Gateway socket for lease enforcement")?;
    let shutdown_handle = stream
        .try_clone()
        .context("failed to duplicate Gateway socket for lease enforcement")?;
    let stream =
        TcpStream::from_std(stream).context("failed to restore asynchronous Gateway socket")?;
    Ok((stream, shutdown_handle))
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

async fn handle_restricted_admission(
    ws: WebSocketStream<TcpStream>,
    admission: RestrictedAdmission,
    deadline: Duration,
    executor: Arc<GatewayAuthService>,
) -> Result<RestrictedExchangeOutcome> {
    run_restricted_exchange(ws, admission, deadline, executor).await
}

async fn close_with_auth_reason(
    mut ws: WebSocketStream<TcpStream>,
    code: u16,
    reason: &'static str,
) -> Result<()> {
    ws.close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: reason.into(),
    }))
    .await
    .context("failed to close rejected auth connection")
}

async fn send_outbound(sender: &mpsc::Sender<Message>, message: Message) -> Result<()> {
    sender
        .send(message)
        .await
        .map_err(|_| anyhow::anyhow!("outbound channel closed"))
}

fn unauthorized_response(message: &str) -> ErrorResponse {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Some(message.to_owned()))
        .expect("failed to build unauthorized websocket handshake response")
}

fn capture_admission(
    admission: std::result::Result<CapturedAdmission, AuthError>,
    capture: &OnceLock<CapturedAdmission>,
) -> std::result::Result<(), ErrorResponse> {
    let admission = match admission {
        Ok(admission) => admission,
        Err(error) => {
            debug!(
                code = error.code().as_str(),
                "websocket auth rejected request"
            );
            return Err(unauthorized_response("missing or invalid bearer token"));
        }
    };

    capture.set(admission).map_err(|_| {
        debug!("websocket authentication attempted duplicate admission capture");
        internal_error_response("websocket authentication state error")
    })
}

fn read_captured_admission(capture: Arc<OnceLock<CapturedAdmission>>) -> Result<CapturedAdmission> {
    Arc::try_unwrap(capture)
        .map_err(|_| anyhow::anyhow!("websocket auth admission capture leaked across connections"))?
        .into_inner()
        .context("websocket handshake completed without an auth admission")
}

fn internal_error_response(message: &str) -> ErrorResponse {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Some(message.to_owned()))
        .expect("failed to build internal websocket handshake response")
}

#[cfg(test)]
mod tests {
    use super::{
        AUTH_FAILURE_STATE_TTL, AuthAbuseLimiter, MAX_AUTH_IN_FLIGHT_PER_ADDRESS,
        capture_admission, duplicate_shutdown_handle, enforce_access_lease,
        read_captured_admission, remaining_access_duration,
    };
    use crate::auth::{
        AccessCredential, AccessJwtSubject, AuthError, AuthErrorCode,
        AuthenticatedSessionPrincipal, CapturedAdmission, PresentedCredential,
        RefreshExchangeContext, RestrictedAdmission, RestrictedAuthContext,
    };
    use crate::session::SessionManager;
    use pioneer_protocol::{AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind};
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::{Arc, OnceLock};
    use std::time::{Duration, Instant};
    use tokio::io::AsyncReadExt;
    use tokio::sync::{mpsc, watch};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::http::StatusCode;

    #[tokio::test]
    async fn failed_or_restricted_admission_cannot_register_a_normal_connection() {
        let capture = OnceLock::new();
        let manager = SessionManager::new();

        let response = capture_admission(
            Err(AuthError::new(AuthErrorCode::InvalidCredential)),
            &capture,
        )
        .expect_err("invalid auth must reject the handshake");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(capture.get().is_none());
        assert!(manager.connection_ids().await.is_empty());

        let restricted = restricted_refresh();
        let capture = Arc::new(OnceLock::new());
        capture_admission(Ok(restricted), capture.as_ref()).unwrap();
        assert!(matches!(
            read_captured_admission(capture).unwrap(),
            CapturedAdmission::Restricted(_)
        ));
        assert!(manager.connection_ids().await.is_empty());
    }

    #[test]
    fn duplicate_and_missing_capture_fail_closed() {
        let capture = OnceLock::new();
        capture_admission(Ok(access("P00000000000000000001")), &capture).unwrap();

        let duplicate = capture_admission(Ok(access("P00000000000000000002")), &capture)
            .expect_err("duplicate capture must fail");
        assert_eq!(duplicate.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(read_captured_admission(Arc::new(OnceLock::new())).is_err());
    }

    #[test]
    fn concurrent_handshakes_use_connection_local_capture_cells() {
        let capture_a = Arc::new(OnceLock::new());
        let capture_b = Arc::new(OnceLock::new());

        std::thread::scope(|scope| {
            let capture_a = capture_a.clone();
            scope.spawn(move || {
                capture_admission(Ok(access("P00000000000000000001")), capture_a.as_ref()).unwrap();
            });
            let capture_b = capture_b.clone();
            scope.spawn(move || {
                capture_admission(Ok(access("P00000000000000000002")), capture_b.as_ref()).unwrap();
            });
        });

        let CapturedAdmission::Access(access_a) = read_captured_admission(capture_a).unwrap()
        else {
            panic!("expected access capture")
        };
        let CapturedAdmission::Access(access_b) = read_captured_admission(capture_b).unwrap()
        else {
            panic!("expected access capture")
        };
        assert_eq!(
            access_a.subject.principal_id.as_str(),
            "P00000000000000000001"
        );
        assert_eq!(
            access_b.subject.principal_id.as_str(),
            "P00000000000000000002"
        );
    }

    #[test]
    fn access_deadline_uses_exact_expiry_boundary() {
        assert_eq!(
            remaining_access_duration(101, std::time::Duration::from_millis(100_250)).unwrap(),
            std::time::Duration::from_millis(750)
        );
        assert!(remaining_access_duration(100, std::time::Duration::from_secs(100)).is_err());
        assert!(remaining_access_duration(99, std::time::Duration::from_secs(100)).is_err());
    }

    #[tokio::test]
    async fn access_expiry_hard_closes_socket_when_outbound_queue_is_full() {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (server, shutdown_handle) = duplicate_shutdown_handle(server).unwrap();

        let (outbound_tx, mut outbound_rx) = mpsc::channel(1);
        outbound_tx
            .try_send(Message::Ping(Vec::new().into()))
            .unwrap();
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (expired_tx, mut expired_rx) = watch::channel(false);
        let principal = AuthenticatedSessionPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: PrincipalId::new("P00000000000000000001").unwrap(),
            kind: PrincipalKind::Superuser,
            role_key: None,
            device_id: DeviceId::new("D00000000000000000001").unwrap(),
            session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            access_jti: "J00000000000000000001".to_owned(),
            access_expires_at_unix: 1,
        };

        let lease = tokio::spawn(enforce_access_lease(
            shutdown_handle,
            outbound_tx,
            principal,
            1,
            Duration::from_millis(20),
            Duration::from_millis(20),
            cancel_rx,
            expired_tx,
        ));
        expired_rx.changed().await.unwrap();
        assert!(*expired_rx.borrow());

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut byte))
            .await
            .expect("socket must close at access expiry")
            .unwrap();
        assert_eq!(read, 0);

        lease.await.unwrap();
        assert!(outbound_rx.try_recv().is_ok());
        drop(server);
    }

    #[test]
    fn auth_abuse_limiter_bounds_parallel_attempts_and_tracks_ip_and_gateway_failures() {
        let limiter = Arc::new(AuthAbuseLimiter::default());
        let address = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let permits = (0..MAX_AUTH_IN_FLIGHT_PER_ADDRESS)
            .map(|_| limiter.try_acquire(address).expect("bounded permit"))
            .collect::<Vec<_>>();
        assert!(limiter.try_acquire(address).is_none());
        drop(permits);

        let first = limiter.try_acquire(address).expect("first failure permit");
        first.record_failure();
        drop(first);
        let penalized = limiter.try_acquire(address).expect("penalized permit");
        assert!(!penalized.penalty().is_zero());
        penalized.record_success();
        drop(penalized);
        let gateway_penalized = limiter
            .try_acquire(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)))
            .expect("gateway-wide penalized permit");
        assert!(!gateway_penalized.penalty().is_zero());
        drop(gateway_penalized);

        let mut state = limiter.state.lock().unwrap();
        state.gateway_last_failure =
            Some(Instant::now() - AUTH_FAILURE_STATE_TTL - Duration::from_millis(1));
        drop(state);
        let reset = limiter
            .try_acquire(address)
            .expect("expired penalty permit");
        assert!(reset.penalty().is_zero());
    }

    fn access(principal_id: &str) -> CapturedAdmission {
        CapturedAdmission::Access(AccessCredential {
            subject: AccessJwtSubject {
                gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
                principal_id: PrincipalId::new(principal_id).unwrap(),
                device_id: DeviceId::new("D00000000000000000001").unwrap(),
                session_id: AuthSessionId::new("S00000000000000000001").unwrap(),
            },
            jti: "J00000000000000000001".to_owned(),
            issued_at_unix: 1,
            expires_at_unix: 2,
        })
    }

    fn restricted_refresh() -> CapturedAdmission {
        let credential = format!(
            "prf_{}",
            "r".repeat(pioneer_protocol::MIN_OPAQUE_CREDENTIAL_BODY_LEN)
        );
        CapturedAdmission::Restricted(RestrictedAdmission::new(
            PresentedCredential::classify(&credential).unwrap(),
            RestrictedAuthContext::Refresh(RefreshExchangeContext),
        ))
    }
}
