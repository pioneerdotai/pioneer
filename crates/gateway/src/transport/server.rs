use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tracing::debug;

use pioneer_config::AppConfig;

use crate::auth::{AuthenticatedPrincipal, JwtAuth};
use crate::message::MessageProcessor;
use crate::session::SessionManager;

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
    auth: JwtAuth,
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
    auth: JwtAuth,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
) -> Result<()> {
    let mut connection_tasks = JoinSet::new();

    loop {
        let config = config.clone();
        let auth = auth.clone();
        let message_processor = message_processor.clone();
        let session_manager = session_manager.clone();

        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer_addr)) => {
                        connection_tasks.spawn(async move {
                            let _ = handle_connection(
                                stream,
                                config,
                                auth,
                                message_processor,
                                session_manager,
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
    config: AppConfig,
    auth: JwtAuth,
    message_processor: Arc<MessageProcessor>,
    session_manager: Arc<SessionManager>,
) -> Result<()> {
    let principal_capture = Arc::new(OnceLock::new());
    let callback_capture = principal_capture.clone();
    let ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
        capture_authenticated_principal(
            auth.authenticate_request(request),
            callback_capture.as_ref(),
        )?;
        Ok(response)
    })
    .await
    .context("websocket handshake failed")?;
    let principal = read_captured_principal(principal_capture.as_ref())?;

    let (mut ws_writer, mut ws_reader) = ws.split();

    let queue_capacity = config.gateway.outbound_queue_capacity.max(1);

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(queue_capacity);

    let connection_id = session_manager
        .register_connection(outbound_tx.clone(), principal)
        .await;
    let connection_context = session_manager.connection_context(connection_id).await?;

    let writer_task = tokio::spawn(async move {
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
        while let Some(payload) = ws_reader.next().await {
            let message = match payload {
                Ok(value) => value,
                Err(error) => {
                    return Err(anyhow::anyhow!("websocket read failed: {error}"));
                }
            };

            match message {
                Message::Text(payload) => {
                    message_processor
                        .process_request(&connection_context, payload.as_ref())
                        .await;
                }
                Message::Binary(payload) => {
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

    session_manager.unregister_connection(connection_id).await;
    message_processor.connection_closed(connection_id).await;
    drop(outbound_tx);
    writer_task.await.context("writer task join failed")??;

    read_result
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

fn capture_authenticated_principal(
    authentication: Result<AuthenticatedPrincipal>,
    capture: &OnceLock<Arc<AuthenticatedPrincipal>>,
) -> std::result::Result<(), ErrorResponse> {
    let principal = match authentication {
        Ok(principal) => Arc::new(principal),
        Err(error) => {
            debug!(error = %format!("{error:#}"), "websocket auth rejected request");
            return Err(unauthorized_response("missing or invalid bearer token"));
        }
    };

    capture.set(principal).map_err(|_| {
        debug!("websocket authentication attempted duplicate principal capture");
        internal_error_response("websocket authentication state error")
    })
}

fn read_captured_principal(
    capture: &OnceLock<Arc<AuthenticatedPrincipal>>,
) -> Result<Arc<AuthenticatedPrincipal>> {
    capture
        .get()
        .cloned()
        .context("websocket handshake completed without an authenticated principal")
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
    use super::{capture_authenticated_principal, read_captured_principal};
    use crate::auth::{AuthenticatedPrincipal, CredentialKind};
    use crate::session::SessionManager;
    use crate::session::test_support::authenticated_test_superuser;
    use pioneer_protocol::{GatewayId, PrincipalId, PrincipalKind};
    use std::sync::{Arc, OnceLock};
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::http::StatusCode;

    #[tokio::test]
    async fn failed_authentication_cannot_register_a_connection() {
        let capture = OnceLock::new();
        let manager = SessionManager::new();

        let response =
            capture_authenticated_principal(Err(anyhow::anyhow!("invalid credential")), &capture)
                .expect_err("invalid auth must reject the handshake");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(capture.get().is_none());
        assert!(manager.connection_ids().await.is_empty());
    }

    #[tokio::test]
    async fn successful_capture_registers_the_exact_stable_principal() {
        let capture = OnceLock::new();
        let manager = SessionManager::new();
        let expected = authenticated_test_superuser();

        capture_authenticated_principal(Ok(expected.as_ref().clone()), &capture)
            .expect("valid authentication must be captured");
        let captured = read_captured_principal(&capture).expect("captured principal");
        assert_eq!(captured.as_ref(), expected.as_ref());

        let (sender, _receiver) = mpsc::channel(1);
        let connection_id = manager.register_connection(sender, captured).await;
        assert_eq!(manager.connection_ids().await, vec![connection_id]);
    }

    #[test]
    fn duplicate_and_missing_capture_fail_closed() {
        let capture = OnceLock::new();
        capture_authenticated_principal(
            Ok(authenticated_test_superuser().as_ref().clone()),
            &capture,
        )
        .unwrap();

        let duplicate =
            capture_authenticated_principal(Ok(principal("P00000000000000000002")), &capture)
                .expect_err("duplicate capture must fail");
        assert_eq!(duplicate.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            read_captured_principal(&capture)
                .unwrap()
                .principal_id
                .as_str(),
            "P00000000000000000001"
        );

        assert!(read_captured_principal(&OnceLock::new()).is_err());
    }

    #[test]
    fn concurrent_handshakes_use_connection_local_capture_cells() {
        let capture_a = Arc::new(OnceLock::new());
        let capture_b = Arc::new(OnceLock::new());

        std::thread::scope(|scope| {
            let capture_a = capture_a.clone();
            scope.spawn(move || {
                capture_authenticated_principal(
                    Ok(authenticated_test_superuser().as_ref().clone()),
                    capture_a.as_ref(),
                )
                .unwrap();
            });
            let capture_b = capture_b.clone();
            scope.spawn(move || {
                capture_authenticated_principal(
                    Ok(principal("P00000000000000000002")),
                    capture_b.as_ref(),
                )
                .unwrap();
            });
        });

        assert_eq!(
            read_captured_principal(capture_a.as_ref())
                .unwrap()
                .principal_id
                .as_str(),
            "P00000000000000000001"
        );
        assert_eq!(
            read_captured_principal(capture_b.as_ref())
                .unwrap()
                .principal_id
                .as_str(),
            "P00000000000000000002"
        );
    }

    fn principal(principal_id: &str) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
            principal_id: PrincipalId::new(principal_id).unwrap(),
            kind: PrincipalKind::Superuser,
            role_key: None,
            credential_kind: CredentialKind::LegacySuperuserJwt,
        }
    }
}
