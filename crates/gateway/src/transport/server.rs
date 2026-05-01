use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tracing::debug;

use pioneer_config::AppConfig;

use crate::auth::JwtAuth;
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
    let ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
        if let Err(error) = auth.authorize_request(request) {
            debug!(error = %format!("{error:#}"), "websocket auth rejected request");
            return Err(unauthorized_response("missing or invalid bearer token"));
        }

        Ok(response)
    })
    .await
    .context("websocket handshake failed")?;

    let (mut ws_writer, mut ws_reader) = ws.split();

    let queue_capacity = config.gateway.outbound_queue_capacity.max(1);

    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(queue_capacity);

    let connection_id = session_manager
        .register_connection(outbound_tx.clone())
        .await;

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
                        .process_request(connection_id, payload.as_ref())
                        .await;
                }
                Message::Binary(payload) => {
                    message_processor
                        .process_binary_frame(connection_id, payload.as_ref())
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
