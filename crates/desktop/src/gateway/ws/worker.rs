use super::*;

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
}

struct ActiveConnectionTask {
    handle: JoinHandle<()>,
    rpc_tx: UnboundedSender<ConnectionRpcCommand>,
}

pub(super) fn spawn_worker(
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
        let _ = event_tx.send(GatewayWsEvent::Connecting {
            connection_id,
            endpoint_id: spec.endpoint_id.clone(),
            endpoint_name: spec.endpoint_name.clone(),
            endpoint_kind: spec.endpoint_kind,
        });

        match connect_websocket(&spec).await {
            Ok(stream) => {
                has_connected = true;
                attempt = 0;
                backoff = spec.timings.reconnect_initial;

                if let Some(sender) = initial_result_tx.take() {
                    let _ = sender.send(Ok(()));
                }

                let _ = event_tx.send(GatewayWsEvent::Connected {
                    connection_id,
                    endpoint_id: spec.endpoint_id.clone(),
                    endpoint_name: spec.endpoint_name.clone(),
                    address: spec.address.clone(),
                });

                let reason =
                    monitor_connection(stream, &spec, connection_id, &event_tx, &mut rpc_rx).await;

                let _ = event_tx.send(GatewayWsEvent::Disconnected {
                    connection_id,
                    endpoint_id: spec.endpoint_id.clone(),
                    endpoint_name: spec.endpoint_name.clone(),
                    endpoint_kind: spec.endpoint_kind,
                    address: spec.address.clone(),
                    reason: reason.clone(),
                });

                attempt = attempt.saturating_add(1);
                let delay = jitter_delay(backoff, spec.timings.reconnect_jitter_percent);
                let delay_ms = duration_to_millis_u64(delay);
                let _ = event_tx.send(GatewayWsEvent::Reconnecting {
                    connection_id,
                    endpoint_id: spec.endpoint_id.clone(),
                    endpoint_name: spec.endpoint_name.clone(),
                    attempt,
                    delay_ms,
                    reason,
                });

                sleep(delay).await;
                backoff = next_backoff(backoff, spec.timings.reconnect_max);
            }
            Err(error) => {
                let message = format!("{error:#}");

                if let Some(sender) = initial_result_tx.take() {
                    let _ = sender.send(Err(message.clone()));

                    if !retry_initial_failure {
                        let _ = event_tx.send(GatewayWsEvent::ConnectFailed {
                            connection_id,
                            endpoint_id: spec.endpoint_id.clone(),
                            endpoint_name: spec.endpoint_name.clone(),
                            endpoint_kind: spec.endpoint_kind,
                            address: spec.address.clone(),
                            error: message,
                        });
                        return;
                    }
                }

                if !has_connected && !retry_initial_failure {
                    return;
                }

                attempt = attempt.saturating_add(1);
                let delay = jitter_delay(backoff, spec.timings.reconnect_jitter_percent);
                let delay_ms = duration_to_millis_u64(delay);
                let _ = event_tx.send(GatewayWsEvent::Reconnecting {
                    connection_id,
                    endpoint_id: spec.endpoint_id.clone(),
                    endpoint_name: spec.endpoint_name.clone(),
                    attempt,
                    delay_ms,
                    reason: message,
                });

                sleep(delay).await;
                backoff = next_backoff(backoff, spec.timings.reconnect_max);
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
    let mut pending_requests = HashMap::new();
    let mut pending_upload_chunks = HashMap::new();

    let disconnect_reason = loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                if Instant::now().duration_since(last_pong) > spec.timings.pong_timeout {
                    break "websocket pong timeout".to_owned();
                }

                if let Err(error) = writer.send(Message::Ping(Vec::new().into())).await {
                    break format!("websocket ping failed: {error}");
                }
            }
            command = rpc_rx.recv() => {
                match command {
                    Some(ConnectionRpcCommand::Request { request_id, payload, response_tx }) => {
                        pending_requests.insert(request_id.clone(), response_tx);
                        if let Err(error) = writer.send(Message::Text(payload.into())).await {
                            if let Some(response_tx) = pending_requests.remove(&request_id) {
                                let _ = response_tx.send(Err(format!("websocket write failed: {error}")));
                            }
                            break format!("websocket write failed: {error}");
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
                    None => {
                        break "websocket command channel closed".to_owned();
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
                            break format!("websocket pong send failed: {error}");
                        }
                        last_pong = Instant::now();
                    }
                    Some(Ok(Message::Close(frame))) => {
                        break frame
                            .map(|value| value.reason.to_string())
                            .unwrap_or_else(|| "websocket closed by peer".to_owned());
                    }
                    Some(Ok(Message::Text(payload))) => {
                        process_text_payload(
                            payload.as_ref(),
                            connection_id,
                            &mut pending_requests,
                            &mut pending_upload_chunks,
                            event_tx,
                        );
                    }
                    Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => {
                        break format!("websocket read failed: {error}");
                    }
                    None => {
                        break "websocket stream ended".to_owned();
                    }
                }
            }
        }
    };

    fail_pending_requests(&mut pending_requests, disconnect_reason.as_str());
    fail_pending_upload_chunks(&mut pending_upload_chunks, disconnect_reason.as_str());
    disconnect_reason
}
