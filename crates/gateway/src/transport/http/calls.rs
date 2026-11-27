use super::GatewayHttpState;
use axum::{
    extract::{
        OriginalUri, State,
        ws::{Message as ClientMessage, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use pioneer_calls::{
    livekit::MAX_SIGNAL_BYTES,
    relay_media::{self, RelaySignal},
};
use std::time::Duration;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message as UpstreamMessage, client::IntoClientRequest, protocol::WebSocketConfig,
    },
};

const MAX_QUERY_BYTES: usize = 16 * 1024;

struct SignalRequest {
    token: String,
    query: String,
    resume: bool,
}

pub(super) async fn upgrade(
    State(state): State<GatewayHttpState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some(calls) = state.message_processor.calls() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let Some(signal) = signal_request(&uri, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(admission) = calls
        .admit(state.message_processor.call_store(), &signal.token)
        .await
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    // A relay registration belongs to one signaling socket. A signal-only resume
    // cannot reuse its subscriber/publisher routes. Both SDKs escalate a failed
    // resume to a full reconnect, which negotiates and registers fresh ICE routes.
    if signal.resume && calls.media.relay_udp_addr().is_some() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let mut upstream_url = calls
        .media
        .local_signal_url(uri.path(), Some(&signal.query));
    if upstream_url.set_scheme("ws").is_err() {
        return StatusCode::BAD_GATEWAY.into_response();
    }
    let Ok(mut request) = upstream_url.as_str().into_client_request() else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    let Ok(upstream_token) = calls.media.upstream_token(&signal.token) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(authorization) = format!("Bearer {upstream_token}").parse() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    request
        .headers_mut()
        .insert(header::AUTHORIZATION, authorization);
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_SIGNAL_BYTES))
        .max_frame_size(Some(MAX_SIGNAL_BYTES))
        .max_write_buffer_size(MAX_SIGNAL_BYTES * 4);
    let Ok(Ok((upstream, _))) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async_with_config(request, Some(config), true),
    )
    .await
    else {
        return StatusCode::BAD_GATEWAY.into_response();
    };
    drop(admission);
    let media = if let Some(local) = calls.media.relay_udp_addr() {
        let Ok((room, identity)) = calls.media.admission(&signal.token) else {
            return StatusCode::UNAUTHORIZED.into_response();
        };
        let Some(target) = state.message_processor.call_media_target().await else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        let Ok(transport) = target.open().await else {
            return StatusCode::BAD_GATEWAY.into_response();
        };
        Some((local, transport, room, identity))
    } else {
        None
    };
    let shutdown = calls.shutdown.clone();
    let processor = state.message_processor;
    upgrade
        .max_frame_size(MAX_SIGNAL_BYTES)
        .max_message_size(MAX_SIGNAL_BYTES)
        .max_write_buffer_size(MAX_SIGNAL_BYTES * 4)
        .on_upgrade(move |socket| async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = async {
                    if let Some((local, transport, room, identity)) = media {
                        let Some(calls) = processor.calls() else { return };
                        let (registration, forward) = relay_media::session(transport.stream, local);
                        let signaling = RelaySignal::new(registration, transport.public_addr, &calls.media, room, identity);
                        let mut remote_shutdown = transport.shutdown;
                        tokio::select! {
                            _ = remote_shutdown.wait_for(|stopped| *stopped) => {}
                            result = forward => {
                                if result.is_err() {
                                    tracing::debug!("call media relay stream closed");
                                }
                            }
                            _ = bridge(socket, upstream, Some(signaling)) => {}
                        }
                    } else {
                        bridge(socket, upstream, None).await;
                    }
                } => {}
            }
        })
}

pub(super) async fn validate(
    State(state): State<GatewayHttpState>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let Some(calls) = state.message_processor.calls() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let Some(signal) = signal_request(&uri, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(_admission) = calls
        .admit(state.message_processor.call_store(), &signal.token)
        .await
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(upstream_token) = calls.media.upstream_token(&signal.token) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match calls
        .media
        .validate_signal(uri.path(), &signal.query, &upstream_token)
        .await
    {
        Ok(true) => (StatusCode::OK, "success").into_response(),
        Ok(false) => StatusCode::BAD_REQUEST.into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

fn signal_request(uri: &Uri, headers: &HeaderMap) -> Option<SignalRequest> {
    let query = uri.query().unwrap_or_default();
    if query.len() > MAX_QUERY_BYTES {
        return None;
    }
    let mut query_token = None;
    let mut resume = false;
    let mut forward = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if key == "access_token" {
            if query_token.replace(value.into_owned()).is_some() {
                return None;
            }
        } else {
            resume |= key == "reconnect" && value == "1";
            forward.append_pair(&key, &value);
        }
    }
    let token = match headers.get(header::AUTHORIZATION) {
        Some(value) => value.to_str().ok()?.strip_prefix("Bearer ")?.to_owned(),
        None => query_token?,
    };
    if token.is_empty() || token.len() > 4096 {
        return None;
    }
    Some(SignalRequest {
        token,
        query: forward.finish(),
        resume,
    })
}

async fn bridge<T>(
    client: WebSocket,
    upstream: WebSocketStream<T>,
    mut signaling: Option<RelaySignal<'_>>,
) where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut client_write, mut client_read) = client.split();
    let (mut upstream_write, mut upstream_read) = upstream.split();
    let to_upstream = async {
        while let Some(Ok(message)) = client_read.next().await {
            let message = match message {
                ClientMessage::Binary(bytes) => UpstreamMessage::Binary(bytes),
                ClientMessage::Text(text) => UpstreamMessage::Text(text.as_str().into()),
                ClientMessage::Close(_) => break,
                // Ping and pong apply to their own transport hop.
                ClientMessage::Ping(_) | ClientMessage::Pong(_) => continue,
            };
            if upstream_write.send(message).await.is_err() {
                break;
            }
        }
    };
    let to_client = async {
        while let Some(Ok(message)) = upstream_read.next().await {
            let message = match (message, signaling.as_mut()) {
                (UpstreamMessage::Binary(bytes), Some(signaling)) => match signaling
                    .binary(&bytes)
                    .await
                {
                    Ok(Some(bytes)) => ClientMessage::Binary(bytes.into()),
                    Ok(None) => continue,
                    Err(_) => {
                        tracing::debug!("call relay could not forward LiveKit binary signaling");
                        break;
                    }
                },
                (UpstreamMessage::Text(text), Some(signaling)) => {
                    match signaling.text(text.as_str()).await {
                        Ok(Some(text)) => ClientMessage::Text(text.into()),
                        Ok(None) => continue,
                        Err(_) => {
                            tracing::debug!("call relay could not forward LiveKit text signaling");
                            break;
                        }
                    }
                }
                (UpstreamMessage::Binary(bytes), None) => ClientMessage::Binary(bytes),
                (UpstreamMessage::Text(text), None) => ClientMessage::Text(text.as_str().into()),
                (UpstreamMessage::Close(_), _) => break,
                _ => continue,
            };
            if client_write.send(message).await.is_err() {
                break;
            }
        }
    };
    tokio::select! {
        _ = to_upstream => {},
        _ = to_client => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_detects_signal_resume_without_forwarding_the_client_token() {
        let uri = "/rtc/v1?reconnect=1&sid=PA_example&access_token=test"
            .parse()
            .unwrap();
        let request = signal_request(&uri, &HeaderMap::new()).unwrap();
        assert!(request.resume);
        assert_eq!(request.query, "reconnect=1&sid=PA_example");
        assert_eq!(request.token, "test");

        let uri = "/rtc?reconnect=0&access_token=test".parse().unwrap();
        assert!(!signal_request(&uri, &HeaderMap::new()).unwrap().resume);
    }
}
