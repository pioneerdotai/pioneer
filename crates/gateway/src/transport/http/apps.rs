//! Resource transport for exactly one authorized App execution. No HTML/JS rewriting.
use super::{GatewayHttpState, auth::new_request_id, errors::HttpError, views::admit_view_grant};
use crate::view_grants::ViewGrantSubject;
use axum::{
    body::Body,
    extract::{
        ConnectInfo, FromRequestParts, OriginalUri, Path, State,
        ws::{Message, WebSocketUpgrade},
    },
    http::{HeaderValue, Request},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use std::{net::SocketAddr, time::Duration};

pub(super) async fn app_view(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    Path((grant, path)): Path<(String, String)>,
    request: Request<Body>,
) -> Result<Response, HttpError> {
    let id = new_request_id();
    if path.len() > 4096
        || path.contains(['\\', '\0'])
        || path.split('/').any(|v| matches!(v, "." | ".."))
        || uri.query().is_some_and(|q| q.len() > 4096)
    {
        return Err(HttpError::bad_request(id));
    }
    let (mut parts, body) = request.into_parts();
    let (context, lease) =
        admit_view_grant(&state, peer, &parts.headers, &grant, true, id.clone()).await?;
    let ViewGrantSubject::App(scope) = &lease.grant.scope.subject else {
        return Err(HttpError::not_found(id));
    };
    // Stable across this user's login sessions/devices. App backends can keep
    // personal state alongside installation-wide data without receiving tokens.
    // Construct it from admitted authority; never forward a caller's header.
    let app_user = HeaderValue::from_str(context.principal().principal_id.as_str())
        .map_err(|_| HttpError::service_unavailable(id.clone()))?;
    let content_security_policy = app_content_security_policy(
        &parts.headers,
        &parts.uri,
        state
            .config
            .gateway
            .trusted_proxy_peers
            .contains(&peer.ip()),
    )
    .map_err(|_| HttpError::bad_request(id.clone()))?;
    let (address, proxy_credential, execution) = state
        .message_processor
        .app_ui_endpoint(&context, scope)
        .await
        .map_err(|_| HttpError::not_found(id.clone()))?;
    let mut target = url::Url::parse(&format!("http://{address}/"))
        .map_err(|_| HttpError::service_unavailable(id.clone()))?;
    target.set_path(&format!("/{path}"));
    target.set_query(uri.query());
    let stream_lease = state
        .http_streams
        .acquire_app(
            &context.principal().session_id,
            context.principal().principal_id.as_str(),
            &scope.workspace_id,
        )
        .map_err(|e| super::content::map_stream_admission(e, id.clone()))?;
    let stream_cancel = stream_lease.cancellation_token();
    // A relative reference survives a reverse proxy or client Gateway base prefix.
    // Resolve it against the browser request URL to obtain this grant root.
    let depth = path
        .split('/')
        .filter(|part| !part.is_empty())
        .count()
        .saturating_sub(usize::from(!path.ends_with('/')));
    let base = if depth == 0 {
        "./".to_owned()
    } else {
        "../".repeat(depth)
    };
    // Both native shells sandbox App documents into an opaque origin. Storage
    // and ambient browser authority are unavailable; this grant is the resource scope.
    if parts
        .headers
        .get("origin")
        .is_some_and(|v| v.as_bytes() != b"null")
    {
        return Err(HttpError::not_found(id));
    }
    if parts.method == axum::http::Method::OPTIONS {
        return Response::builder()
            .status(204)
            .header("access-control-allow-origin", "null")
            .header(
                "access-control-allow-methods",
                "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS",
            )
            .header(
                "access-control-allow-headers",
                "content-type, range, if-range, if-none-match, if-modified-since",
            )
            .header("cache-control", "no-store")
            .body(Body::empty())
            .map_err(|_| HttpError::service_unavailable(id));
    }
    if parts
        .headers
        .get("upgrade")
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"))
    {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        target
            .set_scheme("ws")
            .map_err(|_| HttpError::bad_request(id.clone()))?;
        let mut upstream_request = target
            .as_str()
            .into_client_request()
            .map_err(|_| HttpError::bad_request(id.clone()))?;
        upstream_request.headers_mut().insert(
            "pioneer-app-execution-credential",
            HeaderValue::from_str(proxy_credential.expose_secret())
                .map_err(|_| HttpError::service_unavailable(id.clone()))?,
        );
        upstream_request
            .headers_mut()
            .insert("pioneer-app-user-id", app_user);
        upstream_request.headers_mut().insert(
            "pioneer-app-base-path",
            HeaderValue::from_str(&base).map_err(|_| HttpError::bad_request(id.clone()))?,
        );
        let offers =
            websocket_offers(&parts.headers).map_err(|_| HttpError::bad_request(id.clone()))?;
        let (upstream, selected) = connect_upstream(upstream_request, &offers)
            .await
            .map_err(|_| HttpError::service_unavailable(id.clone()))?;
        let upgrade = WebSocketUpgrade::from_request_parts(&mut parts, &state)
            .await
            .map_err(|_| HttpError::bad_request(id))?;
        return Ok(upgrade.protocols(selected).max_frame_size(1024*1024).max_message_size(1024*1024).on_upgrade(move |browser| async move {
            let _stream_lease = stream_lease;
            let (mut bs, mut br) = browser.split(); let (mut us, mut ur) = upstream.split();
            let forward = async { while let Some(Ok(message)) = br.next().await {
                let message = match message { Message::Text(v) => tokio_tungstenite::tungstenite::Message::Text(v.to_string().into()), Message::Binary(v) => tokio_tungstenite::tungstenite::Message::Binary(v), Message::Ping(v) => tokio_tungstenite::tungstenite::Message::Ping(v), Message::Pong(v) => tokio_tungstenite::tungstenite::Message::Pong(v), Message::Close(_) => break };
                if us.send(message).await.is_err() { break; }
            } };
            let reverse = async { while let Some(Ok(message)) = ur.next().await {
                let message = match message { tokio_tungstenite::tungstenite::Message::Text(v) => Message::Text(v.to_string().into()), tokio_tungstenite::tungstenite::Message::Binary(v) => Message::Binary(v), tokio_tungstenite::tungstenite::Message::Ping(v) => Message::Ping(v), tokio_tungstenite::tungstenite::Message::Pong(v) => Message::Pong(v), _ => break };
                if bs.send(message).await.is_err() { break; }
            } };
            let revoked = lease.cancellation_token(); let expiry_service = state.view_grants.clone();
            tokio::select! { _ = forward => {}, _ = reverse => {}, _ = execution.cancelled() => {}, _ = stream_cancel.cancelled() => {}, _ = revoked.cancelled() => {}, _ = expiry_service.wait_app_expiry(&grant) => {} }
        }));
    }
    let revoked = lease.cancellation_token();
    let body = tokio::select! {
        biased;
        _ = stream_cancel.cancelled() => return Err(HttpError::not_found(id)),
        _ = execution.cancelled() => return Err(HttpError::not_found(id)),
        _ = revoked.cancelled() => return Err(HttpError::not_found(id)),
        result = tokio::time::timeout(Duration::from_secs(30), axum::body::to_bytes(body, 8*1024*1024)) => result.map_err(|_| HttpError::bad_request(id.clone()))?.map_err(|_| HttpError::bad_request(id.clone()))?,
    };
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| HttpError::service_unavailable(id.clone()))?;
    let mut request = client
        .request(parts.method, target)
        .header("pioneer-app-base-path", base)
        .header(
            "pioneer-app-execution-credential",
            proxy_credential.expose_secret(),
        )
        .header("pioneer-app-user-id", app_user)
        .body(body);
    // Browser/user credentials and cookies never cross the execution boundary.
    for name in [
        "content-type",
        "accept",
        "accept-language",
        "range",
        "if-range",
        "if-none-match",
        "if-modified-since",
    ] {
        if let Some(value) = parts.headers.get(name) {
            request = request.header(name, value);
        }
    }
    let upstream = tokio::select! {
        biased;
        _ = stream_cancel.cancelled() => return Err(HttpError::not_found(id)),
        _ = revoked.cancelled() => return Err(HttpError::not_found(id)),
        _ = execution.cancelled() => return Err(HttpError::not_found(id)),
        result = tokio::time::timeout(Duration::from_secs(30), request.send()) => result.map_err(|_| HttpError::service_unavailable(id.clone()))?.map_err(|_| HttpError::service_unavailable(id.clone()))?,
    };
    let mut response = Response::builder().status(upstream.status());
    for name in [
        "content-type",
        "content-length",
        "content-encoding",
        "etag",
        "last-modified",
        "content-range",
        "accept-ranges",
        "location",
    ] {
        if let Some(value) = upstream.headers().get(name) {
            response = response.header(name, value);
        }
    }
    let grants = state.view_grants.clone();
    let expiry = Box::pin(async move { grants.wait_app_expiry(&grant).await });
    let stream = futures_util::stream::unfold(
        (
            upstream.bytes_stream(),
            lease,
            stream_lease,
            execution,
            revoked,
            stream_cancel,
            expiry,
        ),
        move |(
            mut upstream,
            lease,
            mut capacity,
            execution,
            revoked,
            stream_cancel,
            mut expiry,
        )| async move {
            use super::streams::StreamOutcome;
            let next = tokio::select! {
                _ = execution.cancelled() => { capacity.set_outcome(StreamOutcome::Cancelled); None },
                _ = revoked.cancelled() => { capacity.set_outcome(StreamOutcome::Cancelled); None },
                _ = stream_cancel.cancelled() => { capacity.set_outcome(StreamOutcome::Cancelled); None },
                _ = &mut expiry => { capacity.set_outcome(StreamOutcome::Cancelled); None },
                result = tokio::time::timeout(Duration::from_secs(30), upstream.next()) => match result {
                    Ok(None) => { capacity.set_outcome(StreamOutcome::Completed); None },
                    Ok(Some(Ok(bytes))) => { capacity.record_bytes(bytes.len()); Some(Ok(bytes)) },
                    Ok(Some(Err(error))) => { capacity.set_outcome(StreamOutcome::Failed); Some(Err(error)) },
                    Err(_) => { capacity.set_outcome(StreamOutcome::IdleTimeout); None },
                },
            };
            next.map(|chunk| {
                (
                    chunk,
                    (
                        upstream,
                        lease,
                        capacity,
                        execution,
                        revoked,
                        stream_cancel,
                        expiry,
                    ),
                )
            })
        },
    );
    // App interfaces may compile WebAssembly without enabling JavaScript eval.
    response
        .header("cache-control", "no-store")
        .header("referrer-policy", "no-referrer")
        .header("x-content-type-options", "nosniff")
        .header("cross-origin-resource-policy", "cross-origin")
        .header("access-control-allow-origin", "null")
        .header(
            "access-control-expose-headers",
            "content-range, accept-ranges, etag, last-modified",
        )
        .header("content-security-policy", content_security_policy)
        .body(Body::from_stream(stream))
        .map_err(|_| HttpError::service_unavailable(id))
}

pub(super) async fn app_view_root(
    State(state): State<GatewayHttpState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    Path(grant): Path<String>,
    request: Request<Body>,
) -> Result<Response, HttpError> {
    app_view(
        State(state),
        ConnectInfo(peer),
        OriginalUri(uri),
        Path((grant, String::new())),
        request,
    )
    .await
}

fn app_content_security_policy(
    headers: &axum::http::HeaderMap,
    uri: &axum::http::Uri,
    trusted_proxy: bool,
) -> anyhow::Result<String> {
    let forwarded = if trusted_proxy {
        let mut values = headers.get_all("x-forwarded-host").iter();
        let value = values.next();
        anyhow::ensure!(values.next().is_none(), "apps.invalid_public_authority");
        value.map(|value| value.to_str()).transpose()?
    } else {
        None
    };
    let authority = forwarded
        .or_else(|| uri.authority().map(|value| value.as_str()))
        .or_else(|| headers.get("host").and_then(|value| value.to_str().ok()))
        .ok_or_else(|| anyhow::anyhow!("apps.invalid_public_authority"))?;
    // Accept only a concrete DNS/IP authority and port, never CSP syntax,
    // wildcards, user information or a forwarded multi-host list.
    anyhow::ensure!(
        authority.len() <= 320
            && authority
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".-:[]".contains(&byte)),
        "apps.invalid_public_authority"
    );
    let endpoint = url::Url::parse(&format!("http://{authority}/"))?;
    anyhow::ensure!(
        endpoint.host_str().is_some(),
        "apps.invalid_public_authority"
    );
    // Sandboxed WebKit documents can reject connect-src 'self'. Explicit
    // sources preserve the same Gateway authority for fetch and WebSocket.
    Ok(format!(
        "default-src 'self' data: blob:; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; style-src 'self' 'unsafe-inline'; connect-src 'self' http://{authority} https://{authority} ws://{authority} wss://{authority}; frame-src 'none'; object-src 'none'; worker-src 'none'; base-uri 'self'; form-action 'self'"
    ))
}

fn protocol_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}
fn websocket_offers(headers: &axum::http::HeaderMap) -> anyhow::Result<Vec<String>> {
    let mut offers = Vec::new();
    let mut bytes = 0usize;
    for header in headers.get_all("sec-websocket-protocol") {
        bytes += header.as_bytes().len();
        anyhow::ensure!(bytes <= 2048, "apps.invalid_websocket_protocol");
        for value in header.to_str()?.split(',').map(str::trim) {
            anyhow::ensure!(
                protocol_token(value) && offers.len() < 16 && !offers.iter().any(|v| v == value),
                "apps.invalid_websocket_protocol"
            );
            offers.push(value.to_owned());
        }
    }
    Ok(offers)
}
async fn connect_upstream(
    mut request: tokio_tungstenite::tungstenite::handshake::client::Request,
    offers: &[String],
) -> anyhow::Result<(
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Option<String>,
)> {
    if !offers.is_empty() {
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_str(&offers.join(", "))?,
        );
    }
    let (socket, response) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async_with_config(
            request,
            Some(
                tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                    .max_message_size(Some(1024 * 1024))
                    .max_frame_size(Some(1024 * 1024)),
            ),
            true, // TCP_NODELAY for interactive App WebSocket traffic.
        ),
    )
    .await??;
    anyhow::ensure!(
        !response.headers().contains_key("sec-websocket-extensions"),
        "apps.unrequested_websocket_extensions"
    );
    let values = response.headers().get_all("sec-websocket-protocol");
    anyhow::ensure!(
        values.iter().count() <= 1,
        "apps.invalid_websocket_protocol"
    );
    let selected = values
        .iter()
        .next()
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()?;
    if let Some(protocol) = &selected {
        anyhow::ensure!(
            protocol_token(protocol) && offers.contains(protocol),
            "apps.invalid_websocket_protocol"
        );
    }
    Ok((socket, selected))
}

#[cfg(test)]
mod websocket_negotiation_regressions {
    use super::*;
    use tokio_tungstenite::tungstenite::{
        client::IntoClientRequest,
        handshake::server::{Request, Response},
    };
    #[tokio::test]
    async fn upstream_handshake_preserves_offers_and_rejects_invalid_selection() {
        for (offer, selection, succeeds) in [
            (Some("chat, events"), Some("events"), true),
            (None, None, true),
            (Some("chat"), Some("other"), false),
            (Some("chat"), Some("bad protocol"), false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let socket = tokio_tungstenite::accept_hdr_async(
                    stream,
                    move |request: &Request, mut response: Response| {
                        assert_eq!(
                            request
                                .headers()
                                .get("sec-websocket-protocol")
                                .map(|v| v.to_str().unwrap()),
                            offer
                        );
                        if let Some(protocol) = selection {
                            response.headers_mut().insert(
                                "sec-websocket-protocol",
                                HeaderValue::from_str(protocol).unwrap(),
                            );
                        }
                        Ok(response)
                    },
                )
                .await;
                if let Ok(mut socket) = socket {
                    let _ = socket
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            "ready".into(),
                        ))
                        .await;
                }
            });
            let mut headers = axum::http::HeaderMap::new();
            if let Some(offer) = offer {
                headers.insert(
                    "sec-websocket-protocol",
                    HeaderValue::from_str(offer).unwrap(),
                );
            }
            let offers = websocket_offers(&headers).unwrap();
            let request = format!("ws://{address}/ui/socket")
                .into_client_request()
                .unwrap();
            let result = connect_upstream(request, &offers).await;
            assert_eq!(result.is_ok(), succeeds);
            if succeeds {
                let (mut socket, selected) = result.unwrap();
                assert_eq!(selected.as_deref(), selection);
                assert_eq!(
                    socket.next().await.unwrap().unwrap().into_text().unwrap(),
                    "ready"
                );
            }
            server.await.unwrap();
        }
    }
    #[test]
    fn browser_protocol_offers_are_bounded_http_tokens() {
        for invalid in [
            "",
            "chat,,events",
            "bad protocol",
            "chat, chat",
            "chat;parameter",
        ] {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                "sec-websocket-protocol",
                HeaderValue::from_str(invalid).unwrap(),
            );
            assert!(websocket_offers(&headers).is_err());
        }
    }
}

#[cfg(test)]
mod content_security_policy_regressions {
    use super::*;

    #[test]
    fn opaque_app_connections_name_only_the_requested_gateway() {
        for (host, forwarded, trusted, expected) in [
            ("127.0.0.1:18778", None, false, "127.0.0.1:18778"),
            ("[::1]:18778", None, false, "[::1]:18778"),
            (
                "internal:18778",
                Some("gateway.example:8443"),
                true,
                "gateway.example:8443",
            ),
            (
                "gateway.example",
                Some("attacker.example"),
                false,
                "gateway.example",
            ),
        ] {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert("host", host.parse().unwrap());
            if let Some(forwarded) = forwarded {
                headers.insert("x-forwarded-host", forwarded.parse().unwrap());
            }
            let policy =
                app_content_security_policy(&headers, &"/".parse().unwrap(), trusted).unwrap();
            let connect = policy
                .split(';')
                .map(str::trim)
                .find(|directive| directive.starts_with("connect-src "))
                .unwrap();
            assert_eq!(
                connect,
                format!(
                    "connect-src 'self' http://{expected} https://{expected} ws://{expected} wss://{expected}"
                )
            );
            assert!(policy.contains("'wasm-unsafe-eval'"));
            assert!(!policy.contains("'unsafe-eval'"));
        }
    }

    #[test]
    fn public_authority_cannot_inject_or_broaden_the_policy() {
        for host in [
            "*.example.com",
            "gateway.example; connect-src *",
            "gateway.example,attacker.example",
            "user@gateway.example",
            "gateway.example/path",
            "gateway.example:99999",
        ] {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert("host", "internal:18778".parse().unwrap());
            headers.insert("x-forwarded-host", host.parse().unwrap());
            assert!(app_content_security_policy(&headers, &"/".parse().unwrap(), true).is_err());
        }
        let mut headers = axum::http::HeaderMap::new();
        headers.append("x-forwarded-host", "gateway.example".parse().unwrap());
        headers.append("x-forwarded-host", "attacker.example".parse().unwrap());
        assert!(app_content_security_policy(&headers, &"/".parse().unwrap(), true).is_err());
        assert!(
            app_content_security_policy(&Default::default(), &"/".parse().unwrap(), false).is_err()
        );
    }
}
