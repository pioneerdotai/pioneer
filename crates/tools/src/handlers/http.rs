use crate::error::ToolError;
use crate::network_policy::{NetworkPolicyChecker, NetworkPolicyDecision, enforce_network_url};
use futures_util::StreamExt;
use pioneer_protocol::TurnExecutionSecuritySnapshot;
use pioneer_protocol::TurnNetworkMode;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::header::{
    ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, HeaderMap, HeaderValue, PRAGMA, USER_AGENT,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::net::IpAddr;
use std::time::Duration;
use std::time::Instant;
use url::Url;

#[derive(Debug, Clone)]
pub(crate) struct BufferedHttpRequest {
    pub method: String,
    pub url: String,
    pub timeout_ms: u64,
    pub follow_redirects: bool,
    pub user_agent: Option<String>,
    pub headers: HashMap<String, String>,
    pub query: Option<JsonMap<String, JsonValue>>,
    pub body: Option<JsonValue>,
    pub max_bytes: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct BufferedHttpResponse {
    pub request_url: String,
    pub final_url: String,
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub truncated: bool,
    pub elapsed_ms: u64,
}

pub(crate) fn build_http_client(
    timeout_ms: u64,
    follow_redirects: bool,
    user_agent: Option<&str>,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
    operation: &str,
) -> Result<reqwest::Client, ToolError> {
    let mut headers = HeaderMap::new();
    let user_agent = user_agent
        .and_then(|value| HeaderValue::from_str(value).ok())
        .unwrap_or_else(|| HeaderValue::from_static("Mozilla/5.0"));
    headers.insert(USER_AGENT, user_agent);
    headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
        ),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));

    let redirect = if follow_redirects {
        let security_snapshot = security_snapshot.cloned();
        let operation = operation.to_owned();
        reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= 10 {
                return attempt.error(HttpRedirectPolicyError::TooManyRedirects);
            }
            let Some(snapshot) = security_snapshot.as_ref() else {
                return attempt.error(HttpRedirectPolicyError::Denied(
                    "missing turn execution security snapshot".to_owned(),
                ));
            };
            match NetworkPolicyChecker::check_url(
                snapshot,
                attempt.url().as_str(),
                format!("{operation} redirect"),
            ) {
                NetworkPolicyDecision::Allowed(_) => attempt.follow(),
                NetworkPolicyDecision::Denied(deny) => {
                    attempt.error(HttpRedirectPolicyError::Denied(deny.message))
                }
            }
        })
    } else {
        reqwest::redirect::Policy::none()
    };

    let mut builder = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_millis(timeout_ms))
        .redirect(redirect);
    if let Some(snapshot) = security_snapshot {
        builder = builder.dns_resolver(PolicyDnsResolver {
            snapshot: snapshot.clone(),
        });
        if snapshot.network.mode != TurnNetworkMode::Enabled {
            // An ambient HTTP(S)/ALL_PROXY is a second connection authority:
            // it can resolve the URL itself and bypass this client's pinned
            // DNS/private-address checks. Restricted native turns have no
            // proxy capability in their immutable policy, so fail closed by
            // disabling process-environment proxy discovery.
            builder = builder.no_proxy();
        }
    }
    builder
        .build()
        .map_err(|error| ToolError::internal(format!("failed to build http client: {error}")))
}

pub(crate) async fn execute_buffered_http_request(
    request: BufferedHttpRequest,
    security_snapshot: Option<&TurnExecutionSecuritySnapshot>,
    operation: &str,
) -> Result<BufferedHttpResponse, ToolError> {
    if request.max_bytes == 0 {
        return Err(ToolError::invalid_arguments(
            "http request max_bytes must be greater than 0",
        ));
    }

    validate_http_url(request.url.as_str())?;
    enforce_network_url(security_snapshot, request.url.as_str(), operation)?;
    let client = build_http_client(
        request.timeout_ms.max(1),
        request.follow_redirects,
        request.user_agent.as_deref(),
        security_snapshot,
        operation,
    )?;

    let method_text = request.method.trim().to_uppercase();
    let method = reqwest::Method::from_bytes(method_text.as_bytes()).map_err(|error| {
        ToolError::invalid_arguments(format!("invalid HTTP method `{}`: {error}", request.method))
    })?;

    let mut outgoing = client.request(method, request.url.as_str());
    for (name, value) in request.headers {
        if !name.trim().is_empty() {
            outgoing = outgoing.header(name, value);
        }
    }

    if let Some(query) = request.query.as_ref() {
        outgoing = outgoing.query(query);
    }

    if let Some(body) = request.body.as_ref() {
        outgoing = outgoing.json(body);
    }

    let started = Instant::now();
    let response = outgoing
        .send()
        .await
        .map_err(|error| map_http_request_error(error, "request failed"))?;

    let status_code = response.status().as_u16();
    let final_url = response.url().to_string();
    let headers = response
        .headers()
        .iter()
        .map(|(key, value)| {
            (
                key.as_str().to_owned(),
                value.to_str().unwrap_or_default().to_owned(),
            )
        })
        .collect::<HashMap<_, _>>();

    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let mut truncated = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            ToolError::execution_failed(format!("response stream error: {error}"))
        })?;

        if body.len() >= request.max_bytes {
            truncated = true;
            break;
        }

        let remaining = request.max_bytes.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }

        body.extend_from_slice(&chunk);
    }

    Ok(BufferedHttpResponse {
        request_url: request.url,
        final_url,
        status_code,
        headers,
        body,
        truncated,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

#[derive(Debug)]
enum HttpRedirectPolicyError {
    Denied(String),
    TooManyRedirects,
}

#[derive(Debug)]
struct NetworkDnsPolicyError {
    host: String,
    address: IpAddr,
}

impl fmt::Display for NetworkDnsPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "network sandbox denied DNS resolution of `{}` to non-public address {}",
            self.host, self.address
        )
    }
}

impl StdError for NetworkDnsPolicyError {}

#[derive(Clone)]
struct PolicyDnsResolver {
    snapshot: TurnExecutionSecuritySnapshot,
}

impl Resolve for PolicyDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        let snapshot = self.snapshot.clone();
        Box::pin(async move {
            let resolved = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|error| Box::new(error) as Box<dyn StdError + Send + Sync>)?;
            let mut addresses = Vec::new();
            for address in resolved {
                if !resolved_address_allowed(&snapshot, host.as_str(), address.ip()) {
                    return Err(Box::new(NetworkDnsPolicyError {
                        host: host.clone(),
                        address: address.ip(),
                    }) as Box<dyn StdError + Send + Sync>);
                }
                if !addresses.contains(&address) {
                    addresses.push(address);
                }
            }
            if addresses.is_empty() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "DNS resolution returned no addresses",
                )) as Box<dyn StdError + Send + Sync>);
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

fn resolved_address_allowed(
    snapshot: &TurnExecutionSecuritySnapshot,
    host: &str,
    address: IpAddr,
) -> bool {
    match snapshot.network.mode {
        TurnNetworkMode::Enabled => true,
        TurnNetworkMode::Disabled => false,
        TurnNetworkMode::Restricted => {
            !ip_is_non_public(address)
                || (snapshot.network.allow_localhost
                    && (host.eq_ignore_ascii_case("localhost")
                        || host.to_ascii_lowercase().ends_with(".localhost"))
                    && address.is_loopback())
        }
    }
}

fn ip_is_non_public(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let [a, b, c, d] = address.octets();
            a == 0
                || a == 10
                || a == 127
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                // IETF protocol assignments are non-global except the two
                // explicitly globally reachable anycast addresses.
                || (a == 192 && b == 0 && c == 0 && d != 9 && d != 10)
                || (a == 192 && b == 0 && c == 2)
                // The former 6to4 relay block is deprecated; its remaining
                // active 6a44 anycast address is explicitly non-global.
                || (a == 192 && b == 88 && c == 99)
                || (a == 192 && b == 168)
                || (a == 198 && (b == 18 || b == 19))
                || (a == 198 && b == 51 && c == 100)
                || (a == 203 && b == 0 && c == 113)
                || (224..=239).contains(&a)
                || a >= 240
        }
        IpAddr::V6(address) => {
            let segments = address.segments();
            // IPv4-mapped addresses are themselves non-global regardless of
            // whether the embedded IPv4 address is public.
            if matches!(segments, [0, 0, 0, 0, 0, 0xffff, _, _]) {
                return true;
            }

            // The well-known NAT64 prefix is globally reachable, but it must
            // not become an encoding-based route to an IPv4 special-use
            // destination.
            if matches!(segments, [0x64, 0xff9b, 0, 0, 0, 0, _, _]) {
                let octets = address.octets();
                let embedded =
                    std::net::Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]);
                return ip_is_non_public(IpAddr::V4(embedded));
            }

            // IANA currently allocates ordinary globally routable unicast
            // IPv6 from 2000::/3. Everything outside it is special-use,
            // reserved, local, multicast, or otherwise non-global (apart
            // from the NAT64 exception handled above).
            if (segments[0] & 0xe000) != 0x2000 {
                return true;
            }

            let globally_reachable_ietf_exception =
                matches!(segments, [0x2001, 1, 0, 0, 0, 0, 0, 1..=3])
                    || matches!(segments, [0x2001, 3, _, _, _, _, _, _])
                    || matches!(segments, [0x2001, 4, 0x112, _, _, _, _, _])
                    || matches!(segments, [0x2001, 0x20..=0x3f, _, _, _, _, _, _]);
            let ietf_protocol_assignment = segments[0] == 0x2001 && segments[1] < 0x0200;

            (ietf_protocol_assignment && !globally_reachable_ietf_exception)
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
                || segments[0] == 0x2002
                || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
        }
    }
}

impl fmt::Display for HttpRedirectPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Denied(message) => {
                write!(formatter, "network sandbox denied redirect: {message}")
            }
            Self::TooManyRedirects => formatter.write_str("redirect limit exceeded"),
        }
    }
}

impl StdError for HttpRedirectPolicyError {}

pub(crate) fn map_http_request_error(error: reqwest::Error, context: &str) -> ToolError {
    let mut source: Option<&(dyn StdError + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(policy_error) = current.downcast_ref::<HttpRedirectPolicyError>() {
            return match policy_error {
                HttpRedirectPolicyError::Denied(message) => {
                    ToolError::Rejected(format!("network sandbox denied HTTP redirect: {message}"))
                }
                HttpRedirectPolicyError::TooManyRedirects => {
                    ToolError::execution_failed("HTTP redirect limit exceeded")
                }
            };
        }
        if let Some(policy_error) = current.downcast_ref::<NetworkDnsPolicyError>() {
            return ToolError::Rejected(policy_error.to_string());
        }
        source = current.source();
    }
    ToolError::execution_failed(format!("{context}: {error}"))
}

pub(crate) fn validate_http_url(input: &str) -> Result<(), ToolError> {
    let parsed = Url::parse(input)
        .map_err(|error| ToolError::invalid_arguments(format!("invalid url `{input}`: {error}")))?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        other => Err(ToolError::invalid_arguments(format!(
            "unsupported url scheme `{other}` (expected http or https)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{TurnNetworkMode, TurnNetworkPolicySnapshot};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn exact_loopback_snapshot() -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp", 0);
        let mut network = TurnNetworkPolicySnapshot::disabled();
        network.mode = TurnNetworkMode::Restricted;
        network.allowed_domains = vec!["=127.0.0.1".to_owned()];
        snapshot.network = network.clone();
        snapshot.sandbox.network = network;
        snapshot
    }

    fn exact_localhost_snapshot(allow_localhost: bool) -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp", 0);
        let mut network = TurnNetworkPolicySnapshot::disabled();
        network.mode = TurnNetworkMode::Restricted;
        network.allowed_domains = vec!["=localhost".to_owned()];
        network.allow_localhost = allow_localhost;
        snapshot.network = network.clone();
        snapshot.sandbox.network = network;
        snapshot
    }

    fn exact_origin_snapshot(origin: &str) -> TurnExecutionSecuritySnapshot {
        let mut snapshot = TurnExecutionSecuritySnapshot::unrestricted_full_access("/tmp", 0);
        let mut network = TurnNetworkPolicySnapshot::disabled();
        network.mode = TurnNetworkMode::Restricted;
        network.allowed_domains = vec![format!("={origin}")];
        snapshot.network = network.clone();
        snapshot.sandbox.network = network;
        snapshot
    }

    #[tokio::test]
    async fn buffered_http_rejects_redirect_outside_exact_host_grant_before_following_it() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("redirect test listener");
        let address = listener.local_addr().expect("redirect test address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("initial request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.expect("read request");
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: http://localhost:9/blocked\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .expect("write redirect");
        });

        let snapshot = exact_loopback_snapshot();
        let error = execute_buffered_http_request(
            BufferedHttpRequest {
                method: "GET".to_owned(),
                url: format!("http://127.0.0.1:{}/start", address.port()),
                timeout_ms: 2_000,
                follow_redirects: true,
                user_agent: None,
                headers: HashMap::new(),
                query: None,
                body: None,
                max_bytes: 1024,
            },
            Some(&snapshot),
            "web_fetch",
        )
        .await
        .expect_err("redirect to an ungranted host must fail closed");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("redirect")),
            "redirect policy denial must remain a sandbox rejection"
        );
        server.await.expect("redirect server task");
    }

    #[tokio::test]
    async fn buffered_http_rejects_same_host_redirect_to_unapproved_port() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("redirect test listener");
        let address = listener.local_addr().expect("redirect test address");
        let blocked_port = if address.port() == u16::MAX {
            address.port() - 1
        } else {
            address.port() + 1
        };
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("initial request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.expect("read request");
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{blocked_port}/blocked\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write redirect");
        });

        let origin = format!("http://127.0.0.1:{}", address.port());
        let snapshot = exact_origin_snapshot(origin.as_str());
        let error = execute_buffered_http_request(
            BufferedHttpRequest {
                method: "GET".to_owned(),
                url: format!("{origin}/start"),
                timeout_ms: 2_000,
                follow_redirects: true,
                user_agent: None,
                headers: HashMap::new(),
                query: None,
                body: None,
                max_bytes: 1024,
            },
            Some(&snapshot),
            "web_fetch",
        )
        .await
        .expect_err("redirect to an unapproved port must fail closed");

        assert!(
            matches!(error, ToolError::Rejected(message) if message.contains("redirect")),
            "same-host port denial must remain a sandbox rejection"
        );
        server.await.expect("redirect server task");
    }

    #[test]
    fn restricted_dns_policy_rejects_private_and_reserved_addresses() {
        let snapshot = exact_localhost_snapshot(false);
        for address in [
            "0.1.2.3",
            "127.0.0.1",
            "10.0.0.1",
            "169.254.1.1",
            "100.64.0.1",
            "192.0.2.1",
            "192.88.99.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "::1",
            "100::1",
            "100:0:0:1::1",
            "2001:2::1",
            "2001:db8::1",
            "2002::1",
            "3fff::1",
            "5f00::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::ffff:93.184.216.34",
            "64:ff9b::10.0.0.1",
        ] {
            let address = address.parse().expect("test IP");
            assert!(!resolved_address_allowed(
                &snapshot,
                "public.example",
                address
            ));
        }
        assert!(resolved_address_allowed(
            &snapshot,
            "public.example",
            "93.184.216.34".parse().expect("public IP")
        ));
        for address in [
            "192.0.0.9",
            "192.0.0.10",
            "2001:1::1",
            "2001:1::2",
            "2001:1::3",
            "2001:3::1",
            "2001:4:112::1",
            "2001:20::1",
            "2001:30::1",
            "2606:2800:220:1:248:1893:25c8:1946",
            "64:ff9b::93.184.216.34",
        ] {
            let address = address.parse().expect("globally reachable test IP");
            assert!(
                resolved_address_allowed(&snapshot, "public.example", address),
                "globally reachable address {address} must remain available"
            );
        }
    }

    #[tokio::test]
    async fn restricted_http_rejects_dns_name_resolving_to_loopback_without_localhost_capability() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("DNS policy listener");
        let address = listener.local_addr().expect("listener address");
        let snapshot = exact_localhost_snapshot(false);

        let error = execute_buffered_http_request(
            BufferedHttpRequest {
                method: "GET".to_owned(),
                url: format!("http://localhost:{}/blocked", address.port()),
                timeout_ms: 2_000,
                follow_redirects: false,
                user_agent: None,
                headers: HashMap::new(),
                query: None,
                body: None,
                max_bytes: 1024,
            },
            Some(&snapshot),
            "web_fetch",
        )
        .await
        .expect_err("DNS-to-loopback must require localhost capability");

        assert!(matches!(error, ToolError::Rejected(message) if message.contains("DNS")));
    }

    #[tokio::test]
    async fn restricted_http_client_ignores_ambient_proxy() {
        let proxy = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ambient proxy test listener");
        let proxy_address = proxy.local_addr().expect("ambient proxy address");
        let proxy_was_used = Arc::new(AtomicBool::new(false));
        let accepted = proxy_was_used.clone();
        let proxy_task = tokio::spawn(async move {
            let Ok((mut stream, _)) = proxy.accept().await else {
                return;
            };
            accepted.store(true, Ordering::SeqCst);
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\nConnection: close\r\n\r\nproxy-bypass",
                )
                .await;
        });

        let status = tokio::process::Command::new(
            std::env::current_exe().expect("current tools test executable"),
        )
        .args([
            "--exact",
            "handlers::http::tests::restricted_http_ambient_proxy_child",
            "--nocapture",
        ])
        .env("PIONEER_RESTRICTED_HTTP_PROXY_CHILD", "1")
        .env(
            "HTTP_PROXY",
            format!("http://127.0.0.1:{}", proxy_address.port()),
        )
        .env(
            "HTTPS_PROXY",
            format!("http://127.0.0.1:{}", proxy_address.port()),
        )
        .env(
            "ALL_PROXY",
            format!("http://127.0.0.1:{}", proxy_address.port()),
        )
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .status()
        .await
        .expect("isolated ambient-proxy child test");

        tokio::task::yield_now().await;
        proxy_task.abort();
        let _ = proxy_task.await;
        assert!(status.success(), "ambient-proxy child test failed");
        assert!(
            !proxy_was_used.load(Ordering::SeqCst),
            "restricted HTTP client connected through ambient proxy authority"
        );
    }

    #[tokio::test]
    async fn restricted_http_ambient_proxy_child() {
        if std::env::var_os("PIONEER_RESTRICTED_HTTP_PROXY_CHILD").is_none() {
            return;
        }
        let snapshot = exact_origin_snapshot("http://pioneer.invalid");
        let result = execute_buffered_http_request(
            BufferedHttpRequest {
                method: "GET".to_owned(),
                url: "http://pioneer.invalid/restricted".to_owned(),
                timeout_ms: 2_000,
                follow_redirects: false,
                user_agent: None,
                headers: HashMap::new(),
                query: None,
                body: None,
                max_bytes: 1024,
            },
            Some(&snapshot),
            "web_fetch",
        )
        .await;
        assert!(
            result.is_err(),
            "restricted HTTP unexpectedly succeeded through an ambient proxy"
        );
    }
}
