use crate::types::ProviderTimeoutPolicy;
use crate::types::{
    ProviderHttpErrorBodyTooLarge, ProviderResponseLimits, ProviderResponseTooLarge,
};
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use reqwest::{Client, Proxy, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use std::cell::RefCell;

pub(crate) fn build_client(policy: ProviderTimeoutPolicy) -> Client {
    let proxy_url = active_provider_proxy_url();
    build_client_with_proxy(policy, proxy_url.as_deref()).expect("failed to build HTTP client")
}

pub fn validate_proxy_url(proxy_url: &str) -> Result<String> {
    let proxy_url = proxy_url.trim();
    if proxy_url.is_empty() {
        return Err(anyhow!("provider proxy URL must not be empty"));
    }
    // Proxy URLs may contain credentials.  Do not attach the raw value (or
    // reqwest's parser error, which can echo it) to the returned diagnostic.
    Proxy::all(proxy_url).map_err(|_| anyhow!("invalid provider proxy URL"))?;
    Ok(proxy_url.to_owned())
}

pub(crate) fn with_provider_proxy<T>(proxy_url: Option<&str>, f: impl FnOnce() -> T) -> T {
    ACTIVE_PROVIDER_PROXY_URL.with(|active| {
        let previous = active.replace(proxy_url.map(str::to_owned));
        let result = f();
        active.replace(previous);
        result
    })
}

fn build_client_with_proxy(
    policy: ProviderTimeoutPolicy,
    proxy_url: Option<&str>,
) -> Result<Client> {
    let mut builder = Client::builder().connect_timeout(policy.connect_timeout);
    if let Some(proxy_url) = proxy_url {
        builder = builder
            .proxy(Proxy::all(proxy_url).map_err(|_| anyhow!("invalid provider proxy URL"))?);
    }
    builder.build().context("failed to build HTTP client")
}

fn active_provider_proxy_url() -> Option<String> {
    ACTIVE_PROVIDER_PROXY_URL.with(|active| active.borrow().clone())
}

thread_local! {
    static ACTIVE_PROVIDER_PROXY_URL: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub(crate) fn non_stream_request(
    builder: RequestBuilder,
    policy: ProviderTimeoutPolicy,
) -> RequestBuilder {
    builder.timeout(policy.non_stream_request_timeout)
}

pub(crate) fn stream_request(
    builder: RequestBuilder,
    policy: ProviderTimeoutPolicy,
) -> RequestBuilder {
    if let Some(timeout) = policy.max_stream_duration {
        builder.timeout(timeout)
    } else {
        builder
    }
}

pub(crate) async fn send_stream_request(
    builder: RequestBuilder,
    policy: ProviderTimeoutPolicy,
) -> Result<Response> {
    let send = stream_request(builder, policy).send();
    tokio::time::timeout(policy.first_chunk_timeout, send)
        .await
        .map_err(|_| {
            anyhow!(
                "provider stream timed out before response headers after {:?}",
                policy.first_chunk_timeout
            )
        })?
        .map_err(Into::into)
}

pub(crate) async fn read_response_bytes_bounded(
    response: Response,
    limit: usize,
    component: &'static str,
) -> Result<Vec<u8>> {
    let status = response.status();
    let content_length = response.content_length();
    if content_length.is_some_and(|length| length > limit as u64) {
        let limit_error = ProviderResponseTooLarge::new(
            component,
            limit,
            content_length.unwrap_or(u64::MAX).min(usize::MAX as u64) as usize,
        );
        return if status.is_success() {
            Err(limit_error.into())
        } else {
            Err(ProviderHttpErrorBodyTooLarge {
                status: status.as_u16(),
                limit: limit_error,
            }
            .into())
        };
    }

    let capacity = content_length
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(limit);
    let mut body = Vec::with_capacity(capacity);
    let mut byte_stream = response.bytes_stream();
    while let Some(chunk) = byte_stream.next().await {
        let chunk = chunk?;
        let observed = body.len().saturating_add(chunk.len());
        if observed > limit {
            let limit_error = ProviderResponseTooLarge::new(component, limit, observed);
            return if status.is_success() {
                Err(limit_error.into())
            } else {
                Err(ProviderHttpErrorBodyTooLarge {
                    status: status.as_u16(),
                    limit: limit_error,
                }
                .into())
            };
        }
        body.extend_from_slice(chunk.as_ref());
    }
    Ok(body)
}

pub(crate) async fn read_response_text_bounded(
    response: Response,
    limit: usize,
    component: &'static str,
) -> Result<String> {
    let body = read_response_bytes_bounded(response, limit, component).await?;
    String::from_utf8(body).context("provider response body was not valid UTF-8")
}

pub(crate) async fn read_response_json_bounded<T: DeserializeOwned>(
    response: Response,
    limits: ProviderResponseLimits,
    component: &'static str,
) -> Result<T> {
    let body = read_response_bytes_bounded(response, limits.max_transport_bytes, component).await?;
    serde_json::from_slice(body.as_slice()).context("provider response body was not valid JSON")
}

/// Wrap a provider stream with the same hard byte limit used for non-stream
/// responses.  The wrapper checks `Content-Length` before the first read and
/// emits one typed error when a chunked/decompressed stream crosses the limit.
pub(crate) fn bounded_response_stream(
    response: Response,
    limit: usize,
    component: &'static str,
) -> BoxStream<'static, Result<Bytes>> {
    let content_length = response.content_length();
    let byte_stream = response.bytes_stream();
    Box::pin(stream::unfold(
        Some((byte_stream, 0usize)),
        move |state| async move {
            let Some((mut byte_stream, total)) = state else {
                return None;
            };

            if let Some(length) = content_length
                && length > limit as u64
            {
                return Some((
                    Err(ProviderResponseTooLarge::new(
                        component,
                        limit,
                        length.min(usize::MAX as u64) as usize,
                    )
                    .into()),
                    None,
                ));
            }

            match byte_stream.next().await {
                Some(Ok(chunk)) => {
                    let observed = total.saturating_add(chunk.len());
                    if observed > limit {
                        Some((
                            Err(ProviderResponseTooLarge::new(component, limit, observed).into()),
                            None,
                        ))
                    } else {
                        Some((Ok(chunk), Some((byte_stream, observed))))
                    }
                }
                Some(Err(error)) => Some((Err(error.into()), None)),
                None => None,
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn response_server(response: &'static [u8]) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("provider response listener");
        let address = listener.local_addr().expect("provider response address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("provider response request");
            let mut request = [0_u8; 2048];
            let _ = stream
                .read(&mut request)
                .await
                .expect("read provider request");
            stream
                .write_all(response)
                .await
                .expect("write provider response");
        });
        (format!("http://{address}/response"), server)
    }

    #[test]
    fn validate_proxy_url_accepts_http_and_socks5_urls() {
        assert_eq!(
            validate_proxy_url(" http://user:pass@127.0.0.1:8080 ").expect("http proxy"),
            "http://user:pass@127.0.0.1:8080"
        );
        assert_eq!(
            validate_proxy_url("socks5://127.0.0.1:1080").expect("socks proxy"),
            "socks5://127.0.0.1:1080"
        );
    }

    #[test]
    fn validate_proxy_url_rejects_empty_urls() {
        let error = validate_proxy_url("   ").expect_err("empty proxy should fail");
        assert!(
            format!("{error:#}").contains("proxy URL must not be empty"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn invalid_proxy_url_diagnostics_redact_embedded_credentials() {
        let secret = "proxy-password-should-not-leak";
        let raw = format!("http://proxy-user:{secret}@[");
        let error = validate_proxy_url(raw.as_str()).expect_err("malformed proxy should fail");
        let rendered = format!("{error:#}");
        assert_eq!(rendered, "invalid provider proxy URL");
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("proxy-user"));
    }

    #[test]
    fn client_builder_diagnostics_redact_embedded_credentials() {
        let secret = "builder-password-should-not-leak";
        let raw = format!("http://builder-user:{secret}@[");
        let error = build_client_with_proxy(ProviderTimeoutPolicy::default(), Some(raw.as_str()))
            .expect_err("malformed proxy should fail client construction");
        let rendered = format!("{error:#}");
        assert_eq!(rendered, "invalid provider proxy URL");
        assert!(!rendered.contains(secret));
        assert!(!rendered.contains("builder-user"));
    }

    #[tokio::test]
    async fn bounded_json_reader_rejects_content_length_before_json_decode() {
        let (url, server) = response_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 32\r\nConnection: close\r\n\r\nnot-json-not-json-not-json-not-json",
        )
        .await;
        let client = Client::builder()
            .no_proxy()
            .build()
            .expect("test HTTP client");
        let response = client.get(url).send().await.expect("provider response");
        let mut limits = ProviderResponseLimits::default();
        limits.max_transport_bytes = 8;

        let error = read_response_json_bounded::<serde_json::Value>(response, limits, "json_body")
            .await
            .expect_err("oversized JSON transport must be rejected");
        let oversized = error
            .downcast_ref::<ProviderResponseTooLarge>()
            .expect("typed response size error");
        assert_eq!(oversized.component, "json_body");
        assert_eq!(oversized.limit, 8);
        server.await.expect("provider response server");
    }

    #[tokio::test]
    async fn bounded_stream_rejects_chunked_sse_after_cumulative_limit() {
        let (url, server) = response_server(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n3\r\ndef\r\n0\r\n\r\n",
        )
        .await;
        let client = Client::builder()
            .no_proxy()
            .build()
            .expect("test HTTP client");
        let response = client.get(url).send().await.expect("provider response");
        let mut stream = bounded_response_stream(response, 5, "sse_body");
        assert_eq!(
            stream
                .next()
                .await
                .expect("first stream item")
                .expect("first chunk"),
            bytes::Bytes::from_static(b"abc")
        );
        let error = stream
            .next()
            .await
            .expect("size error item")
            .expect_err("chunked stream must stop at its cumulative limit");
        let oversized = error
            .downcast_ref::<ProviderResponseTooLarge>()
            .expect("typed stream size error");
        assert_eq!(oversized.component, "sse_body");
        assert_eq!(oversized.limit, 5);
        server.await.expect("provider response server");
    }

    #[tokio::test]
    async fn bounded_reader_accepts_exact_limit_and_rejects_limit_plus_one() {
        for (body, should_succeed) in [
            (b"12345678".as_slice(), true),
            (b"123456789".as_slice(), false),
        ] {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                String::from_utf8_lossy(body)
            );
            let leaked: &'static [u8] = Box::leak(response.into_bytes().into_boxed_slice());
            let (url, server) = response_server(leaked).await;
            let response = Client::builder()
                .no_proxy()
                .build()
                .unwrap()
                .get(url)
                .send()
                .await
                .unwrap();
            assert_eq!(
                read_response_bytes_bounded(response, 8, "boundary")
                    .await
                    .is_ok(),
                should_succeed
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn oversized_http_error_retains_retry_relevant_status() {
        let (url, server) = response_server(
            b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 32\r\nConnection: close\r\n\r\nrate-limit-rate-limit-rate-limit!",
        )
        .await;
        let response = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .await
            .unwrap();
        let error = read_response_text_bounded(response, 8, "provider_error_body")
            .await
            .expect_err("oversized error body must be bounded");
        let typed = error
            .downcast_ref::<ProviderHttpErrorBodyTooLarge>()
            .expect("HTTP status must survive body truncation");
        assert_eq!(typed.status, 429);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_text_reader_rejects_invalid_utf8_without_lossy_expansion() {
        let (url, server) = response_server(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\xff\xff",
        )
        .await;
        let response = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .await
            .unwrap();
        let error = read_response_text_bounded(response, 2, "utf8")
            .await
            .expect_err("invalid UTF-8 must fail");
        assert!(format!("{error:#}").contains("not valid UTF-8"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bounded_reader_limits_decompressed_bytes() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&vec![b'x'; 64 * 1024]).unwrap();
        let compressed = encoder.finish().unwrap();
        let mut wire = format!(
            "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            compressed.len()
        )
        .into_bytes();
        wire.extend_from_slice(&compressed);
        let leaked: &'static [u8] = Box::leak(wire.into_boxed_slice());
        let (url, server) = response_server(leaked).await;
        let response = Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .get(url)
            .send()
            .await
            .unwrap();
        let error = read_response_bytes_bounded(response, 1024, "compressed_body")
            .await
            .expect_err("decompressed payload must be measured at the transport boundary");
        assert!(error.downcast_ref::<ProviderResponseTooLarge>().is_some());
        server.await.unwrap();
    }
}
