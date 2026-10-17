//! Preserve the SDK's HTTP response/retry contract and discard unsafe errors.
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use std::time::Duration;

pub(super) struct OtlpHttpClient {
    client: reqwest::blocking::Client,
    signal: &'static str,
}
impl std::fmt::Debug for OtlpHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtlpHttpClient")
            .field("signal", &self.signal)
            .finish()
    }
}
#[derive(Debug)]
struct SafeHttpFailure {
    phase: &'static str,
    cause: &'static str,
}
impl std::fmt::Display for SafeHttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OTLP HTTP {} failed ({})", self.phase, self.cause)
    }
}
impl std::error::Error for SafeHttpFailure {}

impl OtlpHttpClient {
    pub(super) fn new(timeout: Duration, signal: &'static str) -> anyhow::Result<Self> {
        // Same blocking client/runtime as the pinned SDK. Build off the async
        // runtime, without the SDK's silent fallback to an unbounded default.
        let client = std::thread::spawn(move || {
            reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()
        })
        .join()
        .map_err(|_| anyhow::anyhow!("OTLP HTTP client builder failed"))?
        .map_err(|_| anyhow::anyhow!("OTLP HTTP client initialization failed"))?;
        Ok(Self { client, signal })
    }

    fn failure(&self, phase: &'static str, error: &reqwest::Error) -> HttpError {
        let cause = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect"
        } else if error.is_body() {
            "body"
        } else if error.is_decode() {
            "decode"
        } else if error.is_redirect() {
            "redirect"
        } else {
            "other"
        };
        tracing::debug!(target: "pioneer_observability::otlp", signal = self.signal,
            phase, cause, "OTLP HTTP attempt failed");
        // No reqwest source: it can own the endpoint URL and credentials.
        Box::new(SafeHttpFailure { phase, cause })
    }

    fn send_once(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let request = request.try_into().map_err(|_| {
            Box::new(SafeHttpFailure {
                phase: "request",
                cause: "invalid_request",
            }) as HttpError
        })?;
        let mut response = self
            .client
            .execute(request)
            .map_err(|error| self.failure("send", &error))?;
        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        // Non-success responses belong to the SDK retry classifier. Their
        // untrusted body is unnecessary and must not appear in SDK debug logs.
        let body = if status.is_success() {
            response
                .bytes()
                .map_err(|error| self.failure("response_body", &error))?
        } else {
            tracing::debug!(target: "pioneer_observability::otlp", signal = self.signal,
                phase = "http_status", http_status = status.as_u16(), "OTLP HTTP attempt rejected");
            Bytes::new()
        };
        let mut result = Response::new(body);
        *result.status_mut() = status;
        *result.headers_mut() = headers;
        Ok(result)
    }
}
#[async_trait::async_trait]
impl HttpClient for OtlpHttpClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        self.send_once(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn rejected_status_and_retry_after_reach_sdk_without_raw_body() {
        for status in [403, 429, 503] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0; 4096];
                stream.read(&mut request).unwrap();
                write!(stream, "HTTP/1.1 {status} Rejected\r\nRetry-After: 2\r\nContent-Length: 14\r\nConnection: close\r\n\r\nprivate-canary").unwrap();
            });
            let client = OtlpHttpClient::new(Duration::from_secs(2), "traces").unwrap();
            let response = client
                .send_once(
                    Request::builder()
                        .uri(format!("http://{address}/"))
                        .body(Bytes::new())
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(response.headers()["retry-after"], "2");
            assert!(response.body().is_empty());
            server.join().unwrap();
        }
    }

    #[test]
    fn truncated_success_body_keeps_stage_and_discards_secret_url() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 4096];
            stream.read(&mut request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort",
                )
                .unwrap();
        });
        let client = OtlpHttpClient::new(Duration::from_secs(2), "traces").unwrap();
        let error = client
            .send_once(
                Request::builder()
                    .uri(format!("http://{address}/?key=private-canary"))
                    .body(Bytes::new())
                    .unwrap(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("response_body"));
        assert!(!format!("{error:?} {error}").contains("private-canary"));
        assert!(error.source().is_none());
        server.join().unwrap();
    }
}
