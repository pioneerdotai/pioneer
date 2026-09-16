//! Public unauthenticated catalog inputs; no model inference or provider secrets.
use super::generator::{SOURCE_URLS, SourceResponse, SourceSnapshot};
use anyhow::{Result, ensure};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
const MAX_SOURCE_BYTES: usize = 64 * 1024 * 1024;

async fn fetch_source(client: &reqwest::Client, url: &str) -> Result<SourceResponse> {
    let mut response = client.get(url).send().await?.error_for_status()?;
    ensure!(
        response.status() == reqwest::StatusCode::OK,
        "incomplete catalog response"
    );
    ensure!(
        response
            .content_length()
            .is_none_or(|v| v <= MAX_SOURCE_BYTES as u64),
        "catalog source exceeds size limit"
    );
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            bytes.len() + chunk.len() <= MAX_SOURCE_BYTES,
            "catalog source exceeds size limit"
        );
        bytes.extend_from_slice(&chunk);
    }
    let body: Value = serde_json::from_slice(&bytes)?;
    ensure!(body.is_object(), "catalog source must be an object");
    Ok(SourceResponse {
        status: 200,
        body,
        error: None,
    })
}
fn build_client(proxy_url: Option<&str>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(60));
    if let Some(proxy_url) = proxy_url {
        let proxy_url = crate::http::validate_proxy_url(proxy_url)?;
        builder = builder.proxy(
            reqwest::Proxy::all(proxy_url)
                .map_err(|_| anyhow::anyhow!("invalid model catalog proxy URL"))?,
        );
    }
    builder.build().map_err(Into::into)
}

pub(super) async fn fetch_snapshot(proxy_url: Option<&str>) -> Result<SourceSnapshot> {
    let client = build_client(proxy_url)?;
    let mut sources = BTreeMap::new();
    for url in SOURCE_URLS {
        sources.insert(url.to_owned(), fetch_source(&client, url).await?);
    }
    let snapshot = SourceSnapshot {
        captured_at: chrono::Utc::now().to_rfc3339(),
        sources,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn public_source_transport_handles_json_and_http_failure_without_credentials() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        for (status, body, success) in [
            ("200 OK", r#"{"data":[{"id":"fixture"}]}"#, true),
            ("503 Unavailable", "{}", false),
            ("200 OK", "invalid", false),
        ] {
            let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = server.local_addr().unwrap();
            let task = tokio::spawn(async move {
                let (mut socket, _) = server.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let count = socket.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..count]).to_lowercase();
                assert!(!request.contains("authorization:"));
                socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            });
            let result = fetch_source(
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &format!("http://{address}/models"),
            )
            .await;
            assert_eq!(result.is_ok(), success);
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn configured_proxy_carries_catalog_request_without_leaking_credentials_to_errors() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = proxy.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = proxy.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let count = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..count]);
            assert!(request.starts_with("GET http://catalog.invalid/models "));
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .await
                .unwrap();
        });
        let client = build_client(Some(&format!("http://{address}"))).unwrap();
        fetch_source(&client, "http://catalog.invalid/models")
            .await
            .unwrap();
        task.await.unwrap();

        let error = build_client(Some("http://secret-user:secret-password@["))
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret-user"));
        assert!(!error.contains("secret-password"));
    }
}
