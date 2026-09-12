//! HTTP fixtures exercise the production stream decoder; no external service.
use super::{AnthropicProvider, OpenAiProvider};
use crate::{ChatMessage, ChatRequest, Provider, TokenUsage};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn fixture(body: String) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let (offset, size) = loop {
            let mut bytes = [0; 4096];
            let count = stream.read(&mut bytes).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&bytes[..count]);
            assert!(request.len() < 65536);
            if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
                let size: usize = headers
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                break (end + 4, size);
            }
        };
        while request.len() < offset + size {
            let mut bytes = [0; 4096];
            let count = stream.read(&mut bytes).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&bytes[..count]);
        }
        let parsed = serde_json::from_slice(&request[offset..offset + size]).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        // Deliberately split frame and UTF-8 boundaries.
        for chunk in response.as_bytes().chunks(7) {
            stream.write_all(chunk).await.unwrap();
        }
        parsed
    });
    (format!("http://{address}"), server)
}
fn request() -> ChatRequest {
    ChatRequest {
        model: "fixture".into(),
        messages: vec![ChatMessage::user("test")],
        temperature: None,
        max_tokens: Some(100),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}

#[tokio::test]
async fn openai_usage_after_finish_is_read_before_terminal_without_cache_double_count() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Привет\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":140,\"completion_tokens\":9,\"prompt_tokens_details\":{\"cached_tokens\":100}}}\n\n",
        "data: [DONE]\n\n").to_owned();
    let (url, server) = fixture(body).await;
    let provider = OpenAiProvider::with_base_url("fixture-key", url);
    let mut stream = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.stream_chat(request()),
    )
    .await
    .unwrap();
    let mut usage = TokenUsage::default();
    let mut text = String::new();
    let mut terminal = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert!(!terminal);
        text.push_str(&chunk.delta);
        if let Some(snapshot) = chunk.usage {
            usage.update(&snapshot);
        }
        if chunk.is_final {
            assert_eq!(usage.input_tokens, Some(140));
            assert_eq!(usage.output_tokens, Some(9));
            terminal = true;
        }
    }
    assert!(terminal);
    assert_eq!(text, "Привет");
    assert_eq!(
        server.await.unwrap()["stream_options"]["include_usage"],
        true
    );
}

#[tokio::test]
async fn anthropic_stream_merges_start_cache_input_and_cumulative_output() {
    let body = concat!(
        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_creation_input_tokens\":20,\"cache_read_input_tokens\":100,\"output_tokens\":1}}}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":null},\"usage\":{\"output_tokens\":5}}\n\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":8}}\n\n",
        "data: {\"type\":\"message_stop\"}\n\n").to_owned();
    let (url, server) = fixture(body).await;
    let provider = AnthropicProvider::with_base_url("fixture-key", url);
    let mut stream = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.stream_chat(request()),
    )
    .await
    .unwrap();
    let mut usage = TokenUsage::default();
    let mut terminal = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        if let Some(snapshot) = chunk.usage {
            usage.update(&snapshot);
        }
        terminal |= chunk.is_final;
    }
    assert!(terminal);
    assert_eq!(usage.input_tokens, Some(130));
    assert_eq!(usage.output_tokens, Some(8));
    server.await.unwrap();
}

#[tokio::test]
async fn missing_usage_stays_unknown_and_missing_finish_is_error() {
    for (body, succeeds) in [
        (
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
            true,
        ),
        ("data: [DONE]\n\n", false),
    ] {
        let (url, server) = fixture(body.to_owned()).await;
        let provider = OpenAiProvider::with_base_url("fixture-key", url);
        let chunks: Vec<_> = crate::attachments::runtime::with_async_authority_scope(
            "usage-fixture-authority".into(),
            provider.stream_chat(request()),
        )
        .await
        .unwrap()
        .collect()
        .await;
        assert_eq!(chunks.iter().all(Result::is_ok), succeeds);
        for chunk in chunks.into_iter().flatten() {
            assert!(chunk.usage.is_none());
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn dropping_stream_closes_pending_http_transport() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut received = Vec::new();
        loop {
            let mut bytes = [0; 4096];
            let n = socket.read(&mut bytes).await.unwrap();
            assert!(n > 0);
            received.extend_from_slice(&bytes[..n]);
            if let Some(end) = received.windows(4).position(|v| v == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&received[..end]).to_lowercase();
                let size: usize = headers
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if received.len() >= end + 4 + size {
                    break;
                }
            }
        }
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 100000\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"start\"}}]}\n\n").await.unwrap();
        let mut bytes = [0; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(2), socket.read(&mut bytes)).await;
        assert!(
            matches!(closed, Ok(Ok(0)) | Ok(Err(_))),
            "HTTP producer survived receiver cancellation"
        );
    });
    let provider = OpenAiProvider::with_base_url("fixture-key", format!("http://{address}"));
    let mut stream = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.stream_chat(request()),
    )
    .await
    .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().delta, "start");
    drop(stream);
    server.await.unwrap();
}
