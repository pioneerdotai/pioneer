//! HTTP fixtures exercise the production stream decoder; no external service.
use super::{AnthropicProvider, OpenAiProvider, OpenRouterProvider};
use crate::{ChatMessage, ChatRequest, Provider, TokenUsage};
use futures_util::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn fixture(body: String) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
    fixture_with_headers(body, String::new(), "200 OK").await
}
async fn fixture_with_headers(
    body: String,
    headers: String,
    status: &'static str,
) -> (String, tokio::task::JoinHandle<serde_json::Value>) {
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
            "HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
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
fn request(model: &str) -> ChatRequest {
    ChatRequest {
        model: model.into(),
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
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":140,\"completion_tokens\":9,\"prompt_tokens_details\":{\"cached_tokens\":100}}}\n\n",
        "data: [DONE]\n\n").to_owned();
    let (url, server) = fixture(body).await;
    let provider = OpenAiProvider::with_base_url("fixture-key", url);
    let mut stream = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.stream_chat(request("gpt-4o")),
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
async fn openrouter_ignores_repeated_empty_terminal_choice_and_preserves_usage() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"memory\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\n",
        "data: [DONE]\n\n"
    )
    .to_owned();
    let (url, server) = fixture(body).await;
    let provider = OpenRouterProvider::with_base_url("fixture-key", url);
    let chunks: Vec<_> = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.stream_chat(request("openai/gpt-4o")),
    )
    .await
    .unwrap()
    .collect()
    .await;

    assert!(chunks.iter().all(Result::is_ok));
    let chunks = chunks.into_iter().flatten().collect::<Vec<_>>();
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.delta.as_str())
            .collect::<String>(),
        "memory"
    );
    let usage = chunks
        .iter()
        .find_map(|chunk| chunk.usage.as_ref())
        .expect("usage frame should be preserved");
    assert_eq!(usage.input_tokens, Some(12));
    assert_eq!(usage.output_tokens, Some(3));
    assert!(chunks.last().is_some_and(|chunk| chunk.is_final));
    server.await.unwrap();
}

#[tokio::test]
async fn openai_rejects_real_payload_after_finish_reason() {
    let body = concat!(
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"late\"},\"finish_reason\":null}]}\n\n",
        "data: [DONE]\n\n"
    )
    .to_owned();
    let (url, server) = fixture(body).await;
    let provider = OpenAiProvider::with_base_url("fixture-key", url);
    let chunks: Vec<_> = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.stream_chat(request("gpt-4o")),
    )
    .await
    .unwrap()
    .collect()
    .await;

    assert!(chunks.iter().any(|chunk| {
        chunk.as_ref().is_err_and(|error| {
            error
                .to_string()
                .contains("provider sent payload after finish_reason")
        })
    }));
    server.await.unwrap();
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
        provider.stream_chat(request("claude-sonnet-4-5")),
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
            provider.stream_chat(request("gpt-4o")),
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
        provider.stream_chat(request("gpt-4o")),
    )
    .await
    .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().delta, "start");
    drop(stream);
    server.await.unwrap();
}

#[tokio::test]
async fn compatible_stream_usage_opt_in_is_profile_specific_and_terminal_is_cumulative() {
    use super::compatible::{AuthStyle, OpenAiCompatibleProvider};
    for (profile, opt_in) in [
        ("groq", true),
        ("deepseek", true),
        ("fireworks", true),
        ("custom", false),
        ("mistral", false),
    ] {
        let body=concat!(
            "data: {\"id\":\"chat-native-1\",\"model\":\"reported-model\",\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":140,\"completion_tokens\":19,\"prompt_tokens_details\":{\"cached_tokens\":100,\"cache_write_tokens\":20},\"completion_tokens_details\":{\"reasoning_tokens\":10}}}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":140,\"completion_tokens\":19}}\n\n",
            "data: [DONE]\n\n").to_owned();
        let (url, server) = fixture(body).await;
        let provider =
            OpenAiCompatibleProvider::new(profile, url, "fixture-key", AuthStyle::Bearer);
        let mut stream = crate::attachments::runtime::with_async_authority_scope(
            "usage-fixture-authority".into(),
            provider.stream_chat(request("fixture")),
        )
        .await
        .unwrap();
        let mut usage = TokenUsage::default();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap();
            if let Some(snapshot) = chunk.usage {
                usage.update(&snapshot);
            }
        }
        assert_eq!(usage.input_tokens, Some(140));
        assert_eq!(usage.output_tokens, Some(19));
        assert_eq!(usage.cache_read_input_tokens, Some(100));
        assert_eq!(usage.cache_write_input_tokens, Some(20));
        assert_eq!(usage.reasoning_tokens, Some(10));
        assert_eq!(usage.generation_id.as_deref(), Some("chat-native-1"));
        assert_eq!(usage.reported_model.as_deref(), Some("reported-model"));
        let request = server.await.unwrap();
        assert_eq!(request.get("stream_options").is_some(), opt_in, "{profile}");
    }
}

#[tokio::test]
async fn groq_request_id_is_distinct_and_profile_specific() {
    use super::compatible::{AuthStyle, OpenAiCompatibleProvider};
    for profile in ["groq", "custom"] {
        let body = r#"{"id":"chatcmpl-completion","model":"actual","x_groq":{"id":"req-native","secret":"SECRET"},"choices":[{"message":{"content":"answer"},"finish_reason":"stop"}]}"#;
        let (url, server) = fixture(body.into()).await;
        let provider = OpenAiCompatibleProvider::new(profile, url, "fixture", AuthStyle::Bearer);
        let response = crate::attachments::runtime::with_async_authority_scope(
            "usage-fixture-authority".into(),
            provider.chat(request("fixture")),
        )
        .await
        .unwrap();
        let usage = response.usage.unwrap();
        assert_eq!(usage.generation_id.as_deref(), Some("chatcmpl-completion"));
        assert_eq!(
            usage.request_id.as_deref(),
            if profile == "groq" {
                Some("req-native")
            } else {
                None
            }
        );
        assert_eq!(usage.input_tokens, None);
        assert!(!serde_json::to_string(&usage).unwrap().contains("SECRET"));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn openrouter_header_is_preserved_on_chat_rejection_and_stream_before_body() {
    for (body, status) in [
        ("", "200 OK"),
        (r#"{"error":{"message":"SECRET"}}"#, "200 OK"),
        (
            r#"{"error":{"message":"SECRET"}}"#,
            "503 Service Unavailable",
        ),
    ] {
        let (url, server) = fixture_with_headers(
            body.into(),
            "X-Generation-Id: gen-header\r\n".into(),
            status,
        )
        .await;
        let provider = OpenRouterProvider::with_base_url("fixture", url);
        let result = crate::attachments::runtime::with_async_authority_scope(
            "usage-fixture-authority".into(),
            provider.stream_chat(request("openai/gpt-4o")),
        )
        .await;
        let mut usage = TokenUsage::default();
        match result {
            Err(error) => usage
                .update(crate::usage::error_usage(&error).expect("HTTP failure retains header")),
            Ok(mut stream) => {
                let first = stream.next().await.unwrap().unwrap();
                usage.update(first.usage.as_ref().unwrap());
                assert!(stream.next().await.unwrap().is_err());
            }
        }
        assert_eq!(usage.generation_id.as_deref(), Some("gen-header"));
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, None);
        assert!(!serde_json::to_string(&usage).unwrap().contains("SECRET"));
        server.await.unwrap();
    }
    let (url, server) = fixture_with_headers(
        "invalid JSON".into(),
        "X-Generation-Id: gen-chat\r\n".into(),
        "200 OK",
    )
    .await;
    let provider = OpenRouterProvider::with_base_url("fixture", url);
    let error = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.chat(request("openai/gpt-4o")),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        crate::usage::error_usage(&error)
            .unwrap()
            .generation_id
            .as_deref(),
        Some("gen-chat")
    );
    server.await.unwrap();
    let body = r#"{"id":"gen-body-conflict","model":"actual-model","choices":[{"message":{"content":"answer"},"finish_reason":"stop"}]}"#;
    let (url, server) = fixture_with_headers(
        body.into(),
        "X-Generation-Id: gen-header\r\n".into(),
        "200 OK",
    )
    .await;
    let provider = OpenRouterProvider::with_base_url("fixture", url);
    let response = crate::attachments::runtime::with_async_authority_scope(
        "usage-fixture-authority".into(),
        provider.chat(request("openai/gpt-4o")),
    )
    .await
    .unwrap();
    let usage = response.usage.unwrap();
    assert_eq!(usage.generation_id.as_deref(), Some("gen-header"));
    assert_eq!(usage.reported_model.as_deref(), Some("actual-model"));
    assert_eq!(usage.input_tokens, None);
    server.await.unwrap();
}
#[tokio::test]
async fn openrouter_matching_and_conflicting_body_use_header_policy_without_double_counts() {
    for body_id in ["gen-header", "gen-conflict"] {
        let body = format!(
            "data: {{\"id\":\"{body_id}\",\"choices\":[],\"usage\":{{\"prompt_tokens\":10,\"completion_tokens\":2}}}}\n\ndata: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\ndata: [DONE]\n\n"
        );
        let (url, server) =
            fixture_with_headers(body, "X-Generation-Id: gen-header\r\n".into(), "200 OK").await;
        let provider = OpenRouterProvider::with_base_url("fixture", url);
        let mut stream = crate::attachments::runtime::with_async_authority_scope(
            "usage-fixture-authority".into(),
            provider.stream_chat(request("openai/gpt-4o")),
        )
        .await
        .unwrap();
        let mut usage = TokenUsage::default();
        while let Some(chunk) = stream.next().await {
            if let Some(snapshot) = chunk.unwrap().usage {
                usage.update(&snapshot);
                usage.update(&snapshot);
            }
        }
        assert_eq!(usage.generation_id.as_deref(), Some("gen-header"));
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.output_tokens, Some(2));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn anthropic_returned_model_survives_nonstream_and_message_start_without_usage() {
    for streaming in [false, true] {
        let body = if streaming {
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-native\",\"model\":\"claude-returned\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n"
        } else {
            r#"{"id":"msg-native","model":"claude-returned","content":[{"type":"text","text":"answer"}],"stop_reason":"end_turn"}"#
        };
        let (url, server) = fixture(body.into()).await;
        let provider = AnthropicProvider::with_base_url("fixture", url);
        let mut usage = TokenUsage::default();
        if streaming {
            let mut stream = crate::attachments::runtime::with_async_authority_scope(
                "usage-fixture-authority".into(),
                provider.stream_chat(request("claude-sonnet-4-5")),
            )
            .await
            .unwrap();
            while let Some(chunk) = stream.next().await {
                if let Some(snapshot) = chunk.unwrap().usage {
                    usage.update(&snapshot);
                }
            }
        } else {
            let response = crate::attachments::runtime::with_async_authority_scope(
                "usage-fixture-authority".into(),
                provider.chat(request("claude-sonnet-4-5")),
            )
            .await
            .unwrap();
            usage.update(response.usage.as_ref().unwrap());
        }
        assert_eq!(usage.reported_model.as_deref(), Some("claude-returned"));
        assert_eq!(usage.generation_id.as_deref(), Some("msg-native"));
        assert_eq!(usage.input_tokens, None);
        server.await.unwrap();
    }
}
