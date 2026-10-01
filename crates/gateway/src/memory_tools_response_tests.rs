use super::*;
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream};
use pioneer_provider::{ChatResponse, ProviderCapabilities, ProviderToolCall};

struct CompletionProvider {
    streaming: bool,
    response: ChatResponse,
    chunks: Vec<StreamChunk>,
}

fn response(termination: ProviderTermination) -> ChatResponse {
    ChatResponse {
        text: r#"{"facts":[]}"#.to_owned(),
        termination,
        usage: None,
        reasoning_content: None,
        tool_calls: vec![],
        provider_replay_state: None,
    }
}

#[async_trait]
impl Provider for CompletionProvider {
    fn name(&self) -> &str {
        "completion-fixture"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: self.streaming,
            ..Default::default()
        }
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        assert!(!self.streaming);
        assert!(request.tools.is_none());
        Ok(self.response.clone())
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<StreamChunk>>> {
        assert!(self.streaming);
        assert!(request.tools.is_none());
        Ok(Box::pin(stream::iter(
            self.chunks.clone().into_iter().map(Ok),
        )))
    }
}

fn metadata<'a>(error: &'a HookError, key: &str) -> &'a str {
    error
        .metadata
        .get(&HookMetadataKey::new(key).unwrap())
        .unwrap()
}

#[tokio::test]
async fn extractor_completion_policy_stream_and_non_stream() {
    for (termination, class) in [
        (ProviderTermination::Complete, None),
        (ProviderTermination::ToolCalls, Some("tool_calls")),
        (ProviderTermination::Length, Some("length")),
        (
            ProviderTermination::ContentFiltered,
            Some("content_filtered"),
        ),
        (ProviderTermination::Safety, Some("safety")),
        (ProviderTermination::Cancelled, Some("cancelled")),
        (ProviderTermination::ProviderError, Some("provider_error")),
        (
            ProviderTermination::Unknown("PRIVATE_TERMINATION_CANARY".to_owned()),
            Some("unknown_termination"),
        ),
    ] {
        for streaming in [false, true] {
            let provider = CompletionProvider {
                streaming,
                response: response(termination.clone()),
                chunks: vec![
                    StreamChunk::delta(r#"{"facts":[]}"#),
                    StreamChunk::final_chunk_with(termination.clone()),
                ],
            };
            let result =
                request_post_turn_extractor_json(&provider, "selected-model", "extract".to_owned())
                    .await;
            if let Some(class) = class {
                let error = result.unwrap_err();
                assert!(!error.retryable);
                assert_eq!(metadata(&error, "failure_class"), class);
                assert_eq!(metadata(&error, "failure_stage"), "transport_completion");
                assert_eq!(metadata(&error, "model"), "selected-model");
                assert_eq!(metadata(&error, "provider"), "completion-fixture");
                assert!(!format!("{error:?}").contains("PRIVATE_TERMINATION_CANARY"));
            } else {
                assert_eq!(result.unwrap(), r#"{"facts":[]}"#);
            }
        }
    }
}

#[tokio::test]
async fn extractor_eof_even_after_valid_json_is_retryable_but_missing_final_termination_is_not() {
    for text in ["", "{", r#"{"facts":[]}"#] {
        let provider = CompletionProvider {
            streaming: true,
            response: response(ProviderTermination::Complete),
            chunks: vec![StreamChunk::delta(text)],
        };
        let error = request_post_turn_extractor_json(&provider, "model", "extract".to_owned())
            .await
            .unwrap_err();
        assert!(error.retryable);
        assert_eq!(metadata(&error, "failure_class"), "stream_truncated");
    }
    let mut final_chunk = StreamChunk::delta(r#"{"facts":[]}"#);
    final_chunk.is_final = true;
    let provider = CompletionProvider {
        streaming: true,
        response: response(ProviderTermination::Complete),
        chunks: vec![final_chunk],
    };
    let error = request_post_turn_extractor_json(&provider, "model", "extract".to_owned())
        .await
        .unwrap_err();
    assert!(!error.retryable);
    assert_eq!(metadata(&error, "failure_class"), "missing_termination");
}

#[tokio::test]
async fn extractor_unexpected_tools_and_byte_limit_fail_closed() {
    let tool = ProviderToolCall {
        id: "PRIVATE_TOOL_CANARY".to_owned(),
        name: "PRIVATE_TOOL_CANARY".to_owned(),
        arguments: "PRIVATE_TOOL_CANARY".to_owned(),
    };
    for streaming in [false, true] {
        let mut text_response = response(ProviderTermination::Complete);
        text_response.tool_calls = vec![tool.clone()];
        let provider = CompletionProvider {
            streaming,
            response: text_response,
            chunks: vec![
                StreamChunk::tool_calls(vec![tool.clone()]),
                StreamChunk::final_chunk_with(ProviderTermination::Complete),
            ],
        };
        let error = request_post_turn_extractor_json(&provider, "model", "extract".to_owned())
            .await
            .unwrap_err();
        assert_eq!(metadata(&error, "failure_class"), "unexpected_tool_calls");
        assert!(!error.retryable);
        assert!(!format!("{error:?}").contains("PRIVATE_TOOL_CANARY"));

        let mut large = response(ProviderTermination::Complete);
        large.text = "x".repeat(MAX_POST_TURN_EXTRACTOR_RAW_BYTES + 1);
        let provider = CompletionProvider {
            streaming,
            response: large.clone(),
            chunks: vec![
                StreamChunk::delta("x".repeat(MAX_POST_TURN_EXTRACTOR_RAW_BYTES)),
                StreamChunk::delta("x"),
                StreamChunk::final_chunk_with(ProviderTermination::Complete),
            ],
        };
        let error = request_post_turn_extractor_json(&provider, "model", "extract".to_owned())
            .await
            .unwrap_err();
        assert_eq!(metadata(&error, "failure_class"), "response_too_large");
        assert!(!error.retryable);
    }
}

#[test]
fn extractor_format_classification_never_quotes_response_values() {
    for raw in [
        r#"{"facts":"PRIVATE_FORMAT_CANARY"}"#,
        r#"{"facts":[{"semantic":{"intent":"PRIVATE_FORMAT_CANARY"}}]}"#,
        r#"{"facts":PRIVATE_FORMAT_CANARY}"#,
    ] {
        let error = validate_memory_post_turn_response_format(raw).unwrap_err();
        for origin in [ResponseOrigin::Fresh, ResponseOrigin::Checkpoint] {
            let hook_error =
                memory_response_format_error("provider", "model", origin, raw.len(), error);
            assert!(!format!("{hook_error:?}").contains("PRIVATE_FORMAT_CANARY"));
            assert_eq!(
                hook_error.retryable,
                matches!(origin, ResponseOrigin::Fresh)
            );
        }
    }
}

#[tokio::test]
async fn stream_errors_require_typed_proof_for_completion_retry_policy() {
    let provider = CompletionProvider {
        streaming: true,
        response: response(ProviderTermination::Complete),
        chunks: vec![],
    };
    for message in [
        "stream stall",
        "provider stream ended before a terminal marker",
        "malformed OpenRouter SSE frame",
    ] {
        let stream = Box::pin(stream::iter(vec![Err(anyhow::anyhow!(message))]));
        let error = collect_post_turn_extractor_stream(stream, &provider, "model", "initial")
            .await
            .unwrap_err();
        assert_eq!(
            error.code.as_str(),
            "memory.post_turn_extractor.provider_stream_stall"
        );
        assert!(error.retryable);
    }
    for cause in [
        pioneer_provider::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker,
        pioneer_provider::failure::ProviderStreamIncomplete::DoneWithoutFinishReason,
    ] {
        let stream = Box::pin(stream::iter(vec![Err(anyhow::Error::from(cause))]));
        let error = collect_post_turn_extractor_stream(stream, &provider, "model", "initial")
            .await
            .unwrap_err();
        assert_eq!(
            error.code.as_str(),
            "memory.post_turn_extractor.completion_stream_truncated"
        );
        assert_eq!(metadata(&error, "failure_stage"), "transport_completion");
        assert!(error.retryable);
    }
}
