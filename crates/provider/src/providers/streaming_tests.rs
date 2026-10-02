//! In-memory wire regressions. These use the production decoders without HTTP,
//! servers, model inference or credentials.
use super::*;
use crate::{ProviderTermination, StreamChunk};
use anyhow::Result;
use bytes::Bytes;
use futures_util::{StreamExt, stream, stream::BoxStream};
use serde_json::json;

fn wire(value: serde_json::Value) -> String {
    format!("data: {value}\n\n")
}

fn fragmented(wire: &str) -> BoxStream<'static, Result<Bytes>> {
    Box::pin(stream::iter(
        wire.as_bytes()
            .iter()
            .map(|byte| Ok(Bytes::copy_from_slice(&[*byte])))
            .collect::<Vec<_>>(),
    ))
}

fn chat_decoders(wire: &str) -> Vec<BoxStream<'static, Result<StreamChunk>>> {
    vec![
        OpenAiProvider::decode_stream(fragmented(wire)),
        AzureOpenAiProvider::decode_stream(fragmented(wire)),
        OpenRouterProvider::decode_stream(fragmented(wire)),
        OpenAiCompatibleProvider::decode_stream(fragmented(wire), "fixture".into(), false),
        GlmProvider::decode_stream(fragmented(wire)),
        CopilotProvider::decode_stream(fragmented(wire)),
        TelnyxProvider::decode_stream(fragmented(wire)),
    ]
}

#[tokio::test]
async fn anthropic_preserves_initial_block_payload_and_cumulative_terminal_usage() {
    let input = wire(
        json!({"type":"message_start","message":{"usage":{"input_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}),
    ) + &wire(
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"initial reasoning"}}),
    ) + &wire(json!({"type":"content_block_stop","index":0}))
        + &wire(
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"initial text"}}),
        )
        + &wire(
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":" delta"}}),
        )
        + &wire(json!({"type":"content_block_stop","index":1}))
        + &wire(
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}),
        )
        + &wire(json!({"type":"message_stop"}));
    let chunks = AnthropicProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        chunks
            .iter()
            .map(|chunk| chunk.delta.as_str())
            .collect::<String>(),
        "initial text delta"
    );
    assert_eq!(
        chunks
            .iter()
            .filter_map(|chunk| chunk.reasoning_delta.as_deref())
            .collect::<String>(),
        "initial reasoning"
    );
    assert!(chunks.iter().any(|chunk| {
        chunk
            .usage
            .as_ref()
            .is_some_and(|usage| usage.output_tokens == Some(5))
    }));
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::Complete)
    );
}

#[tokio::test]
async fn all_chat_decoders_preserve_interleaved_calls_utf8_and_late_usage() {
    let mut input =
        wire(json!({"choices":[{"delta":{"content":"Привет 🌍", "reasoning_content":"trace"}}]}));
    input += &wire(json!({"choices":[{"delta":{"tool_calls":[
        {"index":1,"id":"b","function":{"name":"second","arguments":"{\"x\":"}},
        {"index":0,"id":"a","function":{"name":"first","arguments":"{"}}
    ]}}]}));
    input += &wire(json!({"choices":[{"delta":{"tool_calls":[
        {"index":0,"function":{"arguments":"\"y\":2}"}},
        {"index":1,"function":{"arguments":"1}"}}
    ]},"finish_reason":"tool_calls"}]}));
    input += &wire(json!({"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":7}}));
    input += "data: [DONE]\n\n";
    for decoder in chat_decoders(&input) {
        let chunks = decoder
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            chunks.iter().map(|c| c.delta.as_str()).collect::<String>(),
            "Привет 🌍"
        );
        let calls = chunks
            .iter()
            .flat_map(|c| &c.tool_calls)
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            (&calls[0].id, &calls[1].id),
            (&"a".to_owned(), &"b".to_owned())
        );
        assert_eq!(calls[0].arguments, "{\"y\":2}");
        assert_eq!(calls[1].arguments, "{\"x\":1}");
        assert!(
            chunks
                .iter()
                .any(|c| c.usage.as_ref().is_some_and(|u| u.output_tokens == Some(7)))
        );
        assert_eq!(
            chunks.last().unwrap().termination,
            Some(ProviderTermination::ToolCalls)
        );
    }
}

#[tokio::test]
async fn all_chat_decoders_reject_empty_partial_and_aborted_terminal_frames() {
    for input in [
        String::new(),
        wire(json!({"choices":[{"delta":{"content":"partial"}}]})),
        wire(json!({"choices":[{"delta":{},"finish_reason":"stop"}]})),
        "data: [DONE]\n\n".into(),
        "data: [DONE]\n".into(),
        "data: {\"choices\":".into(),
    ] {
        for decoder in chat_decoders(&input) {
            let chunks = decoder.collect::<Vec<_>>().await;
            assert!(chunks.iter().any(Result::is_err), "{input}");
            assert!(
                !chunks.iter().any(|c| c.as_ref().is_ok_and(|c| c.is_final)),
                "{input}"
            );
        }
    }
}

#[tokio::test]
async fn all_chat_decoders_do_not_hide_native_error_after_finish() {
    let mut input = wire(json!({"choices":[{"delta":{},"finish_reason":"stop"}]}));
    input += &wire(
        json!({"error":{"code":503,"type":"server_error","message":"private credential and payload"}}),
    );
    input += "data: [DONE]\n\n";
    for decoder in chat_decoders(&input) {
        let chunks = decoder.collect::<Vec<_>>().await;
        assert!(chunks.iter().any(Result::is_err));
        let error = chunks
            .iter()
            .find_map(|chunk| chunk.as_ref().err())
            .unwrap();
        assert_eq!(
            crate::failure::classify_stream_error(error).unwrap().class,
            pioneer_protocol::ProviderFailureClass::Provider5xx
        );
        assert!(!format!("{error:?}").contains("private"));
        assert!(!chunks.iter().any(|c| c.as_ref().is_ok_and(|c| c.is_final)));
    }
}

#[tokio::test]
async fn repeated_terminal_usage_reason_is_allowed_but_changed_reason_is_rejected() {
    for reason in ["stop", "error"] {
        let input = wire(json!({"choices":[{"delta":{},"finish_reason":"stop"}]}))
            + &wire(
                json!({"choices":[{"delta":{"content":""},"finish_reason":reason}],"usage":{"completion_tokens":7}}),
            )
            + "data: [DONE]\n\n";
        for decoder in chat_decoders(&input) {
            let chunks = decoder.collect::<Vec<_>>().await;
            if reason == "stop" {
                assert!(chunks.iter().all(Result::is_ok));
                assert!(chunks.last().unwrap().as_ref().unwrap().is_final);
            } else {
                assert!(chunks.iter().any(Result::is_err));
                assert!(
                    !chunks
                        .iter()
                        .any(|chunk| chunk.as_ref().is_ok_and(|chunk| chunk.is_final))
                );
            }
        }
    }
}

#[tokio::test]
async fn anthropic_native_error_has_safe_structured_cause_before_and_after_partial_call() {
    for prefix in [
        String::new(),
        wire(json!({"type":"message_start","message":{"usage":{"input_tokens":3}}}))
            + &wire(
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"write","input":{}}}),
            )
            + &wire(
                json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"x\":"}}),
            ),
    ] {
        let input = prefix
            + "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"credential=secret payload\"}}\n\n";
        let chunks = AnthropicProvider::decode_stream(fragmented(&input))
            .collect::<Vec<_>>()
            .await;
        let error = chunks.iter().find_map(|c| c.as_ref().err()).unwrap();
        assert_eq!(
            crate::failure::anthropic_stream_error(error),
            Some(crate::failure::AnthropicStreamError::Overloaded)
        );
        let classification = crate::failure::classify_stream_error(error).unwrap();
        assert_eq!(
            classification.provider_code.as_deref(),
            Some("overloaded_error")
        );
        assert_eq!(
            classification.class,
            pioneer_protocol::ProviderFailureClass::Provider5xx
        );
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(!chunks.iter().any(|c| {
            c.as_ref()
                .is_ok_and(|c| c.is_final || !c.tool_calls.is_empty())
        }));
    }
}

#[tokio::test]
async fn anthropic_parallel_calls_require_closed_blocks_and_message_stop() {
    let mut input = wire(json!({"type":"message_start","message":{}}));
    for index in 0..2 {
        input += &wire(
            json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":format!("t{index}"),"name":"read","input":{}}}),
        );
    }
    for index in [1, 0] {
        input += &wire(
            json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"🌍\"}"}}),
        );
        input += &wire(json!({"type":"content_block_stop","index":index}));
    }
    input += &wire(
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}),
    );
    let incomplete = AnthropicProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await;
    assert!(incomplete.iter().any(Result::is_err));
    input += &wire(json!({"type":"message_stop"}));
    let chunks = AnthropicProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(chunks.iter().flat_map(|c| &c.tool_calls).count(), 2);
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::ToolCalls)
    );
}

#[tokio::test]
async fn gemini_waits_for_late_usage_and_does_not_hide_late_error() {
    let input =
        wire(json!({"candidates":[{"content":{"parts":[{"text":"🌍"}]},"finishReason":"STOP"}]}));
    let usage = wire(json!({"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2}}));
    let chunks = GeminiProvider::decode_stream(fragmented(&(input.clone() + &usage)))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::Complete)
    );
    assert!(
        chunks
            .iter()
            .any(|c| c.usage.as_ref().is_some_and(|u| u.input_tokens == Some(4)))
    );
    let input = input + &wire(json!({"error":{"code":429,"message":"secret"}}));
    let chunks = GeminiProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await;
    let error = chunks.iter().find_map(|c| c.as_ref().err()).unwrap();
    assert_eq!(
        crate::failure::classify_stream_error(error).unwrap().class,
        pioneer_protocol::ProviderFailureClass::RateLimit
    );
    assert!(!format!("{error:?}").contains("secret"));
    assert!(!chunks.iter().any(|c| c.as_ref().is_ok_and(|c| c.is_final)));
}

#[tokio::test]
async fn ollama_keeps_terminal_content_and_detects_native_error_and_eof() {
    let input = format!(
        "{}\n",
        json!({"message":{"content":"🌍","thinking":"trace"},"done":true,"prompt_eval_count":4,"eval_count":2})
    );
    let chunks = OllamaProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        chunks.iter().map(|c| c.delta.as_str()).collect::<String>(),
        "🌍"
    );
    assert_eq!(
        chunks.last().unwrap().usage.as_ref().unwrap().output_tokens,
        Some(2)
    );
    for input in [
        "{\"error\":\"secret\"}\n",
        "{\"message\":{\"content\":\"partial\"},\"done\":false}\n",
        "",
    ] {
        let chunks = OllamaProvider::decode_stream(fragmented(input))
            .collect::<Vec<_>>()
            .await;
        assert!(chunks.iter().any(Result::is_err));
        assert!(!chunks.iter().any(|c| c.as_ref().is_ok_and(|c| c.is_final)));
    }
}

struct PendingWire {
    prefix: Option<Bytes>,
    dropped: Option<tokio::sync::oneshot::Sender<()>>,
}
impl futures_util::Stream for PendingWire {
    type Item = Result<Bytes>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.prefix.take() {
            Some(prefix) => std::task::Poll::Ready(Some(Ok(prefix))),
            None => std::task::Poll::Pending,
        }
    }
}
impl Drop for PendingWire {
    fn drop(&mut self) {
        if let Some(sender) = self.dropped.take() {
            let _ = sender.send(());
        }
    }
}

#[tokio::test]
async fn dropping_consumer_cancels_pending_transport_before_and_after_partial_output() {
    let chat_prefix = wire(
        json!({"choices":[{"delta":{"content":"partial","tool_calls":[{"index":0,"id":"t","function":{"name":"write","arguments":"{"}}]}}]}),
    );
    let anthropic_prefix = wire(json!({"type":"message_start"}))
        + &wire(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"partial"}}),
        )
        + &wire(json!({"type":"content_block_stop","index":0}))
        + &wire(
            json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t","name":"write","input":{}}}),
        )
        + &wire(
            json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{"}}),
        );
    for decoder in 0..11 {
        for has_prefix in [false, true] {
            let prefix = match decoder {
                8 => anthropic_prefix.clone(),
                9 => wire(json!({"candidates":[{"content":{"parts":[{"text":"partial"}]}}]})),
                10 => format!(
                    "{}\n",
                    json!({"message":{"content":"partial","tool_calls":[{"function":{"name":"write","arguments":{}}}]},"done":false})
                ),
                _ => chat_prefix.clone(),
            };
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let input: BoxStream<'static, Result<Bytes>> = Box::pin(PendingWire {
                prefix: has_prefix.then(|| Bytes::from(prefix)),
                dropped: Some(sender),
            });
            let mut chunks = match decoder {
                0 => OpenAiProvider::decode_stream(input),
                1 => AzureOpenAiProvider::decode_stream(input),
                2 => OpenRouterProvider::decode_stream(input),
                3 => OpenAiCompatibleProvider::decode_stream(input, "fixture".into(), false),
                4 => GlmProvider::decode_stream(input),
                5 => CopilotProvider::decode_stream(input),
                6 => TelnyxProvider::decode_stream(input),
                7 => DeepSeekProvider::validate_stream(
                    "deepseek-reasoner".into(),
                    OpenAiCompatibleProvider::decode_stream(input, "deepseek".into(), true),
                ),
                8 => AnthropicProvider::decode_stream(input),
                9 => GeminiProvider::decode_stream(input),
                _ => OllamaProvider::decode_stream(input),
            };
            if has_prefix {
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    loop {
                        if chunks.next().await.unwrap().unwrap().delta == "partial" {
                            break;
                        }
                    }
                })
                .await
                .unwrap();
            }
            drop(chunks);
            tokio::time::timeout(std::time::Duration::from_secs(1), receiver)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn ollama_distinct_no_id_calls_across_frames_remain_distinct_even_if_identical() {
    let call = json!({"function":{"name":"read","arguments":{"path":"x"}}});
    let mut input = String::new();
    for _ in 0..2 {
        input += &format!(
            "{}\n",
            json!({"message":{"tool_calls":[call.clone()]},"done":false})
        );
    }
    input += &format!(
        "{}\n",
        json!({"message":{},"done":true,"done_reason":"stop"})
    );
    let chunks = OllamaProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let calls = chunks
        .iter()
        .flat_map(|c| &c.tool_calls)
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert_ne!(calls[0].id, calls[1].id);
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::ToolCalls)
    );
}

#[tokio::test]
async fn native_terminal_reasons_are_immutable_even_with_same_normalized_outcome() {
    for (first, second) in [
        ("MAX_TOKENS", "STOP"),
        ("SAFETY", "STOP"),
        ("STOP", "SAFETY"),
        ("SAFETY", "RECITATION"),
    ] {
        let input = wire(
            json!({"candidates":[{"content":{"parts":[{"text":"partial"}]},"finishReason":first}]}),
        ) + &wire(json!({"candidates":[{"finishReason":second}]}));
        let chunks = GeminiProvider::decode_stream(fragmented(&input))
            .collect::<Vec<_>>()
            .await;
        assert!(
            chunks
                .iter()
                .any(|c| c.as_ref().is_ok_and(|c| c.delta == "partial"))
        );
        assert_contradictory_chunks(&chunks, 0);
    }
    for (first, second) in [
        ("max_tokens", "end_turn"),
        ("end_turn", "max_tokens"),
        ("end_turn", "stop_sequence"),
        ("tool_use", "end_turn"),
    ] {
        let input = anthropic_closed_call()
            + &wire(json!({"type":"message_delta","delta":{"stop_reason":first}}))
            + &wire(json!({"type":"message_delta","delta":{"stop_reason":second}}))
            + &wire(json!({"type":"message_stop"}));
        let chunks = AnthropicProvider::decode_stream(fragmented(&input))
            .collect::<Vec<_>>()
            .await;
        assert_contradictory_chunks(&chunks, 1);
    }
    // Legacy snake_case is recognized by today's parser; NOT canonical Gemini conformance (G02).
    let input = wire(
        json!({"candidates":[{"content":{"parts":[{"function_call":{"name":"read","args":{}}}]},"finishReason":"STOP"}]}),
    ) + &wire(json!({"candidates":[{"finishReason":"MAX_TOKENS"}]}));
    let chunks = GeminiProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await;
    assert_contradictory_chunks(&chunks, 1);
}

fn assert_contradictory_chunks(chunks: &[Result<StreamChunk>], calls: usize) {
    assert!(chunks.iter().any(|c| {
        c.as_ref()
            .is_err_and(|error| error.to_string().contains("contradictory"))
    }));
    assert_eq!(
        chunks
            .iter()
            .filter_map(|c| c.as_ref().ok())
            .flat_map(|c| &c.tool_calls)
            .count(),
        calls
    );
    assert!(!chunks.iter().any(|c| c.as_ref().is_ok_and(|c| c.is_final)));
}

async fn assert_failed_decode(decoder: BoxStream<'static, Result<StreamChunk>>) {
    let chunks = decoder.collect::<Vec<_>>().await;
    assert!(chunks.iter().any(Result::is_err));
    assert!(
        !chunks
            .iter()
            .any(|chunk| chunk.as_ref().is_ok_and(|chunk| chunk.is_final))
    );
}

fn anthropic_closed_call() -> String {
    wire(json!({"type":"message_start","message":{}}))
        + &wire(
            json!({"type":"content_block_start","index":7,"content_block":{"type":"tool_use","id":"valid","name":"read","input":{"path":"initial"}}}),
        )
        + &wire(json!({"type":"content_block_stop","index":7}))
}

#[tokio::test]
async fn native_repeated_reasons_and_reasonless_metadata_preserve_late_usage() {
    let input = anthropic_closed_call()
        + &wire(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}))
        + &wire(json!({"type":"message_delta","delta":{},"usage":{"output_tokens":4}}))
        + &wire(
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}),
        )
        + &wire(json!({"type":"message_stop"}));
    let chunks = AnthropicProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::ToolCalls)
    );
    assert!(
        chunks
            .iter()
            .any(|c| c.usage.as_ref().is_some_and(|u| u.output_tokens == Some(9)))
    );
    let input = wire(
        json!({"candidates":[{"content":{"parts":[{"text":"text"}]},"finishReason":"STOP"}]}),
    ) + &wire(json!({"candidates":[{}]}))
        + &wire(
            json!({"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":9}}),
        );
    let chunks = GeminiProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::Complete)
    );
    assert!(
        chunks
            .iter()
            .any(|c| c.usage.as_ref().is_some_and(|u| u.output_tokens == Some(9)))
    );
}

#[tokio::test]
async fn anthropic_malformed_second_block_invalidates_previously_collected_call() {
    for block in [
        json!({"type":"tool_use","id":"distinct","name":"read","input":{}}),
        json!({"type":"tool_use","name":"read","input":{}}),
        json!({"type":"tool_use","id":"distinct","input":{}}),
        json!({"type":"tool_use","id":"distinct","name":"read"}),
    ] {
        let index = if block["id"] == "distinct"
            && block.get("name").is_some()
            && block.get("input").is_some()
        {
            7
        } else {
            11
        };
        let input = anthropic_closed_call()
            + &wire(json!({"type":"content_block_start","index":index,"content_block":block}))
            + &wire(json!({"type":"content_block_stop","index":index}))
            + &wire(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}))
            + &wire(json!({"type":"message_stop"}));
        assert_failed_decode(AnthropicProvider::decode_stream(fragmented(&input))).await;
    }
    for input in [
        wire(json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}})),
        wire(json!({"type":"message_start"}))
            + &wire(json!({"type":"content_block_start","index":0})),
        wire(json!({"type":"message_start"}))
            + &wire(
                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"initial"}}),
            )
            + &wire(json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}})),
        anthropic_closed_call()
            + &wire(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}))
            + &wire(
                json!({"type":"content_block_start","index":11,"content_block":{"type":"text","text":"late"}}),
            ),
        wire(json!({"type":"message_start"}))
            + &wire(
                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
            )
            + &wire(
                json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}),
            ),
    ] {
        assert_failed_decode(AnthropicProvider::decode_stream(fragmented(&input))).await;
    }
}

#[tokio::test]
async fn anthropic_unknown_future_events_blocks_and_deltas_are_gracefully_ignored() {
    let input = anthropic_closed_call()
        + &wire(json!({"type":"future_event","payload":"ignored"}))
        + &wire(
            json!({"type":"content_block_start","index":42,"content_block":{"type":"future_block"}}),
        )
        + &wire(
            json!({"type":"content_block_delta","index":42,"delta":{"type":"future_delta","text":"not text output"}}),
        )
        + &wire(json!({"type":"content_block_stop","index":42}))
        + &wire(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}))
        + &wire(json!({"type":"message_stop"}));
    let chunks = AnthropicProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(chunks.iter().flat_map(|c| &c.tool_calls).count(), 1);
    assert!(chunks.iter().all(|c| c.delta.is_empty()));
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::ToolCalls)
    );
}

#[test]
fn ollama_non_stream_requires_affirmative_done_before_tool_normalization() {
    for done in [None, Some(false), Some(true)] {
        for tools in [false, true] {
            for reason in ["stop", "length", "content_filter", "unknown_native_reason"] {
                let message = if tools {
                    json!({"tool_calls":[{"function":{"name":"read","arguments":{}}}]})
                } else {
                    json!({"content":"text"})
                };
                let mut value = json!({"message":message,"done_reason":reason});
                if let Some(done) = done {
                    value["done"] = json!(done);
                }
                let result = OllamaProvider::decode_chat_fixture(value);
                if done != Some(true) {
                    assert!(result.is_err());
                    continue;
                }
                let response = result.unwrap();
                if reason == "stop" {
                    assert_eq!(
                        response.termination,
                        if tools {
                            ProviderTermination::ToolCalls
                        } else {
                            ProviderTermination::Complete
                        }
                    );
                } else {
                    assert!(!matches!(
                        response.termination,
                        ProviderTermination::Complete | ProviderTermination::ToolCalls
                    ));
                }
            }
        }
    }
}

#[tokio::test]
async fn anthropic_native_error_after_terminal_reason_still_rejects_round() {
    let input = anthropic_closed_call()
        + &wire(json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}))
        + &wire(
            json!({"type":"error","error":{"type":"overloaded_error","message":"private payload"}}),
        )
        + &wire(json!({"type":"message_stop"}));
    assert_failed_decode(AnthropicProvider::decode_stream(fragmented(&input))).await;
}

#[tokio::test]
async fn ollama_stream_terminal_payload_preserves_tool_call_and_usage() {
    let input = format!(
        "{}\n",
        json!({"message":{"content":"terminal","tool_calls":[{"function":{"name":"read","arguments":{"path":"🌍"}}}]},"done":true,"done_reason":"stop","eval_count":6})
    );
    let chunks = OllamaProvider::decode_stream(fragmented(&input))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(chunks.iter().flat_map(|c| &c.tool_calls).count(), 1);
    assert_eq!(
        chunks.iter().map(|c| c.delta.as_str()).collect::<String>(),
        "terminal"
    );
    assert_eq!(
        chunks.last().unwrap().termination,
        Some(ProviderTermination::ToolCalls)
    );
    assert_eq!(
        chunks.last().unwrap().usage.as_ref().unwrap().output_tokens,
        Some(6)
    );
}
