use super::{
    AgentRoundResponse, ChatTurnError, PROVIDER_FIRST_CHUNK_TIMEOUT,
    PROVIDER_INTER_CHUNK_IDLE_TIMEOUT,
};
use crate::AgentEventHub;
use futures_util::{Stream, StreamExt};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemStartedNotification, ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage,
    ProviderTransportKind, TurnItem, TurnItemType,
};
use pioneer_provider::{ChatRequest, Provider, ProviderToolCall, StreamChunk};
use std::sync::Arc;
use tokio::time::timeout;

#[derive(Clone, Copy)]
struct FailureTarget<'a> {
    item_id: &'a str,
    item_type: TurnItemType,
}

impl<'a> FailureTarget<'a> {
    fn new(item_id: &'a str, item_type: TurnItemType) -> Self {
        Self { item_id, item_type }
    }
}

pub(super) async fn request_agent_round(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    force_non_stream: bool,
    event_tx: &AgentEventHub,
) -> Result<AgentRoundResponse, ChatTurnError> {
    if provider.capabilities().streaming && !force_non_stream {
        let provider_name = provider.name().to_owned();
        let model_name = request.model.clone();

        let target = FailureTarget::new(thinking_item_id, TurnItemType::Reasoning);
        let mut stream = provider.stream_chat(request).await.map_err(|error| {
            stream_error_for_target(
                target,
                provider_name.as_str(),
                model_name.as_str(),
                ProviderFailureStage::Connect,
                format!("provider stream error: {error}"),
            )
        })?;

        let mut full_text = String::new();
        let mut full_reasoning = String::new();
        let mut tool_calls = Vec::new();
        let mut saw_final = false;
        let mut seen_any_chunk = false;

        while let Some(chunk) = read_next_stream_chunk(
            &mut stream,
            &mut seen_any_chunk,
            target,
            provider_name.as_str(),
            model_name.as_str(),
        )
        .await?
        {
            if chunk.is_final {
                saw_final = true;
                break;
            }

            if let Some(reasoning_delta) = chunk.reasoning_delta
                && !reasoning_delta.is_empty()
            {
                full_reasoning.push_str(reasoning_delta.as_str());
                super::emit_progress_event(
                    event_tx,
                    AgentProgressEvent::ItemDelta {
                        notification: ItemDeltaNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item_id: thinking_item_id.to_owned(),
                            delta: reasoning_delta,
                            stream: Some(pioneer_protocol::ItemDeltaStream::Generic),
                            payload: None,
                            markdown: None,
                            markdown_version: None,
                        },
                    },
                )
                .await?;
            }

            if !chunk.delta.is_empty() {
                full_text.push_str(chunk.delta.as_str());
            }

            for tool_call in chunk.tool_calls {
                upsert_tool_call(&mut tool_calls, tool_call);
            }
        }

        if !saw_final && seen_any_chunk {
            return Err(stream_error_for_target(
                target,
                provider_name.as_str(),
                model_name.as_str(),
                ProviderFailureStage::Finalize,
                "stream truncated before final chunk".to_owned(),
            ));
        }

        return Ok(AgentRoundResponse {
            text: full_text,
            reasoning: full_reasoning,
            tool_calls,
        });
    }

    let provider_name = provider.name().to_owned();
    let model_name = request.model.clone();

    let response = provider.chat(request).await.map_err(|error| {
        provider_failure_error(
            thinking_item_id,
            TurnItemType::Reasoning,
            provider_name.as_str(),
            model_name.as_str(),
            ProviderTransportKind::NonStream,
            ProviderFailureStage::Connect,
            format!("provider chat error: {error}"),
        )
    })?;

    let reasoning = response.reasoning_content.unwrap_or_default();

    if !reasoning.is_empty() {
        super::emit_progress_event(
            event_tx,
            AgentProgressEvent::ItemDelta {
                notification: ItemDeltaNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: thinking_item_id.to_owned(),
                    delta: reasoning.clone(),
                    stream: Some(pioneer_protocol::ItemDeltaStream::Generic),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            },
        )
        .await?;
    }

    Ok(AgentRoundResponse {
        text: response.text,
        reasoning,
        tool_calls: response.tool_calls,
    })
}

pub(super) async fn stream_provider_response(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    event_tx: &AgentEventHub,
) -> Result<String, ChatTurnError> {
    let provider_name = provider.name().to_owned();
    let model_name = request.model.clone();

    let connect_target = FailureTarget::new(thinking_item_id, TurnItemType::Reasoning);
    let mut stream = provider.stream_chat(request).await.map_err(|e| {
        stream_error_for_target(
            connect_target,
            provider_name.as_str(),
            model_name.as_str(),
            ProviderFailureStage::Connect,
            format!("provider stream error: {e}"),
        )
    })?;

    let mut full_text = String::new();
    let mut reasoning_parts = Vec::new();
    let mut message_started = false;
    let mut stream_tool_calls = Vec::new();
    let mut saw_final = false;
    let mut seen_any_chunk = false;

    while let Some(chunk) = read_next_stream_chunk(
        &mut stream,
        &mut seen_any_chunk,
        response_stream_target(message_started, thinking_item_id, message_item_id),
        provider_name.as_str(),
        model_name.as_str(),
    )
    .await?
    {
        let StreamChunk {
            delta,
            reasoning_delta,
            tool_calls,
            is_final,
        } = chunk;

        if is_final {
            saw_final = true;
            break;
        }

        for tool_call in tool_calls {
            upsert_tool_call(&mut stream_tool_calls, tool_call);
        }

        if let Some(reasoning) = reasoning_delta
            && !reasoning.is_empty()
        {
            reasoning_parts.push(reasoning.clone());

            super::emit_progress_event(
                event_tx,
                AgentProgressEvent::ItemDelta {
                    notification: ItemDeltaNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item_id: thinking_item_id.to_owned(),
                        delta: reasoning,
                        stream: Some(pioneer_protocol::ItemDeltaStream::Generic),
                        payload: None,
                        markdown: None,
                        markdown_version: None,
                    },
                },
            )
            .await?;
        }

        if !delta.is_empty() {
            if !message_started {
                message_started = true;
                let reasoning_text = reasoning_parts.join("");

                super::emit_durable_event(
                    event_tx,
                    AgentDurableEvent::ItemCompleted {
                        notification: ItemCompletedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::Reasoning {
                                id: thinking_item_id.to_owned(),
                                summary: Vec::new(),
                                content: if reasoning_text.is_empty() {
                                    Vec::new()
                                } else {
                                    vec![reasoning_text]
                                },
                            },
                        },
                    },
                )
                .await?;

                super::emit_durable_event(
                    event_tx,
                    AgentDurableEvent::ItemStarted {
                        notification: ItemStartedNotification {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            item: TurnItem::AgentMessage {
                                id: message_item_id.to_owned(),
                                text: String::new(),
                                markdown: None,
                                markdown_version: None,
                            },
                        },
                    },
                )
                .await?;
            }

            full_text.push_str(delta.as_str());
            super::emit_progress_event(
                event_tx,
                AgentProgressEvent::ItemDelta {
                    notification: ItemDeltaNotification {
                        workspace_id: workspace_id.to_owned(),
                        thread_id: thread_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        item_id: message_item_id.to_owned(),
                        delta,
                        stream: Some(pioneer_protocol::ItemDeltaStream::AgentMessage),
                        payload: None,
                        markdown: None,
                        markdown_version: None,
                    },
                },
            )
            .await?;
        }
    }

    if !saw_final && seen_any_chunk {
        return Err(stream_error_for_target(
            response_stream_target(message_started, thinking_item_id, message_item_id),
            provider_name.as_str(),
            model_name.as_str(),
            ProviderFailureStage::Finalize,
            "stream truncated before final chunk".to_owned(),
        ));
    }

    for tool_call in stream_tool_calls {
        super::emit_durable_event(
            event_tx,
            AgentDurableEvent::ItemStarted {
                notification: ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: super::tooling::build_started_tool_turn_item(
                        tool_call.id.clone(),
                        tool_call.name.clone(),
                        tool_call.arguments.clone(),
                        None,
                        None,
                        None,
                    ),
                },
            },
        )
        .await?;

        super::emit_durable_event(
            event_tx,
            AgentDurableEvent::ItemCompleted {
                notification: ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: super::tooling::build_completed_tool_turn_item(
                        tool_call.id,
                        tool_call.name,
                        tool_call.arguments,
                        None,
                        None,
                        None,
                    ),
                },
            },
        )
        .await?;
    }

    if !message_started {
        let reasoning_text = reasoning_parts.join("");

        super::emit_durable_event(
            event_tx,
            AgentDurableEvent::ItemCompleted {
                notification: ItemCompletedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::Reasoning {
                        id: thinking_item_id.to_owned(),
                        summary: Vec::new(),
                        content: if reasoning_text.is_empty() {
                            Vec::new()
                        } else {
                            vec![reasoning_text]
                        },
                    },
                },
            },
        )
        .await?;

        super::emit_durable_event(
            event_tx,
            AgentDurableEvent::ItemStarted {
                notification: ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::AgentMessage {
                        id: message_item_id.to_owned(),
                        text: String::new(),
                        markdown: None,
                        markdown_version: None,
                    },
                },
            },
        )
        .await?;
    }

    let assistant_text = full_text;

    super::emit_durable_event(
        event_tx,
        AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::AgentMessage {
                    id: message_item_id.to_owned(),
                    text: assistant_text.clone(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
    )
    .await?;

    Ok(assistant_text)
}

pub(super) async fn non_stream_provider_response(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    event_tx: &AgentEventHub,
) -> Result<String, ChatTurnError> {
    let provider_name = provider.name().to_owned();
    let model_name = request.model.clone();

    let response = provider.chat(request).await.map_err(|e| {
        provider_failure_error(
            thinking_item_id,
            TurnItemType::Reasoning,
            provider_name.as_str(),
            model_name.as_str(),
            ProviderTransportKind::NonStream,
            ProviderFailureStage::Connect,
            format!("provider error: {e}"),
        )
    })?;

    let reasoning_content = match &response.reasoning_content {
        Some(rc) if !rc.is_empty() => vec![rc.clone()],
        _ => Vec::new(),
    };

    super::emit_durable_event(
        event_tx,
        AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::Reasoning {
                    id: thinking_item_id.to_owned(),
                    summary: Vec::new(),
                    content: reasoning_content,
                },
            },
        },
    )
    .await?;

    super::emit_durable_event(
        event_tx,
        AgentDurableEvent::ItemStarted {
            notification: ItemStartedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::AgentMessage {
                    id: message_item_id.to_owned(),
                    text: String::new(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
    )
    .await?;

    let assistant_text = response.text;

    if !assistant_text.is_empty() {
        super::emit_progress_event(
            event_tx,
            AgentProgressEvent::ItemDelta {
                notification: ItemDeltaNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: message_item_id.to_owned(),
                    delta: assistant_text.clone(),
                    stream: Some(pioneer_protocol::ItemDeltaStream::AgentMessage),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            },
        )
        .await?;
    }

    super::emit_durable_event(
        event_tx,
        AgentDurableEvent::ItemCompleted {
            notification: ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::AgentMessage {
                    id: message_item_id.to_owned(),
                    text: assistant_text.clone(),
                    markdown: None,
                    markdown_version: None,
                },
            },
        },
    )
    .await?;

    Ok(assistant_text)
}

fn response_stream_target<'a>(
    message_started: bool,
    thinking_item_id: &'a str,
    message_item_id: &'a str,
) -> FailureTarget<'a> {
    if message_started {
        FailureTarget::new(message_item_id, TurnItemType::AgentMessage)
    } else {
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning)
    }
}

fn stream_error_for_target(
    target: FailureTarget<'_>,
    provider: &str,
    model: &str,
    stage: ProviderFailureStage,
    error_message: String,
) -> ChatTurnError {
    provider_failure_error(
        target.item_id,
        target.item_type,
        provider,
        model,
        ProviderTransportKind::Stream,
        stage,
        error_message,
    )
}

fn upsert_tool_call(tool_calls: &mut Vec<ProviderToolCall>, incoming: ProviderToolCall) {
    if incoming.id.is_empty() {
        if !tool_calls.iter().any(|existing| {
            existing.name == incoming.name && existing.arguments == incoming.arguments
        }) {
            tool_calls.push(incoming);
        }
        return;
    }

    if let Some(existing) = tool_calls
        .iter_mut()
        .find(|existing| existing.id == incoming.id)
    {
        merge_tool_call(existing, incoming);
        return;
    }

    tool_calls.push(incoming);
}

fn merge_tool_call(existing: &mut ProviderToolCall, incoming: ProviderToolCall) {
    if should_replace_tool_name(existing.name.as_str(), incoming.name.as_str()) {
        existing.name = incoming.name;
    }
    if should_replace_tool_arguments(existing.arguments.as_str(), incoming.arguments.as_str()) {
        existing.arguments = incoming.arguments;
    }
}

fn should_replace_tool_name(current: &str, next: &str) -> bool {
    if next.is_empty() {
        return false;
    }
    if current.is_empty() {
        return true;
    }
    if next == current {
        return false;
    }
    next.len() > current.len()
}

fn should_replace_tool_arguments(current: &str, next: &str) -> bool {
    if next.trim().is_empty() || next.trim() == "{}" {
        return false;
    }
    if current.trim().is_empty() || current.trim() == "{}" {
        return true;
    }
    if next == current {
        return false;
    }
    next.len() >= current.len()
}

async fn read_next_stream_chunk<S>(
    stream: &mut S,
    seen_any_chunk: &mut bool,
    target: FailureTarget<'_>,
    provider_name: &str,
    model_name: &str,
) -> Result<Option<StreamChunk>, ChatTurnError>
where
    S: Stream<Item = anyhow::Result<StreamChunk>> + Unpin,
{
    let stage = if *seen_any_chunk {
        ProviderFailureStage::MidStream
    } else {
        ProviderFailureStage::FirstChunk
    };

    let wait = if *seen_any_chunk {
        PROVIDER_INTER_CHUNK_IDLE_TIMEOUT
    } else {
        PROVIDER_FIRST_CHUNK_TIMEOUT
    };

    let next_chunk = timeout(wait, stream.next()).await.map_err(|_| {
        stream_error_for_target(
            target,
            provider_name,
            model_name,
            stage,
            "stream stall: chunk timeout exceeded".to_owned(),
        )
    })?;

    let Some(chunk_result) = next_chunk else {
        return Ok(None);
    };

    *seen_any_chunk = true;

    let chunk = chunk_result.map_err(|error| {
        stream_error_for_target(
            target,
            provider_name,
            model_name,
            ProviderFailureStage::MidStream,
            format!("stream chunk error: {error}"),
        )
    })?;

    Ok(Some(chunk))
}

pub(super) fn provider_failure_error(
    item_id: &str,
    item_type: TurnItemType,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
    stage: ProviderFailureStage,
    error_message: String,
) -> ChatTurnError {
    let lower = error_message.to_ascii_lowercase();
    let http_status = extract_http_status(error_message.as_str());
    let retry_after_ms = extract_retry_after_ms(lower.as_str());
    let provider_code = extract_provider_code(error_message.as_str());
    let class = classify_provider_failure_class(
        lower.as_str(),
        stage,
        http_status,
        provider_code.as_deref(),
    );
    let is_recoverable_hint = !matches!(
        class,
        ProviderFailureClass::InvalidRequest | ProviderFailureClass::PermissionDenied
    );

    ChatTurnError::ProviderFailure {
        item_id: item_id.to_owned(),
        item_type,
        failure: ProviderFailureDetails {
            provider: provider.to_owned(),
            model: model.to_owned(),
            transport,
            class,
            stage,
            http_status,
            provider_code,
            retry_after_ms,
            is_recoverable_hint,
            message: Some(error_message),
        },
    }
}

fn classify_provider_failure_class(
    message_lower: &str,
    stage: ProviderFailureStage,
    http_status: Option<u16>,
    provider_code: Option<&str>,
) -> ProviderFailureClass {
    if matches!(
        stage,
        ProviderFailureStage::FirstChunk | ProviderFailureStage::MidStream
    ) && message_lower.contains("stream stall")
    {
        return ProviderFailureClass::StreamStall;
    }
    if message_lower.contains("stream truncated") {
        return ProviderFailureClass::StreamTruncated;
    }
    if message_lower.contains("max_output_tokens")
        || message_lower.contains("maximum output tokens")
        || message_lower.contains("output token limit")
    {
        return ProviderFailureClass::MaxOutputTokens;
    }
    if message_lower.contains("prompt too long")
        || message_lower.contains("context length")
        || message_lower.contains("maximum context")
        || http_status == Some(413)
    {
        return ProviderFailureClass::PromptTooLong;
    }
    if http_status == Some(429) || message_lower.contains("rate limit") {
        return ProviderFailureClass::RateLimit;
    }
    if http_status.is_some_and(|status| (500..600).contains(&status)) {
        return ProviderFailureClass::Provider5xx;
    }
    if http_status == Some(401)
        || (http_status == Some(403)
            && (message_lower.contains("token expired")
                || message_lower.contains("token revoked")
                || message_lower.contains("unauthorized")
                || message_lower.contains("authentication")))
        || provider_code
            .map(|value| {
                value.contains("invalid_api_key")
                    || value.contains("auth")
                    || value.contains("token_expired")
            })
            .unwrap_or(false)
    {
        return ProviderFailureClass::AuthExpired;
    }
    if is_image_input_capability_mismatch(message_lower) {
        return ProviderFailureClass::InvalidRequest;
    }
    if http_status == Some(404)
        || message_lower.contains("model not found")
        || message_lower.contains("unknown model")
        || message_lower.contains("no such model")
    {
        return ProviderFailureClass::ModelNotFound;
    }
    if http_status == Some(403)
        || message_lower.contains("permission denied")
        || message_lower.contains("forbidden")
    {
        return ProviderFailureClass::PermissionDenied;
    }
    if http_status == Some(400)
        || message_lower.contains("invalid request")
        || message_lower.contains("bad request")
    {
        return ProviderFailureClass::InvalidRequest;
    }
    if message_lower.contains("error sending request")
        || message_lower.contains("connection")
        || message_lower.contains("dns")
        || message_lower.contains("timed out")
        || message_lower.contains("connection reset")
        || message_lower.contains("broken pipe")
    {
        return ProviderFailureClass::NetworkTransient;
    }
    if matches!(
        stage,
        ProviderFailureStage::FirstChunk | ProviderFailureStage::MidStream
    ) {
        return ProviderFailureClass::StreamStall;
    }
    ProviderFailureClass::Unknown
}

fn is_image_input_capability_mismatch(message_lower: &str) -> bool {
    message_lower.contains("image input")
        && (message_lower.contains("no endpoints found")
            || message_lower.contains("does not support")
            || message_lower.contains("not support")
            || message_lower.contains("unsupported"))
}

fn extract_http_status(message: &str) -> Option<u16> {
    let bytes = message.as_bytes();
    for window in bytes.windows(3) {
        if window.iter().all(|b| b.is_ascii_digit()) {
            let value = std::str::from_utf8(window).ok()?.parse::<u16>().ok()?;
            if (100..600).contains(&value) {
                return Some(value);
            }
        }
    }
    None
}

fn extract_provider_code(message: &str) -> Option<String> {
    let marker = "\"code\":\"";
    let start = message.find(marker)?;
    let rest = &message[start + marker.len()..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

fn extract_retry_after_ms(message_lower: &str) -> Option<u64> {
    let marker = "retry-after";
    let index = message_lower.find(marker)?;
    let rest = &message_lower[index + marker.len()..];
    let seconds = rest
        .chars()
        .skip_while(|ch| !ch.is_ascii_digit())
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    let secs = seconds.parse::<u64>().ok()?;
    Some(secs.saturating_mul(1000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openrouter_image_input_endpoint_error_is_terminal_invalid_request() {
        let error = r#"provider stream error: OpenRouter API error (404 Not Found): {"error":{"message":"No endpoints found that support image input","code":404}}"#;

        let ChatTurnError::ProviderFailure { failure, .. } = provider_failure_error(
            "reasoning_item",
            TurnItemType::Reasoning,
            "openrouter",
            "deepseek/deepseek-v4-flash",
            ProviderTransportKind::Stream,
            ProviderFailureStage::Connect,
            error.to_owned(),
        ) else {
            panic!("expected provider failure");
        };

        assert_eq!(failure.class, ProviderFailureClass::InvalidRequest);
        assert_eq!(failure.http_status, Some(404));
        assert!(!failure.is_recoverable_hint);
    }

    #[test]
    fn plain_404_still_maps_to_model_not_found() {
        assert_eq!(
            classify_provider_failure_class(
                "provider stream error: api error (404 not found): model not found",
                ProviderFailureStage::Connect,
                Some(404),
                None,
            ),
            ProviderFailureClass::ModelNotFound
        );
    }

    #[test]
    fn upsert_tool_call_replaces_partial_name_for_same_id() {
        let mut calls = Vec::new();
        upsert_tool_call(
            &mut calls,
            ProviderToolCall {
                id: "call_1".to_owned(),
                name: "tas".to_owned(),
                arguments: "{}".to_owned(),
            },
        );
        upsert_tool_call(
            &mut calls,
            ProviderToolCall {
                id: "call_1".to_owned(),
                name: "task_wait".to_owned(),
                arguments: "{\"taskIds\":[\"a\"]}".to_owned(),
            },
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].name, "task_wait");
        assert_eq!(calls[0].arguments, "{\"taskIds\":[\"a\"]}");
    }

    #[test]
    fn upsert_tool_call_keeps_distinct_ids() {
        let mut calls = Vec::new();
        upsert_tool_call(
            &mut calls,
            ProviderToolCall {
                id: "call_1".to_owned(),
                name: "task_create".to_owned(),
                arguments: "{\"title\":\"A\"}".to_owned(),
            },
        );
        upsert_tool_call(
            &mut calls,
            ProviderToolCall {
                id: "call_2".to_owned(),
                name: "task_wait".to_owned(),
                arguments: "{\"taskIds\":[\"x\"]}".to_owned(),
            },
        );

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[1].id, "call_2");
    }
}
