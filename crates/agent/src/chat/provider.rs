use super::{AgentRoundResponse, ChatTurnError};
use crate::AgentEventHub;
use futures_util::{Stream, StreamExt};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemStartedNotification, ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage,
    ProviderTransportKind, TurnItem, TurnItemType,
};
use pioneer_provider::{
    ChatRequest, Provider, ProviderFailureClassification, ProviderTermination,
    ProviderTimeoutPolicy, ProviderToolCall, StreamChunk, TokenUsage,
};
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

fn total_token_usage(usage: Option<&TokenUsage>) -> Option<u64> {
    let usage = usage?;
    if usage.input_tokens.is_none() && usage.output_tokens.is_none() {
        return None;
    }

    Some(
        usage
            .input_tokens
            .unwrap_or_default()
            .saturating_add(usage.output_tokens.unwrap_or_default()),
    )
}

pub(super) async fn request_agent_round(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    force_non_stream: bool,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
) -> Result<AgentRoundResponse, ChatTurnError> {
    if provider.capabilities().streaming && !force_non_stream {
        let provider_name = provider.name().to_owned();
        let model_name = request.model.clone();

        let target = FailureTarget::new(thinking_item_id, TurnItemType::Reasoning);
        let mut stream = provider.stream_chat(request).await.map_err(|error| {
            adapter_error_for_target(
                target,
                provider.as_ref(),
                model_name.as_str(),
                ProviderTransportKind::Stream,
                ProviderFailureStage::Connect,
                "provider stream error",
                &error,
            )
        })?;

        let mut full_text = String::new();
        let mut full_reasoning = String::new();
        let mut tool_calls = Vec::new();
        let mut provider_replay_state = None;
        let mut termination = None;
        let mut seen_any_chunk = false;

        while let Some(chunk) = read_next_stream_chunk(
            &mut stream,
            &mut seen_any_chunk,
            target,
            provider,
            model_name.as_str(),
            provider_timeout_policy,
        )
        .await?
        {
            if chunk.is_final {
                termination = chunk.termination;
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

            validate_provider_tool_calls(
                chunk.tool_calls.as_slice(),
                target,
                provider_name.as_str(),
                model_name.as_str(),
                ProviderTransportKind::Stream,
            )?;
            for tool_call in chunk.tool_calls {
                upsert_tool_call(&mut tool_calls, tool_call);
            }
            if chunk.provider_replay_state.is_some() {
                provider_replay_state = chunk.provider_replay_state;
            }
        }

        let termination = require_round_termination(
            termination,
            tool_calls.as_slice(),
            target,
            provider_name.as_str(),
            model_name.as_str(),
            ProviderTransportKind::Stream,
        )?;
        return Ok(AgentRoundResponse {
            text: full_text,
            reasoning: full_reasoning,
            tool_calls,
            provider_replay_state,
            provider_token_count: None,
            termination,
        });
    }

    let model_name = request.model.clone();

    let response = provider.chat(request).await.map_err(|error| {
        adapter_error_for_target(
            FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
            provider.as_ref(),
            model_name.as_str(),
            ProviderTransportKind::NonStream,
            ProviderFailureStage::Connect,
            "provider chat error",
            &error,
        )
    })?;

    let provider_token_count = total_token_usage(response.usage.as_ref());
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

    let termination = require_round_termination(
        Some(response.termination),
        response.tool_calls.as_slice(),
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
        provider.name(),
        model_name.as_str(),
        ProviderTransportKind::NonStream,
    )?;
    Ok(AgentRoundResponse {
        text: response.text,
        reasoning,
        tool_calls: response.tool_calls,
        provider_replay_state: response.provider_replay_state,
        provider_token_count,
        termination,
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
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
) -> Result<String, ChatTurnError> {
    let provider_name = provider.name().to_owned();
    let model_name = request.model.clone();

    let connect_target = FailureTarget::new(thinking_item_id, TurnItemType::Reasoning);
    let mut stream = provider.stream_chat(request).await.map_err(|error| {
        adapter_error_for_target(
            connect_target,
            provider.as_ref(),
            model_name.as_str(),
            ProviderTransportKind::Stream,
            ProviderFailureStage::Connect,
            "provider stream error",
            &error,
        )
    })?;

    let mut full_text = String::new();
    let mut reasoning_parts = Vec::new();
    let mut message_started = false;
    let mut stream_tool_calls = Vec::new();
    let mut termination = None;
    let mut seen_any_chunk = false;

    while let Some(chunk) = read_next_stream_chunk(
        &mut stream,
        &mut seen_any_chunk,
        response_stream_target(message_started, thinking_item_id, message_item_id),
        provider,
        model_name.as_str(),
        provider_timeout_policy,
    )
    .await?
    {
        let StreamChunk {
            delta,
            reasoning_delta,
            tool_calls,
            is_final,
            provider_replay_state: _,
            termination: chunk_termination,
        } = chunk;

        if is_final {
            termination = chunk_termination;
            break;
        }

        validate_provider_tool_calls(
            tool_calls.as_slice(),
            response_stream_target(message_started, thinking_item_id, message_item_id),
            provider_name.as_str(),
            model_name.as_str(),
            ProviderTransportKind::Stream,
        )?;
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
                                phase: Default::default(),
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

    require_round_termination(
        termination,
        stream_tool_calls.as_slice(),
        response_stream_target(message_started, thinking_item_id, message_item_id),
        provider_name.as_str(),
        model_name.as_str(),
        ProviderTransportKind::Stream,
    )?;
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
                        pioneer_protocol::TurnItemExecutionClass::Standard,
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
                        pioneer_protocol::TurnItemExecutionClass::Standard,
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
                        phase: Default::default(),
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
        AgentDurableEvent::TurnFinalizationPrepared {
            notification: ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::AgentMessage {
                    id: message_item_id.to_owned(),
                    text: assistant_text.clone(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            generation: super::TURN_FINALIZATION_GENERATION,
            task_finalization_revision: None,
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
    let model_name = request.model.clone();

    let response = provider.chat(request).await.map_err(|error| {
        adapter_error_for_target(
            FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
            provider.as_ref(),
            model_name.as_str(),
            ProviderTransportKind::NonStream,
            ProviderFailureStage::Connect,
            "provider error",
            &error,
        )
    })?;

    require_round_termination(
        Some(response.termination.clone()),
        response.tool_calls.as_slice(),
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
        provider.name(),
        model_name.as_str(),
        ProviderTransportKind::NonStream,
    )?;
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
                    phase: Default::default(),
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
        AgentDurableEvent::TurnFinalizationPrepared {
            notification: ItemCompletedNotification {
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                item: TurnItem::AgentMessage {
                    id: message_item_id.to_owned(),
                    text: assistant_text.clone(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            generation: super::TURN_FINALIZATION_GENERATION,
            task_finalization_revision: None,
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

fn adapter_error_for_target(
    target: FailureTarget<'_>,
    provider: &dyn Provider,
    model: &str,
    transport: ProviderTransportKind,
    stage: ProviderFailureStage,
    prefix: &str,
    error: &anyhow::Error,
) -> ChatTurnError {
    provider_failure_error_with_classification(
        target.item_id,
        target.item_type,
        provider.name(),
        model,
        transport,
        stage,
        format!("{prefix}: {error}"),
        provider.classify_failure(error),
    )
}

fn require_round_termination(
    termination: Option<ProviderTermination>,
    tool_calls: &[ProviderToolCall],
    target: FailureTarget<'_>,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
) -> Result<ProviderTermination, ChatTurnError> {
    let termination = termination.ok_or_else(|| {
        provider_failure_error_with_classification(
            target.item_id,
            target.item_type,
            provider,
            model,
            transport,
            ProviderFailureStage::Finalize,
            "provider response ended without a terminal marker".to_owned(),
            Some(ProviderFailureClassification::new(
                ProviderFailureClass::StreamTruncated,
            )),
        )
    })?;

    validate_provider_tool_calls(tool_calls, target, provider, model, transport)?;

    let failure = match &termination {
        ProviderTermination::Complete if !tool_calls.is_empty() => Some((
            ProviderFailureClass::MalformedProviderRequest,
            "provider declared a complete text response while returning tool calls".to_owned(),
        )),
        ProviderTermination::ToolCalls if tool_calls.is_empty() => Some((
            ProviderFailureClass::MalformedProviderRequest,
            "provider declared tool calls but returned no complete tool call".to_owned(),
        )),
        ProviderTermination::Complete | ProviderTermination::ToolCalls => None,
        ProviderTermination::Length => Some((
            ProviderFailureClass::MaxOutputTokens,
            "provider stopped because the output token limit was reached".to_owned(),
        )),
        ProviderTermination::ContentFiltered | ProviderTermination::Safety => Some((
            ProviderFailureClass::ProviderRejected,
            "provider stopped the response because of a content or safety policy".to_owned(),
        )),
        ProviderTermination::Cancelled => Some((
            ProviderFailureClass::ProviderRejected,
            "provider cancelled the response before completion".to_owned(),
        )),
        ProviderTermination::ProviderError => Some((
            ProviderFailureClass::Provider5xx,
            "provider terminated the response with an error".to_owned(),
        )),
        ProviderTermination::Unknown(reason) => Some((
            ProviderFailureClass::Unknown,
            format!("provider returned an unknown terminal reason `{reason}`"),
        )),
    };

    if let Some((class, message)) = failure {
        return Err(provider_failure_error_with_classification(
            target.item_id,
            target.item_type,
            provider,
            model,
            transport,
            ProviderFailureStage::Finalize,
            message,
            Some(ProviderFailureClassification::new(class)),
        ));
    }

    Ok(termination)
}

fn validate_provider_tool_calls(
    tool_calls: &[ProviderToolCall],
    target: FailureTarget<'_>,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
) -> Result<(), ChatTurnError> {
    let mut provider_call_ids = std::collections::HashSet::with_capacity(tool_calls.len());
    for call in tool_calls {
        let malformed = call.id.trim().is_empty()
            || call.name.trim().is_empty()
            || serde_json::from_str::<serde_json::Value>(call.arguments.as_str()).is_err()
            || !provider_call_ids.insert(call.id.as_str());
        if malformed {
            return Err(provider_failure_error_with_classification(
                target.item_id,
                target.item_type,
                provider,
                model,
                transport,
                ProviderFailureStage::Finalize,
                "provider returned an incomplete or duplicate tool-call identity".to_owned(),
                Some(ProviderFailureClassification::new(
                    ProviderFailureClass::MalformedProviderRequest,
                )),
            ));
        }
    }
    Ok(())
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
    provider: &Arc<dyn Provider>,
    model_name: &str,
    provider_timeout_policy: ProviderTimeoutPolicy,
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
        provider_timeout_policy.inter_chunk_idle_timeout
    } else {
        provider_timeout_policy.first_chunk_timeout
    };

    let next_chunk = timeout(wait, stream.next()).await.map_err(|_| {
        stream_error_for_target(
            target,
            provider.name(),
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
        adapter_error_for_target(
            target,
            provider.as_ref(),
            model_name,
            ProviderTransportKind::Stream,
            ProviderFailureStage::MidStream,
            "stream chunk error",
            &error,
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
    provider_failure_error_with_classification(
        item_id,
        item_type,
        provider,
        model,
        transport,
        stage,
        error_message,
        None,
    )
}

fn provider_failure_error_with_classification(
    item_id: &str,
    item_type: TurnItemType,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
    stage: ProviderFailureStage,
    error_message: String,
    classification: Option<ProviderFailureClassification>,
) -> ChatTurnError {
    let lower = error_message.to_ascii_lowercase();
    let inferred_http_status = extract_http_status(error_message.as_str());
    let inferred_retry_after_ms = extract_retry_after_ms(lower.as_str());
    let inferred_provider_code = extract_provider_code(error_message.as_str());
    let inferred_class = classify_provider_failure_message(error_message.as_str(), stage);
    let (class, http_status, provider_code, retry_after_ms) = match classification {
        Some(classification) => (
            classification.class,
            classification.http_status.or(inferred_http_status),
            classification.provider_code.or(inferred_provider_code),
            classification.retry_after_ms.or(inferred_retry_after_ms),
        ),
        None => (
            inferred_class,
            inferred_http_status,
            inferred_provider_code,
            inferred_retry_after_ms,
        ),
    };
    let is_recoverable_hint = provider_failure_class_is_recoverable(class);

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

fn provider_failure_class_is_recoverable(class: ProviderFailureClass) -> bool {
    matches!(
        class,
        ProviderFailureClass::NetworkTransient
            | ProviderFailureClass::RateLimit
            | ProviderFailureClass::Provider5xx
            | ProviderFailureClass::AuthExpired
            | ProviderFailureClass::AuthOrPermission
            | ProviderFailureClass::ModelNotFound
            | ProviderFailureClass::PromptTooLong
            | ProviderFailureClass::ContextTooLarge
            | ProviderFailureClass::MaxOutputTokens
            | ProviderFailureClass::StreamStall
            | ProviderFailureClass::StreamTruncated
            | ProviderFailureClass::EmptyResponse
            | ProviderFailureClass::UnsupportedImageInput
            | ProviderFailureClass::UnsupportedToolCalling
            | ProviderFailureClass::UnsupportedStreaming
            | ProviderFailureClass::PermissionDenied
    )
}

pub(crate) fn classify_provider_failure_message(
    error_message: &str,
    stage: ProviderFailureStage,
) -> ProviderFailureClass {
    let lower = error_message.to_ascii_lowercase();
    let http_status = extract_http_status(error_message);
    let provider_code = extract_provider_code(error_message);
    classify_provider_failure_class(lower.as_str(), stage, http_status, provider_code.as_deref())
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
        || message_lower.contains("context too long")
        || message_lower.contains("context window")
        || http_status == Some(413)
    {
        return ProviderFailureClass::ContextTooLarge;
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
        return ProviderFailureClass::UnsupportedImageInput;
    }
    if is_tool_calling_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedToolCalling;
    }
    if is_streaming_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedStreaming;
    }
    if is_unsupported_parameter(message_lower, provider_code) {
        return ProviderFailureClass::UnsupportedParameter;
    }
    if is_generic_capability_mismatch(message_lower) {
        return ProviderFailureClass::UnsupportedCapability;
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
        return ProviderFailureClass::AuthOrPermission;
    }
    if is_malformed_provider_request(message_lower, provider_code) {
        return ProviderFailureClass::MalformedProviderRequest;
    }
    if http_status == Some(400)
        || message_lower.contains("invalid request")
        || message_lower.contains("bad request")
    {
        return ProviderFailureClass::ProviderRejected;
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

fn is_tool_calling_capability_mismatch(message_lower: &str) -> bool {
    (message_lower.contains("tool call")
        || message_lower.contains("tool use")
        || message_lower.contains("function call")
        || message_lower.contains("tools"))
        && (message_lower.contains("does not support")
            || message_lower.contains("not support")
            || message_lower.contains("unsupported")
            || message_lower.contains("no endpoints found"))
}

fn is_streaming_capability_mismatch(message_lower: &str) -> bool {
    message_lower.contains("stream")
        && (message_lower.contains("does not support")
            || message_lower.contains("not support")
            || message_lower.contains("unsupported")
            || message_lower.contains("streaming disabled"))
}

fn is_unsupported_parameter(message_lower: &str, provider_code: Option<&str>) -> bool {
    provider_code
        .map(|value| {
            value.contains("unsupported_parameter")
                || value.contains("unknown_parameter")
                || value.contains("unrecognized_parameter")
        })
        .unwrap_or(false)
        || message_lower.contains("unsupported parameter")
        || message_lower.contains("unknown parameter")
        || message_lower.contains("unrecognized parameter")
        || message_lower.contains("unrecognized request argument")
        || message_lower.contains("extra inputs are not permitted")
}

fn is_generic_capability_mismatch(message_lower: &str) -> bool {
    (message_lower.contains("does not support")
        || message_lower.contains("not support")
        || message_lower.contains("unsupported"))
        && (message_lower.contains("capability")
            || message_lower.contains("feature")
            || message_lower.contains("modality")
            || message_lower.contains("endpoint"))
}

fn is_malformed_provider_request(message_lower: &str, provider_code: Option<&str>) -> bool {
    provider_code
        .map(|value| {
            value.contains("invalid_request_error")
                || value.contains("invalid_request")
                || value.contains("bad_request")
        })
        .unwrap_or(false)
        && (message_lower.contains("schema")
            || message_lower.contains("malformed")
            || message_lower.contains("invalid json")
            || message_lower.contains("parse"))
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
    fn openrouter_image_input_endpoint_error_is_recoverable_capability_rejection() {
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

        assert_eq!(failure.class, ProviderFailureClass::UnsupportedImageInput);
        assert_eq!(failure.http_status, Some(404));
        assert!(failure.is_recoverable_hint);
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
    fn provider_400_bad_request_is_non_retryable_provider_rejection() {
        let error = r#"provider stream error: API error (400 Bad Request): {"error":{"message":"bad request","code":400}}"#;

        let ChatTurnError::ProviderFailure { failure, .. } = provider_failure_error(
            "reasoning_item",
            TurnItemType::Reasoning,
            "openrouter",
            "minimax/minimax-m3",
            ProviderTransportKind::Stream,
            ProviderFailureStage::MidStream,
            error.to_owned(),
        ) else {
            panic!("expected provider failure");
        };

        assert_eq!(failure.class, ProviderFailureClass::ProviderRejected);
        assert_eq!(failure.http_status, Some(400));
        assert!(!failure.is_recoverable_hint);
    }

    #[test]
    fn unsupported_streaming_maps_to_provider_neutral_class() {
        assert_eq!(
            classify_provider_failure_class(
                "provider error: this model does not support streaming",
                ProviderFailureStage::Connect,
                Some(400),
                None,
            ),
            ProviderFailureClass::UnsupportedStreaming
        );
    }

    #[test]
    fn unsupported_parameter_maps_to_provider_neutral_class() {
        assert_eq!(
            classify_provider_failure_class(
                "provider error: unrecognized request argument: reasoning_effort",
                ProviderFailureStage::Connect,
                Some(400),
                Some("unsupported_parameter"),
            ),
            ProviderFailureClass::UnsupportedParameter
        );
    }

    #[test]
    fn unsupported_reasoning_parameter_preserves_provider_error_text() {
        let message = "provider error: unrecognized request argument: reasoning_effort".to_owned();

        let ChatTurnError::ProviderFailure { failure, .. } = provider_failure_error(
            "reasoning_item",
            TurnItemType::Reasoning,
            "openai",
            "gpt-5.5",
            ProviderTransportKind::NonStream,
            ProviderFailureStage::Connect,
            message.clone(),
        ) else {
            panic!("expected provider failure");
        };

        assert_eq!(failure.class, ProviderFailureClass::UnsupportedParameter);
        assert_eq!(failure.message.as_deref(), Some(message.as_str()));
        assert!(!failure.is_recoverable_hint);
    }

    #[test]
    fn adapter_classification_overrides_provider_neutral_fallback() {
        let ChatTurnError::ProviderFailure { failure, .. } =
            provider_failure_error_with_classification(
                "reasoning_item",
                TurnItemType::Reasoning,
                "future-provider",
                "future-model",
                ProviderTransportKind::Stream,
                ProviderFailureStage::Connect,
                "provider error: HTTP 400 opaque rejection".to_owned(),
                Some(ProviderFailureClassification {
                    class: ProviderFailureClass::UnsupportedStreaming,
                    http_status: Some(400),
                    provider_code: Some("streaming_not_supported".to_owned()),
                    retry_after_ms: None,
                }),
            )
        else {
            panic!("expected provider failure");
        };

        assert_eq!(failure.class, ProviderFailureClass::UnsupportedStreaming);
        assert_eq!(
            failure.provider_code.as_deref(),
            Some("streaming_not_supported")
        );
        assert!(failure.is_recoverable_hint);
    }

    #[test]
    fn context_length_maps_to_context_too_large() {
        assert_eq!(
            classify_provider_failure_class(
                "provider error: maximum context length exceeded",
                ProviderFailureStage::Connect,
                Some(400),
                None,
            ),
            ProviderFailureClass::ContextTooLarge
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

    #[test]
    fn output_limit_is_not_accepted_as_a_successful_round() {
        let error = require_round_termination(
            Some(ProviderTermination::Length),
            &[],
            FailureTarget::new("reasoning", TurnItemType::Reasoning),
            "provider",
            "model",
            ProviderTransportKind::Stream,
        )
        .unwrap_err();

        let ChatTurnError::ProviderFailure { failure, .. } = error else {
            panic!("expected provider failure");
        };
        assert_eq!(failure.class, ProviderFailureClass::MaxOutputTokens);
        assert!(failure.is_recoverable_hint);
    }

    #[test]
    fn eof_without_terminal_marker_is_stream_truncation_even_with_no_chunks() {
        let error = require_round_termination(
            None,
            &[],
            FailureTarget::new("reasoning", TurnItemType::Reasoning),
            "provider",
            "model",
            ProviderTransportKind::Stream,
        )
        .unwrap_err();

        let ChatTurnError::ProviderFailure { failure, .. } = error else {
            panic!("expected provider failure");
        };
        assert_eq!(failure.class, ProviderFailureClass::StreamTruncated);
    }

    #[test]
    fn tool_terminal_reason_requires_a_complete_tool_call() {
        let error = require_round_termination(
            Some(ProviderTermination::ToolCalls),
            &[],
            FailureTarget::new("reasoning", TurnItemType::Reasoning),
            "provider",
            "model",
            ProviderTransportKind::NonStream,
        )
        .unwrap_err();

        let ChatTurnError::ProviderFailure { failure, .. } = error else {
            panic!("expected provider failure");
        };
        assert_eq!(
            failure.class,
            ProviderFailureClass::MalformedProviderRequest
        );
    }

    #[test]
    fn duplicate_or_malformed_tool_calls_fail_before_execution() {
        for calls in [
            vec![
                ProviderToolCall {
                    id: "same".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{}".to_owned(),
                },
                ProviderToolCall {
                    id: "same".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{}".to_owned(),
                },
            ],
            vec![ProviderToolCall {
                id: "call".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{".to_owned(),
            }],
        ] {
            let error = require_round_termination(
                Some(ProviderTermination::ToolCalls),
                calls.as_slice(),
                FailureTarget::new("reasoning", TurnItemType::Reasoning),
                "provider",
                "model",
                ProviderTransportKind::Stream,
            )
            .unwrap_err();
            let ChatTurnError::ProviderFailure { failure, .. } = error else {
                panic!("expected provider failure");
            };
            assert_eq!(
                failure.class,
                ProviderFailureClass::MalformedProviderRequest
            );
        }
    }
}
