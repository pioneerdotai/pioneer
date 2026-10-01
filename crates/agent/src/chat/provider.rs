use super::{AgentRoundResponse, ChatTurnError};
use crate::AgentEventHub;
use futures_util::{Stream, StreamExt};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemStartedNotification, ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage,
    ProviderTransportKind, TurnItem, TurnItemType,
};
use pioneer_provider::failure::{
    classify_http_error_body_too_large, classify_provider_failure_class, extract_provider_code,
    extract_retry_after_ms,
};
use pioneer_provider::{
    ChatRequest, Provider, ProviderFailureClassification, ProviderResponseLimits,
    ProviderResponseTooLarge, ProviderTermination, ProviderTimeoutPolicy, ProviderToolCall,
    StreamChunk, TokenUsage,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::timeout;

struct NativeProviderRoundMetric {
    started: Instant,
    finished: bool,
}

impl NativeProviderRoundMetric {
    fn start() -> Self {
        Self {
            started: Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, outcome: pioneer_observability::NativeLifecycleOutcome) {
        self.finished = true;
        pioneer_observability::record_native_lifecycle_event(
            pioneer_observability::NativeLifecycleEventMetric {
                stage: pioneer_observability::NativeLifecycleStage::ProviderRound,
                outcome,
                provider_class: pioneer_observability::NativeProviderClass::Api,
                elapsed: Some(self.started.elapsed()),
            },
        );
    }
}

impl Drop for NativeProviderRoundMetric {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(pioneer_observability::NativeLifecycleOutcome::Failed);
        }
    }
}

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

fn bind_replay_to_response_target(
    replay: &mut Option<pioneer_provider::ProviderReplayState>,
    provider: &str,
    model: &str,
) {
    if let Some(replay) = replay.as_mut()
        && replay.provider == provider
    {
        replay.model = Some(model.to_owned());
    }
}

/// Persist the provider accumulator, including opaque state absent from UI text.
/// This acknowledged observation precedes failure/recovery and never becomes an
/// executable assistant/tool round. Cancellation still drops this owned future.
#[allow(clippy::too_many_arguments)]
async fn persist_failed_provider_observation(
    events: &AgentEventHub,
    thread: &str,
    turn: &str,
    item: &str,
    text: &str,
    reasoning: &str,
    calls: &[ProviderToolCall],
    replay: Option<&pioneer_provider::ProviderReplayState>,
    error: &ChatTurnError,
) -> Result<(), ChatTurnError> {
    if !matches!(error, ChatTurnError::ProviderFailure { .. })
        || (text.is_empty() && reasoning.is_empty() && calls.is_empty() && replay.is_none())
    {
        return Ok(());
    }
    let mut message = pioneer_provider::ChatMessage::assistant(text);
    message.reasoning_content = (!reasoning.is_empty()).then(|| reasoning.to_owned());
    message.tool_calls = (!calls.is_empty()).then(|| calls.to_vec());
    message.provider_replay_state = replay.cloned();
    super::persist_provider_history_message(
        events,
        thread,
        turn,
        item,
        &pioneer_provider::CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: item.into(),
            termination: ProviderTermination::ProviderError,
            message,
            calls: Vec::new(),
        },
    )
    .await
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
    pioneer_observability::turn_startup::dispatched(turn_id);
    let mut lifecycle_metric = NativeProviderRoundMetric::start();
    if provider.capabilities().streaming && !force_non_stream {
        let provider_name = provider.name().to_owned();
        let model_name = request.model.clone();

        let target = FailureTarget::new(thinking_item_id, TurnItemType::Reasoning);
        let pioneer_provider::ProviderStream {
            mut stream,
            diagnostics,
        } = {
            let _startup_part = pioneer_observability::turn_startup::stage(
                turn_id,
                pioneer_observability::turn_startup::Stage::ProviderConnect,
            );
            provider
                .stream_chat_with_diagnostics(request)
                .await
                .map_err(|error| {
                    adapter_error_for_target(
                        target,
                        provider.as_ref(),
                        model_name.as_str(),
                        ProviderTransportKind::Stream,
                        ProviderFailureStage::Connect,
                        "provider stream error",
                        &error,
                    )
                })?
        };

        let mut full_text = String::new();
        let mut full_reasoning = String::new();
        let mut tool_calls = Vec::new();
        let mut provider_replay_state = None;
        let mut termination = None;
        let mut usage: Option<pioneer_provider::TokenUsage> = None;
        let mut seen_any_chunk = false;
        let response_limits = ProviderResponseLimits::default();

        let received = async {
            while let Some(mut chunk) = read_next_stream_chunk(
                &mut stream,
                &mut seen_any_chunk,
                &diagnostics,
                target,
                provider,
                model_name.as_str(),
                provider_timeout_policy,
            )
            .await?
            {
                observe_startup_chunk(turn_id, &chunk);
                if let Err(error) = response_limits.validate_stream_chunk(&chunk) {
                    return Err(provider_response_limit_error(
                        target,
                        provider_name.as_str(),
                        model_name.as_str(),
                        ProviderTransportKind::Stream,
                        error,
                    ));
                }
                if let Some(snapshot) = &chunk.usage {
                    usage.get_or_insert_with(Default::default).update(snapshot);
                }
                if chunk.provider_replay_state.is_some() {
                    provider_replay_state = chunk.provider_replay_state.take();
                    bind_replay_to_response_target(
                        &mut provider_replay_state,
                        provider_name.as_str(),
                        model_name.as_str(),
                    );
                }
                if chunk.is_final {
                    termination = chunk.termination;
                    break;
                }

                validate_stream_append_limits(
                    &response_limits,
                    full_text.len(),
                    full_reasoning.len(),
                    chunk.delta.as_str(),
                    chunk.reasoning_delta.as_deref(),
                    target,
                    provider_name.as_str(),
                    model_name.as_str(),
                    ProviderTransportKind::Stream,
                )?;

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
                                payload: Some(
                                    serde_json::json!({"startup_output_kind":"reasoning"}),
                                ),
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
                if let Err(error) = response_limits.validate_accumulated(
                    full_text.as_str(),
                    full_reasoning.as_str(),
                    tool_calls.as_slice(),
                    provider_replay_state.as_ref(),
                ) {
                    return Err(provider_response_limit_error(
                        target,
                        provider_name.as_str(),
                        model_name.as_str(),
                        ProviderTransportKind::Stream,
                        error,
                    ));
                }
            }

            require_round_termination(
                termination,
                tool_calls.as_slice(),
                target,
                provider_name.as_str(),
                model_name.as_str(),
                ProviderTransportKind::Stream,
            )
        }
        .await;
        let termination = match received {
            Ok(termination) => termination,
            Err(error) => {
                persist_failed_provider_observation(
                    event_tx,
                    thread_id,
                    turn_id,
                    thinking_item_id,
                    &full_text,
                    &full_reasoning,
                    &tool_calls,
                    provider_replay_state.as_ref(),
                    &error,
                )
                .await?;
                return Err(error);
            }
        };
        lifecycle_metric.finish(pioneer_observability::NativeLifecycleOutcome::Succeeded);
        return Ok(AgentRoundResponse {
            text: full_text,
            reasoning: full_reasoning,
            tool_calls,
            provider_replay_state,
            provider_token_count: total_token_usage(usage.as_ref()),
            usage,
            termination,
        });
    }

    let model_name = request.model.clone();

    let mut response = provider.chat(request).await.map_err(|error| {
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
    bind_replay_to_response_target(
        &mut response.provider_replay_state,
        provider.name(),
        model_name.as_str(),
    );

    let startup_output = if !response.text.is_empty() {
        Some(pioneer_observability::turn_startup::Output::BufferedText)
    } else if response
        .reasoning_content
        .as_ref()
        .is_some_and(|s| !s.is_empty())
    {
        Some(pioneer_observability::turn_startup::Output::BufferedReasoning)
    } else if !response.tool_calls.is_empty() {
        Some(pioneer_observability::turn_startup::Output::ToolCall)
    } else {
        None
    };
    if let Some(output) = startup_output {
        pioneer_observability::turn_startup::runtime_output(turn_id, output);
    }

    let response_limits = ProviderResponseLimits::default();
    if let Err(error) = response_limits.validate_chat_response(&response) {
        return Err(provider_response_limit_error(
            FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
            provider.name(),
            model_name.as_str(),
            ProviderTransportKind::NonStream,
            error,
        ));
    }

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
                    payload: Some(serde_json::json!({"startup_output_kind":"buffered_reasoning"})),
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
    lifecycle_metric.finish(pioneer_observability::NativeLifecycleOutcome::Succeeded);
    Ok(AgentRoundResponse {
        text: response.text,
        reasoning,
        tool_calls: response.tool_calls,
        provider_replay_state: response.provider_replay_state,
        provider_token_count,
        usage: response.usage,
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
) -> Result<(String, Option<pioneer_provider::TokenUsage>), ChatTurnError> {
    pioneer_observability::turn_startup::dispatched(turn_id);
    let mut lifecycle_metric = NativeProviderRoundMetric::start();
    let provider_name = provider.name().to_owned();
    let model_name = request.model.clone();

    let connect_target = FailureTarget::new(thinking_item_id, TurnItemType::Reasoning);
    let pioneer_provider::ProviderStream {
        mut stream,
        diagnostics,
    } = {
        let _startup_part = pioneer_observability::turn_startup::stage(
            turn_id,
            pioneer_observability::turn_startup::Stage::ProviderConnect,
        );
        provider
            .stream_chat_with_diagnostics(request)
            .await
            .map_err(|error| {
                adapter_error_for_target(
                    connect_target,
                    provider.as_ref(),
                    model_name.as_str(),
                    ProviderTransportKind::Stream,
                    ProviderFailureStage::Connect,
                    "provider stream error",
                    &error,
                )
            })?
    };

    let mut full_text = String::new();
    let mut reasoning_parts = String::new();
    let mut message_started = false;
    let mut stream_tool_calls = Vec::new();
    let mut termination = None;
    let mut usage: Option<pioneer_provider::TokenUsage> = None;
    let mut seen_any_chunk = false;
    let response_limits = ProviderResponseLimits::default();

    let mut provider_replay_state = None;
    let received: Result<(), ChatTurnError> = async {
        while let Some(chunk) = read_next_stream_chunk(
            &mut stream,
            &mut seen_any_chunk,
            &diagnostics,
            response_stream_target(message_started, thinking_item_id, message_item_id),
            provider,
            model_name.as_str(),
            provider_timeout_policy,
        )
        .await?
        {
            observe_startup_chunk(turn_id, &chunk);
            if let Err(error) = response_limits.validate_stream_chunk(&chunk) {
                return Err(provider_response_limit_error(
                    response_stream_target(message_started, thinking_item_id, message_item_id),
                    provider_name.as_str(),
                    model_name.as_str(),
                    ProviderTransportKind::Stream,
                    error,
                ));
            }
            let StreamChunk {
                usage: chunk_usage,
                delta,
                reasoning_delta,
                tool_calls,
                is_final,
                provider_replay_state: chunk_replay,
                termination: chunk_termination,
            } = chunk;

            if let Some(snapshot) = &chunk_usage {
                usage.get_or_insert_with(Default::default).update(snapshot);
            }
            if chunk_replay.is_some() {
                provider_replay_state = chunk_replay;
                bind_replay_to_response_target(
                    &mut provider_replay_state,
                    provider_name.as_str(),
                    model_name.as_str(),
                );
            }
            if is_final {
                termination = chunk_termination;
                break;
            }

            let target = response_stream_target(message_started, thinking_item_id, message_item_id);
            validate_stream_append_limits(
                &response_limits,
                full_text.len(),
                reasoning_parts.len(),
                delta.as_str(),
                reasoning_delta.as_deref(),
                target,
                provider_name.as_str(),
                model_name.as_str(),
                ProviderTransportKind::Stream,
            )?;

            validate_provider_tool_calls(
                tool_calls.as_slice(),
                target,
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
                reasoning_parts.push_str(reasoning.as_str());

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
                            payload: Some(serde_json::json!({"startup_output_kind":"reasoning"})),
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
                    let reasoning_text = reasoning_parts.clone();

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

            if let Err(error) = response_limits.validate_accumulated(
                full_text.as_str(),
                reasoning_parts.as_str(),
                stream_tool_calls.as_slice(),
                None,
            ) {
                return Err(provider_response_limit_error(
                    response_stream_target(message_started, thinking_item_id, message_item_id),
                    provider_name.as_str(),
                    model_name.as_str(),
                    ProviderTransportKind::Stream,
                    error,
                ));
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
        Ok(())
    }
    .await;
    if let Err(error) = received {
        persist_failed_provider_observation(
            event_tx,
            thread_id,
            turn_id,
            thinking_item_id,
            &full_text,
            &reasoning_parts,
            &stream_tool_calls,
            provider_replay_state.as_ref(),
            &error,
        )
        .await?;
        return Err(error);
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
        let reasoning_text = reasoning_parts;

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

    lifecycle_metric.finish(pioneer_observability::NativeLifecycleOutcome::Succeeded);
    Ok((assistant_text, usage))
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
) -> Result<(String, Option<pioneer_provider::TokenUsage>), ChatTurnError> {
    pioneer_observability::turn_startup::dispatched(turn_id);
    let mut lifecycle_metric = NativeProviderRoundMetric::start();
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

    if !response.text.is_empty() {
        pioneer_observability::turn_startup::runtime_output(
            turn_id,
            pioneer_observability::turn_startup::Output::BufferedText,
        );
    } else if response
        .reasoning_content
        .as_ref()
        .is_some_and(|s| !s.is_empty())
    {
        pioneer_observability::turn_startup::runtime_output(
            turn_id,
            pioneer_observability::turn_startup::Output::BufferedReasoning,
        );
    }
    let response_limits = ProviderResponseLimits::default();
    if let Err(error) = response_limits.validate_chat_response(&response) {
        return Err(provider_response_limit_error(
            FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
            provider.name(),
            model_name.as_str(),
            ProviderTransportKind::NonStream,
            error,
        ));
    }

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
                    payload: Some(serde_json::json!({"startup_output_kind":"buffered_text"})),
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

    lifecycle_metric.finish(pioneer_observability::NativeLifecycleOutcome::Succeeded);
    Ok((assistant_text, response.usage))
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

fn adapter_error_for_target(
    target: FailureTarget<'_>,
    provider: &dyn Provider,
    model: &str,
    transport: ProviderTransportKind,
    stage: ProviderFailureStage,
    prefix: &str,
    error: &anyhow::Error,
) -> ChatTurnError {
    let classification = if let Some(http_error) =
        error.downcast_ref::<pioneer_provider::ProviderHttpErrorBodyTooLarge>()
    {
        Some(classify_http_error_body_too_large(http_error.status))
    } else if error.downcast_ref::<ProviderResponseTooLarge>().is_some() {
        Some(ProviderFailureClassification::new(
            ProviderFailureClass::ProviderRejected,
        ))
    } else {
        provider.classify_failure(error)
    };
    provider_failure_error_with_classification(
        target.item_id,
        target.item_type,
        provider.name(),
        model,
        transport,
        stage,
        format!("{prefix}: {error}"),
        classification,
    )
}

fn provider_response_limit_error(
    target: FailureTarget<'_>,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
    error: ProviderResponseTooLarge,
) -> ChatTurnError {
    provider_failure_error_with_classification(
        target.item_id,
        target.item_type,
        provider,
        model,
        transport,
        ProviderFailureStage::Finalize,
        error.to_string(),
        Some(ProviderFailureClassification::new(
            ProviderFailureClass::ProviderRejected,
        )),
    )
}

fn validate_stream_append_limits(
    limits: &ProviderResponseLimits,
    text_bytes: usize,
    reasoning_bytes: usize,
    text_delta: &str,
    reasoning_delta: Option<&str>,
    target: FailureTarget<'_>,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
) -> Result<(), ChatTurnError> {
    if let Err(error) =
        limits.validate_text_bytes(text_bytes.saturating_add(text_delta.len()), "response_text")
    {
        return Err(provider_response_limit_error(
            target, provider, model, transport, error,
        ));
    }
    if let Some(reasoning_delta) = reasoning_delta
        && let Err(error) =
            limits.validate_reasoning_bytes(reasoning_bytes.saturating_add(reasoning_delta.len()))
    {
        return Err(provider_response_limit_error(
            target, provider, model, transport, error,
        ));
    }
    Ok(())
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
    diagnostics: &pioneer_provider::ProviderStreamDiagnostics,
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
        let mut classification =
            ProviderFailureClassification::new(ProviderFailureClass::StreamStall);
        classification.request_id = diagnostics.request_id();
        provider_failure_error_with_classification(
            target.item_id,
            target.item_type,
            provider.name(),
            model_name,
            ProviderTransportKind::Stream,
            stage,
            "stream stall: chunk timeout exceeded".to_owned(),
            Some(classification),
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

#[cfg(test)]
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
    let error_reason = classification.as_ref().and_then(|value| value.error_reason);
    let request_id = classification
        .as_ref()
        .and_then(|value| value.request_id.clone());
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
            error_reason,
            request_id,
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn failure_from_overridden_endpoint(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
    ) -> ProviderFailureDetails {
        failure_from_overridden_endpoint_body(provider_name, status, message, stream_request, None)
            .await
    }

    async fn failure_from_overridden_endpoint_body(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
        response_body: Option<String>,
    ) -> ProviderFailureDetails {
        failure_from_local_endpoint(
            provider_name,
            status,
            message,
            stream_request,
            response_body,
            true,
            "",
            false,
        )
        .await
    }

    // Direct adapter fixtures exercise the standard endpoint diagnostic path,
    // without the registry's extra custom-endpoint redaction masking leaks.
    async fn failure_from_local_endpoint(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
        response_body: Option<String>,
        redacted: bool,
        headers: &'static str,
        truncated_transport: bool,
    ) -> ProviderFailureDetails {
        failure_from_local_response(
            provider_name,
            status,
            message,
            stream_request,
            response_body.map(String::into_bytes),
            redacted,
            headers,
            truncated_transport,
        )
        .await
    }

    async fn failure_from_local_response(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
        response_body: Option<Vec<u8>>,
        redacted: bool,
        headers: &'static str,
        truncated_transport: bool,
    ) -> ProviderFailureDetails {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            failure_from_local_response_inner(
                provider_name,
                status,
                message,
                stream_request,
                response_body,
                redacted,
                headers,
                truncated_transport,
            ),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "local provider failure fixture exceeded its deadline: provider={provider_name}, status={status}, stream={stream_request}, redacted={redacted}, truncated={truncated_transport}"
            )
        })
    }

    async fn failure_from_local_response_inner(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
        response_body: Option<Vec<u8>>,
        redacted: bool,
        headers: &'static str,
        truncated_transport: bool,
    ) -> ProviderFailureDetails {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_address = listener.local_addr().unwrap();
        let secret = "override-private-token";
        let url = format!("http://{}/{secret}/v1", listener.local_addr().unwrap());
        let echoed_url = url.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Read the complete request before closing the connection. Closing
            // with unread request bytes can reset the socket on Linux, obscuring
            // the response failure this fixture is intended to exercise.
            let mut request = Vec::new();
            loop {
                let mut buffer = [0u8; 2048];
                let count = stream.read(&mut buffer).await.unwrap();
                assert!(count > 0, "fixture request ended before its body");
                assert!(request.len() + count <= 8192, "fixture request too large");
                request.extend_from_slice(&buffer[..count]);
                if let Some(start) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                    let header = std::str::from_utf8(&request[..start]).unwrap();
                    let length = header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .expect("fixture request Content-Length");
                    if request.len() >= start + 4 + length {
                        break;
                    }
                }
            }
            let body = response_body.unwrap_or_else(|| {
                serde_json::json!({
                    "error": {"message": format!("{message} at {echoed_url}")}
                })
                .to_string()
                .into_bytes()
            });
            stream.write_all(format!(
                "HTTP/1.1 {status} Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n",
                body.len() + usize::from(truncated_transport) * 100
            ).as_bytes()).await.unwrap();
            // A bounded reader can close immediately after seeing Content-Length.
            let _ = stream.write_all(&body).await;
        });
        let registry = pioneer_provider::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("key".into()), |_, _| Ok(None),
            move |_, _| Ok(Some(url.clone())), ProviderTimeoutPolicy::default(),
        );
        let provider: Arc<dyn Provider> = if redacted {
            registry
                .get_or_create_for_workspace("fixture-workspace", provider_name)
                .unwrap()
        } else {
            assert_eq!(provider_name, "openrouter");
            Arc::new(
                pioneer_provider::providers::OpenRouterProvider::with_base_url(
                    "key",
                    format!("http://{}/v1", listener_address),
                ),
            )
        };
        let request = ChatRequest {
            model: "fixture".into(),
            messages: vec![pioneer_provider::ChatMessage::user("hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        let error = if stream_request {
            match provider.stream_chat(request).await {
                Err(error) => error,
                Ok(mut stream) => {
                    use futures_util::StreamExt;
                    loop {
                        if let Err(error) = stream.next().await.expect("error event expected") {
                            break error;
                        }
                    }
                }
            }
        } else {
            provider.chat(request).await.err().unwrap()
        };
        server.await.unwrap();
        assert_eq!(error.chain().count(), 1);
        assert!(!error.chain().any(
            |cause| cause.is::<std::string::FromUtf8Error>() || cause.is::<serde_json::Error>()
        ));
        assert!(!format!("{error:#?}").contains("private_fixture"));
        assert!(!format!("{error:#?}").contains(secret));
        assert!(
            !error
                .chain()
                .any(|cause| format!("{cause:?}").contains(secret))
        );
        let ChatTurnError::ProviderFailure { failure, .. } = adapter_error_for_target(
            FailureTarget::new("item", TurnItemType::Reasoning),
            provider.as_ref(),
            "fixture",
            if stream_request {
                ProviderTransportKind::Stream
            } else {
                ProviderTransportKind::NonStream
            },
            if status == 200 && stream_request {
                ProviderFailureStage::MidStream
            } else {
                ProviderFailureStage::Connect
            },
            "provider request failed",
            &error,
        ) else {
            panic!("expected provider failure")
        };
        assert!(!serde_json::to_string(&failure).unwrap().contains(secret));
        failure
    }

    #[tokio::test]
    async fn openrouter_agent_deadlines_keep_request_owned_ids_without_progress_chunks() {
        use pioneer_protocol::RecoveryDiagnostic;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for agent_round in [false, true] {
            for custom_endpoint in [false, true] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}/v1", listener.local_addr().unwrap());
                let server = tokio::spawn(async move {
                    let mut connections = tokio::task::JoinSet::new();
                    for _ in 0..4 {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        connections.spawn(async move {
                            let mut request = Vec::new();
                            let model = loop {
                                let mut buffer = [0; 2048];
                                let count = socket.read(&mut buffer).await.unwrap();
                                assert!(count > 0 && request.len() + count <= 8192);
                                request.extend_from_slice(&buffer[..count]);
                                if let Some(start) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                                    if let Ok(body) = serde_json::from_slice::<serde_json::Value>(&request[start + 4..]) {
                                        break body["model"].as_str().unwrap().to_owned();
                                    }
                                }
                            };
                            let header = if model == "none" { "" } else { "X-Generation-Id: gen-header\r\n" };
                            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\n{header}\r\n").as_bytes()).await.unwrap();
                            if model == "body" {
                                socket.write_all(b"data: {\"id\":\"gen-body\",\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n").await.unwrap();
                            } else if model == "id_only" {
                                socket.write_all(b"data: {\"id\":\"gen-id_only\",\"choices\":[]}\n\ndata: {\"id\":\"https://private_fixture\",\"choices\":[]}\n\n").await.unwrap();
                            }
                            // Stay open until the agent deadline cancels this request.
                            let mut buffer = [0; 1];
                            let _ = socket.read(&mut buffer).await;
                        });
                    }
                    while let Some(result) = connections.join_next().await {
                        result.unwrap();
                    }
                });
                let mut policy = ProviderTimeoutPolicy::default();
                policy.first_chunk_timeout = std::time::Duration::from_millis(500);
                policy.inter_chunk_idle_timeout = std::time::Duration::from_millis(100);
                let registry = if custom_endpoint {
                    pioneer_provider::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                        |_, _| Ok("key".into()), |_, _| Ok(None),
                        move |_, _| Ok(Some(url.clone())), policy,
                    )
                } else {
                    // Exercise the authority wrapper with its standard-endpoint
                    // behavior as well as the custom-endpoint redaction above.
                    pioneer_provider::ProviderRegistry::with_provider("openrouter",
                        Arc::new(pioneer_provider::providers::OpenRouterProvider::with_base_url_and_timeout_policy("key", url, policy)))
                };
                let provider = registry
                    .get_or_create_for_workspace("fixture", "openrouter")
                    .unwrap();
                // Same provider, concurrent requests, including a request with no ID.
                let calls = ["header", "body", "id_only", "none"]
                    .into_iter()
                    .map(|model| {
                        let provider = provider.clone();
                        async move {
                            let hub = AgentEventHub::new();
                            let mut events = hub.take_durable_receiver().await.unwrap();
                            let acknowledger = tokio::spawn(async move {
                                while events.recv().await.is_some() {
                                    events.acknowledge_last(Ok(()));
                                }
                            });
                            let request = ChatRequest {
                                model: model.to_owned(),
                                messages: vec![pioneer_provider::ChatMessage::user("hello")],
                                temperature: None,
                                max_tokens: None,
                                tools: None,
                                tool_choice: None,
                                parallel_tool_calls: None,
                                reasoning: None,
                                compiled_prompt: None,
                            };
                            let result = if agent_round {
                                request_agent_round(
                                    &provider, request, "ws", "thread", model, "thinking", false,
                                    policy, &hub,
                                )
                                .await
                                .map(|_| ())
                            } else {
                                stream_provider_response(
                                    &provider, request, "ws", "thread", model, "thinking",
                                    "message", policy, &hub,
                                )
                                .await
                                .map(|_| ())
                            };
                            acknowledger.abort();
                            let _ = acknowledger.await;
                            let Err(ChatTurnError::ProviderFailure { failure, .. }) = result else {
                                panic!("agent timeout expected");
                            };
                            assert_eq!(failure.class, ProviderFailureClass::StreamStall);
                            assert_eq!(
                                failure.stage,
                                if model == "body" {
                                    ProviderFailureStage::MidStream
                                } else {
                                    ProviderFailureStage::FirstChunk
                                }
                            );
                            assert_eq!(failure.transport, ProviderTransportKind::Stream);
                            assert_eq!(failure.http_status, None);
                            assert!(failure.error_reason.is_none());
                            assert!(failure.is_recoverable_hint);
                            let expected = match model {
                                "header" => Some("gen-header"),
                                "body" => Some("gen-body"),
                                "id_only" => Some("gen-id_only"),
                                _ => None,
                            };
                            let diagnostic = RecoveryDiagnostic::provider(&failure);
                            assert_eq!(
                                diagnostic
                                    .last_failure
                                    .as_ref()
                                    .unwrap()
                                    .request_id
                                    .clone()
                                    .map(String::from)
                                    .as_deref(),
                                expected
                            );
                            if let Some(id) = expected {
                                assert!(!diagnostic.public_message().contains(id));
                            }
                            assert!(
                                !serde_json::to_string(&failure)
                                    .unwrap()
                                    .contains("private_fixture")
                            );
                        }
                    });
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    futures_util::future::join_all(calls),
                )
                .await
                .unwrap();
                server.abort();
                let _ = server.await;
            }
        }
    }

    #[tokio::test]
    async fn openrouter_post_header_failures_keep_facts_and_discard_owned_response_bytes() {
        use pioneer_protocol::RecoveryDiagnostic;
        for redacted in [false, true] {
            for (body, truncated, expected) in [
                (
                    vec![b'x'; 16 * 1024 + 1],
                    false,
                    ProviderFailureClass::Provider5xx,
                ),
                (
                    b"private_fixture incomplete response".to_vec(),
                    true,
                    if redacted {
                        ProviderFailureClass::NetworkTransient
                    } else {
                        ProviderFailureClass::Unknown
                    },
                ),
                (
                    [b"private_fixture ".as_slice(), &[0xff]].concat(),
                    false,
                    ProviderFailureClass::Unknown,
                ),
            ] {
                let failure = failure_from_local_response(
                    "openrouter",
                    502,
                    "",
                    false,
                    Some(body),
                    redacted,
                    "X-Generation-Id: gen-header\r\nRetry-After: 3\r\n",
                    truncated,
                )
                .await;
                assert_eq!(failure.class, expected);
                assert_eq!(failure.http_status, Some(502));
                assert_eq!(failure.retry_after_ms, Some(3000));
                assert!(failure.error_reason.is_none());
                let diagnostic = RecoveryDiagnostic::provider(&failure);
                let last = diagnostic.last_failure.unwrap();
                assert_eq!(
                    last.request_id.map(String::from).as_deref(),
                    Some("gen-header")
                );
                assert_eq!(last.retry_after_ms, Some(3000));
                assert_eq!(last.http_status, Some(502));
            }
            for (body, expected_id) in [
                ("private_fixture invalid JSON", "gen-header"),
                (
                    r#"{"id":"gen-body","choices":"private_fixture"}"#,
                    "gen-body",
                ),
                (
                    r#"{"id":"https://private_fixture","choices":[]}"#,
                    "gen-header",
                ),
                (r#"{"id":"gen-body","choices":[]}"#, "gen-body"),
                (
                    r#"{"id":"gen-body","choices":[{"message":{"content":""}}]}"#,
                    "gen-body",
                ),
            ] {
                let failure = failure_from_local_response(
                    "openrouter",
                    200,
                    "",
                    false,
                    Some(body.as_bytes().to_vec()),
                    redacted,
                    "X-Generation-Id: gen-header\r\nRetry-After: 3\r\n",
                    false,
                )
                .await;
                assert_eq!(failure.class, ProviderFailureClass::Unknown);
                assert_eq!(failure.http_status, None);
                assert!(failure.error_reason.is_none());
                assert_eq!(failure.retry_after_ms, Some(3000));
                let diagnostic = RecoveryDiagnostic::provider(&failure);
                assert!(!diagnostic.public_message().contains(expected_id));
                let last = diagnostic.last_failure.unwrap();
                assert_eq!(
                    last.request_id.map(String::from).as_deref(),
                    Some(expected_id)
                );
                assert_eq!(last.http_status, None);
                assert_eq!(last.retry_after_ms, Some(3000));
            }
        }
    }

    #[tokio::test]
    async fn standard_openrouter_error_envelopes_reach_recovery_without_raw_metadata() {
        use pioneer_protocol::{ProviderErrorReason, RecoveryDiagnostic};
        for stream in [false, true] {
            for metadata in [
                None,
                Some(
                    serde_json::json!({"error_type": "provider_unavailable", "raw": "private_fixture"}),
                ),
                Some(serde_json::json!({"error_type": "private_fixture"})),
                Some(serde_json::json!("private_fixture")),
                Some(serde_json::json!(["private_fixture"])),
                Some(serde_json::json!({"error_type": {"private_fixture": true}})),
            ] {
                let known = metadata.as_ref().and_then(|value| value.get("error_type"))
                    == Some(&serde_json::json!("provider_unavailable"));
                let mut envelope = serde_json::json!({"id": "gen-fixture", "error": {
                    "code": 502, "message": "Provider returned an empty response override-private-token"
                }});
                if let Some(metadata) = metadata {
                    envelope["error"]["metadata"] = metadata;
                }
                // Non-streaming HTTP 200 intentionally has no choices.
                let body = if stream {
                    format!("data: {envelope}\n\n")
                } else {
                    envelope.to_string()
                };
                let failure = failure_from_local_endpoint(
                    "openrouter",
                    200,
                    "",
                    stream,
                    Some(body),
                    false,
                    "",
                    false,
                )
                .await;
                assert_eq!(failure.class, ProviderFailureClass::Provider5xx);
                assert_eq!(failure.http_status, Some(502));
                assert_eq!(
                    failure
                        .request_id
                        .as_ref()
                        .map(|id| String::from(id.clone()))
                        .as_deref(),
                    Some("gen-fixture")
                );
                let diagnostic = RecoveryDiagnostic::provider(&failure);
                assert!(!diagnostic.public_message().contains("gen-fixture"));
                assert_eq!(
                    diagnostic.last_failure.as_ref().unwrap().error_reason,
                    known.then_some(ProviderErrorReason::ProviderUnavailable)
                );
                for value in [
                    serde_json::to_string(&failure).unwrap(),
                    serde_json::to_string(&diagnostic).unwrap(),
                    diagnostic.public_message(),
                ] {
                    assert!(!value.contains("private_fixture"));
                    assert!(!value.contains("override-private-token"));
                }
            }
        }
        let choice_error = serde_json::json!({"id":"gen-fixture", "choices":[{"error":{
            "code":502,"metadata":{"error_type":"provider_unavailable"}
        }}]})
        .to_string();
        for stream in [false, true] {
            let body = if stream {
                format!("data: {choice_error}\n\n")
            } else {
                choice_error.clone()
            };
            let failure = failure_from_local_endpoint(
                "openrouter",
                200,
                "",
                stream,
                Some(body),
                false,
                "",
                false,
            )
            .await;
            assert_eq!(
                RecoveryDiagnostic::provider(&failure)
                    .last_failure
                    .unwrap()
                    .error_reason,
                Some(ProviderErrorReason::ProviderUnavailable)
            );
        }
    }

    #[tokio::test]
    async fn openrouter_generation_ids_survive_header_fallback_and_stream_failures() {
        use pioneer_protocol::RecoveryDiagnostic;
        for (id, headers, expected) in [
            (None, "X-Generation-Id: gen-header\r\n", "gen-header"),
            (
                Some("https://private_fixture"),
                "X-Generation-Id: gen-header\r\nX-Request-Id: req-header\r\n",
                "gen-header",
            ),
            (
                Some("gen-body"),
                "X-Generation-Id: gen-header\r\n",
                "gen-body",
            ),
            (
                None,
                "X-Generation-Id: invalid\r\nX-Request-Id: req-header\r\n",
                "req-header",
            ),
        ] {
            let body = serde_json::json!({"id": id, "error":{"code":502}}).to_string();
            let failure = failure_from_local_endpoint(
                "openrouter",
                502,
                "",
                false,
                Some(body),
                false,
                headers,
                false,
            )
            .await;
            assert_eq!(
                RecoveryDiagnostic::provider(&failure)
                    .last_failure
                    .unwrap()
                    .request_id
                    .map(String::from)
                    .as_deref(),
                Some(expected)
            );
        }
        for (suffix, truncated_transport) in [
            ("", false),
            ("", true),
            ("data: {", false),
            (
                "data: {\"id\":\"https://private_fixture\",\"choices\":[]}\n\n",
                false,
            ),
            (
                "data: {\"id\": \"private_fixture\", \"choices\": \"private_fixture\"}\n\n",
                false,
            ),
        ] {
            let body = format!("data: {{\"id\":\"gen-body\",\"choices\":[]}}\n\n{suffix}");
            let failure = failure_from_local_endpoint(
                "openrouter",
                200,
                "",
                true,
                Some(body),
                false,
                "X-Generation-Id: gen-header\r\n",
                truncated_transport,
            )
            .await;
            assert_eq!(failure.class, ProviderFailureClass::StreamStall);
            assert_eq!(failure.http_status, None);
            let diagnostic = RecoveryDiagnostic::provider(&failure);
            assert_eq!(
                diagnostic
                    .last_failure
                    .unwrap()
                    .request_id
                    .map(String::from)
                    .as_deref(),
                Some("gen-body")
            );
        }
    }

    #[tokio::test]
    async fn openrouter_typed_http_and_sse_facts_reach_safe_recovery_diagnostics() {
        use pioneer_protocol::{ProviderErrorReason, ProviderRequestId, RecoveryDiagnostic};
        for stream in [false, true] {
            for (code, id, expected_reason, expected_id) in [
                (
                    Some("provider_unavailable"),
                    "gen-fixture_123",
                    Some(ProviderErrorReason::ProviderUnavailable),
                    true,
                ),
                (None, "gen-fixture_123", None, true),
                (Some("future-secret-value"), "gen-fixture_123", None, true),
                (
                    Some("provider_overloaded"),
                    "https://secret.example?token=credential",
                    Some(ProviderErrorReason::ProviderOverloaded),
                    false,
                ),
            ] {
                let envelope = serde_json::json!({
                    "id": id,
                    "error": {
                        "message": "JSON error injected into SSE stream retry-after: 2 override-private-token",
                        "code": 502,
                        "type": "provider_overloaded",
                        "metadata": {"error_type": code, "raw": "credential /private/request history", "provider_code": "secret-value"}
                    }
                });
                let body = if stream {
                    let mut error_envelope = envelope.clone();
                    error_envelope.as_object_mut().unwrap().remove("id");
                    format!(
                        "data: {}\n\ndata: {error_envelope}\n\n",
                        serde_json::json!({"id": id, "choices": []})
                    )
                } else {
                    envelope.to_string()
                };
                let failure = failure_from_overridden_endpoint_body(
                    "openrouter",
                    if stream { 200 } else { 502 },
                    "",
                    stream,
                    Some(body),
                )
                .await;
                assert_eq!(failure.class, ProviderFailureClass::Provider5xx);
                assert_eq!(failure.http_status, Some(502));
                assert_eq!(failure.retry_after_ms, Some(2000));
                assert_eq!(
                    failure.stage,
                    if stream {
                        ProviderFailureStage::MidStream
                    } else {
                        ProviderFailureStage::Connect
                    }
                );
                assert_eq!(failure.error_reason, expected_reason);
                assert_eq!(failure.request_id, expected_id.then(|| ProviderRequestId::try_from("gen-fixture_123".to_owned()).unwrap()));
                let diagnostic = RecoveryDiagnostic::provider(&failure);
                assert_eq!(
                    diagnostic.last_failure.as_ref().unwrap().error_reason,
                    expected_reason
                );
                assert_eq!(
                    diagnostic.last_failure.as_ref().unwrap().request_id,
                    failure.request_id
                );
                let public = diagnostic.public_message();
                if let Some(reason) = expected_reason {
                    assert!(public.contains(reason.public_description()));
                } else {
                    assert!(!public.contains("overloaded"));
                }
                assert!(!public.contains("gen-fixture"));
                let stored = serde_json::to_string(&failure).unwrap();
                let safe = serde_json::to_string(&diagnostic).unwrap();
                for secret in [
                    "credential",
                    "secret.example",
                    "future-secret",
                    "secret-value",
                    "override-private-token",
                    "/private",
                    "history",
                ] {
                    assert!(!stored.contains(secret));
                    assert!(!safe.contains(secret));
                }
            }
        }
    }

    #[tokio::test]
    async fn overridden_endpoint_preserves_recovery_classification_and_hides_raw_url() {
        for (provider, status, message, stream, class, recoverable) in [
            (
                "openai",
                400,
                "maximum context length exceeded",
                false,
                ProviderFailureClass::ContextTooLarge,
                true,
            ),
            (
                "openrouter",
                404,
                "No endpoints found that support image input",
                true,
                ProviderFailureClass::UnsupportedImageInput,
                true,
            ),
            (
                "openai",
                400,
                "this model does not support streaming",
                true,
                ProviderFailureClass::UnsupportedStreaming,
                true,
            ),
            (
                "openai",
                400,
                "ordinary bad request",
                false,
                ProviderFailureClass::ProviderRejected,
                false,
            ),
            (
                "openai",
                429,
                "rate limit; retry-after: 3",
                false,
                ProviderFailureClass::RateLimit,
                true,
            ),
        ] {
            let failure = failure_from_overridden_endpoint(provider, status, message, stream).await;
            assert_eq!(failure.class, class, "{provider}: {message}");
            assert_eq!(failure.http_status, Some(status));
            assert_eq!(failure.is_recoverable_hint, recoverable);
            if status == 429 {
                assert_eq!(failure.retry_after_ms, Some(3000));
            }
        }
    }

    #[tokio::test]
    async fn successful_http_stream_error_keeps_capability_class_without_endpoint() {
        use futures_util::StreamExt;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secret = "stream-capability-private-token";
        let url = format!("http://{}/{secret}/v1", listener.local_addr().unwrap());
        let echoed_url = url.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            socket.read(&mut request).await.unwrap();
            let event = format!(
                "data: {}\n\n",
                serde_json::json!({"error": {"message": format!(
                    "this model does not support streaming at {echoed_url}"
                )}})
            );
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{event}",
                event.len()
            ).as_bytes()).await.unwrap();
        });
        let registry = pioneer_provider::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok(String::new()), |_, _| Ok(None),
            move |_, _| Ok(Some(url.clone())), ProviderTimeoutPolicy::default(),
        );
        let provider = registry.get_or_create_for_workspace("a", "vllm").unwrap();
        let mut chunks = provider
            .stream_chat(ChatRequest {
                model: "fixture".into(),
                messages: vec![pioneer_provider::ChatMessage::user("hello")],
                temperature: None,
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            })
            .await
            .unwrap();
        let error = chunks.next().await.unwrap().unwrap_err();
        server.await.unwrap();
        assert!(!format!("{error:#?}").contains(secret));
        let ChatTurnError::ProviderFailure { failure, .. } = adapter_error_for_target(
            FailureTarget::new("item", TurnItemType::Reasoning),
            provider.as_ref(),
            "fixture",
            ProviderTransportKind::Stream,
            ProviderFailureStage::MidStream,
            "provider stream failed",
            &error,
        ) else {
            panic!("expected provider failure")
        };
        assert_eq!(failure.class, ProviderFailureClass::UnsupportedStreaming);
        assert!(failure.is_recoverable_hint);
        assert!(!serde_json::to_string(&failure).unwrap().contains(secret));
    }

    #[tokio::test]
    async fn endpoint_path_is_absent_from_durable_failure_details() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let secret = "durable-private-path";
        let url = format!("http://{}/{secret}/v1", listener.local_addr().unwrap());
        let echoed_url = url.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 8192];
            stream.read(&mut request).await.unwrap();
            let body = format!("{{\"error\":\"rate limit at {echoed_url}\"}}");
            stream.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let registry = pioneer_provider::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
            |_, _| Ok("key".into()), |_, _| Ok(None),
            move |_, _| Ok(Some(url.clone())), ProviderTimeoutPolicy::default(),
        );
        let provider = registry.get_or_create_for_workspace("a", "openai").unwrap();
        let error = provider
            .chat(ChatRequest {
                model: "fixture".into(),
                messages: vec![pioneer_provider::ChatMessage::user("hello")],
                temperature: None,
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            })
            .await
            .unwrap_err();
        server.await.unwrap();
        let ChatTurnError::ProviderFailure { failure, .. } = adapter_error_for_target(
            FailureTarget::new("item", TurnItemType::Reasoning),
            provider.as_ref(),
            "fixture",
            ProviderTransportKind::NonStream,
            ProviderFailureStage::Connect,
            "provider request failed",
            &error,
        ) else {
            panic!("expected provider failure")
        };
        assert!(!serde_json::to_string(&failure).unwrap().contains(secret));
        assert_eq!(failure.class, ProviderFailureClass::RateLimit);
        assert_eq!(failure.http_status, Some(429));
        assert!(failure.is_recoverable_hint);
    }

    #[test]
    fn provider_response_replay_is_bound_to_the_exact_execution_model() {
        let mut replay = Some(pioneer_provider::ProviderReplayState::new(
            "openrouter",
            serde_json::json!({"opaque":"state"}),
        ));
        bind_replay_to_response_target(&mut replay, "openrouter", "model-a");
        assert_eq!(replay.as_ref().unwrap().model.as_deref(), Some("model-a"));

        let mut unattributed = Some(pioneer_provider::ProviderReplayState::new(
            "unexpected-provider",
            serde_json::json!({"opaque":"state"}),
        ));
        bind_replay_to_response_target(&mut unattributed, "openrouter", "model-a");
        assert!(unattributed.as_ref().unwrap().model.is_none());
    }

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
                    is_network_error: false,
                    error_reason: None,
                    request_id: None,
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

#[cfg(test)]
mod partial_observation_tests {
    use super::*;
    struct FailingStream;
    #[async_trait::async_trait]
    impl Provider for FailingStream {
        fn name(&self) -> &str {
            "partial-fixture"
        }
        fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
            pioneer_provider::ProviderCapabilities {
                streaming: true,
                ..Default::default()
            }
        }
        async fn chat(&self, _: ChatRequest) -> anyhow::Result<pioneer_provider::ChatResponse> {
            anyhow::bail!("unexpected non-stream transport")
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            Ok(futures_util::stream::iter(vec![
                Ok(StreamChunk::reasoning("reasoning actually received")),
                Ok(StreamChunk::delta("partial answer actually received")),
                Ok(StreamChunk::provider_replay_state(
                    pioneer_provider::ProviderReplayState::new(
                        "partial-fixture",
                        serde_json::json!({"opaque":"retained bytes"}),
                    ),
                )),
                Ok(StreamChunk::tool_calls(vec![ProviderToolCall {
                    id: "not-executed".into(),
                    name: "tool".into(),
                    arguments: "{}".into(),
                }])),
                Err(anyhow::anyhow!("connection reset mid-stream")),
            ])
            .boxed())
        }
    }
    #[tokio::test]
    async fn compaction_failed_partial_is_acknowledged_before_stream_failure_returns() {
        for agent in [false, true] {
            let provider: Arc<dyn Provider> = Arc::new(FailingStream);
            let hub = AgentEventHub::new();
            let mut events = hub.take_durable_receiver().await.unwrap();
            let request = ChatRequest {
                model: "fixture".into(),
                messages: vec![pioneer_provider::ChatMessage::user("request")],
                temperature: None,
                max_tokens: None,
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            };
            let receive = async {
                loop {
                    let event = events.recv().await.unwrap();
                    assert!(!matches!(
                        event,
                        AgentDurableEvent::TurnFinalizationPrepared { .. }
                    ));
                    let observation = match event {
                        AgentDurableEvent::TurnProviderHistoryAppended { payload, .. } => {
                            Some(payload)
                        }
                        _ => None,
                    };
                    events.acknowledge_last(Ok(()));
                    if let Some(payload) = observation {
                        break payload;
                    }
                }
            };
            let call = async {
                if agent {
                    request_agent_round(
                        &provider,
                        request,
                        "ws",
                        "thread",
                        "turn",
                        "thinking",
                        false,
                        ProviderTimeoutPolicy::default(),
                        &hub,
                    )
                    .await
                    .map(|_| ())
                } else {
                    stream_provider_response(
                        &provider,
                        request,
                        "ws",
                        "thread",
                        "turn",
                        "thinking",
                        "message",
                        ProviderTimeoutPolicy::default(),
                        &hub,
                    )
                    .await
                    .map(|_| ())
                }
            };
            let (result, payload) =
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    tokio::join!(call, receive)
                })
                .await
                .unwrap();
            assert!(matches!(result, Err(ChatTurnError::ProviderFailure { .. })));
            let envelope: pioneer_provider::CanonicalProviderRoundEnvelope =
                serde_json::from_value(payload).unwrap();
            assert_eq!(envelope.termination, ProviderTermination::ProviderError);
            assert_eq!(envelope.message.content, "partial answer actually received");
            assert_eq!(
                envelope.message.reasoning_content.as_deref(),
                Some("reasoning actually received")
            );
            assert_eq!(envelope.message.tool_calls.unwrap()[0].id, "not-executed");
            assert_eq!(
                envelope.message.provider_replay_state.unwrap().payload["opaque"],
                "retained bytes"
            );
            assert!(envelope.calls.is_empty());
        }
    }
}

fn observe_startup_chunk(turn_id: &str, chunk: &pioneer_provider::StreamChunk) {
    use pioneer_observability::turn_startup::{self, Output};
    let output = if !chunk.delta.is_empty() {
        Some(Output::Text)
    } else if chunk
        .reasoning_delta
        .as_deref()
        .is_some_and(|s| !s.is_empty())
    {
        Some(Output::Reasoning)
    } else if !chunk.tool_calls.is_empty() {
        Some(Output::ToolCall)
    } else {
        None
    };
    if let Some(output) = output {
        turn_startup::runtime_output(turn_id, output);
    }
}
