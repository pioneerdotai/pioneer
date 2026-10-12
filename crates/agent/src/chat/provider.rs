use super::{AgentRoundResponse, ChatTurnError};
use crate::AgentEventHub;
use futures_util::{Stream, StreamExt};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemDeltaNotification,
    ItemStartedNotification, ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage,
    ProviderTransportKind, TurnItem, TurnItemType,
};
use pioneer_provider::failure::classify_http_error_body_too_large;
#[cfg(test)]
use pioneer_provider::failure::classify_provider_failure_class;
use pioneer_provider::{
    ChatRequest, Provider, ProviderFailureClassification, ProviderResponseLimits,
    ProviderResponseTooLarge, ProviderTermination, ProviderTimeoutPolicy, ProviderToolCall,
    StreamChunk, TokenUsage,
};
use std::sync::Arc;
use std::time::Instant;

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
    usage.input_tokens?.checked_add(usage.output_tokens?)
}

pub(super) async fn await_provider_dispatch<T>(
    stream: bool,
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    let Some(context) = pioneer_provider::current_provider_attempt_context() else {
        return future.await;
    };
    let deadline = if stream {
        context.first_output_deadline
    } else {
        context.deadline
    };
    let timeout_error = || {
        let mut classification = ProviderFailureClassification::new(if stream {
            ProviderFailureClass::StreamStall
        } else {
            ProviderFailureClass::NetworkTransient
        });
        classification.provider_code = Some(
            if stream {
                "provider_first_output_deadline_exceeded"
            } else {
                "provider_attempt_deadline_exceeded"
            }
            .to_owned(),
        );
        anyhow::Error::new(pioneer_provider::failure::ProviderTransportFailure { classification })
    };
    if context.cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
        return Err(timeout_error());
    }
    tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(timeout_error()),
        result = tokio::time::timeout_at(deadline, future) => result.map_err(|_| timeout_error())?,
    }
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

/// A final no-tool response can contain native state absent from UI text.
/// Keep it in the existing acknowledged history route before finalization.
#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_completed_response(
    events: &AgentEventHub,
    thread: &str,
    turn: &str,
    item: &str,
    reasoning_item: &str,
    text: &str,
    reasoning: &str,
    replay: Option<&pioneer_provider::ProviderReplayState>,
) -> Result<(), ChatTurnError> {
    if replay.is_none() && reasoning.is_empty() {
        return Ok(());
    }
    let mut message = pioneer_provider::ChatMessage::assistant(text);
    message.reasoning_content = (!reasoning.is_empty()).then(|| reasoning.to_owned());
    message.provider_replay_state = replay.cloned();
    super::persist_provider_history_message(
        events,
        thread,
        turn,
        reasoning_item,
        &pioneer_provider::CanonicalProviderRoundEnvelope {
            version: 1,
            round_id: item.into(),
            termination: pioneer_provider::ProviderTermination::Complete,
            message,
            calls: Vec::new(),
        },
    )
    .await
}

/// Persist the provider accumulator, including opaque state absent from UI text.
/// This acknowledged observation precedes failure/recovery and never becomes an
/// executable assistant/tool round. Cancellation still drops this owned future.
#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_failed_provider_observation(
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

async fn request_agent_round_observed(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    force_non_stream: bool,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
    observation: &ProviderAttemptObservation<'_>,
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
            await_provider_dispatch(true, provider.stream_chat_with_diagnostics(request))
                .await
                .map_err(|error| {
                    observation.observe_error(&error);
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
                observation,
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
                    observation.observe_and_publish(snapshot).await?;
                }
                if chunk.provider_replay_state.is_some() {
                    provider_replay_state = chunk.provider_replay_state.take();
                    bind_replay_to_response_target(
                        &mut provider_replay_state,
                        provider_name.as_str(),
                        model_name.as_str(),
                    );
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
                    // Buffered provider text is physical attempt activity even
                    // when no provisional final message is published yet.
                    event_tx.publish_confirmed_activity(
                        workspace_id.into(),
                        thread_id.into(),
                        turn_id.into(),
                        thinking_item_id.into(),
                        TurnItemType::Reasoning,
                    );
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
                if chunk.is_final {
                    termination = chunk.termination;
                    break;
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

    let mut response = await_provider_dispatch(false, provider.chat(request))
        .await
        .map_err(|error| {
            observation.observe_error(&error);
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
    if let Some(usage) = &response.usage {
        observation.observe_and_publish(usage).await?;
    }
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

    let termination = match require_round_termination(
        Some(response.termination),
        response.tool_calls.as_slice(),
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
        provider.name(),
        model_name.as_str(),
        ProviderTransportKind::NonStream,
    ) {
        Ok(termination) => termination,
        Err(error) => {
            persist_failed_provider_observation(
                event_tx,
                thread_id,
                turn_id,
                thinking_item_id,
                &response.text,
                &reasoning,
                &response.tool_calls,
                response.provider_replay_state.as_ref(),
                &error,
            )
            .await?;
            return Err(error);
        }
    };
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

async fn stream_provider_response_observed(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
    observation: &ProviderAttemptObservation<'_>,
    finalization: Option<&super::StandardFinalization<'_>>,
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
        await_provider_dispatch(true, provider.stream_chat_with_diagnostics(request))
            .await
            .map_err(|error| {
                observation.observe_error(&error);
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
            observation,
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
                observation.observe_and_publish(snapshot).await?;
            }
            if chunk_replay.is_some() {
                provider_replay_state = chunk_replay;
                bind_replay_to_response_target(
                    &mut provider_replay_state,
                    provider_name.as_str(),
                    model_name.as_str(),
                );
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
            if is_final {
                termination = chunk_termination;
                break;
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
    if let Err(error) = require_standard_final_answer(
        &full_text,
        &stream_tool_calls,
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
        &provider_name,
        &model_name,
        ProviderTransportKind::Stream,
    ) {
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
    let task_revision = if let Some(gate) = finalization {
        gate.check(workspace_id, thread_id, turn_id, &full_text, usage.as_ref())
            .await?
    } else {
        None
    };
    persist_completed_response(
        event_tx,
        thread_id,
        turn_id,
        message_item_id,
        thinking_item_id,
        &full_text,
        &reasoning_parts,
        provider_replay_state.as_ref(),
    )
    .await?;
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
            task_finalization_revision: task_revision,
        },
    )
    .await?;

    lifecycle_metric.finish(pioneer_observability::NativeLifecycleOutcome::Succeeded);
    Ok((assistant_text, usage))
}

async fn non_stream_provider_response_observed(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    event_tx: &AgentEventHub,
    observation: &ProviderAttemptObservation<'_>,
    finalization: Option<&super::StandardFinalization<'_>>,
) -> Result<(String, Option<pioneer_provider::TokenUsage>), ChatTurnError> {
    pioneer_observability::turn_startup::dispatched(turn_id);
    let mut lifecycle_metric = NativeProviderRoundMetric::start();
    let model_name = request.model.clone();

    let mut response = await_provider_dispatch(false, provider.chat(request))
        .await
        .map_err(|error| {
            observation.observe_error(&error);
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

    if let Some(usage) = &response.usage {
        observation.observe_and_publish(usage).await?;
    }
    bind_replay_to_response_target(
        &mut response.provider_replay_state,
        provider.name(),
        &model_name,
    );
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

    if let Err(error) = require_round_termination(
        Some(response.termination.clone()),
        response.tool_calls.as_slice(),
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
        provider.name(),
        model_name.as_str(),
        ProviderTransportKind::NonStream,
    ) {
        persist_failed_provider_observation(
            event_tx,
            thread_id,
            turn_id,
            thinking_item_id,
            &response.text,
            response.reasoning_content.as_deref().unwrap_or_default(),
            &response.tool_calls,
            response.provider_replay_state.as_ref(),
            &error,
        )
        .await?;
        return Err(error);
    }
    if let Err(error) = require_standard_final_answer(
        &response.text,
        &response.tool_calls,
        FailureTarget::new(thinking_item_id, TurnItemType::Reasoning),
        provider.name(),
        &model_name,
        ProviderTransportKind::NonStream,
    ) {
        persist_failed_provider_observation(
            event_tx,
            thread_id,
            turn_id,
            thinking_item_id,
            &response.text,
            response.reasoning_content.as_deref().unwrap_or_default(),
            &response.tool_calls,
            response.provider_replay_state.as_ref(),
            &error,
        )
        .await?;
        return Err(error);
    }
    let task_revision = if let Some(gate) = finalization {
        gate.check(
            workspace_id,
            thread_id,
            turn_id,
            &response.text,
            response.usage.as_ref(),
        )
        .await?
    } else {
        None
    };
    persist_completed_response(
        event_tx,
        thread_id,
        turn_id,
        message_item_id,
        thinking_item_id,
        &response.text,
        response.reasoning_content.as_deref().unwrap_or_default(),
        response.provider_replay_state.as_ref(),
    )
    .await?;
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
            task_finalization_revision: task_revision,
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
        format!("{prefix}: provider request failed"),
        pioneer_provider::failure::classify_transport_error(error).or(classification),
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

pub(super) fn validate_standard_final_answer(
    text: &str,
    calls: &[ProviderToolCall],
    item: &str,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
) -> Result<(), ChatTurnError> {
    require_standard_final_answer(
        text,
        calls,
        FailureTarget::new(item, TurnItemType::Reasoning),
        provider,
        model,
        transport,
    )
}

fn require_standard_final_answer(
    text: &str,
    calls: &[ProviderToolCall],
    target: FailureTarget<'_>,
    provider: &str,
    model: &str,
    transport: ProviderTransportKind,
) -> Result<(), ChatTurnError> {
    let failure = if !calls.is_empty() {
        Some((
            ProviderFailureClass::MalformedProviderRequest,
            "provider returned tool calls in a response path without tool execution",
        ))
    } else {
        super::validator::no_tool_final_answer_rejection(text).map(|rejection| {
            (
                rejection.provider_failure_class(),
                rejection.provider_message(),
            )
        })
    };
    if let Some((class, message)) = failure {
        return Err(provider_failure_error_with_classification(
            target.item_id,
            target.item_type,
            provider,
            model,
            transport,
            ProviderFailureStage::Finalize,
            message.into(),
            Some(ProviderFailureClassification::new(class)),
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
    observation: &ProviderAttemptObservation<'_>,
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

    let context = match pioneer_provider::current_provider_attempt_context() {
        Some(context) => context,
        None => pioneer_provider::ProviderAttemptContext::from_started(
            provider_timeout_policy,
            true,
            observation.started,
        )
        .map_err(|_| {
            ChatTurnError::Terminal("provider attempt timeout policy is out of range".into())
        })?,
    };
    let stage_deadline = if *seen_any_chunk {
        let now = tokio::time::Instant::now();
        now + wait.min(context.deadline.saturating_duration_since(now))
    } else {
        context.first_output_deadline
    };
    let deadline = stage_deadline.min(context.deadline);
    let next_chunk = tokio::time::timeout_at(deadline, async {
        loop {
            if context.cancellation.is_cancelled() || tokio::time::Instant::now() >= deadline {
                return Err(());
            }
            let next = tokio::select! {
                biased;
                _ = context.cancellation.cancelled() => return Err(()),
                next = stream.next() => next,
            };
            if let Some(Ok(chunk)) = &next {
                let identity_only = !chunk.is_final
                    && chunk.delta.is_empty()
                    && chunk.reasoning_delta.is_none()
                    && chunk.tool_calls.is_empty()
                    && chunk.provider_replay_state.is_none()
                    && chunk.termination.is_none()
                    && chunk.usage.as_ref().is_some_and(|usage| {
                        usage.input_tokens.is_none()
                            && usage.output_tokens.is_none()
                            && usage.uncached_input_tokens.is_none()
                            && usage.cache_read_input_tokens.is_none()
                            && usage.cache_write_input_tokens.is_none()
                            && usage.reasoning_tokens.is_none()
                            && usage.reported_total_tokens.is_none()
                    });
                if identity_only {
                    // Correlation evidence must survive without resetting the
                    // request's first/inter-chunk deadline or counting as progress.
                    observation.observe(chunk.usage.as_ref().expect("identity usage"));
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            break Ok(next);
        }
    })
    .await
    .unwrap_or(Err(()))
    .map_err(|_| {
        let mut classification =
            ProviderFailureClassification::new(ProviderFailureClass::StreamStall);
        classification.request_id = diagnostics.request_id();
        classification.provider_code = Some(
            if deadline == context.deadline {
                "provider_attempt_deadline_exceeded"
            } else if *seen_any_chunk {
                "provider_idle_deadline_exceeded"
            } else {
                "provider_first_output_deadline_exceeded"
            }
            .to_owned(),
        );
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

    let chunk = chunk_result.map_err(|error| {
        observation.observe_error(&error);
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

    *seen_any_chunk |= !chunk.delta.is_empty()
        || chunk
            .reasoning_delta
            .as_ref()
            .is_some_and(|text| !text.is_empty())
        || !chunk.tool_calls.is_empty()
        || chunk.is_final;
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
    _error_message: String,
    classification: Option<ProviderFailureClassification>,
) -> ChatTurnError {
    let classification = classification
        .unwrap_or_else(|| ProviderFailureClassification::new(ProviderFailureClass::Unknown));
    let ProviderFailureClassification {
        is_network_error: _,
        error_reason,
        request_id,
        class,
        http_status,
        provider_code,
        retry_after_ms,
    } = classification;
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
            message: Some(format!(
                "provider request failed: {class:?} during {stage:?}"
            )),
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
    _error_message: &str,
    _stage: ProviderFailureStage,
) -> ProviderFailureClass {
    // Kept for source compatibility with old diagnostic callers. Text is not
    // authoritative failure evidence and cannot choose any recovery action.
    ProviderFailureClass::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_usage_metadata_is_transparent_to_production_failure_translation() {
        struct Failing {
            message: &'static str,
            classification: Option<ProviderFailureClassification>,
        }
        #[async_trait::async_trait]
        impl Provider for Failing {
            fn name(&self) -> &str {
                "openrouter"
            }
            fn classify_failure(&self, _: &anyhow::Error) -> Option<ProviderFailureClassification> {
                self.classification.clone()
            }
            async fn chat(&self, _: ChatRequest) -> anyhow::Result<pioneer_provider::ChatResponse> {
                Err(anyhow::anyhow!(self.message))
            }
            async fn stream_chat(
                &self,
                _: ChatRequest,
            ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
            {
                let snapshot = TokenUsage {
                    generation_id: Some("gen-header".into()),
                    input_tokens: None,
                    output_tokens: Some(0),
                    cache_read_input_tokens: Some(8),
                    ..Default::default()
                };
                Ok(Box::pin(futures_util::stream::iter(vec![
                    Ok(StreamChunk::usage(snapshot.clone())),
                    Ok(StreamChunk::usage(snapshot)),
                    Err(anyhow::anyhow!(self.message)),
                ])))
            }
        }
        let request = || ChatRequest {
            model: "fixture".into(),
            messages: vec![],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        for (message, stage, class, typed) in [
            (
                "OpenRouter API error 429: {\"code\":\"rate_limit_exceeded\"} retry-after: 2",
                ProviderFailureStage::Connect,
                ProviderFailureClass::Unknown,
                false,
            ),
            (
                "OpenRouter API error 503: unavailable",
                ProviderFailureStage::Connect,
                ProviderFailureClass::Unknown,
                false,
            ),
            (
                "connection reset by peer",
                ProviderFailureStage::MidStream,
                ProviderFailureClass::Unknown,
                false,
            ),
            (
                "provider API error 400: invalid request",
                ProviderFailureStage::MidStream,
                ProviderFailureClass::Unknown,
                false,
            ),
            (
                "provider returned empty response",
                ProviderFailureStage::Finalize,
                ProviderFailureClass::EmptyResponse,
                true,
            ),
            (
                "opaque adapter failure",
                ProviderFailureStage::FirstChunk,
                ProviderFailureClass::RateLimit,
                true,
            ),
        ] {
            let inner = Arc::new(Failing {
                message,
                classification: typed.then(|| ProviderFailureClassification {
                    is_network_error: false,
                    error_reason: None,
                    request_id: None,
                    class,
                    http_status: (class == ProviderFailureClass::RateLimit).then_some(429),
                    provider_code: (class == ProviderFailureClass::RateLimit)
                        .then(|| "native_rate_limit".into()),
                    retry_after_ms: (class == ProviderFailureClass::RateLimit).then_some(7000),
                }),
            });
            let plain = inner.chat(request()).await.err().unwrap();
            let registry = pioneer_provider::ProviderRegistry::new(|_| String::new());
            registry.insert("openrouter", inner.clone()).unwrap();
            let wrapped = registry.get_or_create("openrouter").unwrap();
            let mut stream = wrapped.stream_chat(request()).await.unwrap();
            let observation = ProviderAttemptObservation::new(wrapped.as_ref(), "fixture");
            for _ in 0..2 {
                observation.observe(&stream.next().await.unwrap().unwrap().usage.unwrap());
            }
            let enriched = stream.next().await.unwrap().err().unwrap();
            observation.observe_error(&enriched);
            observation.observe_error(&enriched);
            let translate = |provider: &dyn Provider, error: &anyhow::Error| {
                let ChatTurnError::ProviderFailure { failure, .. } = adapter_error_for_target(
                    FailureTarget::new("item", TurnItemType::Reasoning),
                    provider,
                    "fixture",
                    ProviderTransportKind::Stream,
                    stage,
                    "provider error",
                    error,
                ) else {
                    panic!("expected failure")
                };
                failure
            };
            let before = translate(inner.as_ref(), &plain);
            let after = translate(wrapped.as_ref(), &enriched);
            assert_eq!(after.class, class);
            assert_eq!(after.class, before.class);
            assert_eq!(after.http_status, before.http_status);
            assert_eq!(after.provider_code, before.provider_code);
            assert_eq!(after.retry_after_ms, before.retry_after_ms);
            if !typed {
                assert!(after.http_status.is_none());
                assert!(after.provider_code.is_none());
                assert!(after.retry_after_ms.is_none());
            }
            assert_eq!(after.is_recoverable_hint, before.is_recoverable_hint);
            assert_eq!(after.stage, before.stage);
            assert_eq!(after.transport, before.transport);
            assert!(!after.message.unwrap().contains(message));
            let observed = observation.usage.lock().unwrap();
            assert_eq!(observed.generation_id.as_deref(), Some("gen-header"));
            assert_eq!(observed.input_tokens, None);
            assert_eq!(observed.output_tokens, Some(0));
            assert_eq!(observed.cache_read_input_tokens, Some(8));
        }
    }

    #[tokio::test]
    async fn native_openrouter_header_errors_keep_http_and_retry_hints_without_redaction_override()
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        for (status, class) in [
            (429, ProviderFailureClass::RateLimit),
            (503, ProviderFailureClass::Provider5xx),
        ] {
            for streaming in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!("http://{}", listener.local_addr().unwrap());
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    // Read the complete request before closing the connection. Closing
                    // with unread request bytes can reset the socket on Linux, obscuring
                    // the response failure this fixture is intended to exercise.
                    let mut request = Vec::new();
                    loop {
                        let mut buffer = [0u8; 2048];
                        let count = socket.read(&mut buffer).await.unwrap();
                        assert!(count > 0, "fixture request ended before its body");
                        assert!(request.len() + count <= 8192, "fixture request too large");
                        request.extend_from_slice(&buffer[..count]);
                        if let Some(start) =
                            request.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                        {
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

                    let body = r#"{"error":{"code":"native_rejection","message":"SECRET_BODY retry-after: 2"}}"#;
                    socket.write_all(format!("HTTP/1.1 {status} Error\r\nContent-Type: application/json\r\nX-Generation-Id: gen-native-header\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                });
                // Native adapter + normal registry wrapper; no endpoint override
                // resolver, so the default non-redacting error path is exercised.
                let registry = pioneer_provider::ProviderRegistry::new(|_| String::new());
                registry
                    .insert(
                        "openrouter",
                        Arc::new(
                            pioneer_provider::providers::OpenRouterProvider::with_base_url(
                                "key", url,
                            ),
                        ),
                    )
                    .unwrap();
                let provider = registry.get_or_create("openrouter").unwrap();
                let request = ChatRequest {
                    model: "fixture".into(),
                    messages: vec![],
                    temperature: None,
                    max_tokens: None,
                    tools: None,
                    tool_choice: None,
                    parallel_tool_calls: None,
                    reasoning: None,
                    compiled_prompt: None,
                };
                let error = if streaming {
                    provider.stream_chat(request).await.err().unwrap()
                } else {
                    provider.chat(request).await.err().unwrap()
                };
                server.await.unwrap();
                let classification = provider.classify_failure(&error).unwrap();
                assert_eq!(classification.http_status, Some(status));
                let transport = if streaming {
                    ProviderTransportKind::Stream
                } else {
                    ProviderTransportKind::NonStream
                };
                let translate = |error: &anyhow::Error| {
                    let ChatTurnError::ProviderFailure { failure, .. } = adapter_error_for_target(
                        FailureTarget::new("item", TurnItemType::Reasoning),
                        provider.as_ref(),
                        "fixture",
                        transport,
                        ProviderFailureStage::Connect,
                        "provider error",
                        error,
                    ) else {
                        panic!("expected provider failure")
                    };
                    failure
                };
                let ChatTurnError::ProviderFailure {
                    failure: before, ..
                } = provider_failure_error_with_classification(
                    "item",
                    TurnItemType::Reasoning,
                    provider.name(),
                    "fixture",
                    transport,
                    ProviderFailureStage::Connect,
                    pioneer_provider::usage::classification_source(&error).to_string(),
                    Some(classification),
                )
                else {
                    panic!("expected failure")
                };
                let after = translate(&error);
                assert_eq!(after.class, class);
                assert_eq!(after.http_status, Some(status));
                assert_eq!(after.provider_code, None);
                assert_eq!(after.retry_after_ms, Some(2000));
                assert_eq!(after.class, before.class);
                assert_eq!(after.http_status, before.http_status);
                assert_eq!(after.provider_code, before.provider_code);
                assert_eq!(after.retry_after_ms, before.retry_after_ms);
                assert_eq!(after.is_recoverable_hint, before.is_recoverable_hint);
                assert!(!after.message.unwrap().contains("SECRET_BODY"));
                let usage = pioneer_provider::usage::error_usage(&error).unwrap();
                assert_eq!(usage.generation_id.as_deref(), Some("gen-native-header"));
                assert_eq!(usage.input_tokens, None);
                assert_eq!(usage.output_tokens, None);
            }
        }
    }

    async fn failure_from_overridden_endpoint(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
    ) -> ProviderFailureDetails {
        failure_from_local_endpoint(
            provider_name,
            status,
            message,
            stream_request,
            None,
            true,
            "X-Generation-Id: gen-private-header\r\nRetry-After: 3\r\n",
            false,
        )
        .await
    }

    async fn failure_from_overridden_endpoint_body(
        provider_name: &'static str,
        status: u16,
        message: &'static str,
        stream_request: bool,
        response_body: Option<String>,
    ) -> ProviderFailureDetails {
        let headers = if provider_name == "openrouter" && response_body.is_none() {
            "X-Generation-Id: gen-private-header\r\nRetry-After: 2\r\n"
        } else {
            "Retry-After: 2\r\n"
        };
        failure_from_local_endpoint(
            provider_name,
            status,
            message,
            stream_request,
            response_body,
            true,
            headers,
            false,
        )
        .await
    }

    // Injected adapter fixtures use the required authority wrapper without
    // custom-endpoint redaction masking leaks in standard diagnostics.
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
            // A raw adapter lacks the authority scope needed during input
            // preparation and would fail before connecting to the fixture.
            // Injecting it supplies that scope while leaving redaction disabled.
            pioneer_provider::ProviderRegistry::with_provider(
                "openrouter",
                Arc::new(
                    pioneer_provider::providers::OpenRouterProvider::with_base_url(
                        "key",
                        format!("http://{}/v1", listener_address),
                    ),
                ),
            )
            .get_or_create("openrouter")
            .unwrap()
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
        if provider_name == "openrouter" && headers.contains("gen-private-header") {
            let usage = pioneer_provider::usage::error_usage(&error).unwrap();
            assert_eq!(usage.generation_id.as_deref(), Some("gen-private-header"));
            assert_eq!(usage.input_tokens, None);
        }
        // Anyhow wraps usage in a context whose standard Error type differs
        // from the metadata type. Check the original adapter cause chain.
        let mut source = pioneer_provider::usage::classification_source(&error).source();
        while let Some(cause) = source {
            assert!(cause.is::<pioneer_provider::failure::ProviderStreamIncomplete>());
            source = cause.source();
        }
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
    async fn ordinary_final_without_native_state_or_reasoning_does_not_expect_ui_aliases() {
        let hub = AgentEventHub::new();
        let mut receiver = hub.take_durable_receiver().await.unwrap();
        persist_completed_response(
            &hub, "thread", "turn", "final", "thinking", "answer", "", None,
        )
        .await
        .unwrap();
        assert!(futures_util::FutureExt::now_or_never(receiver.recv()).is_none());
    }

    #[tokio::test]
    async fn completed_native_response_waits_for_durable_history_acknowledgement() {
        let hub = AgentEventHub::new();
        let mut receiver = hub.take_durable_receiver().await.unwrap();
        let state = pioneer_provider::ProviderReplayState::for_model(
            "gemini",
            "fixture",
            serde_json::json!({"schema_version":2,"parts":[{"text":"answer","thoughtSignature":"signed"}]}),
        );
        let publish = persist_completed_response(
            &hub,
            "thread",
            "turn",
            "final",
            "thinking",
            "answer",
            "",
            Some(&state),
        );
        let receive = async {
            let AgentDurableEvent::TurnProviderHistoryAppended {
                payload, item_id, ..
            } = receiver.recv().await.unwrap()
            else {
                panic!("expected canonical history");
            };
            assert_eq!(item_id, "thinking");
            let envelope: pioneer_provider::CanonicalProviderRoundEnvelope =
                serde_json::from_value(payload).unwrap();
            assert_eq!(
                envelope.message.provider_replay_state.as_ref(),
                Some(&state)
            );
            assert!(envelope.calls.is_empty());
            receiver.acknowledge_last(Ok(()));
        };
        let (result, ()) = tokio::join!(publish, receive);
        result.unwrap();
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
                    ProviderFailureClass::Provider5xx,
                ),
                (
                    [b"private_fixture ".as_slice(), &[0xff]].concat(),
                    false,
                    ProviderFailureClass::Provider5xx,
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
                ProviderFailureClass::ProviderRejected,
                false,
            ),
            (
                "openrouter",
                404,
                "No endpoints found that support image input",
                true,
                ProviderFailureClass::ProviderRejected,
                false,
            ),
            (
                "openai",
                400,
                "this model does not support streaming",
                true,
                ProviderFailureClass::ProviderRejected,
                false,
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
                "openrouter",
                503,
                "temporary upstream failure",
                false,
                ProviderFailureClass::Provider5xx,
                true,
            ),
            (
                "openrouter",
                429,
                "rate limit; retry-after: 3",
                true,
                ProviderFailureClass::RateLimit,
                true,
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
                serde_json::json!({"error": {"code":"unsupported_streaming", "message": format!(
                    "arbitrary secret detail at {echoed_url}"
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

        assert_eq!(failure.class, ProviderFailureClass::Unknown);
        assert!(failure.http_status.is_none());
        assert!(!failure.is_recoverable_hint);
    }

    #[test]
    fn plain_404_still_maps_to_model_not_found() {
        assert_eq!(
            classify_provider_failure_class(
                "provider stream error: api error (404 not found): model not found",
                ProviderFailureStage::Connect,
                Some(404),
                Some("model_not_found"),
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

        assert_eq!(failure.class, ProviderFailureClass::Unknown);
        assert!(failure.http_status.is_none());
        assert!(!failure.is_recoverable_hint);
    }

    #[test]
    fn unsupported_streaming_maps_to_provider_neutral_class() {
        assert_eq!(
            classify_provider_failure_class(
                "provider error: this model does not support streaming",
                ProviderFailureStage::Connect,
                Some(400),
                Some("unsupported_streaming"),
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

        assert_eq!(failure.class, ProviderFailureClass::Unknown);
        assert!(
            !failure
                .message
                .as_deref()
                .unwrap()
                .contains(message.as_str())
        );
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
                "arbitrary body must not affect typed classification",
                ProviderFailureStage::Connect,
                Some(400),
                Some("context_length_exceeded"),
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
                Ok(StreamChunk::usage(TokenUsage {
                    generation_id: Some("gen-header".into()),
                    request_id: Some("req-distinct".into()),
                    reported_model: Some("actual-returned".into()),
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    ..Default::default()
                })),
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
                let mut history = None;
                loop {
                    let event = events.recv().await.unwrap();
                    assert!(!matches!(
                        event,
                        AgentDurableEvent::TurnFinalizationPrepared { .. }
                    ));
                    let usage = match event {
                        AgentDurableEvent::TurnProviderHistoryAppended { payload, .. } => {
                            history = Some(payload);
                            None
                        }
                        AgentDurableEvent::ItemCompleted { notification } => {
                            match notification.item {
                                TurnItem::SystemEvent { code, details, .. }
                                    if code.as_deref() == Some("provider_usage") =>
                                {
                                    details
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    };
                    events.acknowledge_last(Ok(()));
                    if let Some(details) = usage {
                        assert_eq!(details["status"], "failed");
                        assert_eq!(details["usage"]["generation_id"], "gen-header");
                        assert_eq!(details["usage"]["request_id"], "req-distinct");
                        assert_eq!(details["usage"]["reported_model"], "actual-returned");
                        assert_eq!(details["usage"]["input_tokens"], 10);
                        assert_eq!(details["usage"]["output_tokens"], 2);
                        assert!(!details.to_string().contains("request\""));
                        break history.expect("partial history precedes failure");
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

/// One item per physical chat dispatch; snapshots are never additional charges.
/// Start records survive cancellation. Only provider-reported partial counters
/// survive error; no usage is invented for a failed connect or missing terminal.
pub(super) struct ProviderAttemptObservation<'a> {
    publication: Option<(&'a AgentEventHub, String, String, String)>,
    publication_failure: std::sync::atomic::AtomicU8,
    started: tokio::time::Instant,
    id: String,
    window_index: Option<u32>,
    provider: String,
    model: String,
    api: String,
    route: Option<String>,
    usage: std::sync::Mutex<TokenUsage>,
}
impl<'a> ProviderAttemptObservation<'a> {
    pub(super) fn new(provider: &dyn Provider, model: &str) -> Self {
        Self {
            publication: None,
            publication_failure: std::sync::atomic::AtomicU8::new(0),
            started: tokio::time::Instant::now(),
            id: super::generate_id(super::TURN_ITEM_ID_LEN),
            window_index: None,
            provider: provider.name().to_owned(),
            model: model.to_owned(),
            api: provider.usage_api().to_owned(),
            route: provider.usage_route(),
            usage: std::sync::Mutex::new(TokenUsage::default()),
        }
    }
    pub(super) fn with_window_index(mut self, window_index: Option<u32>) -> Self {
        self.window_index = window_index;
        self
    }
    pub(super) fn with_publication(
        mut self,
        events: &'a AgentEventHub,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Self {
        self.publication = Some((
            events,
            workspace.to_owned(),
            thread.to_owned(),
            turn.to_owned(),
        ));
        self
    }
    pub(super) async fn observe_and_publish(
        &self,
        snapshot: &TokenUsage,
    ) -> Result<(), ChatTurnError> {
        let changed = {
            let mut usage = self.usage.lock().expect("usage observation");
            let previous = (usage.input_tokens, usage.output_tokens);
            usage.update(snapshot);
            previous != (usage.input_tokens, usage.output_tokens)
        };
        if changed && let Some((events, workspace, thread, turn)) = &self.publication {
            let item = self.item(None);
            let result = Self::publish_usage_event(
                events,
                AgentDurableEvent::ProviderUsageObserved {
                    notification: pioneer_protocol::ItemUpdatedNotification {
                        workspace_id: workspace.clone(),
                        thread_id: thread.clone(),
                        turn_id: turn.clone(),
                        item,
                    },
                },
            )
            .await;
            if let Err(error) = &result {
                self.record_publication_failure(error);
            }
            result?;
        }
        Ok(())
    }
    async fn publish_usage_event(
        events: &AgentEventHub,
        event: AgentDurableEvent,
    ) -> Result<(), ChatTurnError> {
        let publish = super::emit_durable_event(events, event);
        let Some(context) = pioneer_provider::current_provider_attempt_context() else {
            return publish.await;
        };
        let unavailable =
            || ChatTurnError::Transient("provider usage commit exceeded attempt deadline".into());
        if context.cancellation.is_cancelled() || tokio::time::Instant::now() >= context.deadline {
            return Err(unavailable());
        }
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(unavailable()),
            result = tokio::time::timeout_at(context.deadline, publish) => result.map_err(|_| unavailable())?,
        }
    }
    fn record_publication_failure(&self, error: &ChatTurnError) {
        let kind = match error {
            ChatTurnError::Terminal(_) => 3,
            ChatTurnError::Transient(_) => 1,
            _ => 2,
        };
        self.publication_failure
            .fetch_max(kind, std::sync::atomic::Ordering::Relaxed);
    }
    pub(super) fn publication_failure(&self) -> Option<pioneer_protocol::TurnFailureDisposition> {
        use pioneer_protocol::TurnFailureDisposition;
        match self
            .publication_failure
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            0 => None,
            1 => Some(TurnFailureDisposition::Transient),
            3 => Some(TurnFailureDisposition::Permanent),
            _ => Some(TurnFailureDisposition::Unknown),
        }
    }
    pub(super) fn observe(&self, snapshot: &TokenUsage) {
        self.usage
            .lock()
            .expect("usage observation")
            .update(snapshot);
    }
    pub(super) fn observe_error(&self, error: &anyhow::Error) {
        if let Some(snapshot) = pioneer_provider::usage::error_usage(error) {
            self.observe(snapshot);
        }
    }
    pub(super) fn known_token_count(&self) -> u64 {
        let usage = self.usage.lock().expect("usage observation");
        usage
            .input_tokens
            .unwrap_or(0)
            .saturating_add(usage.output_tokens.unwrap_or(0))
    }
    pub(super) fn token_count(&self) -> Option<u64> {
        total_token_usage(Some(&self.usage.lock().expect("usage observation")))
    }
    pub(super) fn context(
        &self,
        policy: ProviderTimeoutPolicy,
        streaming: bool,
    ) -> anyhow::Result<pioneer_provider::ProviderAttemptContext> {
        pioneer_provider::ProviderAttemptContext::from_started(policy, streaming, self.started)
    }
    fn item(&self, completed: Option<bool>) -> TurnItem {
        let mut usage = self.usage.lock().expect("usage observation").clone();
        if let Some(raw) = &usage.raw_usage {
            usage.raw_usage = Some(pioneer_provider::usage::bounded_usage(raw));
        }
        TurnItem::SystemEvent {
            id: self.id.clone(),
            level: pioneer_protocol::SystemEventLevel::Info,
            message: "Provider usage observation".into(),
            code: Some("provider_usage".into()),
            details: Some(serde_json::json!({"schema_version":1,
                "nativeMethod":"provider/usage/observed", "observation_id":self.id,
                "physical_attempt_id":usage.physical_attempt_id,
                "window_index":self.window_index,
                "request_sent":serde_json::Value::Null,
                "provider":self.provider,"model":self.model,"api":self.api,"route":self.route,
                "status":match completed {None=>"started",Some(true)=>"completed",Some(false)=>"failed"},
                "complete":completed == Some(true),"usage":usage})),
        }
    }
    pub(super) async fn publish(
        &self,
        events: &AgentEventHub,
        workspace: &str,
        thread: &str,
        turn: &str,
        completed: Option<bool>,
    ) -> Result<(), ChatTurnError> {
        let item = self.item(completed);
        let event = if completed.is_none() {
            AgentDurableEvent::ItemStarted {
                notification: ItemStartedNotification {
                    workspace_id: workspace.into(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item,
                },
            }
        } else {
            AgentDurableEvent::ItemCompleted {
                notification: ItemCompletedNotification {
                    workspace_id: workspace.into(),
                    thread_id: thread.into(),
                    turn_id: turn.into(),
                    item,
                },
            }
        };
        let result = Self::publish_usage_event(events, event).await;
        if let Err(error) = &result {
            self.record_publication_failure(error);
        }
        result
    }
}

#[cfg(test)]
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
    request_agent_round_accounted(
        provider,
        request,
        workspace_id,
        thread_id,
        turn_id,
        thinking_item_id,
        force_non_stream,
        provider_timeout_policy,
        event_tx,
        None,
    )
    .await
}

fn retry_invalidated_reference<T>(
    result: &Result<T, ChatTurnError>,
    attempt: usize,
    known_tokens: u64,
) -> bool {
    attempt == 0
        && known_tokens == 0
        && matches!(result,
        Err(ChatTurnError::ProviderFailure { failure, .. })
            if failure.class == ProviderFailureClass::ProviderRejected
            && failure.stage == ProviderFailureStage::Connect
            && failure.provider_code.as_deref() == Some(pioneer_provider::failure::REJECTED_CACHED_FILE_REFERENCE_CODE))
}

pub(super) async fn request_agent_round_accounted(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    force_non_stream: bool,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
    mut accounting: Option<&mut super::ChatExecutionWindowStats>,
) -> Result<AgentRoundResponse, ChatTurnError> {
    let context = pioneer_provider::ProviderAttemptContext::from_started(
        provider_timeout_policy,
        provider.capabilities().streaming && !force_non_stream,
        tokio::time::Instant::now(),
    )
    .map_err(|_| {
        ChatTurnError::Terminal("provider attempt timeout policy is out of range".into())
    })?;
    for attempt in 0..2 {
        let observation = ProviderAttemptObservation::new(provider.as_ref(), &request.model)
            .with_window_index(accounting.as_ref().map(|stats| stats.window_index))
            .with_publication(event_tx, workspace_id, thread_id, turn_id);
        let attempt_context = pioneer_provider::ProviderAttemptContext {
            cancellation: context.cancellation.child_token(),
            ..context.clone()
        };
        let mut observation_started = false;
        let result = pioneer_provider::with_provider_attempt_context(attempt_context, async {
            observation
                .publish(event_tx, workspace_id, thread_id, turn_id, None)
                .await?;
            observation_started = true;
            request_agent_round_observed(
                provider,
                request.clone(),
                workspace_id,
                thread_id,
                turn_id,
                thinking_item_id,
                force_non_stream,
                provider_timeout_policy,
                event_tx,
                &observation,
            )
            .await
        })
        .await;
        if let Some(stats) = accounting.as_deref_mut() {
            stats.record_provider_round(
                result.as_ref().map_or(0, |round| round.tool_calls.len()),
                Some(observation.known_token_count()),
            );
            stats.provider_token_usage_unknown |= observation.token_count().is_none();
        }
        if observation_started {
            observation
                .publish(
                    event_tx,
                    workspace_id,
                    thread_id,
                    turn_id,
                    Some(result.is_ok()),
                )
                .await?;
        }
        if retry_invalidated_reference(&result, attempt, observation.known_token_count()) {
            continue;
        }
        return result;
    }
    unreachable!("bounded provider reference retry always returns")
}

#[cfg(test)]
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
) -> Result<(String, Option<TokenUsage>), ChatTurnError> {
    stream_provider_response_accounted(
        provider,
        request,
        workspace_id,
        thread_id,
        turn_id,
        thinking_item_id,
        message_item_id,
        provider_timeout_policy,
        event_tx,
        None,
        None,
    )
    .await
}

pub(super) async fn stream_provider_response_accounted(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
    mut accounting: Option<&mut super::ChatExecutionWindowStats>,
    finalization: Option<&super::StandardFinalization<'_>>,
) -> Result<(String, Option<pioneer_provider::TokenUsage>), ChatTurnError> {
    let context = pioneer_provider::ProviderAttemptContext::from_started(
        provider_timeout_policy,
        true,
        tokio::time::Instant::now(),
    )
    .map_err(|_| {
        ChatTurnError::Terminal("provider attempt timeout policy is out of range".into())
    })?;
    for attempt in 0..2 {
        let observation = ProviderAttemptObservation::new(provider.as_ref(), &request.model)
            .with_window_index(accounting.as_ref().map(|stats| stats.window_index))
            .with_publication(event_tx, workspace_id, thread_id, turn_id);
        let attempt_context = pioneer_provider::ProviderAttemptContext {
            cancellation: context.cancellation.child_token(),
            ..context.clone()
        };
        let mut observation_started = false;
        let result = pioneer_provider::with_provider_attempt_context(attempt_context, async {
            observation
                .publish(event_tx, workspace_id, thread_id, turn_id, None)
                .await?;
            observation_started = true;
            stream_provider_response_observed(
                provider,
                request.clone(),
                workspace_id,
                thread_id,
                turn_id,
                thinking_item_id,
                message_item_id,
                provider_timeout_policy,
                event_tx,
                &observation,
                finalization,
            )
            .await
        })
        .await;
        if let Some(stats) = accounting.as_deref_mut() {
            stats.record_provider_round(0, Some(observation.known_token_count()));
            stats.provider_token_usage_unknown |= observation.token_count().is_none();
        }
        if observation_started {
            observation
                .publish(
                    event_tx,
                    workspace_id,
                    thread_id,
                    turn_id,
                    Some(result.is_ok()),
                )
                .await?;
        }
        if retry_invalidated_reference(&result, attempt, observation.known_token_count()) {
            continue;
        }
        return result;
    }
    unreachable!("bounded provider reference retry always returns")
}

pub(super) async fn non_stream_provider_response_with_policy(
    provider: &Arc<dyn Provider>,
    request: ChatRequest,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
    thinking_item_id: &str,
    message_item_id: &str,
    provider_timeout_policy: ProviderTimeoutPolicy,
    event_tx: &AgentEventHub,
    mut accounting: Option<&mut super::ChatExecutionWindowStats>,
    finalization: Option<&super::StandardFinalization<'_>>,
) -> Result<(String, Option<TokenUsage>), ChatTurnError> {
    let context = pioneer_provider::ProviderAttemptContext::from_started(
        provider_timeout_policy,
        false,
        tokio::time::Instant::now(),
    )
    .map_err(|_| {
        ChatTurnError::Terminal("provider attempt timeout policy is out of range".into())
    })?;
    for attempt in 0..2 {
        let observation = ProviderAttemptObservation::new(provider.as_ref(), &request.model)
            .with_window_index(accounting.as_ref().map(|stats| stats.window_index))
            .with_publication(event_tx, workspace_id, thread_id, turn_id);
        let attempt_context = pioneer_provider::ProviderAttemptContext {
            cancellation: context.cancellation.child_token(),
            ..context.clone()
        };
        let mut observation_started = false;
        let result = pioneer_provider::with_provider_attempt_context(attempt_context, async {
            observation
                .publish(event_tx, workspace_id, thread_id, turn_id, None)
                .await?;
            observation_started = true;
            non_stream_provider_response_observed(
                provider,
                request.clone(),
                workspace_id,
                thread_id,
                turn_id,
                thinking_item_id,
                message_item_id,
                event_tx,
                &observation,
                finalization,
            )
            .await
        })
        .await;
        if let Some(stats) = accounting.as_deref_mut() {
            stats.record_provider_round(0, Some(observation.known_token_count()));
            stats.provider_token_usage_unknown |= observation.token_count().is_none();
        }
        if observation_started {
            observation
                .publish(
                    event_tx,
                    workspace_id,
                    thread_id,
                    turn_id,
                    Some(result.is_ok()),
                )
                .await?;
        }
        if retry_invalidated_reference(&result, attempt, observation.known_token_count()) {
            continue;
        }
        return result;
    }
    unreachable!("bounded provider reference retry always returns")
}

#[cfg(test)]
mod terminal_boundary_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixtureProvider {
        chunks: Vec<StreamChunk>,
        fail: bool,
        attempts: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Provider for FixtureProvider {
        fn name(&self) -> &str {
            "fixture"
        }
        fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
            pioneer_provider::ProviderCapabilities {
                streaming: true,
                ..Default::default()
            }
        }
        async fn chat(&self, _: ChatRequest) -> anyhow::Result<pioneer_provider::ChatResponse> {
            anyhow::bail!("unexpected fallback/retry")
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let mut chunks = self.chunks.iter().cloned().map(Ok).collect::<Vec<_>>();
            if self.fail {
                chunks.push(Err(
                    pioneer_provider::failure::ProviderStreamIncomplete::EofWithoutTerminalMarker
                        .into(),
                ));
            }
            Ok(Box::pin(futures_util::stream::iter(chunks)))
        }
    }

    struct ExpiredReferenceProvider {
        code: &'static str,
        deadlines: Arc<std::sync::Mutex<Vec<(tokio::time::Instant, tokio::time::Instant)>>>,
    }
    #[test]
    fn cached_reference_retry_rejects_known_cost_partial_stream_and_second_rejection() {
        let mut classification =
            ProviderFailureClassification::new(ProviderFailureClass::ProviderRejected);
        classification.provider_code =
            Some(pioneer_provider::failure::REJECTED_CACHED_FILE_REFERENCE_CODE.into());
        for (attempt, tokens, stage) in [
            (0, 1, ProviderFailureStage::Connect),
            (0, 0, ProviderFailureStage::MidStream),
            (1, 0, ProviderFailureStage::Connect),
        ] {
            let error: Result<(), ChatTurnError> = Err(provider_failure_error_with_classification(
                "reasoning",
                TurnItemType::Reasoning,
                "fixture",
                "model",
                ProviderTransportKind::Stream,
                stage,
                "fixed failure".into(),
                Some(classification.clone()),
            ));
            assert!(!retry_invalidated_reference(&error, attempt, tokens));
        }
    }
    #[async_trait]
    impl Provider for ExpiredReferenceProvider {
        fn name(&self) -> &str {
            "fixture"
        }
        async fn chat(&self, _: ChatRequest) -> anyhow::Result<pioneer_provider::ChatResponse> {
            let context = pioneer_provider::current_provider_attempt_context().unwrap();
            self.deadlines
                .lock()
                .unwrap()
                .push((context.first_output_deadline, context.deadline));
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let mut classification =
                ProviderFailureClassification::new(ProviderFailureClass::ProviderRejected);
            classification.provider_code = Some(self.code.into());
            classification.http_status = Some(404);
            Err(pioneer_provider::failure::ProviderTransportFailure { classification }.into())
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            anyhow::bail!("unexpected stream")
        }
    }
    #[tokio::test(start_paused = true)]
    async fn generated_expiry_receipt_retries_once_with_original_deadlines_and_distinct_usage() {
        for (code, expected_attempts) in [
            (
                pioneer_provider::failure::REJECTED_CACHED_FILE_REFERENCE_CODE,
                2usize,
            ),
            ("file_not_found", 1usize),
            ("not_found", 1usize),
        ] {
            let deadlines = Arc::new(std::sync::Mutex::new(Vec::new()));
            let provider: Arc<dyn Provider> = Arc::new(ExpiredReferenceProvider {
                code,
                deadlines: deadlines.clone(),
            });
            let hub = AgentEventHub::new();
            let mut receiver = hub.take_durable_receiver().await.unwrap();
            let mut stats = super::super::ChatExecutionWindowStats::new(1);
            let (result, completed_ids) = tokio::join!(
                request_agent_round_accounted(
                    &provider,
                    request(),
                    "ws",
                    "thread",
                    "turn",
                    "reasoning",
                    true,
                    ProviderTimeoutPolicy::default(),
                    &hub,
                    Some(&mut stats)
                ),
                async {
                    let mut completed_ids = Vec::new();
                    while completed_ids.len() < expected_attempts {
                        let event = receiver.recv().await.unwrap();
                        if let AgentDurableEvent::ItemCompleted { notification } = &event {
                            if let TurnItem::SystemEvent { code, id, .. } = &notification.item {
                                if code.as_deref() == Some("provider_usage") {
                                    completed_ids.push(id.clone());
                                }
                            }
                        }
                        receiver.acknowledge_last(Ok(()));
                    }
                    completed_ids
                }
            );
            assert!(result.is_err());
            assert_eq!(stats.agent_round_count as usize, expected_attempts);
            assert_eq!(stats.provider_token_count, Some(0));
            assert!(stats.provider_token_usage_unknown);
            let recorded = deadlines.lock().unwrap();
            assert_eq!(recorded.len(), expected_attempts);
            assert!(recorded.iter().all(|deadline| deadline == &recorded[0]));
            assert_eq!(
                completed_ids
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                expected_attempts
            );
        }
    }

    struct SlowNonStreamProvider;
    #[async_trait]
    impl Provider for SlowNonStreamProvider {
        fn name(&self) -> &str {
            "slow-non-stream"
        }
        fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
            Default::default()
        }
        async fn chat(&self, _: ChatRequest) -> anyhow::Result<pioneer_provider::ChatResponse> {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            anyhow::bail!("unexpected completion after configured deadline")
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            anyhow::bail!("unexpected streaming")
        }
    }
    #[tokio::test(start_paused = true)]
    async fn non_stream_attempt_uses_configured_timeout_and_accounts_failure_once() {
        let provider: Arc<dyn Provider> = Arc::new(SlowNonStreamProvider);
        let hub = AgentEventHub::new();
        let mut policy = ProviderTimeoutPolicy::default();
        policy.non_stream_request_timeout = std::time::Duration::from_millis(50);
        let mut stats = super::super::ChatExecutionWindowStats::new(1);
        let started = tokio::time::Instant::now();
        let (result, _) = tokio::join!(
            non_stream_provider_response_with_policy(
                &provider,
                request(),
                "ws",
                "thread",
                "turn",
                "thinking",
                "message",
                policy,
                &hub,
                Some(&mut stats),
                None,
            ),
            acknowledge_round_events(&hub)
        );
        assert_eq!(started.elapsed(), std::time::Duration::from_millis(50));
        assert!(
            matches!(result, Err(ChatTurnError::ProviderFailure { failure, .. })
            if failure.class == ProviderFailureClass::NetworkTransient)
        );
        assert_eq!(stats.agent_round_count, 1);
        assert_eq!(stats.provider_token_count, Some(0));
        assert!(stats.provider_token_usage_unknown);
    }

    #[tokio::test(start_paused = true)]
    async fn ready_identity_flood_yields_and_retains_the_first_output_deadline() {
        let provider: Arc<dyn Provider> = Arc::new(SlowNonStreamProvider);
        let mut policy = ProviderTimeoutPolicy::default();
        policy.first_chunk_timeout = std::time::Duration::from_millis(100);
        let observation = ProviderAttemptObservation::new(provider.as_ref(), "fixture");
        let context = observation.context(policy, true).unwrap();
        let mut identity = StreamChunk::delta("");
        identity.usage = Some(TokenUsage {
            request_id: Some("req-identity".into()),
            ..Default::default()
        });
        let mut stream = futures_util::stream::repeat(identity).map(Ok::<_, anyhow::Error>);
        let mut seen = false;
        let (result, ()) = tokio::join!(
            pioneer_provider::with_provider_attempt_context(context, async {
                read_next_stream_chunk(
                    &mut stream,
                    &mut seen,
                    &Default::default(),
                    FailureTarget::new("item", TurnItemType::Reasoning),
                    &provider,
                    "fixture",
                    policy,
                    &observation,
                )
                .await
            }),
            async {
                tokio::time::advance(std::time::Duration::from_millis(100)).await;
            }
        );
        assert!(
            matches!(result, Err(ChatTurnError::ProviderFailure { failure, .. })
            if failure.class == ProviderFailureClass::StreamStall)
        );
        assert!(!seen);
        assert_eq!(
            observation.usage.lock().unwrap().request_id.as_deref(),
            Some("req-identity")
        );
        assert_eq!(observation.token_count(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_stream_deadline_does_not_poll_even_a_ready_chunk() {
        let provider: Arc<dyn Provider> = Arc::new(SlowNonStreamProvider);
        let policy = ProviderTimeoutPolicy::default();
        let observation = ProviderAttemptObservation::new(provider.as_ref(), "fixture");
        let context = observation.context(policy, true).unwrap();
        tokio::time::advance(context.first_output_deadline - tokio::time::Instant::now()).await;
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = polls.clone();
        let mut stream = futures_util::stream::poll_fn(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Ready(Some(Ok(StreamChunk::delta("too late"))))
        });
        let result = pioneer_provider::with_provider_attempt_context(context, async {
            read_next_stream_chunk(
                &mut stream,
                &mut false,
                &Default::default(),
                FailureTarget::new("item", TurnItemType::Reasoning),
                &provider,
                "fixture",
                policy,
                &observation,
            )
            .await
        })
        .await;
        assert!(
            matches!(result, Err(ChatTurnError::ProviderFailure { failure, .. })
            if failure.class == ProviderFailureClass::StreamStall)
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn late_headers_do_not_grant_a_second_first_output_budget() {
        let provider: Arc<dyn Provider> = Arc::new(FixtureProvider {
            chunks: vec![],
            fail: false,
            attempts: Arc::new(AtomicUsize::new(0)),
        });
        let mut policy = ProviderTimeoutPolicy::default();
        policy.first_chunk_timeout = std::time::Duration::from_millis(100);
        policy.max_stream_duration = Some(std::time::Duration::from_millis(500));
        let observation = ProviderAttemptObservation::new(provider.as_ref(), "fixture");
        let context = pioneer_provider::ProviderAttemptContext::from_started(
            policy,
            true,
            observation.started,
        )
        .unwrap();
        let started = observation.started;
        let error = pioneer_provider::with_provider_attempt_context(context, async {
            await_provider_dispatch(true, async {
                tokio::time::sleep(std::time::Duration::from_millis(90)).await;
                Ok(())
            })
            .await
            .unwrap();
            let mut stream = Box::pin(futures_util::stream::once(async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                Ok(StreamChunk::delta("meaningful"))
            }));
            let mut seen = false;
            read_next_stream_chunk(
                &mut stream,
                &mut seen,
                &Default::default(),
                FailureTarget::new("item", TurnItemType::Reasoning),
                &provider,
                "fixture",
                policy,
                &observation,
            )
            .await
            .unwrap_err()
        })
        .await;
        assert_eq!(started.elapsed(), std::time::Duration::from_millis(100));
        assert!(
            matches!(error, ChatTurnError::ProviderFailure { failure, .. }
            if failure.stage == ProviderFailureStage::FirstChunk
                && failure.provider_code.as_deref() == Some("provider_first_output_deadline_exceeded"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn meaningful_chunks_cannot_extend_absolute_attempt_cap() {
        let provider: Arc<dyn Provider> = Arc::new(FixtureProvider {
            chunks: vec![],
            fail: false,
            attempts: Arc::new(AtomicUsize::new(0)),
        });
        let mut policy = ProviderTimeoutPolicy::default();
        policy.first_chunk_timeout = std::time::Duration::from_millis(20);
        policy.inter_chunk_idle_timeout = std::time::Duration::from_millis(20);
        policy.max_stream_duration = Some(std::time::Duration::from_millis(35));
        let observation = ProviderAttemptObservation::new(provider.as_ref(), "fixture");
        let context = pioneer_provider::ProviderAttemptContext::from_started(
            policy,
            true,
            observation.started,
        )
        .unwrap();
        let started = observation.started;
        let error = pioneer_provider::with_provider_attempt_context(context, async {
            let mut stream = Box::pin(futures_util::stream::unfold((), |_| async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                Some((Ok(StreamChunk::delta("x")), ()))
            }));
            let mut seen = false;
            for _ in 0..3 {
                assert!(
                    read_next_stream_chunk(
                        &mut stream,
                        &mut seen,
                        &Default::default(),
                        FailureTarget::new("item", TurnItemType::Reasoning),
                        &provider,
                        "fixture",
                        policy,
                        &observation
                    )
                    .await
                    .unwrap()
                    .is_some()
                );
            }
            read_next_stream_chunk(
                &mut stream,
                &mut seen,
                &Default::default(),
                FailureTarget::new("item", TurnItemType::Reasoning),
                &provider,
                "fixture",
                policy,
                &observation,
            )
            .await
            .unwrap_err()
        })
        .await;
        assert_eq!(started.elapsed(), std::time::Duration::from_millis(35));
        assert!(
            matches!(error, ChatTurnError::ProviderFailure { failure, .. }
            if failure.provider_code.as_deref() == Some("provider_attempt_deadline_exceeded"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_attempt_never_dispatches_the_next_stage() {
        let context =
            pioneer_provider::ProviderAttemptContext::new(ProviderTimeoutPolicy::default(), true)
                .unwrap();
        tokio::time::advance(
            context
                .first_output_deadline
                .saturating_duration_since(tokio::time::Instant::now()),
        )
        .await;
        let dispatched = AtomicUsize::new(0);
        let result = pioneer_provider::with_provider_attempt_context(
            context.clone(),
            await_provider_dispatch(true, async {
                dispatched.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(dispatched.load(Ordering::SeqCst), 0);
        assert!(context.cancellation.is_cancelled());
    }

    fn request() -> ChatRequest {
        ChatRequest {
            model: "fixture".into(),
            messages: vec![pioneer_provider::ChatMessage::user("fixture")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }

    async fn acknowledge_round_events(hub: &AgentEventHub) -> Vec<AgentDurableEvent> {
        let mut receiver = hub.take_durable_receiver().await.unwrap();
        let mut events = Vec::new();
        loop {
            let event = receiver.recv().await.unwrap();
            let usage_completed = matches!(&event,
                AgentDurableEvent::ItemCompleted { notification }
                    if matches!(&notification.item,
                        TurnItem::SystemEvent { code, .. }
                            if code.as_deref() == Some("provider_usage")));
            receiver.acknowledge_last(Ok(()));
            events.push(event);
            if usage_completed {
                return events;
            }
        }
    }

    #[tokio::test]
    async fn failed_partial_call_never_becomes_executable_round_or_automatic_retry() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn Provider> = Arc::new(FixtureProvider {
            chunks: vec![StreamChunk::tool_calls(vec![ProviderToolCall {
                id: "call".into(),
                name: "write_file".into(),
                arguments: "{}".into(),
            }])],
            fail: true,
            attempts: attempts.clone(),
        });
        let hub = AgentEventHub::new();
        let (result, events) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                request_agent_round(
                    &provider,
                    request(),
                    "ws",
                    "thread",
                    "turn",
                    "item",
                    false,
                    ProviderTimeoutPolicy::default(),
                    &hub,
                ),
                acknowledge_round_events(&hub),
            )
        })
        .await
        .expect("provider round and all durable acknowledgements must finish");
        let history = events
            .iter()
            .filter_map(|event| match event {
                AgentDurableEvent::TurnProviderHistoryAppended { payload, .. } => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(history.len(), 1, "expected one failed observation");
        for payload in history {
            let envelope: pioneer_provider::CanonicalProviderRoundEnvelope =
                serde_json::from_value(payload.clone()).unwrap();
            assert_eq!(envelope.termination, ProviderTermination::ProviderError);
            assert!(
                envelope.calls.is_empty(),
                "failed observations must not retain executable identities"
            );
        }
        assert!(matches!(result, Err(ChatTurnError::ProviderFailure { .. })));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn final_chunk_payload_is_accumulated_before_terminal_validation() {
        let mut terminal = StreamChunk::final_chunk_with(ProviderTermination::Complete);
        terminal.delta = "terminal text".into();
        let provider: Arc<dyn Provider> = Arc::new(FixtureProvider {
            chunks: vec![terminal],
            fail: false,
            attempts: Arc::new(AtomicUsize::new(0)),
        });
        let hub = AgentEventHub::new();
        let (result, _) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(
                request_agent_round(
                    &provider,
                    request(),
                    "ws",
                    "thread",
                    "turn",
                    "item",
                    false,
                    ProviderTimeoutPolicy::default(),
                    &hub,
                ),
                acknowledge_round_events(&hub),
            )
        })
        .await
        .expect("provider round and all durable acknowledgements must finish");
        let result = result.unwrap();
        assert_eq!(result.text, "terminal text");
    }
}

#[cfg(test)]
mod native_decoder_gate_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Test-only HTTP transport. No ready-made decoder errors: real adapters parse
    // these bytes, then request_agent_round controls access to the tool dispatcher.
    struct CountingTransport {
        inner: Arc<dyn Provider>,
        attempts: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Provider for CountingTransport {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
            self.inner.capabilities()
        }
        async fn chat(
            &self,
            request: ChatRequest,
        ) -> anyhow::Result<pioneer_provider::ChatResponse> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.inner.chat(request).await
        }
        async fn stream_chat(
            &self,
            request: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.inner.stream_chat(request).await
        }
    }
    fn event(value: serde_json::Value) -> String {
        format!("data: {value}\n\n")
    }
    fn closed_call() -> String {
        event(serde_json::json!({"type":"message_start","message":{}}))
            + &event(
                serde_json::json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"first","name":"write_file","input":{}}}),
            )
            + &event(serde_json::json!({"type":"content_block_stop","index":0}))
    }

    async fn check_gate(body: String, ollama: bool, success: bool) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(AtomicUsize::new(0));
            let received_requests = requests.clone();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                loop {
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&buffer[..count]);
                    assert!(request.len() <= 65536);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&request[..end]).unwrap();
                        let length = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap())
                        }).unwrap_or(0);
                        if request.len() >= end + 4 + length { break; }
                    }
                }
                received_requests.fetch_add(1, Ordering::SeqCst);
                let content_type = if ollama { "application/json" } else { "text/event-stream" };
                let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                socket.write_all(headers.as_bytes()).await.unwrap();
                // Wire fragmentation goes through reqwest and the production decoder.
                for bytes in body.as_bytes().chunks(3) {
                    // A malformed frame may make the client close before later frames.
                    if socket.write_all(bytes).await.is_err() { return; }
                }
                let _ = socket.shutdown().await;
            });
            let inner: Arc<dyn Provider> = if ollama {
                Arc::new(pioneer_provider::providers::OllamaProvider::with_base_url(base))
            } else {
                Arc::new(pioneer_provider::providers::AnthropicProvider::with_base_url("test-only", base))
            };
            let attempts = Arc::new(AtomicUsize::new(0));
            let name = if ollama { "ollama" } else { "anthropic" };
            // Input preparation requires the authority scope supplied by the
            // registry, including for text-only requests to the local transport.
            let registry = pioneer_provider::ProviderRegistry::with_provider(
                name,
                Arc::new(CountingTransport { inner, attempts: attempts.clone() }),
            );
            let provider = registry.get_or_create_for_workspace("ws", name).unwrap();
            assert!(provider.authority_fingerprint().is_some());
            let hub = AgentEventHub::new();
            let mut receiver = hub.take_durable_receiver().await.unwrap();
            let observations = Arc::new(AtomicUsize::new(0));
            let count = observations.clone();
            let acknowledgements = tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    if let AgentDurableEvent::TurnProviderHistoryAppended { payload, .. } = event {
                        let envelope: pioneer_provider::CanonicalProviderRoundEnvelope = serde_json::from_value(payload).unwrap();
                        assert_eq!(envelope.termination, ProviderTermination::ProviderError);
                        assert!(envelope.calls.is_empty());
                        assert_eq!(envelope.message.tool_calls.as_ref().unwrap().len(), 1);
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                    receiver.acknowledge_last(Ok(()));
                }
            });
            let request = ChatRequest {
                model: "fixture".into(), messages: vec![pioneer_provider::ChatMessage::user("fixture")],
                temperature: None, max_tokens: None, tools: None, tool_choice: None,
                parallel_tool_calls: None, reasoning: None, compiled_prompt: None,
            };
            let result = request_agent_round(&provider, request, "ws", "thread", "turn", "item", ollama, ProviderTimeoutPolicy::default(), &hub).await;
            let executions = AtomicUsize::new(0);
            // Same boundary used by the runner: dispatch only a successful round.
            if let Ok(round) = &result {
                for _call in &round.tool_calls { executions.fetch_add(1, Ordering::SeqCst); }
            }
            assert_eq!(attempts.load(Ordering::SeqCst), 1, "no automatic retry/fallback");
            assert_eq!(requests.load(Ordering::SeqCst), 1, "must reach the native decoder through HTTP");
            if success {
                assert!(result.is_ok());
                assert_eq!(executions.load(Ordering::SeqCst), 1);
            } else {
                assert!(matches!(result, Err(ChatTurnError::ProviderFailure { .. })));
                assert_eq!(executions.load(Ordering::SeqCst), 0);
                if !ollama { assert_eq!(observations.load(Ordering::SeqCst), 1); }
            }
            server.await.unwrap();
            acknowledgements.abort();
        }).await.unwrap();
    }

    #[tokio::test]
    async fn contradictory_native_finish_after_valid_call_fails_agent_gate() {
        let body = closed_call()
            + &event(
                serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            )
            + &event(
                serde_json::json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
            )
            + &event(serde_json::json!({"type":"message_stop"}));
        check_gate(body, false, false).await;
    }
    #[tokio::test]
    async fn malformed_native_block_after_valid_call_fails_agent_gate() {
        for block in [
            serde_json::json!({"type":"tool_use","id":"second","name":"write_file","input":{}}),
            serde_json::json!({"type":"tool_use","name":"write_file","input":{}}),
            serde_json::json!({"type":"tool_use","id":"second","input":{}}),
        ] {
            let index = if block.get("id").is_some() && block.get("name").is_some() {
                0
            } else {
                1
            };
            let body = closed_call()
                + &event(
                    serde_json::json!({"type":"content_block_start","index":index,"content_block":block}),
                )
                + &event(serde_json::json!({"type":"content_block_stop","index":index}))
                + &event(
                    serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
                )
                + &event(serde_json::json!({"type":"message_stop"}));
            check_gate(body, false, false).await;
        }
    }
    #[tokio::test]
    async fn incomplete_ollama_non_stream_cannot_dispatch_tools() {
        for done in [None, Some(false), Some(true)] {
            let mut body = serde_json::json!({"message":{"tool_calls":[{"function":{"name":"write_file","arguments":{}}}]},"done_reason":"stop"});
            if let Some(done) = done {
                body["done"] = serde_json::json!(done);
            }
            check_gate(body.to_string(), true, done == Some(true)).await;
        }
        let body = closed_call()
            + &event(
                serde_json::json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            )
            + &event(serde_json::json!({"type":"message_stop"}));
        check_gate(body, false, true).await;
    }
}

#[cfg(test)]
mod standard_final_gate_tests {
    use super::*;
    struct ResponseProvider(pioneer_provider::ChatResponse);
    #[async_trait::async_trait]
    impl Provider for ResponseProvider {
        fn name(&self) -> &str {
            "standard-final-fixture"
        }
        fn capabilities(&self) -> pioneer_provider::ProviderCapabilities {
            pioneer_provider::ProviderCapabilities {
                streaming: true,
                ..Default::default()
            }
        }
        async fn chat(&self, _: ChatRequest) -> anyhow::Result<pioneer_provider::ChatResponse> {
            Ok(self.0.clone())
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>>
        {
            Ok(futures_util::stream::iter(vec![
                Ok(StreamChunk::delta(&self.0.text)),
                Ok(StreamChunk::tool_calls(self.0.tool_calls.clone())),
                Ok(StreamChunk::provider_replay_state(
                    self.0.provider_replay_state.clone().unwrap(),
                )),
                Ok(StreamChunk::final_chunk_with(self.0.termination.clone())),
            ])
            .boxed())
        }
    }
    struct UnresolvedTasks;
    #[async_trait::async_trait]
    impl crate::TaskToolProvider for UnresolvedTasks {
        fn terminal_cleanup_runtime_contract(&self) -> &'static str {
            "test.unresolved.v1"
        }
        async fn materialize_task_tools(
            &self,
            _: crate::TaskTurnContext,
        ) -> Result<crate::TaskToolMaterialization, String> {
            Ok(Default::default())
        }
        async fn pending_attached_tasks(
            &self,
            _: crate::TaskTurnContext,
        ) -> Result<Vec<crate::PendingAttachedTask>, String> {
            Ok(vec![crate::PendingAttachedTask {
                task_id: "child".into(),
                run_id: Some("run".into()),
                title: "Child".into(),
                status: "running".into(),
            }])
        }
        async fn review_required_attached_task_observations(
            &self,
            _: crate::TaskTurnContext,
        ) -> Result<Vec<crate::ReviewRequiredTaskObservation>, String> {
            Ok(vec![])
        }
        async fn terminal_attached_task_observations(
            &self,
            _: crate::TaskTurnContext,
        ) -> Result<Vec<crate::TerminalTaskObservation>, String> {
            Ok(vec![])
        }
        async fn cleanup_attached_tasks(
            &self,
            _: crate::TaskTurnContext,
            _: String,
        ) -> Result<(), String> {
            Ok(())
        }
        async fn cleanup_attached_tasks_idempotent(
            &self,
            _: &str,
            _: crate::TaskTurnContext,
            _: String,
        ) -> Result<(), String> {
            Ok(())
        }
    }
    struct UnregisteredArtifact;
    #[async_trait::async_trait]
    impl crate::TurnFinalizationProvider for UnregisteredArtifact {
        async fn check_turn_finalization(
            &self,
            _: crate::TurnFinalizationContext,
        ) -> Result<crate::TurnFinalizationDecision, String> {
            Ok(crate::TurnFinalizationDecision::Retry {
                instruction: "register original artifact".into(),
            })
        }
    }
    #[tokio::test]
    async fn both_standard_transports_gate_unresolved_tasks_and_artifacts_before_durable_final() {
        for stream in [false, true] {
            for task_obligation in [false, true] {
                let provider: Arc<dyn Provider> =
                    Arc::new(ResponseProvider(pioneer_provider::ChatResponse {
                        text: "candidate final".into(),
                        termination: ProviderTermination::Complete,
                        tool_calls: vec![],
                        usage: None,
                        reasoning_content: None,
                        provider_replay_state: Some(pioneer_provider::ProviderReplayState::new(
                            "fixture",
                            serde_json::json!({"opaque":"unchanged"}),
                        )),
                    }));
                let task: Arc<dyn crate::TaskToolProvider> = Arc::new(UnresolvedTasks);
                let artifact: Arc<dyn crate::TurnFinalizationProvider> =
                    Arc::new(UnregisteredArtifact);
                let gate = super::super::StandardFinalization {
                    task: task_obligation.then_some(&task),
                    finalization: (!task_obligation).then_some(&artifact),
                    context: None,
                };
                let hub = AgentEventHub::new();
                let mut receiver = hub.take_durable_receiver().await.unwrap();
                let receive = async {
                    let mut events = vec![];
                    loop {
                        let event = receiver.recv().await.unwrap();
                        let done = matches!(&event, AgentDurableEvent::ItemCompleted { notification } if matches!(&notification.item,
                            TurnItem::SystemEvent { code: Some(code), details: Some(details), .. } if code == "provider_usage" && matches!(details.get("status").and_then(serde_json::Value::as_str), Some("completed" | "failed"))));
                        receiver.acknowledge_last(Ok(()));
                        events.push(event);
                        if done {
                            break events;
                        }
                    }
                };
                let work = async {
                    if stream {
                        stream_provider_response_accounted(
                            &provider,
                            request(),
                            "ws",
                            "thread",
                            "turn",
                            "reasoning",
                            "final",
                            ProviderTimeoutPolicy::default(),
                            &hub,
                            None,
                            Some(&gate),
                        )
                        .await
                    } else {
                        non_stream_provider_response_with_policy(
                            &provider,
                            request(),
                            "ws",
                            "thread",
                            "turn",
                            "reasoning",
                            "final",
                            ProviderTimeoutPolicy::default(),
                            &hub,
                            None,
                            Some(&gate),
                        )
                        .await
                    }
                };
                let (result, events) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        tokio::join!(work, receive)
                    })
                    .await
                    .unwrap();
                assert!(matches!(result, Err(ChatTurnError::Blocked(_))));
                assert!(!events.iter().any(|event| matches!(
                    event,
                    AgentDurableEvent::TurnFinalizationPrepared { .. }
                )));
            }
        }
    }
    fn request() -> ChatRequest {
        ChatRequest {
            model: "fixture".into(),
            messages: vec![pioneer_provider::ChatMessage::user("Exact output")],
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            temperature: None,
            max_tokens: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }
    #[tokio::test]
    async fn both_transports_accept_exact_text_and_reject_empty_truncated_and_unexpected_calls_before_success()
     {
        for stream in [false, true] {
            for (text, termination, calls, failure_class) in [
                (
                    "<tool_call><invoke name=\"read_file\"><arguments>{}</arguments></invoke></tool_call>",
                    ProviderTermination::Complete,
                    vec![],
                    None,
                ),
                (
                    r#"{"name":"read_file","description":"Read","parameters":{"type":"object","properties":{}}}"#,
                    ProviderTermination::Complete,
                    vec![],
                    None,
                ),
                (
                    "  \n",
                    ProviderTermination::Complete,
                    vec![],
                    Some(ProviderFailureClass::EmptyResponse),
                ),
                (
                    "partial",
                    ProviderTermination::Length,
                    vec![],
                    Some(ProviderFailureClass::MaxOutputTokens),
                ),
                (
                    "",
                    ProviderTermination::ToolCalls,
                    vec![ProviderToolCall {
                        id: "unexecuted".into(),
                        name: "write_file".into(),
                        arguments: "{}".into(),
                    }],
                    Some(ProviderFailureClass::MalformedProviderRequest),
                ),
            ] {
                let provider: Arc<dyn Provider> =
                    Arc::new(ResponseProvider(pioneer_provider::ChatResponse {
                        text: text.into(),
                        termination,
                        tool_calls: calls,
                        usage: None,
                        reasoning_content: None,
                        provider_replay_state: Some(pioneer_provider::ProviderReplayState::new(
                            "fixture",
                            serde_json::json!({"opaque":"unchanged signature"}),
                        )),
                    }));
                let hub = AgentEventHub::new();
                let mut receiver = hub.take_durable_receiver().await.unwrap();
                let receive = async {
                    let mut events = vec![];
                    loop {
                        let event = receiver.recv().await.unwrap();
                        let completed = matches!(&event, AgentDurableEvent::ItemCompleted { notification } if matches!(&notification.item,
                            TurnItem::SystemEvent { code: Some(code), details: Some(details), .. } if code == "provider_usage" && matches!(details.get("status").and_then(serde_json::Value::as_str), Some("completed" | "failed"))));
                        receiver.acknowledge_last(Ok(()));
                        events.push(event);
                        if completed {
                            break events;
                        }
                    }
                };
                let work = async {
                    if stream {
                        stream_provider_response(
                            &provider,
                            request(),
                            "ws",
                            "thread",
                            "turn",
                            "reasoning",
                            "final",
                            ProviderTimeoutPolicy::default(),
                            &hub,
                        )
                        .await
                    } else {
                        non_stream_provider_response_with_policy(
                            &provider,
                            request(),
                            "ws",
                            "thread",
                            "turn",
                            "reasoning",
                            "final",
                            ProviderTimeoutPolicy::default(),
                            &hub,
                            None,
                            None,
                        )
                        .await
                    }
                };
                let (result, events) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        tokio::join!(work, receive)
                    })
                    .await
                    .unwrap();
                let finals = events
                    .iter()
                    .filter_map(|event| match event {
                        AgentDurableEvent::TurnFinalizationPrepared { notification, .. } => {
                            match &notification.item {
                                TurnItem::AgentMessage { text, .. } => Some(text.as_str()),
                                _ => None,
                            }
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                match failure_class {
                    None => {
                        assert_eq!(result.unwrap().0, text);
                        assert_eq!(finals, vec![text]);
                    }
                    Some(class) => {
                        assert!(
                            matches!(result, Err(ChatTurnError::ProviderFailure { failure, .. }) if failure.class == class)
                        );
                        assert!(finals.is_empty());
                    }
                }
                assert!(!events.iter().any(|event| matches!(event, AgentDurableEvent::ItemCompleted { notification } if matches!(notification.item, TurnItem::DynamicToolCall { .. } | TurnItem::FileChange { .. } | TurnItem::CommandExecution { .. }))), "no decorative tool success without execution");
            }
        }
    }
}
