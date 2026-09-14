//! Native API service adapter for working-context compaction. It neither
//! executes tools nor shares mutable conversation state with the main agent.
pub mod composition;
pub mod controller;
pub mod history;
pub mod request;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use pioneer_compaction::runner::FailureKind;
use pioneer_compaction::summary::{
    CompletionKind, INSTRUCTIONS, Summarizer, SummaryCompletion, SummaryFailure, SummaryRequest,
};
use pioneer_compaction::{ModelBudget, ModelSelection, Transport, text_tokens};
use pioneer_protocol::{ProviderFailureClass, ProviderFailureStage};
use pioneer_provider::{
    ChatMessage, ChatRequest, Provider, ProviderTermination, ReasoningConfig, ReasoningEffort,
};
use std::sync::Arc;

pub struct NativeSummarizer {
    provider: Arc<dyn Provider>,
    selection: ModelSelection,
    budget: ModelBudget,
}
impl NativeSummarizer {
    /// The integration layer resolves this exact provider instance with the
    /// existing credential authority. No model or instance fallback happens here.
    pub fn new(
        provider: Arc<dyn Provider>,
        selection: ModelSelection,
        budget: ModelBudget,
    ) -> Result<Self> {
        ensure!(
            selection.transport == Transport::Api,
            "native summarizer requires API selection"
        );
        ensure!(
            !selection.instance.is_empty() && !selection.model.is_empty(),
            "invalid summarizer selection"
        );
        Ok(Self {
            provider,
            selection,
            budget,
        })
    }
    fn prepare(&self, request: &SummaryRequest) -> Result<ChatRequest> {
        ensure!(
            request.selection == self.selection,
            "summarizer selection changed after admission"
        );
        ensure!(
            request.output_cap > 0 && request.output_cap <= self.budget.summarizer_cap(u64::MAX)?,
            "unsupported summarizer output cap"
        );
        let reasoning = request
            .selection
            .effort
            .as_deref()
            .map(|effort| {
                let effort = ReasoningEffort::from_str(effort)
                    .ok_or_else(|| anyhow::anyhow!("unsupported native reasoning effort"))?;
                if let Some(capabilities) =
                    pioneer_provider::reasoning_registry::reasoning_capabilities_for_model(
                        self.provider.name(),
                        &request.selection.model,
                    )
                {
                    ensure!(
                        capabilities
                            .effort_options
                            .iter()
                            .any(|value| value == effort.as_str()),
                        "reasoning effort is unavailable for selected model"
                    );
                }
                Ok::<_, anyhow::Error>(if effort == ReasoningEffort::None {
                    ReasoningConfig::Disabled
                } else {
                    ReasoningConfig::Effort(effort)
                })
            })
            .transpose()?;
        Ok(ChatRequest {
            model: request.selection.model.clone(),
            messages: vec![
                ChatMessage::system(INSTRUCTIONS),
                ChatMessage::user(request.data_json()?),
            ],
            temperature: None,
            max_tokens: Some(u32::try_from(request.output_cap)?),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning,
            compiled_prompt: None,
        })
    }
}
#[async_trait]
impl Summarizer for NativeSummarizer {
    fn model_budget(&self) -> ModelBudget {
        self.budget.clone()
    }
    fn input_tokens(&self, request: &SummaryRequest) -> Result<u64> {
        let prepared = self.prepare(request)?;
        let wire = serde_json::json!({"model":prepared.model,"messages":prepared.messages,
            "max_tokens":prepared.max_tokens,"reasoning":request.selection.effort});
        // Count complete text/role/option framing; the ordinary N2 padding is
        // applied by ModelBudget::fits, in addition to this API framing reserve.
        Ok(text_tokens(&wire.to_string()).saturating_add(32))
    }
    async fn summarize(
        &self,
        request: SummaryRequest,
    ) -> Result<SummaryCompletion, SummaryFailure> {
        let invalid = || SummaryFailure {
            diagnostic: None,
            kind: FailureKind::Permanent,
            retry_after_ms: None,
            code: "invalid_summary_request",
        };
        let input = self.input_tokens(&request).map_err(|_| invalid())?;
        if !self.budget.fits(input, request.output_cap, false) {
            return Err(SummaryFailure {
                diagnostic: None,
                kind: FailureKind::Permanent,
                retry_after_ms: None,
                code: "summary_input_overflow",
            });
        }
        let prepared = self.prepare(&request).map_err(|_| invalid())?;
        let response = self.provider.chat(prepared).await.map_err(|error| {
            let classification = self.provider.classify_failure(&error);
            let class = classification.as_ref().map(|c| c.class).unwrap_or_else(|| {
                crate::classify_provider_failure_message(
                    &error.to_string(),
                    ProviderFailureStage::Connect,
                )
            });
            let transient = matches!(
                class,
                ProviderFailureClass::NetworkTransient
                    | ProviderFailureClass::RateLimit
                    | ProviderFailureClass::Provider5xx
            );
            SummaryFailure {
                diagnostic: Some(pioneer_compaction::runner::FailureDiagnostic::new(
                    "provider_request",
                    "summary_provider_failure",
                    &format!("Provider failure class: {:?}", class),
                )),
                kind: if transient {
                    FailureKind::Transient
                } else {
                    FailureKind::Permanent
                },
                retry_after_ms: classification.and_then(|c| c.retry_after_ms),
                code: if transient {
                    "summary_transport_transient"
                } else {
                    "summary_transport_rejected"
                },
            }
        })?;
        let kind = if !response.tool_calls.is_empty() {
            CompletionKind::ToolCall
        } else {
            match response.termination {
                ProviderTermination::Complete => CompletionKind::Complete,
                ProviderTermination::ToolCalls => CompletionKind::ToolCall,
                ProviderTermination::Length => CompletionKind::Limit,
                ProviderTermination::ContentFiltered | ProviderTermination::Safety => {
                    CompletionKind::Refused
                }
                ProviderTermination::Cancelled => CompletionKind::Interrupted,
                ProviderTermination::ProviderError | ProviderTermination::Unknown(_) => {
                    CompletionKind::Unknown
                }
            }
        };
        Ok(SummaryCompletion {
            text: response.text,
            kind,
            input_tokens: response.usage.as_ref().and_then(|u| u.input_tokens),
            output_tokens: response.usage.as_ref().and_then(|u| u.output_tokens),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_compaction::summary::{HEADINGS, SummaryInput, SummaryPart, validate_summary};
    use pioneer_compaction::{CompactionMode, SourceRef};
    use pioneer_provider::{ChatResponse, ProviderFailureClassification, StreamChunk};
    use std::sync::Mutex;
    struct Fake {
        calls: Mutex<Vec<ChatRequest>>,
        termination: ProviderTermination,
        fail: bool,
    }
    #[async_trait]
    impl Provider for Fake {
        fn name(&self) -> &str {
            "fixture"
        }
        async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
            self.calls.lock().unwrap().push(request);
            if self.fail {
                anyhow::bail!("synthetic failure")
            }
            Ok(ChatResponse {
                text: HEADINGS
                    .iter()
                    .map(|h| format!("{h}\nНет сведений.\n"))
                    .collect(),
                usage: None,
                termination: self.termination.clone(),
                reasoning_content: None,
                tool_calls: vec![],
                provider_replay_state: None,
            })
        }
        async fn stream_chat(
            &self,
            _: ChatRequest,
        ) -> Result<futures_util::stream::BoxStream<'static, Result<StreamChunk>>> {
            anyhow::bail!("not used by service adapter")
        }
        fn classify_failure(&self, _: &anyhow::Error) -> Option<ProviderFailureClassification> {
            Some(ProviderFailureClassification {
                class: ProviderFailureClass::RateLimit,
                http_status: Some(429),
                provider_code: None,
                retry_after_ms: Some(12_000),
            })
        }
    }
    fn selection() -> ModelSelection {
        ModelSelection {
            transport: Transport::Api,
            instance: "fixture-instance".into(),
            model: "fixture-model".into(),
            effort: Some("high".into()),
        }
    }
    fn request() -> SummaryRequest {
        SummaryRequest {
            selection: selection(),
            output_cap: 500,
            input: SummaryInput {
                mode: CompactionMode::Normal,
                previous_summary: "previous state".into(),
                reference_only: vec![],
                target_tokens: 500,
                compact_units: vec![SummaryPart {
                    sources: vec![SourceRef {
                        scope: "context:turn".into(),
                        id: "source".into(),
                        version: "revision:1".into(),
                    }],
                    unit: 0,
                    part: 0,
                    last_part: true,
                    text: "untrusted history".into(),
                }],
            },
        }
    }
    fn fake(termination: ProviderTermination, fail: bool) -> Arc<Fake> {
        Arc::new(Fake {
            calls: Mutex::new(vec![]),
            termination,
            fail,
        })
    }
    #[tokio::test]
    async fn native_summary_uses_exact_selection_cap_and_fresh_tool_free_request() {
        let provider = fake(ProviderTermination::Complete, false);
        let service = NativeSummarizer::new(
            provider.clone(),
            selection(),
            ModelBudget::new(Some(128_000), None, Some(16_384)),
        )
        .unwrap();
        let result = service.summarize(request()).await.unwrap();
        validate_summary(&result, 500).unwrap();
        {
            let calls = provider.calls.lock().unwrap();
            assert_eq!(calls.len(), 1);
            let sent = &calls[0];
            assert_eq!(sent.model, "fixture-model");
            assert_eq!(sent.max_tokens, Some(500));
            assert_eq!(
                sent.reasoning,
                Some(ReasoningConfig::Effort(ReasoningEffort::High))
            );
            assert!(sent.tools.is_none() && sent.compiled_prompt.is_none());
            assert_eq!(sent.messages.len(), 2);
            assert_eq!(sent.messages[0].content, INSTRUCTIONS);
            let data: SummaryInput = serde_json::from_str(&sent.messages[1].content).unwrap();
            assert_eq!(data.previous_summary, "previous state");
        }
        let mut changed = request();
        changed.selection.model = "hidden-fallback".into();
        assert!(service.summarize(changed).await.is_err());
        assert_eq!(provider.calls.lock().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn native_summary_preserves_refusal_limit_and_retry_after_classification() {
        for termination in [
            ProviderTermination::Length,
            ProviderTermination::Safety,
            ProviderTermination::Unknown("missing stop".into()),
        ] {
            let provider = fake(termination, false);
            let service =
                NativeSummarizer::new(provider, selection(), ModelBudget::new(None, None, None))
                    .unwrap();
            assert!(validate_summary(&service.summarize(request()).await.unwrap(), 500).is_err());
        }
        let provider = fake(ProviderTermination::Complete, true);
        let service =
            NativeSummarizer::new(provider, selection(), ModelBudget::new(None, None, None))
                .unwrap();
        let failure = service.summarize(request()).await.unwrap_err();
        assert_eq!(failure.kind, FailureKind::Transient);
        assert_eq!(failure.retry_after_ms, Some(12_000));
    }
    #[tokio::test]
    async fn native_summary_rejects_oversized_input_before_provider_call() {
        let provider = fake(ProviderTermination::Complete, false);
        let service = NativeSummarizer::new(
            provider.clone(),
            selection(),
            ModelBudget::new(Some(4096), None, None),
        )
        .unwrap();
        let mut oversized = request();
        oversized.input.compact_units[0].text = "large source ".repeat(5000);
        assert_eq!(
            service.summarize(oversized).await.unwrap_err().code,
            "summary_input_overflow"
        );
        assert!(provider.calls.lock().unwrap().is_empty());
    }
}

/// Rebuild the same bounded model representation when restoring a durable tool
/// result. The caller supplies its authorized canonical result locator.
pub fn restored_tool_result_message(
    message: &pioneer_provider::ChatMessage,
    reference: &str,
) -> Result<pioneer_provider::ChatMessage> {
    crate::chat::bounded_tool_result_message(message, reference)
        .map_err(|error| anyhow::anyhow!("cannot restore bounded tool result: {error:?}"))
}

/// Reduce a saved tool result further when the complete request has less room.
/// The original payload and provenance are unchanged; the caller supplies an
/// authorized locator, as for ordinary restored results.
pub fn restored_tool_result_with_budget(
    message: &pioneer_provider::ChatMessage,
    reference: &str,
    available_tokens: u64,
) -> Result<pioneer_provider::ChatMessage> {
    crate::chat::bounded_tool_result_message_with_budget(message, reference, available_tokens)
        .map_err(|error| anyhow::anyhow!("cannot restore bounded tool result: {error:?}"))
}
