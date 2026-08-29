use crate::{
    providers::compatible::{AuthStyle, OpenAiCompatibleProvider},
    traits::{Provider, ProviderWarmupOutcome},
    types::{
        ChatRequest, ChatResponse, ProviderCapabilities, ProviderFailureClassification,
        ProviderInputCapabilities, ProviderReplayState, ProviderTimeoutPolicy, ReasoningConfig,
        Role, StreamChunk,
    },
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures_util::{StreamExt, stream::BoxStream};
use pioneer_protocol::{ProviderFailureClass, ProviderModelInfo};
use std::fmt;

const PROVIDER_NAME: &str = "deepseek";
const BASE_URL: &str = "https://api.deepseek.com";

/// DeepSeek uses the OpenAI-compatible transport, but its thinking models
/// impose an additional replay contract on assistant tool-call messages.
pub struct DeepSeekProvider {
    transport: OpenAiCompatibleProvider,
}

#[derive(Debug)]
struct MissingDeepSeekReasoningReplay {
    model: String,
}

impl fmt::Display for MissingDeepSeekReasoningReplay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeepSeek thinking replay for model `{}` is missing required `reasoning_content` on an assistant tool-call round",
            self.model
        )
    }
}

impl std::error::Error for MissingDeepSeekReasoningReplay {}

#[derive(Debug)]
struct MissingDeepSeekReasoningResponse {
    model: String,
}

impl fmt::Display for MissingDeepSeekReasoningResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "DeepSeek thinking response for model `{}` returned tool calls without a replayable `reasoning_content` field",
            self.model
        )
    }
}

impl std::error::Error for MissingDeepSeekReasoningResponse {}

impl DeepSeekProvider {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            transport: OpenAiCompatibleProvider::new(
                PROVIDER_NAME,
                BASE_URL,
                api_key,
                AuthStyle::Bearer,
            ),
        }
    }

    pub fn with_timeout_policy(
        api_key: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::new(api_key).with_transport_timeout_policy(timeout_policy)
    }

    pub fn with_input_capabilities(mut self, input_types: ProviderInputCapabilities) -> Self {
        self.transport = self.transport.with_input_capabilities(input_types);
        self
    }

    fn with_transport_timeout_policy(mut self, timeout_policy: ProviderTimeoutPolicy) -> Self {
        self.transport = self.transport.with_timeout_policy(timeout_policy);
        self
    }

    fn thinking_replay_required(&self, request: &ChatRequest) -> bool {
        let model_is_reasoner = request.model.to_ascii_lowercase().contains("reasoner");
        let reasoning_is_enabled = matches!(request.reasoning, Some(ReasoningConfig::Effort(_)));
        let history_contains_reasoning = request.messages.iter().any(|message| {
            message.role == Role::Assistant
                && (message
                    .reasoning_content
                    .as_deref()
                    .is_some_and(|reasoning| !reasoning.trim().is_empty())
                    || self
                        .transport
                        .replay_message(message)
                        .ok()
                        .flatten()
                        .is_some_and(|replay| replay.reasoning_content.is_some()))
        });

        model_is_reasoner || reasoning_is_enabled || history_contains_reasoning
    }

    fn validate_request_replay(&self, request: &ChatRequest) -> Result<()> {
        if !self.thinking_replay_required(request) {
            return Ok(());
        }

        for message in request.messages.iter().filter(|message| {
            message.role == Role::Assistant
                && message
                    .tool_calls
                    .as_ref()
                    .is_some_and(|tool_calls| !tool_calls.is_empty())
        }) {
            let has_reasoning = match self.transport.replay_message(message)? {
                // Field presence is the provider contract. An explicitly
                // returned empty string must be replayed as an empty string.
                Some(replay) => replay.reasoning_content.is_some(),
                None => message
                    .reasoning_content
                    .as_deref()
                    .is_some_and(|reasoning| !reasoning.trim().is_empty()),
            };
            if !has_reasoning {
                return Err(anyhow!(MissingDeepSeekReasoningReplay {
                    model: request.model.clone(),
                }));
            }
        }

        Ok(())
    }

    fn validate_response_replay_state(
        model: &str,
        replay_state: Option<&ProviderReplayState>,
    ) -> Result<()> {
        let has_reasoning = replay_state
            .map(|state| OpenAiCompatibleProvider::decode_replay_state(state, PROVIDER_NAME))
            .transpose()?
            .is_some_and(|replay| replay.reasoning_content.is_some());
        if has_reasoning {
            return Ok(());
        }

        Err(anyhow!(MissingDeepSeekReasoningResponse {
            model: model.to_owned(),
        }))
    }

    fn validate_chat_response(
        model: &str,
        replay_required: bool,
        response: &ChatResponse,
    ) -> Result<()> {
        if replay_required && !response.tool_calls.is_empty() {
            Self::validate_response_replay_state(model, response.provider_replay_state.as_ref())?;
        }
        Ok(())
    }

    fn validate_stream(
        model: String,
        stream: BoxStream<'static, Result<StreamChunk>>,
    ) -> BoxStream<'static, Result<StreamChunk>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);
        tokio::spawn(async move {
            let mut stream = stream;
            let mut pending_replay_state: Option<ProviderReplayState> = None;

            while let Some(result) = stream.next().await {
                let mut chunk = match result {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        if tx.send(Err(error)).await.is_err() {
                            return;
                        }
                        return;
                    }
                };

                if let Some(state) = chunk.provider_replay_state.take() {
                    pending_replay_state = Some(state);
                    continue;
                }

                if !chunk.tool_calls.is_empty() {
                    if let Err(error) = Self::validate_response_replay_state(
                        model.as_str(),
                        pending_replay_state.as_ref(),
                    ) {
                        if tx.send(Err(error)).await.is_err() {
                            return;
                        }
                        return;
                    }
                    if let Some(state) = pending_replay_state.take() {
                        if tx
                            .send(Ok(StreamChunk::provider_replay_state(state)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }

                if tx.send(Ok(chunk)).await.is_err() {
                    return;
                }
            }
        });

        Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
    }
}

#[async_trait]
impl Provider for DeepSeekProvider {
    fn name(&self) -> &str {
        PROVIDER_NAME
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.transport.capabilities()
    }

    fn classify_failure(&self, error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        if error
            .downcast_ref::<MissingDeepSeekReasoningReplay>()
            .is_some()
        {
            return Some(ProviderFailureClassification::new(
                ProviderFailureClass::InvalidRequest,
            ));
        }
        if error
            .downcast_ref::<MissingDeepSeekReasoningResponse>()
            .is_some()
        {
            return Some(ProviderFailureClassification::new(
                ProviderFailureClass::ProviderRejected,
            ));
        }
        self.transport.classify_failure(error)
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        self.validate_request_replay(&request)?;
        let model = request.model.clone();
        let replay_required = self.thinking_replay_required(&request);
        let response = self.transport.chat(request).await?;
        Self::validate_chat_response(model.as_str(), replay_required, &response)?;
        Ok(response)
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        self.validate_request_replay(&request)?;
        let model = request.model.clone();
        let replay_required = self.thinking_replay_required(&request);
        let stream = self.transport.stream_chat(request).await?;
        if replay_required {
            Ok(Self::validate_stream(model, stream))
        } else {
            Ok(stream)
        }
    }

    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        self.transport.list_models().await
    }

    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        self.transport.warmup().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChatMessage, ProviderToolCall, ReasoningEffort};
    use futures_util::{StreamExt, stream};

    fn tool_call() -> ProviderToolCall {
        ProviderToolCall {
            id: "call_1".to_owned(),
            name: "read_file".to_owned(),
            arguments: "{\"path\":\"README.md\"}".to_owned(),
        }
    }

    fn replay_request(model: &str, reasoning_content: Option<&str>) -> ChatRequest {
        ChatRequest {
            model: model.to_owned(),
            messages: vec![ChatMessage::assistant_tool_calls_with_reasoning(
                None::<String>,
                reasoning_content.map(str::to_owned),
                vec![tool_call()],
            )],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }

    #[test]
    fn rejects_incomplete_thinking_history_before_transport() {
        let provider = DeepSeekProvider::new("key");
        let error = provider
            .validate_request_replay(&replay_request("deepseek-reasoner", None))
            .expect_err("incomplete DeepSeek replay must fail locally");

        assert!(
            error
                .downcast_ref::<MissingDeepSeekReasoningReplay>()
                .is_some()
        );
        assert_eq!(
            provider
                .classify_failure(&error)
                .expect("adapter must classify its replay error")
                .class,
            ProviderFailureClass::InvalidRequest
        );
    }

    #[test]
    fn accepts_provider_owned_empty_reasoning_field() {
        let provider = DeepSeekProvider::new("key");
        let calls = vec![tool_call()];
        let state = OpenAiCompatibleProvider::assistant_replay_state(
            PROVIDER_NAME,
            Some(String::new()),
            Some(String::new()),
            calls.as_slice(),
        );
        let request = ChatRequest {
            model: "deepseek-v4-flash".to_owned(),
            messages: vec![ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                calls,
                Some(state),
            )],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
            compiled_prompt: None,
        };

        provider
            .validate_request_replay(&request)
            .expect("field presence, including an empty value, is replayable");
    }

    #[test]
    fn non_thinking_chat_does_not_require_reasoning_replay() {
        DeepSeekProvider::new("key")
            .validate_request_replay(&replay_request("deepseek-chat", None))
            .expect("non-thinking DeepSeek replay remains supported");
    }

    #[tokio::test]
    async fn stream_rejects_missing_reasoning_before_exposing_tool_calls() {
        let state = OpenAiCompatibleProvider::assistant_replay_state(
            PROVIDER_NAME,
            None,
            None,
            &[tool_call()],
        );
        let inner = stream::iter(vec![
            Ok(StreamChunk::provider_replay_state(state)),
            Ok(StreamChunk::tool_calls(vec![tool_call()])),
            Ok(StreamChunk::final_chunk_with(
                crate::ProviderTermination::ToolCalls,
            )),
        ])
        .boxed();

        let chunks = DeepSeekProvider::validate_stream("deepseek-v4-flash".to_owned(), inner)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(chunks.len(), 1);
        let error = chunks[0]
            .as_ref()
            .expect_err("missing reasoning must terminate before tool calls");
        assert!(
            error
                .downcast_ref::<MissingDeepSeekReasoningResponse>()
                .is_some()
        );
        assert_eq!(
            DeepSeekProvider::new("key")
                .classify_failure(error)
                .expect("adapter must classify an incomplete response")
                .class,
            ProviderFailureClass::ProviderRejected
        );
    }

    #[tokio::test]
    async fn stream_preserves_present_empty_reasoning_and_exposes_tool_calls() {
        let state = OpenAiCompatibleProvider::assistant_replay_state(
            PROVIDER_NAME,
            Some(String::new()),
            Some(String::new()),
            &[tool_call()],
        );
        let inner = stream::iter(vec![
            Ok(StreamChunk::provider_replay_state(state.clone())),
            Ok(StreamChunk::tool_calls(vec![tool_call()])),
            Ok(StreamChunk::final_chunk_with(
                crate::ProviderTermination::ToolCalls,
            )),
        ])
        .boxed();

        let chunks = DeepSeekProvider::validate_stream("deepseek-v4-flash".to_owned(), inner)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .expect("an explicitly present empty field must remain valid");

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].provider_replay_state.as_ref(), Some(&state));
        assert_eq!(chunks[1].tool_calls, vec![tool_call()]);
        assert!(chunks[2].is_final);
    }
}
