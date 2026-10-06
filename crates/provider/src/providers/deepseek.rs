use crate::{
    providers::compatible::{AuthStyle, OpenAiCompatibleProvider},
    traits::{Provider, ProviderWarmupOutcome},
    types::{
        ChatRequest, ChatResponse, ProviderCapabilities, ProviderFailureClassification,
        ProviderInputCapabilities, ProviderReplayState, ProviderTimeoutPolicy, Role, StreamChunk,
    },
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures_util::{StreamExt, stream::BoxStream};
use pioneer_protocol::{ProviderFailureClass, ProviderModelInfo};
use std::fmt;

const PROVIDER_NAME: &str = "deepseek";
pub(crate) const BASE_URL: &str = "https://api.deepseek.com";

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
        Self::with_base_url(api_key, BASE_URL)
    }

    fn with_base_url(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            transport: OpenAiCompatibleProvider::new(
                PROVIDER_NAME,
                base_url,
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

    pub fn with_base_url_and_timeout_policy(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
        timeout_policy: ProviderTimeoutPolicy,
    ) -> Self {
        Self::with_base_url(api_key, base_url).with_transport_timeout_policy(timeout_policy)
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
        crate::generation::deepseek_effective_thinking(request)
    }

    fn prepare_request(&self, request: ChatRequest) -> Result<ChatRequest> {
        let request = crate::history::project_request_for_provider("deepseek", request)?;
        self.validate_request_replay(&request)?;
        Ok(request)
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

    pub(super) fn validate_stream(
        model: String,
        stream: BoxStream<'static, Result<StreamChunk>>,
    ) -> BoxStream<'static, Result<StreamChunk>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk>>(64);
        tokio::spawn(async move {
            let mut stream = stream;
            let mut pending_replay_state: Option<ProviderReplayState> = None;

            while let Some(result) = tokio::select! {
                biased;
                _ = tx.closed() => return,
                result = stream.next() => result,
            } {
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
        let request = self.prepare_request(request)?;
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
        let request = self.prepare_request(request)?;
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
    use crate::types::ReasoningConfig;
    #[tokio::test]
    async fn effective_mode_prepares_completed_canonical_rounds_and_preserves_active_guards() {
        let provider = DeepSeekProvider::new("key");
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![tool_call()],
                None,
            ),
            ChatMessage::tool_result("call_1", "read_file", "observed result"),
        ];
        complete_round(&mut messages);
        let canonical_bytes = serde_json::to_string(&messages).unwrap();
        for reasoning in [
            None,
            Some(ReasoningConfig::Disabled),
            Some(ReasoningConfig::Effort(ReasoningEffort::None)),
            Some(ReasoningConfig::Effort(ReasoningEffort::High)),
        ] {
            let off = crate::generation::selected_off(reasoning);
            for stream in [false, true] {
                let mut request = request_with(messages.clone());
                request.model = "deepseek-v4-flash".into();
                request.reasoning = reasoning;
                request.tools = Some(vec![crate::types::ToolDefinition {
                    name: "read_file".into(),
                    description: "Read".into(),
                    parameters: serde_json::json!({"type":"object"}),
                }]);
                let prepared = provider.prepare_request(request).unwrap();
                assert_eq!(prepared.messages[0].tool_calls.is_some(), off);
                let wire = crate::attachments::runtime::with_async_authority_scope(
                    "deepseek-default-mode-fixture".into(),
                    provider
                        .transport
                        .render_chat_request_async_for_test(prepared, stream),
                )
                .await
                .unwrap();
                assert_eq!(wire["stream"], stream);
                assert!(wire.to_string().contains("observed result"));
                assert_eq!(wire["messages"][0].get("tool_calls").is_some(), off);
                assert_eq!(serde_json::to_string(&messages).unwrap(), canonical_bytes);
            }
        }

        for reasoning in [
            None,
            Some(ReasoningConfig::Disabled),
            Some(ReasoningConfig::Effort(ReasoningEffort::None)),
            Some(ReasoningConfig::Effort(ReasoningEffort::High)),
        ] {
            let mut active = messages.clone();
            for message in &mut active {
                message.provenance = None;
            }
            active[0].provider_replay_state = Some(ProviderReplayState::for_model(
                "openrouter",
                "other-model",
                serde_json::json!({"schema_version":1}),
            ));
            let mut request = request_with(active);
            request.model = "deepseek-v4-flash".into();
            request.reasoning = reasoning;
            assert!(provider.prepare_request(request).is_err());
        }
        for protected in [false, true] {
            let mut active = messages.clone();
            if protected {
                for message in &mut active {
                    message.provenance.as_mut().unwrap().protected_input = true;
                }
            } else {
                for message in &mut active {
                    message.provenance = None;
                }
            }
            let mut request = request_with(active);
            request.model = "deepseek-v4-flash".into();
            assert!(provider.prepare_request(request).is_err());
        }
    }

    #[tokio::test]
    async fn mixed_historical_thinking_does_not_override_off_at_final_materialization() {
        use crate::types::{
            AttachmentDataSource, InputTypeSupport, MessageAttachment, MessageContentPart,
            ProviderInputCapabilities,
        };
        let provider =
            DeepSeekProvider::new("key").with_input_capabilities(ProviderInputCapabilities {
                text: true,
                file: InputTypeSupport::native_inline_only(),
                image: InputTypeSupport::disabled(),
                audio: InputTypeSupport::disabled(),
                video: InputTypeSupport::disabled(),
            });
        let mut replay = OpenAiCompatibleProvider::assistant_replay_state(
            "deepseek",
            None,
            Some("historical genuine rationale".into()),
            &[tool_call()],
        );
        replay.model = Some("deepseek-v4-flash".into());
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                Some("historical genuine rationale"),
                vec![tool_call()],
                Some(replay),
            ),
            ChatMessage::tool_result("call_1", "read_file", "historical result"),
        ];
        for id in ["call_2", "call_3"] {
            let mut call = tool_call();
            call.id = id.into();
            messages.push(ChatMessage::assistant_tool_calls_with_provider_state(
                Some("non-thinking call"),
                None::<String>,
                vec![call],
                None,
            ));
            messages.push(ChatMessage::tool_result(
                id,
                "read_file",
                "non-thinking result",
            ));
        }
        complete_round(&mut messages);
        for (index, message) in messages.iter_mut().enumerate() {
            message.provenance.as_mut().unwrap().unit_id = format!("round-{}", index / 2);
        }
        for media in [false, true] {
            let mut canonical = messages.clone();
            if media {
                canonical[3]
                    .content_parts
                    .push(MessageContentPart::file(MessageAttachment {
                        mime_type: "text/plain".into(),
                        name: Some("result.txt".into()),
                        size_bytes: None,
                        sha256: None,
                        artifact: None,
                        source: AttachmentDataSource::Bytes {
                            base64_data: "bWVkaWEgcmVzdWx0".into(),
                        },
                    }));
            }
            let canonical_bytes = serde_json::to_vec(&canonical).unwrap();
            for reasoning in [
                None,
                Some(ReasoningConfig::Disabled),
                Some(ReasoningConfig::Effort(ReasoningEffort::None)),
                Some(ReasoningConfig::Effort(ReasoningEffort::High)),
            ] {
                let off = crate::generation::selected_off(reasoning);
                let mut request = request_with(canonical.clone());
                request.model = "deepseek-v4-flash".into();
                request.reasoning = reasoning;
                request.max_tokens = Some(1024);
                request.tools = Some(vec![crate::types::ToolDefinition {
                    name: "read_file".into(),
                    description: "Read".into(),
                    parameters: serde_json::json!({"type":"object"}),
                }]);
                // Default Provider budget: first request projection, text-only
                // early return OR async media materialization with the same mode.
                let budgeted = crate::attachments::runtime::with_async_authority_scope(
                    "deepseek-mixed-budget-fixture".into(),
                    provider.prepare_input_budget(request.clone()),
                )
                .await
                .unwrap();
                assert_eq!(budgeted.media.len(), usize::from(media));
                assert_eq!(budgeted.request.messages[2].tool_calls.is_some(), off);
                assert!(
                    budgeted
                        .request
                        .messages
                        .iter()
                        .zip(&canonical)
                        .all(|(prepared, original)| prepared.provenance == original.provenance)
                );
                for stream in [false, true] {
                    let prepared = provider.prepare_request(budgeted.request.clone()).unwrap();
                    let wire = crate::attachments::runtime::with_async_authority_scope(
                        "deepseek-mixed-final-fixture".into(),
                        provider
                            .transport
                            .render_chat_request_async_for_test(prepared, stream),
                    )
                    .await
                    .unwrap();
                    // Also materialize directly from wrapper projection. Budget
                    // preparation must not change the final history representation.
                    let direct = crate::attachments::runtime::with_async_authority_scope(
                        "deepseek-mixed-direct-fixture".into(),
                        provider.transport.render_chat_request_async_for_test(
                            provider.prepare_request(request.clone()).unwrap(),
                            stream,
                        ),
                    )
                    .await
                    .unwrap();
                    assert_eq!(wire["messages"], direct["messages"]);
                    assert_eq!(wire["stream"], stream);
                    assert_eq!(wire["max_tokens"], 1024);
                    if off {
                        assert_eq!(wire["thinking"]["type"], "disabled");
                    }
                    for id in ["call_2", "call_3"] {
                        let native_call = wire["messages"].as_array().unwrap().iter().any(|m| {
                            m["tool_calls"]
                                .as_array()
                                .is_some_and(|calls| calls.iter().any(|c| c["id"] == id))
                        });
                        let native_result = wire["messages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|m| m["role"] == "tool" && m["tool_call_id"] == id);
                        assert_eq!(native_call, off);
                        assert_eq!(native_result, off);
                    }
                    assert!(wire.to_string().contains("historical genuine rationale"));
                    assert!(wire.to_string().contains("non-thinking result"));
                }
                for protected in [false, true] {
                    let mut guarded = request.clone();
                    for message in &mut guarded.messages[2..] {
                        if protected {
                            message.provenance.as_mut().unwrap().protected_input = true;
                        } else {
                            message.provenance = None;
                        }
                    }
                    for stream in [false, true] {
                        let prepared = provider.prepare_request(guarded.clone());
                        if off {
                            let wire = crate::attachments::runtime::with_async_authority_scope(
                                "deepseek-active-off-fixture".into(),
                                provider
                                    .transport
                                    .render_chat_request_async_for_test(prepared.unwrap(), stream),
                            )
                            .await
                            .unwrap();
                            assert!(wire["messages"].as_array().unwrap().iter().any(|m| {
                                m["tool_calls"].as_array().is_some_and(|calls| {
                                    calls.iter().any(|call| call["id"] == "call_2")
                                })
                            }));
                        } else {
                            let error = prepared.err().unwrap();
                            assert!(
                                error
                                    .downcast_ref::<MissingDeepSeekReasoningReplay>()
                                    .is_some()
                            );
                        }
                    }
                }
                assert_eq!(serde_json::to_vec(&canonical).unwrap(), canonical_bytes);
            }
        }
    }

    #[tokio::test]
    async fn final_request_aware_preflight_preserves_incompatible_active_replay_guard() {
        let provider = DeepSeekProvider::new("key");
        let mut request =
            request_with(vec![ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![tool_call()],
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "foreign",
                    serde_json::json!({"schema_version":1}),
                )),
            )]);
        request.model = "deepseek-v4-flash".into();
        for reasoning in [
            None,
            Some(ReasoningConfig::Disabled),
            Some(ReasoningConfig::Effort(ReasoningEffort::None)),
            Some(ReasoningConfig::Effort(ReasoningEffort::High)),
        ] {
            request.reasoning = reasoning;
            for stream in [false, true] {
                assert!(provider.prepare_request(request.clone()).is_err());
                let result = crate::attachments::runtime::with_async_authority_scope(
                    "deepseek-incompatible-final-fixture".into(),
                    provider
                        .transport
                        .render_chat_request_async_for_test(request.clone(), stream),
                )
                .await;
                assert!(result.is_err());
            }
        }
    }

    use super::*;
    use crate::types::{
        ChatMessage, MessageProvenance, MessageSourceRef, ProviderToolCall, ReasoningEffort,
    };
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

    fn complete_round(messages: &mut [ChatMessage]) {
        for (index, message) in messages.iter_mut().enumerate() {
            message.provenance = Some(MessageProvenance {
                logical_turn_id: Some("turn".into()),
                workspace_id: "workspace".into(),
                thread_id: "thread".into(),
                context_thread: None,
                unit_id: "round".into(),
                sources: vec![MessageSourceRef {
                    scope: "event:turn".into(),
                    id: format!("source-{index}"),
                    version: "revision:1".into(),
                }],
                complete: true,
                protected_input: false,
                inherited: false,
                source_aliases: vec![],
                ambiguous_input_aliases: vec![],
            });
        }
    }

    fn request_with(messages: Vec<ChatMessage>) -> ChatRequest {
        ChatRequest {
            model: "deepseek-reasoner".into(),
            messages,
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        }
    }

    fn prepare_locally(
        provider: &DeepSeekProvider,
        request: ChatRequest,
    ) -> Result<(ChatRequest, serde_json::Value)> {
        let request = provider.prepare_request(request)?;
        let wire = provider
            .transport
            .render_chat_request_for_test(request.clone())?;
        Ok((request, wire))
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
        let mut state = OpenAiCompatibleProvider::assistant_replay_state(
            PROVIDER_NAME,
            Some(String::new()),
            Some(String::new()),
            calls.as_slice(),
        );
        state.model = Some("deepseek-v4-flash".into());
        let request = ChatRequest {
            model: "deepseek-v4-flash".to_owned(),
            messages: vec![
                ChatMessage::assistant_tool_calls_with_provider_state(
                    None::<String>,
                    None::<String>,
                    calls,
                    Some(state),
                ),
                ChatMessage::tool_result("call_1", "read_file", "file contents"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: Some(ReasoningConfig::effort(ReasoningEffort::High)),
            compiled_prompt: None,
        };

        let original_state = request.messages[0].provider_replay_state.clone();
        let (prepared, wire) = prepare_locally(&provider, request)
            .expect("field presence, including an empty value, is replayable");
        assert_eq!(
            prepared.messages[0].provider_replay_state, original_state,
            "compatible replay must remain byte-for-byte equivalent"
        );
        assert_eq!(wire["messages"][0]["reasoning_content"], "");
        assert_eq!(wire["messages"][1]["tool_call_id"], "call_1");
    }

    #[test]
    fn completed_foreign_round_with_reasoning_reaches_deepseek_wire() {
        let calls = vec![tool_call()];
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                calls,
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source-model",
                    serde_json::json!({
                        "reasoning_details":[{"type":"reasoning.summary","summary":"portable rationale"}]
                    }),
                )),
            ),
            ChatMessage::tool_result("call_1", "read_file", "file contents"),
        ];
        complete_round(&mut messages);

        let (projected, wire) =
            prepare_locally(&DeepSeekProvider::new("key"), request_with(messages)).unwrap();

        assert_eq!(projected.messages.len(), 2);
        assert_eq!(
            projected.messages[0].reasoning_content.as_deref(),
            Some("portable rationale")
        );
        assert!(projected.messages[0].provider_replay_state.is_none());
        assert!(wire.to_string().contains("portable rationale"));
        assert!(wire.to_string().contains("file contents"));
    }

    #[test]
    fn completed_foreign_round_without_reasoning_is_a_non_executable_transcript() {
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![tool_call()],
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source-model",
                    serde_json::json!({"reasoning_details":[]}),
                )),
            ),
            ChatMessage::tool_result("call_1", "read_file", "file contents"),
        ];
        complete_round(&mut messages);

        let (projected, wire) =
            prepare_locally(&DeepSeekProvider::new("key"), request_with(messages)).unwrap();

        assert_eq!(projected.messages.len(), 2);
        assert!(
            projected
                .messages
                .iter()
                .all(|message| message.tool_calls.is_none())
        );
        assert!(
            projected
                .messages
                .iter()
                .all(|message| message.tool_call_id.is_none())
        );
        assert!(
            projected.messages[0]
                .content
                .contains("portable non-executable transcript")
        );
        assert!(projected.messages[0].content.contains("call_1"));
        assert!(projected.messages[1].content.contains("file contents"));
        assert!(
            wire.to_string()
                .contains("portable non-executable transcript")
        );
    }

    #[test]
    fn legacy_unit_with_mixed_rounds_preserves_reasoning_and_compatible_replay() {
        let provider = DeepSeekProvider::new("key");
        let plain_call = tool_call();
        let reasoned_call = ProviderToolCall {
            id: "call_2".into(),
            name: "inspect".into(),
            arguments: "{}".into(),
        };
        let foreign_call = ProviderToolCall {
            id: "call_3".into(),
            name: "verify".into(),
            arguments: "{}".into(),
        };
        let mut replay = OpenAiCompatibleProvider::assistant_replay_state(
            PROVIDER_NAME,
            None,
            Some("second round rationale".into()),
            std::slice::from_ref(&reasoned_call),
        );
        replay.model = Some("deepseek-reasoner".into());
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                Some("first call"),
                None::<String>,
                vec![plain_call],
                None,
            ),
            ChatMessage::user("steering between call and result"),
            ChatMessage::tool_result("call_1", "read_file", "first result"),
            ChatMessage::assistant_tool_calls_with_provider_state(
                Some("second call"),
                None::<String>,
                vec![reasoned_call],
                Some(replay.clone()),
            ),
            ChatMessage::tool_result("call_2", "inspect", "second result"),
            ChatMessage::assistant_tool_calls_with_provider_state(
                Some("third call"),
                Some("common third rationale"),
                vec![foreign_call],
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source-model",
                    serde_json::json!({"reasoning_details":[
                        {"type":"reasoning.summary","summary":"additional third rationale"},
                        {"type":"reasoning.encrypted","data":"opaque-third"}
                    ]}),
                )),
            ),
            ChatMessage::tool_result("call_3", "verify", "third result"),
        ];
        complete_round(&mut messages);
        for message in &mut messages {
            let origin = message.provenance.as_mut().unwrap();
            origin.unit_id = "legacy-task-basis:run".into();
            origin.sources[0].scope = "task-basis:run".into();
            origin.sources[0].id = "run".into();
            origin.sources[0].version = "task-basis-revision:1".into();
            origin.logical_turn_id = None;
            origin.inherited = true;
        }
        let canonical = messages.clone();
        let (projected, wire) = prepare_locally(&provider, request_with(messages.clone())).unwrap();
        assert_eq!(projected.messages.len(), 7);
        assert_eq!(projected.messages[0].role, Role::User);
        assert_eq!(
            projected.messages[1].content,
            "steering between call and result"
        );
        assert_eq!(projected.messages[2].role, Role::User);
        assert_eq!(
            projected.messages[3].provider_replay_state.as_ref(),
            Some(&replay)
        );
        assert_eq!(projected.messages[3].tool_calls, canonical[3].tool_calls);
        assert_eq!(projected.messages[4].role, Role::Tool);
        assert_eq!(projected.messages[5].role, Role::Assistant);
        assert_eq!(projected.messages[6].role, Role::Tool);
        assert_eq!(
            projected.messages[5].reasoning_content.as_deref(),
            Some("common third rationale\n\nadditional third rationale")
        );
        assert_eq!(
            wire.to_string().matches("second round rationale").count(),
            1
        );
        assert_eq!(
            wire.to_string().matches("common third rationale").count(),
            1
        );
        assert_eq!(
            wire.to_string()
                .matches("additional third rationale")
                .count(),
            1
        );
        assert!(!wire.to_string().contains("opaque-third"));
        assert_eq!(
            crate::history::project_request_for_provider(provider.name(), projected.clone())
                .unwrap()
                .messages,
            projected.messages,
        );
        assert_eq!(messages, canonical);
        assert!(
            projected
                .messages
                .iter()
                .zip(&canonical)
                .all(|(a, b)| a.provenance == b.provenance)
        );
    }

    #[test]
    fn encrypted_only_foreign_replay_is_not_exposed_as_reasoning() {
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![tool_call()],
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source-model",
                    serde_json::json!({
                        "reasoning_details":[{"type":"reasoning.encrypted","data":"opaque-secret"}]
                    }),
                )),
            ),
            ChatMessage::tool_result("call_1", "read_file", "file contents"),
        ];
        complete_round(&mut messages);

        let (_, wire) =
            prepare_locally(&DeepSeekProvider::new("key"), request_with(messages)).unwrap();
        assert!(!wire.to_string().contains("opaque-secret"));
        assert!(wire.to_string().contains("file contents"));
    }

    #[test]
    fn unfinished_foreign_replay_fails_before_deepseek_rendering() {
        let mut message = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            None::<String>,
            vec![tool_call()],
            Some(ProviderReplayState::for_model(
                "openrouter",
                "source-model",
                serde_json::json!({"reasoning_details":[]}),
            )),
        );
        message.provenance = None;
        let error = prepare_locally(&DeepSeekProvider::new("key"), request_with(vec![message]))
            .expect_err("an active foreign continuation must fail before wire rendering");
        assert!(
            error
                .downcast_ref::<crate::history::IncompatibleProviderReplayContinuation>()
                .is_some()
        );
    }

    #[test]
    fn compatible_empty_replay_triggers_projection_for_mixed_non_reasoner_history() {
        let provider = DeepSeekProvider::new("key");
        let calls = vec![tool_call()];
        let mut compatible = OpenAiCompatibleProvider::assistant_replay_state(
            PROVIDER_NAME,
            None,
            Some(String::new()),
            calls.as_slice(),
        );
        compatible.model = Some("deepseek-chat".into());
        let mut messages = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                calls.clone(),
                Some(compatible.clone()),
            ),
            ChatMessage::tool_result("call_1", "read_file", "first result"),
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                Some("  \t"),
                calls,
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source-model",
                    serde_json::json!({"reasoning_details":[]}),
                )),
            ),
            ChatMessage::tool_result("call_1", "read_file", "second result"),
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                Some(""),
                vec![tool_call()],
                Some(ProviderReplayState::for_model(
                    "openrouter",
                    "source-model",
                    serde_json::json!({"reasoning_details":[]}),
                )),
            ),
            ChatMessage::tool_result("call_1", "read_file", "third result"),
        ];
        // Distinct planner units prevent duplicate call identities from being
        // mistaken for one round during completion validation.
        complete_round(&mut messages);
        for message in &mut messages[2..4] {
            message.provenance.as_mut().unwrap().unit_id = "second-round".into();
        }
        for message in &mut messages[4..] {
            message.provenance.as_mut().unwrap().unit_id = "third-round".into();
        }
        let mut request = request_with(messages);
        request.model = "deepseek-chat".into();
        let (projected, wire) = prepare_locally(&provider, request).unwrap();
        assert_eq!(
            projected.messages[0].provider_replay_state.as_ref(),
            Some(&compatible)
        );
        assert_eq!(wire["messages"][0]["reasoning_content"], "");
        assert_eq!(projected.messages[2].role, Role::User);
        assert!(projected.messages[2].tool_calls.is_none());
        assert!(projected.messages[3].content.contains("second result"));
        assert!(projected.messages[4].tool_calls.is_none());
        assert!(projected.messages[5].content.contains("third result"));
        assert!(wire.to_string().contains("second result"));
        assert!(wire.to_string().contains("third result"));
    }

    #[test]
    fn completed_same_model_non_thinking_round_is_portable_in_thinking_request() {
        let provider = DeepSeekProvider::new("key");
        for reasoning_field in [None, Some(serde_json::Value::Null)] {
            let mut replay = OpenAiCompatibleProvider::assistant_replay_state(
                PROVIDER_NAME,
                None,
                None,
                &[tool_call()],
            );
            replay.model = Some("deepseek-v4-flash".into());
            if let Some(value) = reasoning_field {
                replay.payload["assistant_message"]["reasoning_content"] = value;
            } else {
                replay.payload["assistant_message"]
                    .as_object_mut()
                    .unwrap()
                    .remove("reasoning_content");
            }
            let mut messages = vec![
                ChatMessage::assistant_tool_calls_with_provider_state(
                    None::<String>,
                    None::<String>,
                    vec![tool_call()],
                    Some(replay.clone()),
                ),
                ChatMessage::tool_result("call_1", "read_file", "observed result"),
            ];
            complete_round(&mut messages);
            let canonical = messages.clone();
            let mut request = request_with(messages);
            request.model = "deepseek-v4-flash".into();
            request.reasoning = Some(ReasoningConfig::effort(ReasoningEffort::High));
            let (projected, wire) = prepare_locally(&provider, request).unwrap();
            assert_eq!(projected.messages.len(), 2);
            assert_eq!(projected.messages[0].role, Role::User);
            assert!(
                projected
                    .messages
                    .iter()
                    .all(|message| message.tool_calls.is_none())
            );
            assert!(wire.to_string().contains("observed result"));
            assert!(!wire.to_string().contains("assistant_message"));
            assert_eq!(canonical[0].provider_replay_state.as_ref(), Some(&replay));
        }

        let mut mixed = vec![
            ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                vec![tool_call()],
                Some(ProviderReplayState::for_model(
                    PROVIDER_NAME,
                    "deepseek-v4-flash",
                    serde_json::json!({"schema_version":1,"assistant_message":{"content":null,"reasoning_content":null,"tool_calls":[]}}),
                )),
            ),
            ChatMessage::tool_result("call_1", "read_file", "mixed result"),
        ];
        complete_round(&mut mixed);
        let mut reasoned = ChatMessage::assistant("later answer");
        reasoned.reasoning_content = Some("later thinking".into());
        mixed.push(reasoned);
        let mut request = request_with(mixed);
        request.model = "deepseek-v4-flash".into();
        let (projected, wire) = prepare_locally(&provider, request).unwrap();
        assert_eq!(projected.messages[0].role, Role::User);
        assert!(wire.to_string().contains("mixed result"));

        let mut active = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            None::<String>,
            vec![tool_call()],
            Some(ProviderReplayState::for_model(
                PROVIDER_NAME,
                "deepseek-v4-flash",
                serde_json::json!({"schema_version":1,"assistant_message":{"content":null,"tool_calls":[]}}),
            )),
        );
        active.provenance = None;
        let mut request = request_with(vec![active]);
        request.model = "deepseek-v4-flash".into();
        request.reasoning = Some(ReasoningConfig::effort(ReasoningEffort::High));
        assert!(prepare_locally(&provider, request).is_err());
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
