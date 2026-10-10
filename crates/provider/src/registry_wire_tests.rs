use super::*;
use crate::{ChatMessage, ProviderTermination, ProviderToolCall, ToolChoice, ToolDefinition};
use std::sync::Mutex;

struct FixtureProvider {
    seen: Arc<Mutex<Vec<ChatRequest>>>,
    unknown_name: bool,
}
impl FixtureProvider {
    fn response(&self, request: ChatRequest) -> ChatResponse {
        let name = if self.unknown_name {
            "unknown_wire_name".to_owned()
        } else {
            request.tools.as_ref().unwrap()[0].name.clone()
        };
        self.seen.lock().unwrap().push(request);
        ChatResponse {
            text: String::new(),
            usage: Some(crate::TokenUsage {
                input_tokens: Some(7),
                output_tokens: Some(3),
                ..Default::default()
            }),
            termination: ProviderTermination::ToolCalls,
            reasoning_content: None,
            tool_calls: vec![ProviderToolCall {
                id: "call".into(),
                name,
                arguments: "{}".into(),
            }],
            provider_replay_state: None,
        }
    }
}
#[async_trait]
impl Provider for FixtureProvider {
    fn name(&self) -> &str {
        "echo"
    }
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        Ok(self.response(request))
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let response = self.response(request);
        Ok(Box::pin(futures_util::stream::iter(vec![
            Ok(StreamChunk::tool_calls(response.tool_calls).with_usage(response.usage)),
            Ok(StreamChunk::final_chunk_with(
                ProviderTermination::ToolCalls,
            )),
        ])))
    }
}
fn request() -> ChatRequest {
    ChatRequest {
        model: "fixture".into(),
        messages: vec![ChatMessage::user("run")],
        temperature: None,
        max_tokens: None,
        tools: Some(vec![ToolDefinition {
            name: "skill.17.render".into(),
            description: "fixture".into(),
            parameters: serde_json::json!({"type":"object"}),
        }]),
        tool_choice: Some(ToolChoice::Tool {
            name: "skill.17.render".into(),
        }),
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}
#[tokio::test]
async fn registry_and_direct_factory_boundary_restore_complete_and_streamed_alias_calls() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = ProviderRegistry::new(|_| String::new());
    registry
        .insert(
            "echo",
            Arc::new(FixtureProvider {
                seen: seen.clone(),
                unknown_name: false,
            }),
        )
        .unwrap();
    let registered = registry.get_or_create("echo").unwrap();
    let direct = bind_direct_provider(
        Box::new(FixtureProvider {
            seen: seen.clone(),
            unknown_name: false,
        }),
        "fixture-authority",
    );
    for provider in [registered.as_ref(), direct.as_ref()] {
        let response = provider.chat(request()).await.unwrap();
        assert_eq!(response.tool_calls[0].name, "skill.17.render");
        assert_eq!(response.usage.unwrap().input_tokens, Some(7));
        let chunks = provider
            .stream_chat(request())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            chunks[0].as_ref().unwrap().tool_calls[0].name,
            "skill.17.render"
        );
        let budget = provider.prepare_input_budget(request()).await.unwrap();
        assert_eq!(budget.request.tools.unwrap()[0].name, "skill.17.render");
    }
    for request in seen.lock().unwrap().iter() {
        let name = &request.tools.as_ref().unwrap()[0].name;
        assert_eq!(name.len(), 64);
        assert!(name.bytes().all(|b| b.is_ascii_alphanumeric()));
        assert!(
            matches!(request.tool_choice.as_ref(), Some(ToolChoice::Tool {name:selected}) if selected == name)
        );
    }
}

#[tokio::test]
async fn failed_alias_inversion_retains_measured_usage_and_never_returns_an_unroutable_call() {
    let provider = bind_direct_provider(
        Box::new(FixtureProvider {
            seen: Arc::new(Mutex::new(Vec::new())),
            unknown_name: true,
        }),
        "fixture-authority",
    );
    let error = provider.chat(request()).await.unwrap_err();
    assert_eq!(
        crate::usage::error_usage(&error).unwrap().input_tokens,
        Some(7)
    );
    assert_eq!(
        provider.classify_failure(&error).unwrap().class,
        pioneer_protocol::ProviderFailureClass::ProviderRejected
    );
    let mut stream = provider.stream_chat(request()).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        crate::usage::error_usage(&error).unwrap().output_tokens,
        Some(3)
    );
    assert_eq!(
        provider.classify_failure(&error).unwrap().class,
        pioneer_protocol::ProviderFailureClass::ProviderRejected
    );
}
