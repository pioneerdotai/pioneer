use crate::types::{ChatRequest, ChatResponse, ProviderCapabilities, StreamChunk};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;

use pioneer_protocol::ProviderModelInfo;

#[async_trait]
pub trait Provider: Send + Sync {
    /// Human-readable provider name (e.g. "openrouter", "anthropic").
    fn name(&self) -> &str;

    /// Declares what features this provider supports.
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    /// Send a chat request and receive the complete response.
    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse>;

    /// Send a chat request and receive a stream of response chunks.
    ///
    /// Chunks may contain text deltas, reasoning deltas, and/or tool calls.
    /// The stream ends with a chunk where `is_final == true`.
    /// Providers that don't support streaming should fall back to calling
    /// `chat()` and returning the full response as a single chunk.
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>>;

    /// List all models available from this provider.
    /// Default implementation returns an error for providers that don't support listing.
    async fn list_models(&self) -> Result<Vec<ProviderModelInfo>> {
        anyhow::bail!("provider '{}' does not support listing models", self.name())
    }

    /// Pre-warm the HTTP connection pool (TLS handshake, DNS, HTTP/2 setup).
    /// Default implementation is a no-op.
    async fn warmup(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        ChatMessage, ChatResponse, ProviderCapabilities, ProviderInputCapabilities, StreamChunk,
        TokenUsage,
    };
    use futures_util::{StreamExt, stream};

    struct MockProvider;

    #[async_trait]
    impl Provider for MockProvider {
        fn name(&self) -> &str {
            "mock"
        }

        fn capabilities(&self) -> ProviderCapabilities {
            ProviderCapabilities {
                streaming: true,
                vision: false,
                tool_calling: false,
                input_types: ProviderInputCapabilities::fallback_for_all_file_types(),
            }
        }

        async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse> {
            Ok(ChatResponse {
                text: "mock response".into(),
                usage: Some(TokenUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(5),
                }),
                reasoning_content: None,
                tool_calls: Vec::new(),
            })
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
            let chunks = vec![
                Ok(StreamChunk::delta("hello ")),
                Ok(StreamChunk::delta("world")),
                Ok(StreamChunk::final_chunk()),
            ];
            Ok(stream::iter(chunks).boxed())
        }
    }

    #[test]
    fn mock_provider_name() {
        let provider = MockProvider;
        assert_eq!(provider.name(), "mock");
    }

    #[test]
    fn mock_provider_capabilities() {
        let provider = MockProvider;
        let caps = provider.capabilities();
        assert!(caps.streaming);
        assert!(!caps.vision);
    }

    #[tokio::test]
    async fn mock_provider_chat() {
        let provider = MockProvider;
        let request = ChatRequest {
            model: "test-model".into(),
            messages: vec![ChatMessage::user("hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            compiled_prompt: None,
        };

        let response = provider.chat(request).await.unwrap();
        assert_eq!(response.text, "mock response");
        assert_eq!(response.usage.as_ref().unwrap().input_tokens, Some(10));
    }

    #[tokio::test]
    async fn mock_provider_stream() {
        let provider = MockProvider;
        let request = ChatRequest {
            model: "test-model".into(),
            messages: vec![ChatMessage::user("hello")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            compiled_prompt: None,
        };

        let mut stream = provider.stream_chat(request).await.unwrap();

        let chunk1 = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk1.delta, "hello ");
        assert!(!chunk1.is_final);

        let chunk2 = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk2.delta, "world");
        assert!(!chunk2.is_final);

        let chunk3 = stream.next().await.unwrap().unwrap();
        assert!(chunk3.is_final);
    }
}
