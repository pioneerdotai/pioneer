use crate::types::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ProviderCapabilities,
    ProviderFailureClassification, StreamChunk,
};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;

use pioneer_protocol::ProviderModelInfo;

/// Result of a safe, non-inference provider warm-up.
///
/// `NotSupported` is intentionally distinct from success: adapters that do
/// not expose a harmless health/authentication endpoint must not be reported
/// as verified merely because the default implementation did no work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderWarmupOutcome {
    Completed,
    NotSupported,
}

#[async_trait]
pub trait Provider: Send + Sync {
    /// Human-readable provider name (e.g. "openrouter", "anthropic").
    fn name(&self) -> &str;

    /// Non-secret identity of the credential/account/endpoint authority that
    /// created this instance. Registry-managed providers always expose one;
    /// directly constructed test providers may remain unbound.
    fn authority_fingerprint(&self) -> Option<&str> {
        None
    }

    /// Declares what features this provider supports.
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    /// Classify a provider-owned error when the adapter has structured
    /// knowledge that the provider-neutral fallback cannot have.
    ///
    /// Returning `None` is always safe: unknown request rejections are not
    /// retried, while transport/HTTP transient failures are classified by the
    /// agent's provider-neutral fallback.
    fn classify_failure(&self, _error: &anyhow::Error) -> Option<ProviderFailureClassification> {
        None
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

    /// List embedding models available from this provider.
    /// Default implementation returns an error for providers that don't support embeddings.
    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        anyhow::bail!(
            "provider '{}' does not support listing embedding models",
            self.name()
        )
    }

    /// List transcription models available from this provider.
    /// Default implementation returns an error for providers that don't support transcription.
    async fn list_transcription_models(&self) -> Result<Vec<ProviderModelInfo>> {
        anyhow::bail!(
            "provider '{}' does not support listing transcription models",
            self.name()
        )
    }

    /// Create embeddings for one or more input texts.
    /// Default implementation returns an error for providers that do not support embeddings.
    async fn embed(&self, _request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        anyhow::bail!("provider '{}' does not support embeddings", self.name())
    }

    /// Run a safe readiness probe without making a paid inference request.
    ///
    /// Adapters without a harmless health/authentication endpoint return
    /// `NotSupported`; callers must preserve that distinction instead of
    /// treating a no-op as proof of readiness.
    async fn warmup(&self) -> Result<ProviderWarmupOutcome> {
        Ok(ProviderWarmupOutcome::NotSupported)
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
                embeddings: false,
                transcription: false,
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
                provider_replay_state: None,
                termination: crate::ProviderTermination::Complete,
            })
        }

        async fn stream_chat(
            &self,
            _request: ChatRequest,
        ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
            let chunks = vec![
                Ok(StreamChunk::delta("hello ")),
                Ok(StreamChunk::delta("world")),
                Ok(StreamChunk::final_chunk_with(
                    crate::ProviderTermination::Complete,
                )),
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

    #[test]
    fn provider_failure_classification_is_optional_and_conservative() {
        let provider = MockProvider;
        let error = anyhow::anyhow!("opaque provider rejection");
        assert!(provider.classify_failure(&error).is_none());
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
            reasoning: None,
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
            reasoning: None,
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

    #[tokio::test]
    async fn provider_defaults_to_unsupported_transcription_listing() {
        let error = MockProvider
            .list_transcription_models()
            .await
            .expect_err("mock provider should not list transcription models");

        assert_eq!(
            error.to_string(),
            "provider 'mock' does not support listing transcription models"
        );
    }

    #[tokio::test]
    async fn provider_default_warmup_is_explicitly_unverified() {
        assert_eq!(
            MockProvider.warmup().await.expect("default warm-up"),
            ProviderWarmupOutcome::NotSupported
        );
    }
}
