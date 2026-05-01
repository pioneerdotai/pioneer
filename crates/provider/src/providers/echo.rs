use crate::{
    attachments::{ensure_no_unrendered_attachments, prepare_messages_for_provider},
    tools::parse::parse_embedded_tool_payload,
    types::{
        ChatMessage, ChatRequest, ChatResponse, ProviderCapabilities, ProviderInputCapabilities,
        Role, StreamChunk,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream;
use futures_util::stream::BoxStream;

/// A test provider that echoes the last user message back as the response.
pub struct EchoProvider;

impl EchoProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl crate::traits::Provider for EchoProvider {
    fn name(&self) -> &str {
        "echo"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            vision: false,
            tool_calling: true,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
        }
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse> {
        let prepared = prepare_messages_for_provider(
            self.name(),
            &self.capabilities(),
            request.rendered_messages_with_compiled_prompt().as_slice(),
        )?;
        ensure_no_unrendered_attachments(self.name(), &prepared)?;
        let rendered_messages = prepared.messages;
        let raw_text = extract_last_user_text(rendered_messages.as_slice());
        let parsed = parse_embedded_tool_payload(&raw_text);
        let (text, reasoning_content, tool_calls) = match parsed {
            Some(payload) => (payload.text, payload.reasoning_content, payload.tool_calls),
            None => (raw_text, None, Vec::new()),
        };
        Ok(ChatResponse {
            text,
            usage: None,
            reasoning_content,
            tool_calls,
        })
    }

    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        let response = self.chat(request).await?;
        let mut chunks = Vec::new();
        if let Some(reasoning) = response.reasoning_content {
            if !reasoning.is_empty() {
                chunks.push(Ok(StreamChunk::reasoning(reasoning)));
            }
        }
        if !response.tool_calls.is_empty() {
            chunks.push(Ok(StreamChunk::tool_calls(response.tool_calls)));
        }
        if !response.text.is_empty() {
            chunks.push(Ok(StreamChunk::delta(response.text)));
        }
        chunks.push(Ok(StreamChunk::final_chunk()));
        Ok(Box::pin(stream::iter(chunks)))
    }
}

fn extract_last_user_text(messages: &[ChatMessage]) -> String {
    messages
        .iter()
        .rfind(|m| m.role == Role::User)
        .map(|m| m.content.clone())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Provider;
    use futures_util::StreamExt;

    #[test]
    fn echo_provider_name() {
        let provider = EchoProvider::new();
        assert_eq!(provider.name(), "echo");
    }

    #[tokio::test]
    async fn echo_provider_chat_returns_user_text() {
        let provider = EchoProvider::new();
        let request = ChatRequest {
            model: "test".into(),
            messages: vec![
                ChatMessage::system("system prompt"),
                ChatMessage::user("hello world"),
            ],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            compiled_prompt: None,
        };

        let response = provider.chat(request).await.unwrap();
        assert_eq!(response.text, "hello world");
    }

    #[tokio::test]
    async fn echo_provider_stream_returns_user_text() {
        let provider = EchoProvider::new();
        let request = ChatRequest {
            model: "test".into(),
            messages: vec![ChatMessage::user("streaming test")],
            temperature: None,
            max_tokens: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            compiled_prompt: None,
        };

        let mut stream = provider.stream_chat(request).await.unwrap();

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.delta, "streaming test");
        assert!(!chunk.is_final);

        let final_chunk = stream.next().await.unwrap().unwrap();
        assert!(final_chunk.is_final);
    }
}
