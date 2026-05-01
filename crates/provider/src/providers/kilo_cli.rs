use crate::{
    attachments::{ensure_no_unrendered_attachments, prepare_messages_for_provider},
    tools::parse::parse_embedded_tool_payload,
    types::{
        ChatRequest, ChatResponse, ProviderCapabilities, ProviderInputCapabilities, StreamChunk,
        TokenUsage,
    },
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, stream};
use std::path::PathBuf;
use tokio::process::Command;

pub struct KiloCliProvider {
    binary_path: PathBuf,
}

impl KiloCliProvider {
    pub fn new() -> Self {
        Self {
            binary_path: PathBuf::from("kilo"),
        }
    }

    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self {
            binary_path: path.into(),
        }
    }
}

#[async_trait]
impl crate::traits::Provider for KiloCliProvider {
    fn name(&self) -> &str {
        "kilo_cli"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
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
        let user_message = rendered_messages
            .iter()
            .rev()
            .find(|m| m.role == crate::types::Role::User)
            .map(|m| m.content.clone())
            .ok_or_else(|| anyhow!("no user message in request"))?;

        let mut child = Command::new(&self.binary_path)
            .arg("--print")
            .arg("-")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin.write_all(user_message.as_bytes()).await?;
        }

        let output = child.wait_with_output().await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("kilo command failed: {stderr}"));
        }

        let raw_text = String::from_utf8(output.stdout)?;
        let parsed = parse_embedded_tool_payload(&raw_text);
        let (text, reasoning_content, tool_calls) = match parsed {
            Some(payload) => (payload.text, payload.reasoning_content, payload.tool_calls),
            None => (raw_text, None, Vec::new()),
        };

        Ok(ChatResponse {
            text,
            usage: Some(TokenUsage::default()),
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
        Ok(stream::iter(chunks).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_with_default_path() {
        let provider = KiloCliProvider::new();
        assert_eq!(provider.binary_path, PathBuf::from("kilo"));
    }

    #[test]
    fn creates_with_custom_path() {
        let provider = KiloCliProvider::with_path("/usr/local/bin/kilo");
        assert_eq!(provider.binary_path, PathBuf::from("/usr/local/bin/kilo"));
    }

    #[test]
    fn provider_name() {
        use crate::traits::Provider;
        let provider = KiloCliProvider::new();
        assert_eq!(provider.name(), "kilo_cli");
    }

    #[test]
    fn provider_capabilities() {
        use crate::traits::Provider;
        let provider = KiloCliProvider::new();
        let caps = provider.capabilities();
        assert!(!caps.streaming);
        assert!(!caps.vision);
    }
}
