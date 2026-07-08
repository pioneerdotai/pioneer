use anyhow::{Result, bail};
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use pioneer_protocol::{ProviderModelCapabilities, ProviderModelInfo, ProviderModelLimits};

use crate::traits::Provider;
use crate::types::{
    ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse, ProviderCapabilities,
    ProviderInputCapabilities, StreamChunk,
};

const LOCAL_PROVIDER_ID: &str = "local";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalEmbeddingModelInfo {
    pub id: &'static str,
    pub display_name: &'static str,
    pub description: &'static str,
    pub dimension: usize,
    pub max_tokens: usize,
    pub model_url: &'static str,
    pub tokenizer_url: &'static str,
    pub default: bool,
}

pub const LOCAL_EMBEDDING_MODELS: &[LocalEmbeddingModelInfo] = &[
    LocalEmbeddingModelInfo {
        id: "bge-small-en-v1.5",
        display_name: "BGE Small EN v1.5",
        description: "Local 384-dimensional embedding model for fast memory recall.",
        dimension: 384,
        max_tokens: 512,
        model_url: "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/main/tokenizer.json",
        default: true,
    },
    LocalEmbeddingModelInfo {
        id: "bge-base-en-v1.5",
        display_name: "BGE Base EN v1.5",
        description: "Local 768-dimensional embedding model with higher recall quality.",
        dimension: 768,
        max_tokens: 512,
        model_url: "https://huggingface.co/BAAI/bge-base-en-v1.5/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/BAAI/bge-base-en-v1.5/resolve/main/tokenizer.json",
        default: false,
    },
    LocalEmbeddingModelInfo {
        id: "nomic-embed-text-v1.5",
        display_name: "Nomic Embed Text v1.5",
        description: "Local 768-dimensional embedding model for general semantic retrieval.",
        dimension: 768,
        max_tokens: 512,
        model_url: "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5/resolve/main/tokenizer.json",
        default: false,
    },
    LocalEmbeddingModelInfo {
        id: "gte-large",
        display_name: "GTE Large",
        description: "Local 1024-dimensional embedding model for higher quality retrieval.",
        dimension: 1024,
        max_tokens: 512,
        model_url: "https://huggingface.co/thenlper/gte-large/resolve/main/onnx/model.onnx",
        tokenizer_url: "https://huggingface.co/thenlper/gte-large/resolve/main/tokenizer.json",
        default: false,
    },
];

pub fn local_embedding_model_info(model: &str) -> Option<&'static LocalEmbeddingModelInfo> {
    LOCAL_EMBEDDING_MODELS
        .iter()
        .find(|candidate| candidate.id == model)
}

#[derive(Debug, Clone, Default)]
pub struct LocalProvider;

impl LocalProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Provider for LocalProvider {
    fn name(&self) -> &str {
        LOCAL_PROVIDER_ID
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: false,
            vision: false,
            tool_calling: false,
            embeddings: true,
            input_types: ProviderInputCapabilities::disabled_for_all_file_types(),
        }
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse> {
        bail!("provider '{LOCAL_PROVIDER_ID}' does not support chat")
    }

    async fn stream_chat(
        &self,
        _request: ChatRequest,
    ) -> Result<BoxStream<'static, Result<StreamChunk>>> {
        bail!("provider '{LOCAL_PROVIDER_ID}' does not support chat streaming")
    }

    async fn list_embedding_models(&self) -> Result<Vec<ProviderModelInfo>> {
        Ok(LOCAL_EMBEDDING_MODELS
            .iter()
            .map(local_embedding_model_to_provider_model)
            .collect())
    }

    async fn embed(&self, _request: EmbeddingRequest) -> Result<EmbeddingResponse> {
        bail!(
            "provider '{LOCAL_PROVIDER_ID}' uses the gateway local embedding runtime for embedding execution"
        )
    }
}

fn local_embedding_model_to_provider_model(model: &LocalEmbeddingModelInfo) -> ProviderModelInfo {
    ProviderModelInfo {
        id: model.id.to_owned(),
        name: Some(model.display_name.to_owned()),
        description: Some(model.description.to_owned()),
        created: None,
        provider: LOCAL_PROVIDER_ID.to_owned(),
        owned_by: Some("local".to_owned()),
        limits: ProviderModelLimits {
            max_input_tokens: Some(model.max_tokens as u64),
            max_output_tokens: None,
            context_window: Some(model.max_tokens as u64),
        },
        capabilities: ProviderModelCapabilities {
            embeddings: Some(true),
            ..ProviderModelCapabilities::default()
        },
        pricing: None,
        active: Some(true),
        family: Some("embedding".to_owned()),
        lifecycle_status: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_provider_lists_embedding_models_as_records() {
        let provider = LocalProvider::new();
        let models = provider
            .list_embedding_models()
            .await
            .expect("local embedding models");

        let small = models
            .iter()
            .find(|model| model.id == "bge-small-en-v1.5")
            .expect("bge small model");
        assert_eq!(small.provider, "local");
        assert_eq!(small.name.as_deref(), Some("BGE Small EN v1.5"));
        assert_eq!(small.capabilities.embeddings, Some(true));
    }
}
