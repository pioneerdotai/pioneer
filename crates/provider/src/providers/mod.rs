pub(crate) mod anthropic;
mod azure_openai;
mod bedrock;
mod compatible;
pub(crate) mod copilot;
pub(crate) mod deepseek;
mod echo;
mod embedding;
pub(crate) mod gemini;
pub(crate) mod glm;
mod local;
pub(crate) mod ollama;
pub(crate) mod openai;
pub(crate) mod openrouter;
pub(crate) mod retired;
pub(crate) mod telnyx;

pub use anthropic::AnthropicProvider;
pub use azure_openai::AzureOpenAiProvider;
pub use bedrock::BedrockProvider;
pub use compatible::{AuthStyle, OpenAiCompatibleProvider};
pub use copilot::CopilotProvider;
pub use deepseek::DeepSeekProvider;
pub use echo::EchoProvider;
pub use gemini::GeminiProvider;
pub use glm::GlmProvider;
pub use local::{
    LOCAL_EMBEDDING_MODELS, LOCAL_TRANSCRIPTION_MODELS, LocalEmbeddingModelInfo, LocalProvider,
    LocalTranscriptionArtifactKind, LocalTranscriptionEngine, LocalTranscriptionModelInfo,
    local_embedding_model_info, local_transcription_model_info,
};
pub use ollama::OllamaProvider;
pub use openai::OpenAiProvider;
pub use openrouter::OpenRouterProvider;
pub use telnyx::TelnyxProvider;

#[cfg(test)]
mod usage_tests;
