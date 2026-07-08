//! API provider transport boundary.
//!
//! This crate models model providers exposed through the provider registry.
//! Local CLI-backed agent runtimes such as Codex or Claude CLI belong in
//! `pioneer-cli-agent-runtime` and must not be implemented as `Provider`
//! adapters.

pub mod attachments;
pub mod factory;
mod http;
pub mod providers;
pub mod reasoning_registry;
pub mod registry;
pub mod tools;
pub mod traits;
pub mod types;

pub use attachments::{
    ArtifactExternalRefCacheBackend, ArtifactExternalRefCachePolicy,
    ArtifactExternalRefLookupRequest, ArtifactExternalRefStoreRequest, AttachmentBudgetReport,
    AttachmentCircuitBreakerPolicy, AttachmentNormalizationPolicy, AttachmentPipelineConfig,
    AttachmentRetryPolicy, AttachmentRuntimePolicy, AttachmentSecurityPolicy,
    AttachmentTransportKind, AttachmentTransportPlan, PreparedAttachment, PreparedAttachmentSource,
    PreparedProviderMessages, default_attachment_pipeline_config, infer_mime_from_reference,
    lookup_uploaded_reference_with_artifact, set_artifact_external_ref_cache_backend,
    set_default_attachment_pipeline_config,
};
pub use factory::create_provider;
pub use registry::ProviderRegistry;
pub use traits::Provider;
pub use types::{
    AttachmentArtifactContext, AttachmentDataSource, ChatMessage, ChatRequest, ChatResponse,
    CompiledPromptPayload, EmbeddingRequest, EmbeddingResponse, InputContentType, InputTypeSupport,
    MessageAttachment, MessageContentPart, ModelInputItem, ProviderCapabilities,
    ProviderInputCapabilities, ProviderTimeoutPolicy, ProviderToolCall, ReasoningConfig,
    ReasoningEffort, Role, StreamChunk, TokenUsage, ToolChoice, ToolDefinition,
};
