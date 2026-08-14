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
    lookup_uploaded_reference_with_artifact_for_authority, set_artifact_external_ref_cache_backend,
    set_default_attachment_pipeline_config, store_uploaded_reference_for_authority,
    upload_registry_key_for_authority,
};
pub use factory::create_provider;
pub use http::validate_proxy_url;
pub use registry::{ProviderAuthorityFingerprint, ProviderRegistry};
pub use traits::Provider;
pub use types::{
    AttachmentArtifactContext, AttachmentDataSource, CanonicalProviderRoundEnvelope, ChatMessage,
    ChatRequest, ChatResponse, CompiledPromptPayload, EmbeddingRequest, EmbeddingResponse,
    InputContentType, InputTypeSupport, MessageAttachment, MessageContentPart, ModelInputItem,
    ProviderCallIdentity, ProviderCapabilities, ProviderFailureClassification,
    ProviderInputCapabilities, ProviderReplayState, ProviderTermination, ProviderTimeoutPolicy,
    ProviderToolCall, ReasoningConfig, ReasoningEffort, Role, StreamChunk, TokenUsage, ToolChoice,
    ToolDefinition,
};
