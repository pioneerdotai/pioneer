use crate::types::{AttachmentArtifactContext, ChatMessage, InputContentType};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AttachmentPipelineConfig {
    pub max_bytes_per_attachment: usize,
    pub max_total_bytes_per_request: usize,
    pub max_attachments_per_request: usize,
    pub upload_preferred_min_bytes: usize,
    pub upload_registry: ArtifactExternalRefCachePolicy,
    pub security: AttachmentSecurityPolicy,
    pub normalization: AttachmentNormalizationPolicy,
    pub runtime: AttachmentRuntimePolicy,
}

#[derive(Debug, Clone)]
pub struct ArtifactExternalRefCachePolicy {
    pub enabled: bool,
    pub ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub struct AttachmentSecurityPolicy {
    pub enforce_path_allowlist: bool,
    pub allowed_path_roots: Vec<PathBuf>,
    pub allow_url_sources: bool,
    pub allow_http: bool,
    pub allow_private_network: bool,
    pub max_url_redirects: usize,
    pub url_fetch_timeout_ms: u64,
    pub url_fetch_max_bytes: usize,
    pub url_allowed_domains: Vec<String>,
    pub url_blocked_domains: Vec<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct AttachmentNormalizationPolicy {
    pub strict_mime_match: bool,
    pub max_base64_chars: usize,
    pub max_filename_chars: usize,
}

#[derive(Debug, Clone)]
pub struct AttachmentRetryPolicy {
    pub max_attempts: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub jitter_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AttachmentCircuitBreakerPolicy {
    pub failure_threshold: u32,
    pub open_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AttachmentRuntimePolicy {
    pub retry: AttachmentRetryPolicy,
    pub circuit_breaker: AttachmentCircuitBreakerPolicy,
}

impl Default for AttachmentPipelineConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_attachment: 100 * 1024 * 1024,
            max_total_bytes_per_request: 200 * 1024 * 1024,
            max_attachments_per_request: 64,
            upload_preferred_min_bytes: 512 * 1024,
            upload_registry: ArtifactExternalRefCachePolicy::default(),
            security: AttachmentSecurityPolicy::default(),
            normalization: AttachmentNormalizationPolicy::default(),
            runtime: AttachmentRuntimePolicy::default(),
        }
    }
}

impl Default for ArtifactExternalRefCachePolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            ttl_secs: 7 * 24 * 3600,
        }
    }
}

impl Default for AttachmentSecurityPolicy {
    fn default() -> Self {
        Self {
            enforce_path_allowlist: false,
            allowed_path_roots: Vec::new(),
            // Remote ingestion is opt-in and is additionally constrained by
            // an explicit domain allowlist.
            allow_url_sources: false,
            allow_http: false,
            allow_private_network: false,
            max_url_redirects: 3,
            url_fetch_timeout_ms: 15_000,
            url_fetch_max_bytes: 20 * 1024 * 1024,
            url_allowed_domains: Vec::new(),
            url_blocked_domains: vec!["localhost".to_owned()],
            dry_run: false,
        }
    }
}

impl Default for AttachmentNormalizationPolicy {
    fn default() -> Self {
        Self {
            strict_mime_match: false,
            max_base64_chars: 200 * 1024 * 1024,
            max_filename_chars: 128,
        }
    }
}

impl Default for AttachmentRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 200,
            max_backoff_ms: 2_000,
            jitter_ms: 80,
        }
    }
}

impl Default for AttachmentCircuitBreakerPolicy {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            open_ms: 30_000,
        }
    }
}

impl Default for AttachmentRuntimePolicy {
    fn default() -> Self {
        Self {
            retry: AttachmentRetryPolicy::default(),
            circuit_breaker: AttachmentCircuitBreakerPolicy::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PreparedAttachmentSource {
    Bytes,
    Path { path: String },
    Url { url: String },
    Reference { reference: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentTransportKind {
    Inline,
    Upload,
    DataUrlPart,
    Unsupported,
}

impl AttachmentTransportKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Upload => "upload",
            Self::DataUrlPart => "data_url_part",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AttachmentTransportPlan {
    pub kind: AttachmentTransportKind,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct PreparedAttachment {
    pub message_index: usize,
    pub part_index: usize,
    pub kind: InputContentType,
    pub mime_type: String,
    pub name: String,
    pub size_bytes: usize,
    pub sha256: String,
    pub source: PreparedAttachmentSource,
    pub bytes: Option<Vec<u8>>,
    pub transport_plan: AttachmentTransportPlan,
    pub artifact: Option<AttachmentArtifactContext>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AttachmentBudgetReport {
    pub attachment_count: usize,
    pub total_bytes: usize,
    pub max_attachments: usize,
    pub max_total_bytes: usize,
    pub max_bytes_per_attachment: usize,
}

#[derive(Debug, Clone)]
pub struct PreparedProviderMessages {
    pub messages: Vec<ChatMessage>,
    pub attachments: Vec<PreparedAttachment>,
    pub budget_report: AttachmentBudgetReport,
}

impl PreparedProviderMessages {
    pub fn has_attachments(&self) -> bool {
        !self.attachments.is_empty()
    }

    pub fn attachments_for_message(
        &self,
        message_index: usize,
    ) -> impl Iterator<Item = &PreparedAttachment> {
        self.attachments
            .iter()
            .filter(move |attachment| attachment.message_index == message_index)
    }
}
