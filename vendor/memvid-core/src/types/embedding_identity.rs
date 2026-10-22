use std::collections::BTreeMap;

/// Frame-level embedding metadata keys (stored in `Frame.extra_metadata`).
///
/// These are intentionally persisted per-frame (instead of in the TOC schema) to avoid
/// breaking `.mv2` binary compatibility (the TOC is bincode-encoded and schema-evolution
/// requires explicit legacy decoders).
pub const MEMVID_EMBEDDING_PROVIDER_KEY: &str = "memvid.embedding.provider";
pub const MEMVID_EMBEDDING_MODEL_KEY: &str = "memvid.embedding.model";
pub const MEMVID_EMBEDDING_DIMENSION_KEY: &str = "memvid.embedding.dimension";
pub const MEMVID_EMBEDDING_NORMALIZED_KEY: &str = "memvid.embedding.normalized";

/// Identifies an embedding "vector space" used for semantic search.
///
/// Dimensions alone are not sufficient to guarantee compatibility (multiple models can share a
/// dimension), so production-safe auto-detection should prefer `provider` + `model` when present.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EmbeddingIdentity {
    pub provider: Option<Box<str>>,
    pub model: Option<Box<str>>,
    pub dimension: Option<u32>,
    pub normalized: Option<bool>,
}

impl EmbeddingIdentity {
    /// Parse an embedding identity from a frame's `extra_metadata`.
    ///
    /// Returns `None` if neither provider nor model is present.
    #[must_use]
    pub fn from_extra_metadata(extra: &BTreeMap<String, String>) -> Option<Self> {
        let provider = extra
            .get(MEMVID_EMBEDDING_PROVIDER_KEY)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase().into_boxed_str());

        let model = extra
            .get(MEMVID_EMBEDDING_MODEL_KEY)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string().into_boxed_str());

        if provider.is_none() && model.is_none() {
            return None;
        }

        let dimension = extra
            .get(MEMVID_EMBEDDING_DIMENSION_KEY)
            .map(|value| value.trim())
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|dim| *dim > 0);

        let normalized = extra
            .get(MEMVID_EMBEDDING_NORMALIZED_KEY)
            .map(|value| value.trim().to_ascii_lowercase())
            .and_then(|value| match value.as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            });

        Some(Self {
            provider,
            model,
            dimension,
            normalized,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingIdentityCount {
    pub identity: EmbeddingIdentity,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddingIdentitySummary {
    Unknown,
    Single(EmbeddingIdentity),
    Mixed(Vec<EmbeddingIdentityCount>),
}
