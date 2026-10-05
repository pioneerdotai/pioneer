//! Safe, operation-specific failures. No source error or caller data crosses this boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryManifestFailureClass {
    StorageTransient,
    AuthorizationOrDomain,
    InvalidStoredData,
    Unclassified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryManifestFailureStage {
    Runtime,
    Authorization,
    Active,
    Candidates,
}

impl MemoryManifestFailureStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Authorization => "authorization",
            Self::Active => "active",
            Self::Candidates => "candidates",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryManifestFailure {
    pub class: MemoryManifestFailureClass,
    pub stage: MemoryManifestFailureStage,
    pub sqlite_primary_code: Option<i32>,
    pub sqlite_extended_code: Option<i32>,
}

impl MemoryManifestFailure {
    pub fn new(class: MemoryManifestFailureClass, stage: MemoryManifestFailureStage) -> Self {
        Self {
            class,
            stage,
            sqlite_primary_code: None,
            sqlite_extended_code: None,
        }
    }

    pub fn code(self) -> &'static str {
        match self.class {
            MemoryManifestFailureClass::StorageTransient => {
                "memory.post_turn_extractor.manifest_storage_transient"
            }
            MemoryManifestFailureClass::AuthorizationOrDomain => {
                "memory.post_turn_extractor.manifest_domain_rejected"
            }
            MemoryManifestFailureClass::InvalidStoredData => {
                "memory.post_turn_extractor.manifest_invalid_stored_data"
            }
            MemoryManifestFailureClass::Unclassified => {
                "memory.post_turn_extractor.manifest_unclassified"
            }
        }
    }

    pub fn class_name(self) -> &'static str {
        match self.class {
            MemoryManifestFailureClass::StorageTransient => "storage_transient",
            MemoryManifestFailureClass::AuthorizationOrDomain => "authorization_or_domain",
            MemoryManifestFailureClass::InvalidStoredData => "invalid_stored_data",
            MemoryManifestFailureClass::Unclassified => "unclassified",
        }
    }

    pub fn retryable(self) -> bool {
        self.class == MemoryManifestFailureClass::StorageTransient
    }
}

/// Emitted only at the pure conversion boundary for persisted memory rows/payloads.
/// A JSON failure elsewhere (including inside a backend) is not this marker.
#[derive(Debug)]
pub(crate) struct InvalidStoredMemoryData;

impl std::fmt::Display for InvalidStoredMemoryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid stored memory data")
    }
}
impl std::error::Error for InvalidStoredMemoryData {}

/// Inspectable through anyhow::Context without exposing conversion inputs.
pub fn is_invalid_stored_memory_data(error: &anyhow::Error) -> bool {
    error.downcast_ref::<InvalidStoredMemoryData>().is_some()
}
