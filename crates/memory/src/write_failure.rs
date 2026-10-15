/// Safe typed write classification. No memory payload or raw error is retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryWriteFailure {
    InvalidInput,
    AuthorizationOrDomain,
    StorageTransient,
    Unclassified,
}

impl MemoryWriteFailure {
    pub fn class(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::AuthorizationOrDomain => "authorization_or_domain",
            Self::StorageTransient => "storage_transient",
            Self::Unclassified => "unclassified",
        }
    }

    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "memory.post_turn_extractor.write_invalid_input",
            Self::AuthorizationOrDomain => "memory.post_turn_extractor.write_domain_rejected",
            Self::StorageTransient => "memory.post_turn_extractor.write_storage_transient",
            Self::Unclassified => "memory.post_turn_extractor.write_unclassified",
        }
    }

    pub fn retryable(self) -> bool {
        self == Self::StorageTransient
    }
}

impl std::fmt::Display for MemoryWriteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.class())
    }
}

impl std::error::Error for MemoryWriteFailure {}
