//! Provider-neutral filesystem mutation contracts for the Apply Patch tool.
//!
//! This crate deliberately contains no filesystem access, permission lookup,
//! parser, or gateway state. It owns only bounded request/version vocabulary
//! and stable diagnostics so later layers cannot invent incompatible shapes.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

mod cas;
mod durability;
mod locks;
mod mutation;
mod read;
mod secure_fs;
mod snapshot;
mod target;

pub use cas::{
    CasError, CasErrorCode, CasExpectation, CasRole, CasState, validate_cas, version_on_disk,
};
pub use durability::{
    DurabilityError, DurabilityOptions, FaultPlan, FaultPoint, MetadataPolicy, MetadataWarning,
    apply_safe_add_mode, apply_supported_mode, directory_durability_warnings,
    preserve_supported_mode, supported_mode, sync_parent_directory, unsupported_metadata_warnings,
};
pub use locks::{LockError, LockErrorCode, TargetLockGuard, TargetLockRegistry};
pub use mutation::{
    FileMutationEngine, MutationChange, MutationError, MutationErrorCode, MutationKind,
    MutationOptions, MutationOutcome, MutationSideEffects, MutationSnapshot, PreparedFileStage,
    StageMetadata,
};
pub use read::{
    AllowAllReadAccess, PaginatedReader, ReadAccess, ReadCursor, ReadError, ReadErrorCode,
    ReadPage, ReadRequest,
};
pub(crate) use secure_fs::{StagedFile, ensure_parent_directories};
pub use snapshot::{
    SnapshotEncoding, SnapshotError, SnapshotErrorCode, SnapshotLimits, SnapshotLineEnding,
    SnapshotLineEndings, SnapshotStorage, TextSnapshot, open_directory, open_regular_file,
};
pub use target::{
    CanonicalTarget, TargetExpectation, TargetKind, TargetManifest, TargetMetadataFingerprint,
    TargetResolutionError, TargetResolutionErrorCode, TargetResolver, TargetRole,
    metadata_fingerprint_for_path,
};

pub const PATCH_LIMITS_SCHEMA_VERSION: u16 = 1;
pub const FILE_VERSION_TOKEN_PREFIX: &str = "sha256:";

/// The configured limits are copied into an effective request before parsing.
/// A request is rejected before its text is copied when its byte length is too
/// large.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchLimits {
    pub schema_version: u16,
    pub max_patch_bytes: u64,
    pub max_file_bytes: u64,
    pub max_total_output_bytes: u64,
    pub max_total_snapshot_bytes: u64,
    pub max_operations: u32,
    pub max_chunks_per_update: u32,
    pub max_total_hunks: u32,
    pub max_target_files: u32,
    pub max_path_bytes: u64,
    pub max_parent_targets: u32,
    pub max_candidate_matches: u32,
}

impl Default for PatchLimits {
    fn default() -> Self {
        Self {
            schema_version: PATCH_LIMITS_SCHEMA_VERSION,
            max_patch_bytes: 4 * 1024 * 1024,
            max_file_bytes: 16 * 1024 * 1024,
            max_total_output_bytes: 64 * 1024 * 1024,
            max_total_snapshot_bytes: 128 * 1024 * 1024,
            max_operations: 256,
            max_chunks_per_update: 128,
            max_total_hunks: 1024,
            max_target_files: 768,
            max_path_bytes: 4096,
            max_parent_targets: 1024,
            max_candidate_matches: 128,
        }
    }
}

impl PatchLimits {
    pub fn validate(&self) -> Result<(), PatchError> {
        if self.schema_version != PATCH_LIMITS_SCHEMA_VERSION
            || self.max_patch_bytes == 0
            || self.max_file_bytes == 0
            || self.max_total_output_bytes == 0
            || self.max_total_snapshot_bytes == 0
            || self.max_operations == 0
            || self.max_chunks_per_update == 0
            || self.max_total_hunks == 0
            || self.max_target_files == 0
            || self.max_path_bytes == 0
            || self.max_parent_targets == 0
            || self.max_candidate_matches == 0
        {
            return Err(PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidLimits,
                "patch limits are invalid",
                Retryability::Never,
            ));
        }
        Ok(())
    }
}

/// The wire source is trusted runtime metadata, never a model-supplied field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchRequestSource {
    NativeFreeform,
    NativeFunction,
    ManagedClaude,
}

/// Untrusted model/provider input after bounded construction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchRequest {
    pub schema_version: u16,
    pub patch: String,
    pub source: PatchRequestSource,
}

impl PatchRequest {
    /// Checks the borrowed input before allocating/copying it.
    pub fn from_provider_text(
        patch: &str,
        source: PatchRequestSource,
        limits: PatchLimits,
    ) -> Result<Self, PatchError> {
        limits.validate()?;
        if patch.len() as u64 > limits.max_patch_bytes {
            return Err(PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InputTooLarge,
                "patch input exceeds the configured byte limit",
                Retryability::Never,
            ));
        }
        if patch.trim().is_empty() {
            return Err(PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::PatchEmpty,
                "patch input is empty",
                Retryability::Never,
            ));
        }
        Ok(Self {
            schema_version: PATCH_LIMITS_SCHEMA_VERSION,
            patch: patch.to_owned(),
            source,
        })
    }

    pub fn from_owned(
        patch: String,
        source: PatchRequestSource,
        limits: PatchLimits,
    ) -> Result<Self, PatchError> {
        Self::from_provider_text(&patch, source, limits).map(|mut request| {
            request.patch = patch;
            request
        })
    }
}

/// A canonical, model-copiable content token. The digest covers exact bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct FileVersionToken {
    digest: [u8; 32],
    byte_len: u64,
}

impl FileVersionToken {
    pub const fn new(digest: [u8; 32], byte_len: u64) -> Self {
        Self { digest, byte_len }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let digest = Sha256::digest(bytes);
        let mut digest_bytes = [0; 32];
        digest_bytes.copy_from_slice(&digest);
        Self::new(digest_bytes, bytes.len() as u64)
    }

    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub const fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub fn parse(value: &str) -> Result<Self, PatchError> {
        let Some(rest) = value.strip_prefix(FILE_VERSION_TOKEN_PREFIX) else {
            return Err(PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidVersionToken,
                "version token must start with sha256:",
                Retryability::Never,
            ));
        };
        let Some((digest_text, length_text)) = rest.split_once(':') else {
            return Err(PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidVersionToken,
                "version token must contain digest and byte length",
                Retryability::Never,
            ));
        };
        if digest_text.len() != 64
            || !digest_text.bytes().all(|byte| byte.is_ascii_hexdigit())
            || digest_text.bytes().any(|byte| byte.is_ascii_uppercase())
            || length_text.is_empty()
            || (length_text.len() > 1 && length_text.starts_with('0'))
            || !length_text.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidVersionToken,
                "version token is not canonical lowercase hex plus decimal length",
                Retryability::Never,
            ));
        }
        let bytes = hex::decode(digest_text).map_err(|_| {
            PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidVersionToken,
                "version token digest is invalid",
                Retryability::Never,
            )
        })?;
        let digest: [u8; 32] = bytes.try_into().map_err(|_| {
            PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidVersionToken,
                "version token digest has the wrong length",
                Retryability::Never,
            )
        })?;
        let byte_len = length_text.parse::<u64>().map_err(|_| {
            PatchError::new(
                PatchStage::Normalize,
                PatchErrorCode::InvalidVersionToken,
                "version token byte length is invalid",
                Retryability::Never,
            )
        })?;
        Ok(Self::new(digest, byte_len))
    }
}

impl fmt::Display for FileVersionToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sha256:{}:{}", hex::encode(self.digest), self.byte_len)
    }
}

/// Version metadata returned by a read or captured by a planner. Bytes remain
/// an internal concern of the filesystem layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileContentVersion {
    pub token: FileVersionToken,
}

impl FileContentVersion {
    pub const fn new(token: FileVersionToken) -> Self {
        Self { token }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DestinationGuard {
    MustNotExist,
    Exact(FileVersionToken),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedFileGuard {
    pub source: FileVersionToken,
    pub destination: Option<DestinationGuard>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchStage {
    Normalize,
    Parse,
    Resolve,
    Authorize,
    Prepare,
    Lock,
    Stage,
    Commit,
    Record,
    Recover,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchErrorCode {
    InvalidLimits,
    InvalidPayload,
    PatchSyntaxError,
    PatchEmpty,
    InputTooLarge,
    InvalidVersionToken,
    InvalidRequest,
    TooManyOperations,
    TooManyFiles,
    TooManyHunks,
    InvalidPath,
    PathOutsideAllowedRoot,
    UnauthorizedPath,
    PermissionDenied,
    ContextNotFound,
    AmbiguousContext,
    PreconditionRequired,
    SourceMissing,
    DestinationExists,
    DestinationMissing,
    StaleFile,
    CrossDeviceMove,
    UnsupportedFileType,
    InvalidUtf8,
    FileTooLarge,
    UnsupportedContent,
    LockTimeout,
    IoCreateFailed,
    IoWriteFailed,
    IoSyncFailed,
    IoRenameFailed,
    IoDeleteFailed,
    Io,
    PartialCommit,
    CommitStateUncertain,
    TrackerPublishFailed,
    HistoryCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Retryability {
    Never,
    RetryAfterRead,
    RetryAfterDelay,
    RecoverOnly,
}

/// The optimistic-concurrency boundary that rejected a stale mutation.
/// This is deliberately metadata-only: it helps the model and operators
/// choose a safe recovery action without disclosing current file content or a
/// replacement version token.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardHorizon {
    Observed,
    Prepared,
    Commit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchDiagnostic {
    pub code: PatchErrorCode,
    pub stage: PatchStage,
    pub message: String,
    pub retryability: Retryability,
    pub operation_index: Option<u32>,
    pub path: Option<String>,
    pub guard_horizon: Option<GuardHorizon>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchError {
    pub diagnostic: PatchDiagnostic,
}

impl PatchError {
    pub fn new(
        stage: PatchStage,
        code: PatchErrorCode,
        message: impl Into<String>,
        retryability: Retryability,
    ) -> Self {
        Self {
            diagnostic: PatchDiagnostic {
                code,
                stage,
                message: message.into(),
                retryability,
                operation_index: None,
                path: None,
                guard_horizon: None,
            },
        }
    }

    pub const fn code(&self) -> PatchErrorCode {
        self.diagnostic.code
    }

    pub const fn retryability(&self) -> Retryability {
        self.diagnostic.retryability
    }
}

impl fmt::Display for PatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.diagnostic.code, self.diagnostic.message)
    }
}

impl std::error::Error for PatchError {}

impl fmt::Display for PatchErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::InvalidLimits => "invalid_limits",
            Self::InvalidPayload => "invalid_payload",
            Self::PatchSyntaxError => "patch_syntax_error",
            Self::PatchEmpty => "patch_empty",
            Self::InputTooLarge => "input_too_large",
            Self::InvalidVersionToken => "invalid_version_token",
            Self::InvalidRequest => "invalid_request",
            Self::TooManyOperations => "too_many_operations",
            Self::TooManyFiles => "too_many_files",
            Self::TooManyHunks => "too_many_hunks",
            Self::InvalidPath => "invalid_path",
            Self::PathOutsideAllowedRoot => "path_outside_allowed_root",
            Self::UnauthorizedPath => "unauthorized_path",
            Self::PermissionDenied => "permission_denied",
            Self::ContextNotFound => "context_not_found",
            Self::AmbiguousContext => "ambiguous_context",
            Self::PreconditionRequired => "precondition_required",
            Self::SourceMissing => "source_missing",
            Self::DestinationExists => "destination_exists",
            Self::DestinationMissing => "destination_missing",
            Self::StaleFile => "stale_file",
            Self::CrossDeviceMove => "cross_device_move",
            Self::UnsupportedFileType => "unsupported_file_type",
            Self::InvalidUtf8 => "invalid_utf8",
            Self::FileTooLarge => "file_too_large",
            Self::UnsupportedContent => "unsupported_content",
            Self::LockTimeout => "lock_timeout",
            Self::IoCreateFailed => "io_create_failed",
            Self::IoWriteFailed => "io_write_failed",
            Self::IoSyncFailed => "io_sync_failed",
            Self::IoRenameFailed => "io_rename_failed",
            Self::IoDeleteFailed => "io_delete_failed",
            Self::Io => "io",
            Self::PartialCommit => "partial_commit",
            Self::CommitStateUncertain => "commit_state_uncertain",
            Self::TrackerPublishFailed => "tracker_publish_failed",
            Self::HistoryCapacity => "history_capacity",
        };
        f.write_str(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips_canonically() {
        let token = FileVersionToken::from_bytes(b"hello\n");
        let encoded = token.to_string();
        assert_eq!(FileVersionToken::parse(&encoded), Ok(token));
        assert!(encoded.starts_with("sha256:"));
    }

    #[test]
    fn token_rejects_noncanonical_forms() {
        let token = FileVersionToken::from_bytes(b"hello").to_string();
        assert!(FileVersionToken::parse(&token.to_uppercase()).is_err());
        assert!(FileVersionToken::parse(&token.replace(":5", ":05")).is_err());
        assert!(FileVersionToken::parse("sha256:00:1").is_err());
    }

    #[test]
    fn request_checks_size_before_copy() {
        let limits = PatchLimits {
            max_patch_bytes: 3,
            ..PatchLimits::default()
        };
        let error =
            PatchRequest::from_provider_text("four", PatchRequestSource::NativeFreeform, limits)
                .unwrap_err();
        assert_eq!(error.code(), PatchErrorCode::InputTooLarge);
    }

    #[test]
    fn request_rejects_invalid_limits_and_empty_text() {
        let invalid = PatchLimits {
            max_operations: 0,
            ..PatchLimits::default()
        };
        assert_eq!(
            PatchRequest::from_provider_text("x", PatchRequestSource::NativeFunction, invalid)
                .unwrap_err()
                .code(),
            PatchErrorCode::InvalidLimits
        );
        assert_eq!(
            PatchRequest::from_provider_text(
                "  ",
                PatchRequestSource::NativeFunction,
                PatchLimits::default()
            )
            .unwrap_err()
            .code(),
            PatchErrorCode::PatchEmpty
        );
    }

    #[test]
    fn request_serializes_without_trusted_runtime_fields() {
        let request = PatchRequest::from_provider_text(
            "*** Begin Patch\n*** End Patch",
            PatchRequestSource::ManagedClaude,
            PatchLimits::default(),
        )
        .unwrap();
        let json = serde_json::to_value(request).unwrap();
        assert!(json.get("thread_id").is_none());
        assert!(json.get("environment_id").is_none());
        assert_eq!(json["source"], "managed_claude");
    }
}
