//! Lossless Apply Patch outcomes and durable history vocabulary.
//!
//! This crate defines the durable handoff between PatchEngine execution,
//! SQLite-backed applied-patch history, content-addressed snapshots, and the
//! rebuildable per-turn diff projection. It never reads the current workspace
//! or Git to reconstruct historical state.

use crate::apply_patch::file_mutation::{
    FileContentVersion, PatchDiagnostic, PatchErrorCode, PatchStage, TargetMetadataFingerprint,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

mod codex;
mod db_codex;
mod db_intent;
mod db_projection;
mod db_snapshots;
mod db_store;
mod intent;
mod projection;
mod projector;
mod queries;
mod replay;
mod retention;
mod snapshots;
mod store;

#[cfg(test)]
mod db_tests;

pub use codex::{
    CODEX_AGGREGATE_SCHEMA_VERSION, CODEX_MAX_AGGREGATE_BYTES, CODEX_MAX_EVENT_IDS,
    CODEX_TURN_DIFF_PROTOCOL_REVISION, CodexAggregateError, CodexAggregateEvent,
    CodexAggregateEventContext, CodexAggregateFile, CodexAggregateFileKind, CodexAggregateIngest,
    CodexAggregateProjectionStore, CodexAggregateState, CodexAggregateTracker,
    CodexProtocolSupport, normalize_codex_diff_updated, parse_changed_files, select_codex_protocol,
};
pub use db_codex::SqliteCodexAggregateStore;
pub use db_intent::{BeginNextOutcome, SqliteCommitIntentStore};
pub use db_projection::SqliteTurnDiffStore;
pub use db_snapshots::{SnapshotReconciliationReport, SqliteSnapshotStore};
pub use db_store::SqliteAppliedPatchStore;
pub use intent::{CommitIntentJournal, IntentError, IntentStatus, PatchCommitIntent};
pub use projection::{
    AgentDiffUpdatedProjection, ProjectionError, TURN_DIFF_STATE_SCHEMA_VERSION,
    TurnDiffProjectionStore, TurnDiffState, TurnFilesystemCoverage, TurnFilesystemMutationSource,
};
pub use projector::{
    AggregateFileChange, AggregateProjectionError, TurnAggregate, TurnRecordProjector,
    next_turn_projection_revision, project_turn_records, project_turn_records_with_ordinal_status,
};
pub use queries::{
    AppliedStep, ExecutionHistoryCursor, FileHistoryCursor, FileHistoryEntry, HistoryCoverage,
    HistoryPage, HistoryQueryError, HistoryQueryLimits, HistoryRenderedDiff, ThreadHistoryCursor,
    aggregate_semantic_inputs, authorized_snapshot_reference, query_file_history,
    query_thread_steps, query_turn_steps,
};
pub use replay::{TurnProjectionReplay, replay_turn_pages};
pub use retention::{
    HistoryRetention, RecoveryReport, RecoveryResolution, RetentionError, RetentionReport,
};
pub use snapshots::{
    ContentAddressedSnapshotRef, ContentAddressedSnapshotStore, SnapshotDomain,
    SnapshotReservation, SnapshotStoreError, SnapshotStoreLimits, SnapshotStoreMetrics,
};
pub use store::{AppliedPatchLog, InsertedPatchRecord, RecordStoreError, StoredPatchRecord};

pub const APPLIED_PATCH_RECORD_SCHEMA_VERSION: u16 = 1;
pub const SNAPSHOT_REF_SCHEMA_VERSION: u16 = 1;
pub const APPLY_PATCH_RESULT_SCHEMA_VERSION: u16 = 1;

/// Stable, content-free identifier for one invocation's immutable history
/// record.  The same identity/ordinal pair must produce the same value on
/// replay, before or after SQLite persistence.
pub fn applied_patch_record_id(identity: &InvocationIdentity, ordinal: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(identity.thread_id.as_bytes());
    digest.update([0]);
    digest.update(identity.turn_id.as_bytes());
    digest.update([0]);
    digest.update(identity.invocation_id.as_bytes());
    digest.update(ordinal.to_le_bytes());
    hex::encode(digest.finalize())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfile {
    NativePioneer,
    CodexCli,
    ManagedClaude,
    UnsupportedCli,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnDiffAuthority {
    NativePatchEngine,
    CodexAggregateEvent,
    ManagedClaudePatchEngine,
    Unsupported,
}

impl Default for TurnDiffAuthority {
    fn default() -> Self {
        Self::Unsupported
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryProvenance {
    NativeEngine,
    ManagedClaude,
    Recovery,
    ProviderAggregate,
    Unknown,
}

impl Default for PatchHistoryProvenance {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatchRecordExactness {
    Exact,
    Partial,
    Uncertain,
}

impl Default for PatchRecordExactness {
    fn default() -> Self {
        Self::Uncertain
    }
}

impl PatchRecordExactness {
    pub const fn is_exact(self) -> bool {
        matches!(self, Self::Exact | Self::Partial)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PatchHistoryCoverage {
    EngineVerifiedSteps,
    ProviderReportedSteps { provider: String, protocol: String },
    AggregateOnly { provider: String, protocol: String },
    Incomplete { reason: String },
    Untracked { reason: String },
}

impl PatchHistoryCoverage {
    pub fn provider_reported_steps(
        provider: impl Into<String>,
        protocol: impl Into<String>,
    ) -> Self {
        Self::ProviderReportedSteps {
            provider: provider.into(),
            protocol: protocol.into(),
        }
    }

    pub fn aggregate_only(provider: impl Into<String>, protocol: impl Into<String>) -> Self {
        Self::AggregateOnly {
            provider: provider.into(),
            protocol: protocol.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TurnDiffExactness {
    EngineVerified,
    ProviderReported { provider: String, protocol: String },
    Incomplete { reason: String },
}

impl TurnDiffExactness {
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::EngineVerified | Self::ProviderReported { .. })
    }

    pub fn from_coverage(exact: bool, coverage: &PatchHistoryCoverage) -> Self {
        if !exact {
            return Self::Incomplete {
                reason: match coverage {
                    PatchHistoryCoverage::Incomplete { reason }
                    | PatchHistoryCoverage::Untracked { reason } => reason.clone(),
                    _ => "turn diff exactness could not be proven".to_owned(),
                },
            };
        }
        match coverage {
            PatchHistoryCoverage::EngineVerifiedSteps => Self::EngineVerified,
            PatchHistoryCoverage::ProviderReportedSteps { provider, protocol }
            | PatchHistoryCoverage::AggregateOnly { provider, protocol } => {
                Self::ProviderReported {
                    provider: provider.clone(),
                    protocol: protocol.clone(),
                }
            }
            PatchHistoryCoverage::Incomplete { reason }
            | PatchHistoryCoverage::Untracked { reason } => Self::Incomplete {
                reason: reason.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TurnAuthority {
    pub turn_id: String,
    pub profile: RuntimeProfile,
    pub authority: TurnDiffAuthority,
    pub coverage: PatchHistoryCoverage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthoritySelectionError {
    EmptyTurnId,
    ContradictoryCapabilities,
    UnsupportedProfile,
    AuthorityChangedOnResume,
}

impl TurnAuthority {
    /// Selects authority from trusted runtime facts. Model/provider payloads
    /// are intentionally not accepted by this API.
    pub fn select(
        turn_id: impl Into<String>,
        profile: RuntimeProfile,
        provider_aggregate_available: bool,
        managed_patch_capability: bool,
    ) -> Result<Self, AuthoritySelectionError> {
        let turn_id = turn_id.into();
        if turn_id.trim().is_empty() {
            return Err(AuthoritySelectionError::EmptyTurnId);
        }
        let (authority, coverage) = match profile {
            RuntimeProfile::NativePioneer => {
                if provider_aggregate_available || managed_patch_capability {
                    return Err(AuthoritySelectionError::ContradictoryCapabilities);
                }
                (
                    TurnDiffAuthority::NativePatchEngine,
                    PatchHistoryCoverage::EngineVerifiedSteps,
                )
            }
            RuntimeProfile::CodexCli => {
                if managed_patch_capability {
                    return Err(AuthoritySelectionError::ContradictoryCapabilities);
                }
                if !provider_aggregate_available {
                    return Err(AuthoritySelectionError::UnsupportedProfile);
                }
                (
                    TurnDiffAuthority::CodexAggregateEvent,
                    PatchHistoryCoverage::aggregate_only(
                        "codex",
                        CODEX_TURN_DIFF_PROTOCOL_REVISION,
                    ),
                )
            }
            RuntimeProfile::ManagedClaude => {
                if provider_aggregate_available || !managed_patch_capability {
                    return Err(AuthoritySelectionError::ContradictoryCapabilities);
                }
                (
                    TurnDiffAuthority::ManagedClaudePatchEngine,
                    PatchHistoryCoverage::EngineVerifiedSteps,
                )
            }
            RuntimeProfile::UnsupportedCli => (
                TurnDiffAuthority::Unsupported,
                PatchHistoryCoverage::Untracked {
                    reason: "provider has no supported authoritative patch shape".into(),
                },
            ),
        };
        Ok(Self {
            turn_id,
            profile,
            authority,
            coverage,
        })
    }

    pub fn resume(&self, resumed: &Self) -> Result<(), AuthoritySelectionError> {
        if self.turn_id != resumed.turn_id
            || self.profile != resumed.profile
            || self.authority != resumed.authority
            || self.coverage != resumed.coverage
        {
            return Err(AuthoritySelectionError::AuthorityChangedOnResume);
        }
        Ok(())
    }

    pub fn is_engine_verified(&self) -> bool {
        matches!(self.coverage, PatchHistoryCoverage::EngineVerifiedSteps)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionScope {
    ReadOnlyShared,
    MutationScoped,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MutationInvocation {
    pub identity: InvocationIdentity,
    pub scope: ExecutionScope,
}

impl MutationInvocation {
    pub fn new(identity: InvocationIdentity) -> Self {
        Self {
            identity,
            scope: ExecutionScope::MutationScoped,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApplyPatchResult {
    pub schema_version: u16,
    pub invocation: MutationInvocation,
    pub authority: TurnDiffAuthority,
    pub coverage: PatchHistoryCoverage,
    pub outcome: ApplyPatchOutcome,
}

impl ApplyPatchResult {
    pub fn new(
        invocation: MutationInvocation,
        authority: TurnDiffAuthority,
        coverage: PatchHistoryCoverage,
        outcome: ApplyPatchOutcome,
    ) -> Self {
        Self {
            schema_version: APPLY_PATCH_RESULT_SCHEMA_VERSION,
            invocation,
            authority,
            coverage,
            outcome,
        }
    }

    pub fn is_history_bearing(&self) -> bool {
        self.outcome.is_history_bearing()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "wire_shape")]
pub enum PatchWireInput {
    RawText { patch: String },
    StrictJson { patch: String },
}

impl PatchWireInput {
    pub fn patch_text(&self) -> &str {
        match self {
            Self::RawText { patch } | Self::StrictJson { patch } => patch,
        }
    }

    pub fn into_request(
        self,
        source: crate::apply_patch::file_mutation::PatchRequestSource,
        limits: crate::apply_patch::file_mutation::PatchLimits,
    ) -> Result<
        crate::apply_patch::file_mutation::PatchRequest,
        crate::apply_patch::file_mutation::PatchError,
    > {
        crate::apply_patch::file_mutation::PatchRequest::from_owned(
            match self {
                Self::RawText { patch } | Self::StrictJson { patch } => patch,
            },
            source,
            limits,
        )
    }
}

impl fmt::Display for AuthoritySelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::EmptyTurnId => "turn id is required",
            Self::ContradictoryCapabilities => {
                "runtime capabilities select more than one authority"
            }
            Self::UnsupportedProfile => "runtime profile has no authoritative diff source",
            Self::AuthorityChangedOnResume => "turn authority changed during resume",
        };
        f.write_str(value)
    }
}

impl std::error::Error for AuthoritySelectionError {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InvocationIdentity {
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
}

impl InvocationIdentity {
    pub fn new(
        thread_id: impl Into<String>,
        turn_id: impl Into<String>,
        invocation_id: impl Into<String>,
    ) -> Result<Self, HistoryTypeError> {
        let identity = Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
            invocation_id: invocation_id.into(),
        };
        if identity.thread_id.trim().is_empty()
            || identity.turn_id.trim().is_empty()
            || identity.invocation_id.trim().is_empty()
        {
            return Err(HistoryTypeError::EmptyIdentity);
        }
        Ok(identity)
    }

    pub fn uniqueness_key(&self) -> (&str, &str, &str) {
        (&self.thread_id, &self.turn_id, &self.invocation_id)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct CommitOrdinal(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Add,
    Replace,
    Update,
    Delete,
    Move,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextEncoding {
    Utf8,
    Utf8Bom,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineEnding {
    Lf,
    Crlf,
    Mixed,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LineEndingMetadata {
    pub dominant: LineEnding,
    pub mixed: bool,
    pub final_newline: bool,
}

impl Default for LineEndingMetadata {
    fn default() -> Self {
        Self {
            dominant: LineEnding::None,
            mixed: false,
            final_newline: false,
        }
    }
}

/// A committed snapshot retained in the executor result until the history
/// writer interns it. Exact bytes are deliberately present here; the writer
/// later turns them into a content-addressed `TextSnapshotRef`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedTextSnapshot {
    pub version: FileContentVersion,
    pub bytes: Vec<u8>,
    pub encoding: TextEncoding,
    pub line_endings: LineEndingMetadata,
}

impl CommittedTextSnapshot {
    pub fn from_bytes(
        bytes: Vec<u8>,
        encoding: TextEncoding,
        line_endings: LineEndingMetadata,
    ) -> Self {
        Self {
            version: FileContentVersion::new(
                crate::apply_patch::file_mutation::FileVersionToken::from_bytes(&bytes),
            ),
            bytes,
            encoding,
            line_endings,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommittedPatchChange {
    /// Source operation identity. This is not interchangeable with the
    /// committed-prefix sequence because semantic no-ops do not emit a
    /// history entry.
    pub operation_index: u32,
    /// Ordered commit step within this invocation.
    pub commit_step: u16,
    pub sequence: u32,
    pub kind: ChangeKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub before: Option<CommittedTextSnapshot>,
    pub after: Option<CommittedTextSnapshot>,
    pub overwritten_destination: Option<CommittedTextSnapshot>,
    /// Typed filesystem side effects (such as created or residual parent
    /// directories) are retained with the journaled change.  Host paths are
    /// normalized to bounded markers by the mutation executor.
    pub side_effects: PatchSideEffects,
}

/// The bounded, pre-mutation recovery description retained in a commit
/// intent.  It is intentionally separate from `DurablePatchChange`: the
/// latter contains content-addressed references used by the long-lived log,
/// while this value carries the exact bytes needed to decide after a process
/// crash which ordered operations reached the filesystem.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparedChangeRecovery {
    pub operation_index: u32,
    pub kind: ChangeKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub before: Option<CommittedTextSnapshot>,
    pub after: Option<CommittedTextSnapshot>,
    pub overwritten_destination: Option<CommittedTextSnapshot>,
    pub side_effects: PatchSideEffects,
}

/// Parent-directory identity captured before a patch.  Parent creation is a
/// filesystem side effect of add/move operations, so recovery must retain the
/// same authorization boundary as the file operations instead of inferring it
/// from the current workspace after a crash.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PreparedDirectoryRecovery {
    pub path: String,
    pub existed: bool,
    pub fingerprint: TargetMetadataFingerprint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchRecoveryPlan {
    pub environment_id: String,
    pub workspace_root: String,
    pub authority: TurnDiffAuthority,
    pub changes: Vec<PreparedChangeRecovery>,
    pub parent_directories: Vec<PreparedDirectoryRecovery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppliedPatchDelta {
    pub changes: Vec<CommittedPatchChange>,
    pub exact: bool,
    /// Directories created or left behind by the filesystem primitive.  This
    /// is part of the delta so side effects remain durable even when no file
    /// content change can be represented by the aggregate projection.
    pub side_effects: PatchSideEffects,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PatchSideEffects {
    pub created_directories: Vec<String>,
    pub residual_directories: Vec<String>,
    /// Metadata that the platform intentionally does not claim to preserve
    /// (for example ACLs/xattrs). These warnings are bounded and content-free.
    pub metadata_warnings: Vec<String>,
    pub exact: bool,
}

impl Default for PatchSideEffects {
    fn default() -> Self {
        Self {
            created_directories: Vec::new(),
            residual_directories: Vec::new(),
            metadata_warnings: Vec::new(),
            exact: true,
        }
    }
}

impl PatchSideEffects {
    pub fn merge(&mut self, other: &Self) {
        self.exact &= other.exact;
        self.created_directories
            .extend(other.created_directories.iter().cloned());
        self.residual_directories
            .extend(other.residual_directories.iter().cloned());
        self.metadata_warnings
            .extend(other.metadata_warnings.iter().cloned());
        self.created_directories.sort();
        self.created_directories.dedup();
        self.residual_directories.sort();
        self.residual_directories.dedup();
        self.metadata_warnings.sort();
        self.metadata_warnings.dedup();
    }

    /// Whether the mutation left a durable filesystem effect that cannot be
    /// represented as a file-content change. Parents created and then cleaned
    /// are intentionally excluded; a residual parent or private staging file
    /// must remain history-bearing and recoverable.
    pub fn has_durable_effects(&self) -> bool {
        !self.residual_directories.is_empty()
            || self
                .metadata_warnings
                .iter()
                .any(|warning| warning == "temporary_file_cleanup_failed")
    }
}

impl AppliedPatchDelta {
    pub fn empty() -> Self {
        Self {
            changes: Vec::new(),
            exact: true,
            side_effects: PatchSideEffects {
                exact: true,
                ..PatchSideEffects::default()
            },
        }
    }

    pub fn from_changes(mut changes: Vec<CommittedPatchChange>) -> Self {
        for (sequence, change) in changes.iter_mut().enumerate() {
            let sequence = u32::try_from(sequence).unwrap_or(u32::MAX);
            change.sequence = sequence;
            change.commit_step = u16::try_from(sequence).unwrap_or(u16::MAX);
        }
        Self {
            changes,
            exact: true,
            side_effects: PatchSideEffects {
                exact: true,
                ..PatchSideEffects::default()
            },
        }
    }

    pub fn with_exactness(mut self, exact: bool) -> Self {
        self.exact = exact;
        self.side_effects.exact &= exact;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && !self.side_effects.has_durable_effects()
    }

    /// Appends a later committed prefix while retaining the weaker exactness.
    pub fn append(&mut self, mut other: Self) {
        self.exact &= other.exact;
        self.side_effects.merge(&other.side_effects);
        let offset = self.changes.len() as u32;
        for change in &mut other.changes {
            change.sequence = change.sequence.saturating_add(offset);
            change.commit_step = u16::try_from(change.sequence).unwrap_or(u16::MAX);
        }
        self.changes.extend(other.changes);
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ApplyPatchOutcome {
    Applied {
        delta: AppliedPatchDelta,
    },
    Partial {
        delta: AppliedPatchDelta,
        failure: PatchDiagnostic,
    },
    Rejected {
        failure: PatchDiagnostic,
    },
    Failed {
        delta: AppliedPatchDelta,
        failure: PatchDiagnostic,
    },
    CommitStateUncertain {
        delta: AppliedPatchDelta,
        reason: PatchDiagnostic,
    },
}

impl ApplyPatchOutcome {
    pub fn delta(&self) -> Option<&AppliedPatchDelta> {
        match self {
            Self::Applied { delta }
            | Self::Partial { delta, .. }
            | Self::Failed { delta, .. }
            | Self::CommitStateUncertain { delta, .. } => Some(delta),
            Self::Rejected { .. } => None,
        }
    }

    pub fn is_history_bearing(&self) -> bool {
        self.delta().is_some_and(|delta| !delta.is_empty())
    }

    pub fn status(&self) -> &'static str {
        match self {
            Self::Applied { .. } => "applied",
            Self::Partial { .. } => "partial",
            Self::Rejected { .. } => "rejected",
            Self::Failed { .. } => "failed",
            Self::CommitStateUncertain { .. } => "commit_state_uncertain",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TextSnapshotRef {
    pub schema_version: u16,
    pub content_hash: [u8; 32],
    pub byte_len: u64,
    pub encoding: TextEncoding,
    pub line_endings: LineEndingMetadata,
}

impl TextSnapshotRef {
    pub fn from_snapshot(snapshot: &CommittedTextSnapshot) -> Self {
        Self {
            schema_version: SNAPSHOT_REF_SCHEMA_VERSION,
            content_hash: *snapshot.version.token.digest(),
            byte_len: snapshot.version.token.byte_len(),
            encoding: snapshot.encoding,
            line_endings: snapshot.line_endings,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurablePatchChange {
    pub operation_index: u32,
    pub commit_step: u16,
    pub sequence: u32,
    pub kind: ChangeKind,
    pub source_path: String,
    pub destination_path: Option<String>,
    pub before: Option<TextSnapshotRef>,
    pub after: Option<TextSnapshotRef>,
    pub overwritten_destination: Option<TextSnapshotRef>,
    pub side_effects: PatchSideEffects,
}

impl From<&CommittedPatchChange> for DurablePatchChange {
    fn from(change: &CommittedPatchChange) -> Self {
        Self {
            operation_index: change.operation_index,
            commit_step: change.commit_step,
            sequence: change.sequence,
            kind: change.kind,
            source_path: change.source_path.clone(),
            destination_path: change.destination_path.clone(),
            before: change.before.as_ref().map(TextSnapshotRef::from_snapshot),
            after: change.after.as_ref().map(TextSnapshotRef::from_snapshot),
            overwritten_destination: change
                .overwritten_destination
                .as_ref()
                .map(TextSnapshotRef::from_snapshot),
            side_effects: change.side_effects.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AppliedPatchRecordOutcome {
    Applied,
    Partial {
        failed_stage: PatchStage,
        error_code: PatchErrorCode,
    },
    CommitStateUncertain,
    Gap {
        reason: String,
    },
}

impl AppliedPatchRecordOutcome {
    /// Whether the committed filesystem delta stored by this record is exact.
    ///
    /// `Partial` describes the lifecycle result of the whole invocation: a
    /// later operation failed.  Its already committed prefix is still exact
    /// and engine verified.  Only an uncertain/gap outcome loses delta
    /// exactness.
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Applied | Self::Partial { .. })
    }

    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::CommitStateUncertain | Self::Gap { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppliedPatchRecord {
    pub schema_version: u16,
    pub identity: InvocationIdentity,
    pub environment_id: String,
    pub commit_ordinal: CommitOrdinal,
    pub authority: TurnDiffAuthority,
    pub provenance: PatchHistoryProvenance,
    pub exactness: PatchRecordExactness,
    pub committed_at_unix_ms: i64,
    pub outcome: AppliedPatchRecordOutcome,
    pub changes: Vec<DurablePatchChange>,
    /// Durable effects that are not file-content changes. This keeps residual
    /// parent directories and private staging cleanup failures in the journal
    /// without inventing a Proposal 40 file change.
    pub side_effects: PatchSideEffects,
}

impl AppliedPatchRecord {
    pub fn new(
        identity: InvocationIdentity,
        commit_ordinal: CommitOrdinal,
        outcome: AppliedPatchRecordOutcome,
        changes: Vec<DurablePatchChange>,
    ) -> Self {
        Self {
            schema_version: APPLIED_PATCH_RECORD_SCHEMA_VERSION,
            identity,
            environment_id: String::new(),
            commit_ordinal,
            authority: TurnDiffAuthority::NativePatchEngine,
            provenance: PatchHistoryProvenance::NativeEngine,
            exactness: match &outcome {
                AppliedPatchRecordOutcome::Applied => PatchRecordExactness::Exact,
                AppliedPatchRecordOutcome::Partial { .. } => PatchRecordExactness::Partial,
                AppliedPatchRecordOutcome::CommitStateUncertain
                | AppliedPatchRecordOutcome::Gap { .. } => PatchRecordExactness::Uncertain,
            },
            committed_at_unix_ms: 0,
            outcome,
            changes,
            side_effects: PatchSideEffects::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty() && !self.side_effects.has_durable_effects()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryTypeError {
    EmptyIdentity,
}

impl fmt::Display for HistoryTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdentity => {
                f.write_str("thread, turn and invocation identities are required")
            }
        }
    }
}

impl std::error::Error for HistoryTypeError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::FileVersionToken;

    fn snapshot(value: &[u8]) -> CommittedTextSnapshot {
        CommittedTextSnapshot::from_bytes(
            value.to_vec(),
            TextEncoding::Utf8,
            LineEndingMetadata {
                dominant: LineEnding::Lf,
                mixed: false,
                final_newline: true,
            },
        )
    }

    fn change(sequence: u32, before: &[u8], after: &[u8]) -> CommittedPatchChange {
        CommittedPatchChange {
            operation_index: sequence,
            commit_step: u16::try_from(sequence).unwrap_or(u16::MAX),
            sequence,
            kind: ChangeKind::Update,
            source_path: "src/a.txt".into(),
            destination_path: None,
            before: Some(snapshot(before)),
            after: Some(snapshot(after)),
            overwritten_destination: None,
            side_effects: PatchSideEffects::default(),
        }
    }

    #[test]
    fn delta_preserves_order_and_weakest_exactness() {
        let mut first = AppliedPatchDelta::from_changes(vec![change(0, b"a", b"b")]);
        let second =
            AppliedPatchDelta::from_changes(vec![change(0, b"b", b"c")]).with_exactness(false);
        first.append(second);
        assert_eq!(first.changes[0].sequence, 0);
        assert_eq!(first.changes[1].sequence, 1);
        assert!(!first.exact);
    }

    #[test]
    fn partial_outcome_is_history_bearing_but_rejection_is_not() {
        let delta = AppliedPatchDelta::from_changes(vec![change(0, b"a", b"b")]);
        let failure = PatchDiagnostic {
            code: PatchErrorCode::Io,
            stage: PatchStage::Commit,
            message: "write failed".into(),
            retryability: crate::apply_patch::file_mutation::Retryability::RecoverOnly,
            operation_index: None,
            path: None,
            guard_horizon: None,
        };
        let partial = ApplyPatchOutcome::Partial {
            delta,
            failure: failure.clone(),
        };
        assert!(partial.is_history_bearing());
        assert!(!ApplyPatchOutcome::Rejected { failure }.is_history_bearing());
        let durable_partial = AppliedPatchRecordOutcome::Partial {
            failed_stage: PatchStage::Commit,
            error_code: PatchErrorCode::Io,
        };
        assert!(
            durable_partial.is_exact(),
            "a failed invocation can still have an exact committed prefix"
        );
        assert!(!durable_partial.is_uncertain());
    }

    #[test]
    fn durable_record_round_trips_and_keeps_overwrite_snapshot() {
        let identity = InvocationIdentity::new("thread", "turn", "call").unwrap();
        let original = change(0, b"old", b"new");
        let mut durable = DurablePatchChange::from(&original);
        durable.overwritten_destination = Some(TextSnapshotRef::from_snapshot(&snapshot(b"dest")));
        let record = AppliedPatchRecord::new(
            identity.clone(),
            CommitOrdinal(7),
            AppliedPatchRecordOutcome::Partial {
                failed_stage: PatchStage::Commit,
                error_code: PatchErrorCode::Io,
            },
            vec![durable],
        );
        let encoded = serde_json::to_string(&record).unwrap();
        let decoded: AppliedPatchRecord = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(decoded.identity.uniqueness_key(), identity.uniqueness_key());
        assert_eq!(
            decoded.changes[0]
                .overwritten_destination
                .as_ref()
                .unwrap()
                .byte_len,
            4
        );
    }

    #[test]
    fn canonical_v1_records_do_not_accept_legacy_missing_fields() {
        let record = AppliedPatchRecord::new(
            InvocationIdentity::new("thread", "turn", "call").unwrap(),
            CommitOrdinal(0),
            AppliedPatchRecordOutcome::Applied,
            vec![DurablePatchChange::from(&change(0, b"before", b"after"))],
        );
        let record_json = serde_json::to_value(&record).unwrap();
        for required in [
            "environment_id",
            "authority",
            "provenance",
            "exactness",
            "committed_at_unix_ms",
            "side_effects",
        ] {
            let mut missing = record_json.clone();
            missing.as_object_mut().unwrap().remove(required);
            assert!(
                serde_json::from_value::<AppliedPatchRecord>(missing).is_err(),
                "v1 applied record must require `{required}`"
            );
        }

        for required in ["operation_index", "commit_step", "side_effects"] {
            let mut missing = record_json.clone();
            missing["changes"][0]
                .as_object_mut()
                .unwrap()
                .remove(required);
            assert!(
                serde_json::from_value::<AppliedPatchRecord>(missing).is_err(),
                "v1 durable change must require `{required}`"
            );
        }

        let recovery = PatchRecoveryPlan {
            environment_id: "workspace".to_owned(),
            workspace_root: "/workspace".to_owned(),
            authority: TurnDiffAuthority::NativePatchEngine,
            changes: Vec::new(),
            parent_directories: Vec::new(),
        };
        let recovery_json = serde_json::to_value(&recovery).unwrap();
        for required in ["environment_id", "parent_directories"] {
            let mut missing = recovery_json.clone();
            missing.as_object_mut().unwrap().remove(required);
            assert!(
                serde_json::from_value::<PatchRecoveryPlan>(missing).is_err(),
                "v1 recovery plan must require `{required}`"
            );
        }
    }

    #[test]
    fn snapshot_hash_and_length_are_exact() {
        let snapshot = snapshot(b"value\n");
        assert_eq!(
            snapshot.version.token,
            FileVersionToken::from_bytes(b"value\n")
        );
        let reference = TextSnapshotRef::from_snapshot(&snapshot);
        assert_eq!(reference.byte_len, 6);
        assert_eq!(reference.content_hash, *snapshot.version.token.digest());
    }

    #[test]
    fn identity_rejects_empty_component() {
        assert_eq!(
            InvocationIdentity::new("thread", "", "call"),
            Err(HistoryTypeError::EmptyIdentity)
        );
    }

    #[test]
    fn runtime_matrix_selects_one_immutable_authority() {
        let native =
            TurnAuthority::select("turn", RuntimeProfile::NativePioneer, false, false).unwrap();
        assert_eq!(native.authority, TurnDiffAuthority::NativePatchEngine);
        assert!(native.is_engine_verified());

        let codex = TurnAuthority::select("turn", RuntimeProfile::CodexCli, true, false).unwrap();
        assert_eq!(codex.authority, TurnDiffAuthority::CodexAggregateEvent);
        assert_eq!(
            codex.coverage,
            PatchHistoryCoverage::aggregate_only("codex", CODEX_TURN_DIFF_PROTOCOL_REVISION)
        );

        let claude =
            TurnAuthority::select("turn", RuntimeProfile::ManagedClaude, false, true).unwrap();
        assert_eq!(
            claude.authority,
            TurnDiffAuthority::ManagedClaudePatchEngine
        );
        assert!(claude.is_engine_verified());
    }

    #[test]
    fn contradictory_capabilities_fail_closed() {
        assert_eq!(
            TurnAuthority::select("turn", RuntimeProfile::NativePioneer, true, false),
            Err(AuthoritySelectionError::ContradictoryCapabilities)
        );
        assert_eq!(
            TurnAuthority::select("turn", RuntimeProfile::ManagedClaude, false, false),
            Err(AuthoritySelectionError::ContradictoryCapabilities)
        );
        assert_eq!(
            TurnAuthority::select("turn", RuntimeProfile::CodexCli, false, false),
            Err(AuthoritySelectionError::UnsupportedProfile)
        );
    }

    #[test]
    fn authority_cannot_change_on_resume() {
        let first = TurnAuthority::select("turn", RuntimeProfile::CodexCli, true, false).unwrap();
        let same = TurnAuthority::select("turn", RuntimeProfile::CodexCli, true, false).unwrap();
        assert!(first.resume(&same).is_ok());
        let changed =
            TurnAuthority::select("turn", RuntimeProfile::NativePioneer, false, false).unwrap();
        assert_eq!(
            first.resume(&changed),
            Err(AuthoritySelectionError::AuthorityChangedOnResume)
        );
    }

    #[test]
    fn unsupported_profile_is_truthfully_untracked() {
        let authority =
            TurnAuthority::select("turn", RuntimeProfile::UnsupportedCli, false, false).unwrap();
        assert_eq!(authority.authority, TurnDiffAuthority::Unsupported);
        assert!(matches!(
            authority.coverage,
            PatchHistoryCoverage::Untracked { .. }
        ));
        assert!(!authority.is_engine_verified());
    }

    #[test]
    fn result_schema_round_trips_partial_mutation() {
        let identity = InvocationIdentity::new("thread", "turn", "call").unwrap();
        let invocation = MutationInvocation::new(identity);
        let delta = AppliedPatchDelta::from_changes(vec![change(0, b"a", b"b")]);
        let failure = PatchDiagnostic {
            code: PatchErrorCode::StaleFile,
            stage: PatchStage::Commit,
            message: "file changed under lock".into(),
            retryability: crate::apply_patch::file_mutation::Retryability::RetryAfterRead,
            operation_index: Some(0),
            path: Some("file.txt".to_owned()),
            guard_horizon: Some(crate::apply_patch::file_mutation::GuardHorizon::Commit),
        };
        let result = ApplyPatchResult::new(
            invocation,
            TurnDiffAuthority::NativePatchEngine,
            PatchHistoryCoverage::EngineVerifiedSteps,
            ApplyPatchOutcome::Partial { delta, failure },
        );
        let encoded = serde_json::to_string(&result).unwrap();
        let decoded: ApplyPatchResult = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, result);
        assert!(decoded.is_history_bearing());
        assert_eq!(decoded.invocation.scope, ExecutionScope::MutationScoped);
    }

    #[test]
    fn wire_inputs_share_one_request_constructor() {
        let limits = crate::apply_patch::file_mutation::PatchLimits::default();
        let raw = PatchWireInput::RawText {
            patch: "*** Begin Patch\n*** End Patch".into(),
        };
        let strict = PatchWireInput::StrictJson {
            patch: raw.patch_text().into(),
        };
        let raw_request = raw
            .into_request(
                crate::apply_patch::file_mutation::PatchRequestSource::NativeFreeform,
                limits,
            )
            .unwrap();
        let strict_request = strict
            .into_request(
                crate::apply_patch::file_mutation::PatchRequestSource::NativeFunction,
                limits,
            )
            .unwrap();
        assert_eq!(raw_request.patch, strict_request.patch);
        assert_eq!(raw_request.schema_version, strict_request.schema_version);
    }

    #[test]
    fn result_rejection_is_not_a_mutation_record() {
        let result = ApplyPatchResult::new(
            MutationInvocation::new(InvocationIdentity::new("t", "u", "c").unwrap()),
            TurnDiffAuthority::NativePatchEngine,
            PatchHistoryCoverage::EngineVerifiedSteps,
            ApplyPatchOutcome::Rejected {
                failure: PatchDiagnostic {
                    code: PatchErrorCode::InvalidRequest,
                    stage: PatchStage::Parse,
                    message: "bad envelope".into(),
                    retryability: crate::apply_patch::file_mutation::Retryability::Never,
                    operation_index: None,
                    path: None,
                    guard_horizon: None,
                },
            },
        );
        assert!(!result.is_history_bearing());
        assert_eq!(result.outcome.status(), "rejected");
    }

    #[test]
    fn only_durable_non_file_side_effects_are_history_bearing() {
        let mut cleaned_parent = AppliedPatchDelta::empty();
        cleaned_parent
            .side_effects
            .created_directories
            .push("<created-parent>".to_owned());
        assert!(cleaned_parent.is_empty());

        let mut residual_parent = AppliedPatchDelta::empty();
        residual_parent
            .side_effects
            .residual_directories
            .push("<residual-parent>".to_owned());
        residual_parent.side_effects.exact = false;
        residual_parent.exact = false;
        assert!(!residual_parent.is_empty());
        assert!(
            ApplyPatchOutcome::Failed {
                delta: residual_parent,
                failure: PatchDiagnostic {
                    code: PatchErrorCode::Io,
                    stage: PatchStage::Stage,
                    message: "parent cleanup failed".to_owned(),
                    retryability: crate::apply_patch::file_mutation::Retryability::Never,
                    operation_index: None,
                    path: None,
                    guard_horizon: None,
                },
            }
            .is_history_bearing()
        );

        let mut residual_stage = AppliedPatchDelta::empty();
        residual_stage
            .side_effects
            .metadata_warnings
            .push("temporary_file_cleanup_failed".to_owned());
        assert!(!residual_stage.is_empty());
    }
}
