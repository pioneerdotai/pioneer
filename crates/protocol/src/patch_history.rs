use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PatchHistoryCoverageView {
    EngineVerifiedSteps,
    ProviderReportedSteps { provider: String, protocol: String },
    AggregateOnly { provider: String, protocol: String },
    Incomplete { reason: String },
    Untracked { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TurnDiffExactnessView {
    EngineVerified,
    ProviderReported { provider: String, protocol: String },
    Incomplete { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchHistoryQueryCoverage {
    pub exactness: TurnDiffExactnessView,
    /// Derived machine-friendly flag. It must agree with `exactness`; it is
    /// not an alternate protocol representation.
    pub exact: bool,
    pub coverage: PatchHistoryCoverageView,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_missing_ordinal: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryAuthorityView {
    NativePatchEngine,
    CodexAggregateEvent,
    ManagedClaudePatchEngine,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryProvenanceView {
    NativeEngine,
    ManagedClaude,
    Recovery,
    ProviderAggregate,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchRecordExactnessView {
    Exact,
    Partial,
    Uncertain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryChangeKind {
    Add,
    Replace,
    Update,
    Delete,
    Move,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryTextEncoding {
    Utf8,
    Utf8Bom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryLineEnding {
    Lf,
    Crlf,
    Mixed,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchHistoryLineEndingMetadata {
    pub dominant: PatchHistoryLineEnding,
    pub mixed: bool,
    pub final_newline: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchHistorySnapshotRef {
    pub schema_version: u16,
    /// Lowercase SHA-256 hex. Snapshot bytes are resolved only by an
    /// authorized diff request and are never read from the live workspace.
    pub content_hash: String,
    pub byte_len: u64,
    pub encoding: PatchHistoryTextEncoding,
    pub line_endings: PatchHistoryLineEndingMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PatchHistorySideEffects {
    pub created_directories: Vec<String>,
    pub residual_directories: Vec<String>,
    pub metadata_warnings: Vec<String>,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchHistoryChange {
    pub operation_index: u32,
    pub commit_step: u16,
    pub sequence: u32,
    pub kind: PatchHistoryChangeKind,
    pub source_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<PatchHistorySnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<PatchHistorySnapshotRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overwritten_destination: Option<PatchHistorySnapshotRef>,
    pub side_effects: PatchHistorySideEffects,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryStage {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PatchHistoryErrorCode {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    tag = "status"
)]
pub enum PatchHistoryRecordOutcome {
    Applied,
    Partial {
        failed_stage: PatchHistoryStage,
        error_code: PatchHistoryErrorCode,
    },
    CommitStateUncertain,
    Gap {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchHistoryRecord {
    pub schema_version: u16,
    pub record_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub invocation_id: String,
    pub environment_id: String,
    pub commit_ordinal: u64,
    pub authority: PatchHistoryAuthorityView,
    pub provenance: PatchHistoryProvenanceView,
    pub exactness: PatchRecordExactnessView,
    pub committed_at_unix_ms: i64,
    pub outcome: PatchHistoryRecordOutcome,
    pub changes: Vec<PatchHistoryChange>,
    /// Durable filesystem effects that are not representable as file-content
    /// changes, such as a residual parent directory after failed cleanup.
    pub side_effects: PatchHistorySideEffects,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchAppliedStep {
    pub record: PatchHistoryRecord,
    pub coverage: PatchHistoryQueryCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchFileHistoryEntry {
    pub environment_id: String,
    pub turn_id: String,
    pub ordinal: u64,
    pub invocation_id: String,
    pub change: PatchHistoryChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchThreadHistoryCursor {
    pub turn_id: String,
    pub ordinal: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PatchFileHistoryCursor {
    pub environment_id: String,
    pub turn_id: String,
    pub ordinal: u64,
    pub sequence: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnPatchStepsPageParams {
    pub thread_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnPatchStepsPageResponse {
    pub thread_id: String,
    pub turn_id: String,
    pub items: Vec<PatchAppliedStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<u64>,
    pub coverage: PatchHistoryQueryCoverage,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadPatchStepsPageParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PatchThreadHistoryCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadPatchStepsPageResponse {
    pub thread_id: String,
    pub items: Vec<PatchAppliedStep>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PatchThreadHistoryCursor>,
    pub coverage: PatchHistoryQueryCoverage,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadFilePatchHistoryPageParams {
    pub thread_id: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PatchFileHistoryCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ThreadFilePatchHistoryPageResponse {
    pub thread_id: String,
    pub path: String,
    pub items: Vec<PatchFileHistoryEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<PatchFileHistoryCursor>,
    pub coverage: PatchHistoryQueryCoverage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub enum PatchRecordSelector {
    RecordId { record_id: String },
    Invocation { invocation_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnPatchRecordGetParams {
    pub thread_id: String,
    pub turn_id: String,
    pub selector: PatchRecordSelector,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnPatchRecordGetResponse {
    pub record: PatchHistoryRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    tag = "kind"
)]
pub enum PatchDiffSelection {
    Record {
        selector: PatchRecordSelector,
    },
    Boundary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        after_ordinal: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        through_ordinal: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnPatchDiffGetParams {
    pub thread_id: String,
    pub turn_id: String,
    pub selection: PatchDiffSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TurnPatchDiffGetResponse {
    pub thread_id: String,
    pub turn_id: String,
    pub exactness: TurnDiffExactnessView,
    pub coverage: PatchHistoryCoverageView,
    pub unified_patch: String,
    pub records_rendered: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through_ordinal: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> PatchHistoryRecord {
        PatchHistoryRecord {
            schema_version: 1,
            record_id: "record".to_owned(),
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            invocation_id: "call".to_owned(),
            environment_id: "workspace".to_owned(),
            commit_ordinal: 0,
            authority: PatchHistoryAuthorityView::NativePatchEngine,
            provenance: PatchHistoryProvenanceView::NativeEngine,
            exactness: PatchRecordExactnessView::Exact,
            committed_at_unix_ms: 1,
            outcome: PatchHistoryRecordOutcome::Applied,
            changes: Vec::new(),
            side_effects: PatchHistorySideEffects {
                created_directories: Vec::new(),
                residual_directories: Vec::new(),
                metadata_warnings: Vec::new(),
                exact: true,
            },
        }
    }

    #[test]
    fn canonical_v1_history_response_requires_all_non_optional_fields() {
        let value = serde_json::to_value(record()).unwrap();
        for required in ["environmentId", "changes", "sideEffects"] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(required);
            assert!(
                serde_json::from_value::<PatchHistoryRecord>(missing).is_err(),
                "v1 history record must require `{required}`"
            );
        }

        for required in [
            "createdDirectories",
            "residualDirectories",
            "metadataWarnings",
        ] {
            let mut missing = value.clone();
            missing["sideEffects"]
                .as_object_mut()
                .unwrap()
                .remove(required);
            assert!(
                serde_json::from_value::<PatchHistoryRecord>(missing).is_err(),
                "v1 side effects must require `{required}`"
            );
        }

        let response = TurnPatchStepsPageResponse {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            items: Vec::new(),
            next_cursor: None,
            coverage: PatchHistoryQueryCoverage {
                exactness: TurnDiffExactnessView::EngineVerified,
                exact: true,
                coverage: PatchHistoryCoverageView::EngineVerifiedSteps,
                first_missing_ordinal: None,
            },
        };
        let mut missing_items = serde_json::to_value(response).unwrap();
        missing_items.as_object_mut().unwrap().remove("items");
        assert!(
            serde_json::from_value::<TurnPatchStepsPageResponse>(missing_items).is_err(),
            "v1 history page must require `items`"
        );
    }
}
