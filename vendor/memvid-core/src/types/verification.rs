//! Types describing verification and doctor workflows.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// User-provided preferences that influence how the doctor plans repair work.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DoctorOptions {
    #[serde(default)]
    pub rebuild_time_index: bool,
    #[serde(default)]
    pub rebuild_lex_index: bool,
    #[serde(default)]
    pub rebuild_vec_index: bool,
    #[serde(default)]
    pub vacuum: bool,
    #[serde(default)]
    pub dry_run: bool,
    /// Suppress debug output when true.
    #[serde(default)]
    pub quiet: bool,
}

/// Version identifier embedded in `DoctorPlan` for compatibility checks.
pub const DOCTOR_PLAN_VERSION: u32 = 1;

/// Coarse phases executed by the doctor orchestrator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorPhaseKind {
    Probe,
    HeaderHealing,
    WalReplay,
    IndexRebuild,
    Vacuum,
    Finalize,
    Verify,
}

/// Operation variants that can be scheduled within a phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorActionKind {
    HealHeaderPointer,
    HealTocChecksum,
    ReplayWal,
    DiscardWal,
    RebuildTimeIndex,
    RebuildLexIndex,
    RebuildVecIndex,
    VacuumCompaction,
    RecomputeToc,
    UpdateHeader,
    DeepVerify,
    NoOp,
}

/// Severity assigned to individual findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorSeverity {
    Info,
    Warning,
    Error,
}

/// Stable codes that map onto the error taxonomy for doctor findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum DoctorFindingCode {
    HeaderFooterOffsetMismatch,
    HeaderTocChecksumMismatch,
    HeaderDecodeFailure,
    TocDecodeFailure,
    TocChecksumMismatch,
    TocOutOfBounds,
    WalHasPendingRecords,
    WalSequenceAheadOfHeader,
    WalChecksumMismatch,
    TimeIndexMissing,
    TimeIndexChecksumMismatch,
    TimeIndexUnsorted,
    LexIndexMissing,
    LexIndexCorrupt,
    VecIndexMissing,
    VecIndexCorrupt,
    TantivySnapshotMissing,
    TantivySnapshotCorrupt,
    MerkleMismatch,
    SegmentCatalogInconsistent,
    VacuumIncomplete,
    LockContention,
    UnsupportedFeature,
    InternalError,
}

/// Human-readable description of a detected issue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorFinding {
    /// Stable classification code for the issue.
    pub code: DoctorFindingCode,
    /// Severity hint used for planning and reporting.
    pub severity: DoctorSeverity,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub detail: Option<String>,
}

impl DoctorFinding {
    #[must_use]
    pub fn info(code: DoctorFindingCode, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: DoctorSeverity::Info,
            message: message.into(),
            detail: None,
        }
    }

    #[must_use]
    pub fn warning(code: DoctorFindingCode, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: DoctorSeverity::Warning,
            message: message.into(),
            detail: None,
        }
    }

    #[must_use]
    pub fn error(code: DoctorFindingCode, message: impl Into<String>) -> Self {
        Self {
            code,
            severity: DoctorSeverity::Error,
            message: message.into(),
            detail: None,
        }
    }

    #[must_use]
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

/// Planned sequence of phases and operations required to heal a memory file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorPlan {
    #[serde(default = "DoctorPlan::default_version")]
    pub version: u32,
    /// Memory path the plan was derived from.
    pub file_path: PathBuf,
    #[serde(default)]
    pub options: DoctorOptions,
    #[serde(default)]
    pub findings: Vec<DoctorFinding>,
    #[serde(default)]
    pub phases: Vec<DoctorPhasePlan>,
}

impl DoctorPlan {
    fn default_version() -> u32 {
        DOCTOR_PLAN_VERSION
    }

    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.phases.iter().all(|phase| {
            phase.actions.iter().all(|action| {
                matches!(
                    action.action,
                    DoctorActionKind::DeepVerify | DoctorActionKind::NoOp
                )
            })
        })
    }
}

/// Phase-level plan that groups related actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorPhasePlan {
    pub phase: DoctorPhaseKind,
    #[serde(default)]
    pub actions: Vec<DoctorActionPlan>,
}

/// A single action to be executed during a phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorActionPlan {
    /// Work item to perform.
    pub action: DoctorActionKind,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub reasons: Vec<DoctorFindingCode>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub detail: Option<DoctorActionDetail>,
}

/// Structured payload attached to specific doctor actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DoctorActionDetail {
    HeaderPointer {
        target_footer_offset: u64,
    },
    TocChecksum {
        expected: [u8; 32],
    },
    WalReplay {
        from_sequence: u64,
        to_sequence: u64,
        pending_records: usize,
    },
    TimeIndex {
        expected_entries: u64,
    },
    LexIndex {
        expected_docs: u64,
    },
    VecIndex {
        expected_vectors: u64,
        dimension: u32,
    },
    VacuumStats {
        active_frames: u64,
    },
}

/// Aggregated metrics reported after doctor execution.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DoctorMetrics {
    #[serde(default)]
    pub total_duration_ms: u64,
    #[serde(default)]
    pub phase_durations: Vec<DoctorPhaseDuration>,
    #[serde(default)]
    pub actions_completed: usize,
    #[serde(default)]
    pub actions_skipped: usize,
}

/// Duration recorded for a single phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorPhaseDuration {
    /// Phase identifier.
    pub phase: DoctorPhaseKind,
    /// Wall-clock milliseconds spent in the phase.
    pub duration_ms: u64,
}

/// Final outcome categories emitted by doctor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorStatus {
    Clean,
    Healed,
    Partial,
    Failed,
    PlanOnly,
}

/// Per-phase status summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorPhaseStatus {
    Skipped,
    Executed,
    Failed,
}

/// Outcome for an individual action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorActionStatus {
    Skipped,
    Executed,
    Failed,
}

/// Phase execution report captured after doctor runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorPhaseReport {
    /// Phase that produced this report entry.
    pub phase: DoctorPhaseKind,
    pub status: DoctorPhaseStatus,
    #[serde(default)]
    pub actions: Vec<DoctorActionReport>,
    #[serde(default)]
    pub duration_ms: Option<u64>,
}

/// Action-level execution result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorActionReport {
    /// Action identifier.
    pub action: DoctorActionKind,
    pub status: DoctorActionStatus,
    #[serde(default)]
    pub detail: Option<String>,
}

/// Composite report returned by doctor after executing a plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorReport {
    /// Plan that was executed (possibly with adjustments).
    pub plan: DoctorPlan,
    pub status: DoctorStatus,
    #[serde(default)]
    pub phases: Vec<DoctorPhaseReport>,
    #[serde(default)]
    pub findings: Vec<DoctorFinding>,
    #[serde(default)]
    pub metrics: DoctorMetrics,
    #[serde(default)]
    pub verification: Option<VerificationReport>,
}

/// Metadata returned by `verify` (or attached to a doctor report when requested).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    /// Path of the memory that was verified.
    pub file_path: PathBuf,
    /// Individual checks and their statuses.
    pub checks: Vec<VerificationCheck>,
    /// Aggregate status across all checks.
    pub overall_status: VerificationStatus,
}

/// Individual verification check outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationCheck {
    /// Human-friendly name for the check.
    pub name: String,
    /// Result of the check.
    pub status: VerificationStatus,
    /// Optional machine-parsable details.
    pub details: Option<String>,
}

/// Status for a verification check or overall verification run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Passed,
    Failed,
    Skipped,
}
