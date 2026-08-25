//! Apply Patch tool engine, file-mutation primitives, and durable history.

mod authorize;
mod executor;
pub mod file_mutation;
mod guards;
pub mod history;
mod matcher;
mod observer;
mod parser;
mod planner;
mod prepare;
mod provider_adapter;
mod telemetry;

pub use authorize::{
    AllowAllSandbox, ApprovalReceipt, AuthorizedPatch, DenyAuthorizer, FullAccessAuthorizer,
    PermissionAuthorizer, PermissionEffect, PermissionError, PermissionErrorCode, PermissionIntent,
    PermissionMode, PermissionTarget, SandboxPolicy, authorize,
};
pub use executor::{
    Cancellation, ExecuteOptions, ExecutionReport, ExecutionStatus, NeverCancel, PatchExecutor,
    patch_telemetry,
};
pub use guards::{
    DestinationGuard, GuardError, GuardErrorCode, ValidatedOperation, ValidatedPatchDocument,
    validate_guards,
};
pub use matcher::{
    MatchError, MatchErrorCode, MatchResult, apply_update, apply_update_with_candidate_limit,
};
pub use observer::{
    CommitAdmission, CommitObserver, DurableCommitObserver, InMemoryCommitObserver,
    ObserverAdmission, ObserverError, ObserverErrorCode,
};
pub use parser::{
    AddFile, GuardSyntax, Hunk, HunkLine, Operation, OperationBody, OperationKind, ParseError,
    ParseErrorCode, PatchDocument, ReplaceFile, UpdateFile, parse,
};
pub use planner::{
    PlanError, PlanErrorCode, PlannedChange, PlannedPatch, PlannedSnapshot, VirtualFile,
    VirtualFileOrigin, VirtualWorkspace, plan, plan_with_candidate_limit, plan_with_limits,
};
pub use prepare::{
    ObservedTarget, PrepareError, PrepareErrorCode, PrepareOptions, PreparedFileVersion,
    PreparedPatch, ResolvedPatch, prepare, prepare_resolved, resolve_patch,
};
pub use provider_adapter::{
    NativePatchAdapterError, NativePatchChange, NativePatchError, NativePatchOutcome,
    NativePatchTracking, NativePatchTrackingStatus, normalize_native_patch_payload,
    project_apply_patch_outcome,
};
pub use telemetry::{PatchTelemetry, PatchTelemetrySnapshot, TelemetryStage};
