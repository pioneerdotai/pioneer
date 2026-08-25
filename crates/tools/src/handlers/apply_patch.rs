use crate::apply_patch::file_mutation::{
    FileMutationEngine, PatchError, PatchErrorCode, PatchRequestSource, PatchStage, Retryability,
    TargetRole,
};
use crate::apply_patch::history::{InvocationIdentity, TurnDiffAuthority};
use crate::apply_patch::{
    AllowAllSandbox, CommitAdmission, CommitObserver, ExecutionReport, FullAccessAuthorizer,
    PatchExecutor, TelemetryStage, authorize, patch_telemetry, prepare_resolved,
};
use crate::context::{
    ApplyPatchPreflight, FunctionToolOutput, ToolInvocation, ToolOutput, ToolPayload,
};
use crate::error::ToolError;
use crate::registry::ToolHandler;
use crate::{FilePolicyChecker, FilePolicyDecision, FilePolicyOperation};
use async_trait::async_trait;
use pioneer_observability::{PatchOperationMetric, record_patch_operation};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

pub struct ApplyPatchHandler;

#[async_trait]
impl ToolHandler for ApplyPatchHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        trace: crate::events::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let source = match &invocation.payload {
            ToolPayload::Function { .. } => PatchRequestSource::NativeFunction,
            _ => PatchRequestSource::NativeFreeform,
        };
        self.handle_with_source(invocation, trace, source).await
    }
}

impl ApplyPatchHandler {
    /// Build the one public v1 rejection shape for failures decided before
    /// the handler is dispatched (for example, an orchestrator permission
    /// denial). This keeps permission policy separate from mutation while
    /// avoiding a transport-shaped error for a valid apply_patch call.
    pub fn canonical_rejection_output(
        source: PatchRequestSource,
        profile: &'static str,
        stage: PatchStage,
        code: PatchErrorCode,
        message: impl Into<String>,
        path: Option<String>,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let retryability = match code {
            PatchErrorCode::StaleFile => Retryability::RetryAfterRead,
            PatchErrorCode::LockTimeout | PatchErrorCode::HistoryCapacity => {
                Retryability::RetryAfterDelay
            }
            PatchErrorCode::CommitStateUncertain => Retryability::RecoverOnly,
            _ => Retryability::Never,
        };
        let mut error = PatchError::new(stage, code, message, retryability);
        error.diagnostic.path = path;
        let report = ExecutionReport::rejected_patch_error(&error);
        let mut operation_metric = PatchOperationMetricGuard::new(source, profile);
        operation_metric.set_report(&report, 0);
        operation_metric.tracking = "not_applicable";
        patch_telemetry().record_report(&report, std::time::Duration::ZERO);
        patch_telemetry().record_authority(authority_name(source));
        canonical_patch_output(report, false)
    }

    /// Executes the canonical patch pipeline for a provider adapter.  The
    /// parser/planner/executor are shared; only the trusted transport source
    /// changes.  Runtime context remains in `ToolInvocation` and is never
    /// accepted from the model payload.
    pub async fn handle_with_source(
        &self,
        invocation: ToolInvocation,
        trace: crate::events::ToolEventTrace,
        source: PatchRequestSource,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let native_context = if matches!(
            source,
            PatchRequestSource::NativeFreeform | PatchRequestSource::NativeFunction
        ) {
            trace.native_patch_context()
        } else {
            None
        };
        if matches!(
            source,
            PatchRequestSource::NativeFreeform | PatchRequestSource::NativeFunction
        ) && native_context.is_none()
        {
            return Err(ToolError::Rejected(
                "native apply_patch durable admission is unavailable; refusing filesystem mutation"
                    .to_owned(),
            ));
        }
        self.handle_with_source_inner(
            invocation,
            source,
            native_context
                .as_ref()
                .map(|(identity, observer)| (identity, observer.as_ref() as &dyn CommitObserver)),
            native_context
                .as_ref()
                .map(|(identity, observer)| (identity, observer.as_ref())),
        )
        .await
    }

    /// Same canonical pipeline with a trusted history observer.  The
    /// invocation identity is supplied by the gateway adapter, never parsed
    /// from model-controlled JSON.
    pub async fn handle_with_source_and_observer(
        &self,
        invocation: ToolInvocation,
        _trace: crate::events::ToolEventTrace,
        source: PatchRequestSource,
        identity: &InvocationIdentity,
        observer: &dyn CommitObserver,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        self.handle_with_source_inner(
            invocation,
            source,
            Some((identity, observer)),
            Some((identity, observer)),
        )
        .await
    }

    async fn handle_with_source_inner(
        &self,
        mut invocation: ToolInvocation,
        source: PatchRequestSource,
        observer: Option<(&InvocationIdentity, &dyn CommitObserver)>,
        native_history: Option<(&InvocationIdentity, &dyn CommitObserver)>,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let mut operation_metric = PatchOperationMetricGuard::new(
            source,
            invocation
                .execution_security_snapshot
                .as_ref()
                .map(|snapshot| snapshot.permission_profile.mode.as_str())
                .unwrap_or("missing"),
        );
        let preflight = invocation.apply_patch_preflight.take().ok_or_else(|| {
            ToolError::Rejected(
                "apply_patch canonical permission preflight is missing; refusing filesystem mutation"
                    .to_owned(),
            )
        })?;
        let resolved = match preflight {
            ApplyPatchPreflight::Ready(resolved) => resolved,
            ApplyPatchPreflight::Rejected(report) => {
                let elapsed = operation_metric.started.elapsed();
                operation_metric.set_report(&report, 0);
                patch_telemetry().record_report(&report, elapsed);
                patch_telemetry().record_authority(authority_name(source));
                operation_metric.tracking = "not_applicable";
                return canonical_patch_output(report, false);
            }
        };
        let patch = extract_patch_input(&invocation.payload)?;
        let payload_hash: [u8; 32] = Sha256::digest(patch.as_bytes()).into();
        if resolved.document().input_bytes != patch.len() as u64
            || resolved.document().payload_hash != payload_hash
        {
            return Err(ToolError::Rejected(
                "apply_patch payload changed after permission preflight".to_owned(),
            ));
        }
        // The exact canonical manifest approved by the orchestrator is also
        // the one checked by the execution security snapshot before any
        // source bytes are read.
        let preflight_targets = resolved
            .target_manifest()
            .targets()
            .iter()
            .map(|target| PatchTarget {
                operation: FilePolicyOperation::Write,
                path: target.relative().to_string_lossy().into_owned(),
                parent: target.role == TargetRole::Parent,
            })
            .collect::<Vec<_>>();
        if let Err(error) = enforce_patch_targets(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            preflight_targets.as_slice(),
        ) {
            let report = error.into_report()?;
            let elapsed = operation_metric.started.elapsed();
            operation_metric.set_report(&report, 0);
            patch_telemetry().record_report(&report, elapsed);
            patch_telemetry().record_authority(authority_name(source));
            operation_metric.tracking = "not_applicable";
            return canonical_patch_output(report, false);
        }
        let plan_started = Instant::now();
        let prepared = match prepare_resolved(resolved) {
            Ok(prepared) => prepared,
            Err(error) => {
                patch_telemetry()
                    .record_stage_latency(TelemetryStage::Plan, plan_started.elapsed());
                let report = ExecutionReport::rejected_prepare_error(&error);
                operation_metric.set_report(&report, 0);
                patch_telemetry().record_report(&report, plan_started.elapsed());
                patch_telemetry().record_authority(authority_name(source));
                operation_metric.tracking = "not_applicable";
                return canonical_patch_output(report, false);
            }
        };
        patch_telemetry().record_stage_latency(TelemetryStage::Plan, plan_started.elapsed());
        patch_telemetry().record_plan(
            prepared.target_manifest.targets().len() as u64,
            prepared.total_hunks,
        );
        let policy_targets = prepared
            .target_manifest
            .targets()
            .iter()
            .map(|target| PatchTarget {
                operation: FilePolicyOperation::Write,
                path: target.relative().to_string_lossy().into_owned(),
                parent: target.role == TargetRole::Parent,
            })
            .collect::<Vec<_>>();
        if let Err(error) = enforce_patch_targets(
            invocation.execution_security_snapshot.as_ref(),
            invocation.workdir.as_path(),
            policy_targets.as_slice(),
        ) {
            let report = error.into_report()?;
            let elapsed = operation_metric.started.elapsed();
            operation_metric.set_report(&report, 0);
            patch_telemetry().record_report(&report, elapsed);
            patch_telemetry().record_authority(authority_name(source));
            operation_metric.tracking = "not_applicable";
            return canonical_patch_output(report, false);
        }
        let admission = if observer.is_some() {
            // `for_planned` replaces operation fingerprints and recovery
            // snapshots with the exact under-lock re-plan.  The parent
            // directory identities are copied from the immutable preparation
            // so recovery also accounts for directories created as a side
            // effect of add/move operations.
            let mut admission = CommitAdmission::minimal(&prepared);
            admission.recovery_plan.environment_id = invocation
                .environment
                .get("PIONEER_ENVIRONMENT_ID")
                .cloned()
                .unwrap_or_else(|| {
                    // The fallback is a stable environment identity, not a
                    // display path.  The record is included in structured
                    // tool output and history events, so leaking the host's
                    // absolute workdir here would violate the safe-disclosure
                    // boundary even though the recovery plan still keeps the
                    // trusted root privately for reconciliation.
                    let normalized = invocation.workdir.to_string_lossy().replace('\\', "/");
                    format!(
                        "workspace:{}",
                        hex::encode(Sha256::digest(normalized.as_bytes()))
                    )
                });
            admission.recovery_plan.workspace_root =
                invocation.workdir.to_string_lossy().into_owned();
            admission.recovery_plan.authority = match source {
                PatchRequestSource::NativeFreeform | PatchRequestSource::NativeFunction => {
                    TurnDiffAuthority::NativePatchEngine
                }
                PatchRequestSource::ManagedClaude => TurnDiffAuthority::ManagedClaudePatchEngine,
            };
            Some(admission)
        } else {
            None
        };
        let authorized = authorize(prepared, &AllowAllSandbox, &FullAccessAuthorizer)
            .map_err(|error| ToolError::Rejected(error.to_string()))?;
        // The handler is instantiated as a lightweight registry entry for
        // every tool runtime.  Keep one process-wide engine registry behind
        // those entries so overlapping invocations touching the same
        // canonical target are serialized, while disjoint targets proceed in
        // parallel.  A fresh engine per call would make the lock registry
        // ineffective and would violate the mutation concurrency contract.
        static SHARED_ENGINE: OnceLock<FileMutationEngine> = OnceLock::new();
        let executor = PatchExecutor::new(
            SHARED_ENGINE
                .get_or_init(|| FileMutationEngine::new(Default::default()))
                .clone(),
        );
        let report = if let Some((identity, observer)) = observer {
            executor.execute_with_observer_and_admission(
                &authorized,
                identity,
                observer,
                admission
                    .as_ref()
                    .expect("observer admission metadata should be present"),
                Default::default(),
                &ToolCancellation(invocation.cancellation.clone()),
            )
        } else {
            executor.execute(
                &authorized,
                Default::default(),
                &ToolCancellation(invocation.cancellation.clone()),
            )
        };
        let status = report.status;
        let committed_hunks = committed_hunk_count(&report, &authorized);
        operation_metric.set_report(&report, committed_hunks);
        let outcome = report.into_outcome();
        patch_telemetry().record_authority(authority_name(source));
        let parity = crate::apply_patch::project_apply_patch_outcome(&outcome);
        // Serialize the canonical provider-neutral projection itself. Manual
        // reconstruction previously omitted its required v1 schema_version.
        let payload = serde_json::to_value(&parity).map_err(|error| {
            ToolError::internal(format!(
                "failed to serialize apply_patch projection: {error}"
            ))
        })?;
        let payload = if let Some((identity, observer)) = native_history {
            let mut payload = payload;
            match observer.record(identity) {
                Ok(Some(stored)) => {
                    let stored_record = stored.0;
                    // The record is authoritative, but a successful record
                    // promotion does not by itself prove that the aggregate
                    // projection was persisted.  Query the projection and
                    // expose the exact revision only when it is present.
                    let projection_revision = observer.projection_revision(identity).ok().flatten();
                    operation_metric.tracking =
                        tracking_for_record(&stored_record.record, projection_revision);
                    if let Some(object) = payload.as_object_mut() {
                        object.insert(
                            "history".to_owned(),
                            serde_json::json!({
                                "authority": authority_name(source),
                                "record_id": crate::apply_patch::history::applied_patch_record_id(
                                    identity,
                                    stored_record.record.commit_ordinal.0,
                                ),
                                "record": stored_record.record.clone(),
                                "plan_fingerprint": hex::encode(stored_record.plan_fingerprint),
                            }),
                        );
                        object.insert(
                            "tracking".to_owned(),
                            serde_json::to_value(native_tracking_for_record(
                                &stored_record.record,
                                authority_name(source),
                                projection_revision,
                            ))
                            .unwrap_or_else(|_| {
                                serde_json::json!({
                                    "status": "incomplete",
                                    "authority": "native_patch_engine"
                                })
                            }),
                        );
                    }
                }
                Ok(None) | Err(_) => {
                    // A non-empty delta without a visible record means the
                    // filesystem outcome may already be committed while
                    // durable tracking is not.  Never turn that into a
                    // successful/fully-tracked claim and never retry here.
                    operation_metric.tracking =
                        tracking_without_record(status, parity.history_bearing);
                    if let Some(object) = payload.as_object_mut() {
                        object.insert(
                            "tracking".to_owned(),
                            serde_json::to_value(native_tracking_without_record(
                                status,
                                parity.history_bearing,
                                authority_name(source),
                            ))
                            .unwrap_or_else(|_| {
                                serde_json::json!({
                                    "status": "incomplete",
                                    "authority": "native_patch_engine"
                                })
                            }),
                        );
                    }
                }
            }
            payload
        } else {
            operation_metric.tracking = if matches!(source, PatchRequestSource::ManagedClaude) {
                "provider_managed"
            } else if parity.history_bearing {
                "untracked_no_observer"
            } else {
                "not_applicable"
            };
            payload
        };
        let body = serde_json::to_string_pretty(&payload).map_err(|error| {
            ToolError::internal(format!("failed to serialize apply_patch result: {error}"))
        })?;

        Ok(Box::new(FunctionToolOutput::with_payload(
            body,
            matches!(status, crate::apply_patch::ExecutionStatus::Applied),
            payload,
        )))
    }
}

fn canonical_patch_output(
    report: ExecutionReport,
    history_bearing: bool,
) -> Result<Box<dyn ToolOutput>, ToolError> {
    let status = report.status;
    let parity = crate::apply_patch::project_apply_patch_outcome(&report.into_outcome());
    debug_assert_eq!(parity.history_bearing, history_bearing);
    let payload = serde_json::to_value(&parity).map_err(|error| {
        ToolError::internal(format!(
            "failed to serialize apply_patch projection: {error}"
        ))
    })?;
    let body = serde_json::to_string_pretty(&payload).map_err(|error| {
        ToolError::internal(format!("failed to serialize apply_patch result: {error}"))
    })?;
    Ok(Box::new(FunctionToolOutput::with_payload(
        body,
        matches!(status, crate::apply_patch::ExecutionStatus::Applied),
        payload,
    )))
}

struct PatchOperationMetricGuard {
    runtime: &'static str,
    profile: &'static str,
    authority: &'static str,
    outcome: &'static str,
    failed_stage: Option<&'static str>,
    error_code: Option<&'static str>,
    tracking: &'static str,
    exact: bool,
    committed_files: u64,
    committed_hunks: u64,
    committed_bytes: u64,
    started: Instant,
}

impl PatchOperationMetricGuard {
    fn new(source: PatchRequestSource, profile: &'static str) -> Self {
        Self {
            runtime: runtime_name(source),
            profile,
            authority: authority_name(source),
            outcome: "rejected",
            failed_stage: Some("preflight"),
            error_code: Some("invalid_request"),
            tracking: "not_applicable",
            exact: true,
            committed_files: 0,
            committed_hunks: 0,
            committed_bytes: 0,
            started: Instant::now(),
        }
    }

    fn set_report(&mut self, report: &crate::apply_patch::ExecutionReport, committed_hunks: u64) {
        self.outcome = execution_status_name(report.status);
        self.failed_stage = report
            .failure
            .as_ref()
            .map(|failure| stage_name(failure.stage));
        self.error_code = report
            .failure
            .as_ref()
            .map(|failure| error_code_name(failure.code));
        self.exact = report.delta.exact;
        self.committed_files = report.delta.changes.len().try_into().unwrap_or(u64::MAX);
        self.committed_hunks = committed_hunks;
        self.committed_bytes = report
            .delta
            .changes
            .iter()
            .flat_map(|change| {
                [
                    change.before.as_ref(),
                    change.after.as_ref(),
                    change.overwritten_destination.as_ref(),
                ]
            })
            .flatten()
            .fold(0_u64, |total, snapshot| {
                total.saturating_add(snapshot.bytes.len().try_into().unwrap_or(u64::MAX))
            });
    }
}

impl Drop for PatchOperationMetricGuard {
    fn drop(&mut self) {
        record_patch_operation(PatchOperationMetric {
            runtime: self.runtime,
            profile: self.profile,
            authority: self.authority,
            outcome: self.outcome,
            failed_stage: self.failed_stage,
            error_code: self.error_code,
            tracking: self.tracking,
            exact: self.exact,
            committed_files: self.committed_files,
            committed_hunks: self.committed_hunks,
            committed_bytes: self.committed_bytes,
            elapsed: self.started.elapsed(),
        });
    }
}

fn committed_hunk_count(
    report: &crate::apply_patch::ExecutionReport,
    authorized: &crate::apply_patch::AuthorizedPatch,
) -> u64 {
    report.delta.changes.iter().fold(0_u64, |total, change| {
        let hunks = authorized
            .prepared()
            .document
            .operations
            .get(change.operation_index as usize)
            .map(|operation| match &operation.operation.body {
                crate::apply_patch::OperationBody::Update(update) => update.hunks.len() as u64,
                crate::apply_patch::OperationBody::Add(_)
                | crate::apply_patch::OperationBody::Replace(_)
                | crate::apply_patch::OperationBody::Delete => 1,
            })
            .unwrap_or(1);
        total.saturating_add(hunks)
    })
}

fn runtime_name(source: PatchRequestSource) -> &'static str {
    match source {
        PatchRequestSource::NativeFreeform => "native_freeform",
        PatchRequestSource::NativeFunction => "native_function",
        PatchRequestSource::ManagedClaude => "managed_claude",
    }
}

fn execution_status_name(status: crate::apply_patch::ExecutionStatus) -> &'static str {
    match status {
        crate::apply_patch::ExecutionStatus::Applied => "applied",
        crate::apply_patch::ExecutionStatus::Partial => "partial",
        crate::apply_patch::ExecutionStatus::Rejected => "rejected",
        crate::apply_patch::ExecutionStatus::Failed => "failed",
        crate::apply_patch::ExecutionStatus::CommitStateUncertain => "commit_state_uncertain",
    }
}

fn stage_name(stage: PatchStage) -> &'static str {
    match stage {
        PatchStage::Normalize => "normalize",
        PatchStage::Parse => "parse",
        PatchStage::Resolve => "resolve",
        PatchStage::Authorize => "authorize",
        PatchStage::Prepare => "prepare",
        PatchStage::Lock => "lock",
        PatchStage::Stage => "stage",
        PatchStage::Commit => "commit",
        PatchStage::Record => "record",
        PatchStage::Recover => "recover",
    }
}

fn error_code_name(code: PatchErrorCode) -> &'static str {
    match code {
        PatchErrorCode::InvalidLimits => "invalid_limits",
        PatchErrorCode::InvalidPayload => "invalid_payload",
        PatchErrorCode::PatchSyntaxError => "patch_syntax_error",
        PatchErrorCode::PatchEmpty => "patch_empty",
        PatchErrorCode::InputTooLarge => "input_too_large",
        PatchErrorCode::InvalidVersionToken => "invalid_version_token",
        PatchErrorCode::InvalidRequest => "invalid_request",
        PatchErrorCode::TooManyOperations => "too_many_operations",
        PatchErrorCode::TooManyFiles => "too_many_files",
        PatchErrorCode::TooManyHunks => "too_many_hunks",
        PatchErrorCode::InvalidPath => "invalid_path",
        PatchErrorCode::PathOutsideAllowedRoot => "path_outside_allowed_root",
        PatchErrorCode::UnauthorizedPath => "unauthorized_path",
        PatchErrorCode::PermissionDenied => "permission_denied",
        PatchErrorCode::ContextNotFound => "context_not_found",
        PatchErrorCode::AmbiguousContext => "ambiguous_context",
        PatchErrorCode::PreconditionRequired => "precondition_required",
        PatchErrorCode::SourceMissing => "source_missing",
        PatchErrorCode::DestinationExists => "destination_exists",
        PatchErrorCode::DestinationMissing => "destination_missing",
        PatchErrorCode::StaleFile => "stale_file",
        PatchErrorCode::CrossDeviceMove => "cross_device_move",
        PatchErrorCode::UnsupportedFileType => "unsupported_file_type",
        PatchErrorCode::InvalidUtf8 => "invalid_utf8",
        PatchErrorCode::FileTooLarge => "file_too_large",
        PatchErrorCode::UnsupportedContent => "unsupported_content",
        PatchErrorCode::LockTimeout => "lock_timeout",
        PatchErrorCode::IoCreateFailed => "io_create_failed",
        PatchErrorCode::IoWriteFailed => "io_write_failed",
        PatchErrorCode::IoSyncFailed => "io_sync_failed",
        PatchErrorCode::IoRenameFailed => "io_rename_failed",
        PatchErrorCode::IoDeleteFailed => "io_delete_failed",
        PatchErrorCode::Io => "io",
        PatchErrorCode::PartialCommit => "partial_commit",
        PatchErrorCode::CommitStateUncertain => "commit_state_uncertain",
        PatchErrorCode::TrackerPublishFailed => "tracker_publish_failed",
        PatchErrorCode::HistoryCapacity => "history_capacity",
    }
}

fn tracking_for_record(
    record: &crate::apply_patch::history::AppliedPatchRecord,
    aggregate_revision: Option<u64>,
) -> &'static str {
    match &record.outcome {
        crate::apply_patch::history::AppliedPatchRecordOutcome::Applied
        | crate::apply_patch::history::AppliedPatchRecordOutcome::Partial { .. } => {
            if aggregate_revision.is_some() {
                "recorded_and_projected"
            } else {
                "recorded_projection_pending"
            }
        }
        crate::apply_patch::history::AppliedPatchRecordOutcome::CommitStateUncertain
        | crate::apply_patch::history::AppliedPatchRecordOutcome::Gap { .. } => "incomplete",
    }
}

fn tracking_without_record(
    status: crate::apply_patch::ExecutionStatus,
    history_bearing: bool,
) -> &'static str {
    if history_bearing
        || matches!(
            status,
            crate::apply_patch::ExecutionStatus::CommitStateUncertain
        )
    {
        "incomplete"
    } else {
        "not_applicable"
    }
}

struct ToolCancellation(CancellationToken);

impl crate::apply_patch::Cancellation for ToolCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

pub(crate) fn extract_patch_input(payload: &ToolPayload) -> Result<&str, ToolError> {
    match payload {
        ToolPayload::Custom { input } => Ok(input.as_str()),
        ToolPayload::Function { arguments } => extract_patch_from_json(arguments),
        ToolPayload::Mcp { .. } => Err(ToolError::invalid_arguments(
            "apply_patch does not accept MCP envelopes in this adapter",
        )),
        ToolPayload::LocalShell(_) => Err(ToolError::invalid_arguments(
            "apply_patch does not accept shell payloads",
        )),
        ToolPayload::ToolSearch { .. } => Err(ToolError::invalid_arguments(
            "apply_patch does not accept tool-search payloads",
        )),
    }
}

fn extract_patch_from_json(value: &JsonValue) -> Result<&str, ToolError> {
    let object = value.as_object().ok_or_else(|| {
        ToolError::invalid_arguments("apply_patch expects exactly one `patch` string property")
    })?;
    if object.len() != 1 || !object.contains_key("patch") {
        return Err(ToolError::invalid_arguments(
            "apply_patch expects exactly one `patch` string property",
        ));
    }
    object
        .get("patch")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            ToolError::invalid_arguments("apply_patch expects exactly one `patch` string property")
        })
}

fn native_tracking_for_record(
    record: &crate::apply_patch::history::AppliedPatchRecord,
    authority: &str,
    aggregate_revision: Option<u64>,
) -> crate::apply_patch::NativePatchTracking {
    use crate::apply_patch::{NativePatchTracking, NativePatchTrackingStatus};

    let status = match &record.outcome {
        crate::apply_patch::history::AppliedPatchRecordOutcome::Applied
        | crate::apply_patch::history::AppliedPatchRecordOutcome::Partial { .. } => {
            if aggregate_revision.is_some() {
                NativePatchTrackingStatus::RecordedAndProjected
            } else {
                NativePatchTrackingStatus::RecordedProjectionPending
            }
        }
        crate::apply_patch::history::AppliedPatchRecordOutcome::CommitStateUncertain
        | crate::apply_patch::history::AppliedPatchRecordOutcome::Gap { .. } => {
            NativePatchTrackingStatus::Incomplete
        }
    };
    NativePatchTracking {
        status,
        authority: authority.to_owned(),
        record_id: Some(crate::apply_patch::history::applied_patch_record_id(
            &record.identity,
            record.commit_ordinal.0,
        )),
        commit_ordinal: Some(record.commit_ordinal.0),
        aggregate_revision,
    }
}

fn native_tracking_without_record(
    status: crate::apply_patch::ExecutionStatus,
    history_bearing: bool,
    authority: &str,
) -> crate::apply_patch::NativePatchTracking {
    use crate::apply_patch::{NativePatchTracking, NativePatchTrackingStatus};

    NativePatchTracking {
        status: if history_bearing
            || matches!(
                status,
                crate::apply_patch::ExecutionStatus::CommitStateUncertain
            ) {
            NativePatchTrackingStatus::Incomplete
        } else {
            NativePatchTrackingStatus::NotApplicable
        },
        authority: authority.to_owned(),
        record_id: None,
        commit_ordinal: None,
        aggregate_revision: None,
    }
}

fn authority_name(source: PatchRequestSource) -> &'static str {
    match source {
        PatchRequestSource::ManagedClaude => "managed_claude_patch_engine",
        PatchRequestSource::NativeFreeform | PatchRequestSource::NativeFunction => {
            "native_patch_engine"
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PatchTarget {
    operation: FilePolicyOperation,
    path: String,
    parent: bool,
}

enum PatchTargetPolicyError {
    MissingSecuritySnapshot,
    Denied { path: String, message: String },
}

impl PatchTargetPolicyError {
    fn into_report(self) -> Result<ExecutionReport, ToolError> {
        match self {
            Self::MissingSecuritySnapshot => Err(ToolError::Rejected(
                "apply_patch execution security snapshot is missing; refusing filesystem mutation"
                    .to_owned(),
            )),
            Self::Denied { path, message } => {
                let mut error = PatchError::new(
                    PatchStage::Authorize,
                    PatchErrorCode::PermissionDenied,
                    message,
                    Retryability::Never,
                );
                error.diagnostic.path = Some(path);
                Ok(ExecutionReport::rejected_patch_error(&error))
            }
        }
    }
}

fn enforce_patch_targets(
    snapshot: Option<&pioneer_protocol::TurnExecutionSecuritySnapshot>,
    workdir: &Path,
    targets: &[PatchTarget],
) -> Result<(), PatchTargetPolicyError> {
    let Some(snapshot) = snapshot else {
        return Err(PatchTargetPolicyError::MissingSecuritySnapshot);
    };

    // Report a denied model-declared source/destination before an auxiliary
    // parent authorization target. The complete manifest is still checked
    // before preparation or mutation, but the canonical diagnostic points at
    // the actionable patch path instead of `.` for a root-level add.
    for target in targets
        .iter()
        .filter(|target| !target.parent)
        .chain(targets.iter().filter(|target| target.parent))
    {
        let requested_path = resolve_patch_target_path(workdir, target.path.as_str());
        match FilePolicyChecker::check(snapshot, target.operation, requested_path.as_path()) {
            FilePolicyDecision::Allowed(_) => {}
            FilePolicyDecision::Denied(deny) => {
                return Err(PatchTargetPolicyError::Denied {
                    path: target.path.clone(),
                    message: format!(
                        "filesystem sandbox denied {:?} for patch target `{}`: {}",
                        deny.operation,
                        safe_display_path(workdir, deny.requested_path.as_path()),
                        deny.message
                    ),
                });
            }
        }
    }
    Ok(())
}

fn safe_display_path(workdir: &Path, path: &Path) -> String {
    let root = workdir
        .canonicalize()
        .unwrap_or_else(|_| workdir.to_path_buf());
    let candidate = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    candidate
        .strip_prefix(root.as_path())
        .ok()
        .map(|relative| {
            if relative.as_os_str().is_empty() {
                ".".to_owned()
            } else {
                relative.to_string_lossy().replace('\\', "/")
            }
        })
        .unwrap_or_else(|| "<outside-workspace>".to_owned())
}

fn resolve_patch_target_path(workdir: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    let path = if path.is_absolute() {
        path
    } else {
        workdir.join(path)
    };
    normalize_path_lexically(path)
}

fn normalize_path_lexically(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !matches!(
                    normalized.components().next_back(),
                    Some(Component::RootDir | Component::Prefix(_))
                ) {
                    normalized.pop();
                }
            }
            Component::Normal(value) => normalized.push(value),
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::{DurableCommitObserver, InMemoryCommitObserver};
    use crate::context::ToolCallSource;
    use crate::permissions::extract_permission_intent_with_preflight;
    use pioneer_protocol::{
        TurnExecutionSecuritySnapshot, TurnFilesystemAccess, TurnFilesystemSandboxEntry,
        TurnPermissionMode, TurnPermissionProfileSnapshot, TurnPermissionProfileSource,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn invocation(root: &Path, patch: &str) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_patch_binding".to_owned(),
            tool_name: "apply_patch".to_owned(),
            source: ToolCallSource::Model,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({ "patch": patch }),
            },
            workdir: root.to_path_buf(),
            environment: BTreeMap::new(),
            attempt_id: 1,
            idempotency_key: Some("call_patch_binding".to_owned()),
            recovery: crate::spec::ToolRecoveryMetadata::default(),
            permission_metadata: crate::spec::ToolPermissionMetadata::default(),
            execution_security_snapshot: Some(
                pioneer_protocol::TurnExecutionSecuritySnapshot::unrestricted_full_access(
                    root.to_string_lossy(),
                    1,
                ),
            ),
            apply_patch_preflight: None,
            cancellation: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn native_handler_executes_with_empty_path_and_persists_record() {
        let root = tempfile::tempdir().unwrap();
        let patch =
            "*** Begin Patch\n*** Add File: created.txt\n+created in process\n*** End Patch";
        let mut invocation = invocation(root.path(), patch);
        invocation
            .environment
            .insert("PATH".to_owned(), String::new());
        let (_, preflight) = extract_permission_intent_with_preflight(&invocation);
        invocation.apply_patch_preflight = preflight;

        let identity =
            InvocationIdentity::new("thread_empty_path", "turn_empty_path", "call_empty_path")
                .unwrap();
        let observer = Arc::new(DurableCommitObserver::default());
        assert!(crate::events::register_native_patch_observer(
            &identity,
            observer.clone()
        ));
        let trace = crate::events::ToolEventBus::with_thread_id(4, "thread_empty_path")
            .start_trace("turn_empty_path", "call_empty_path", "apply_patch");

        let result = ApplyPatchHandler.handle(invocation, trace).await;
        crate::events::unregister_native_patch_observer(&identity);

        let output = result.expect("native in-process handler should not depend on PATH");
        let payload = output.raw_json();
        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["status"], "applied");
        assert!(payload.get("stdout").is_none());
        assert!(payload.get("stderr").is_none());
        assert!(payload.get("exit_code").is_none());
        assert_eq!(
            std::fs::read_to_string(root.path().join("created.txt")).unwrap(),
            "created in process"
        );
        let (record, committed) = observer
            .record(&identity)
            .expect("observer lookup")
            .expect("native handler must publish one durable record");
        assert_eq!(record.record.changes.len(), 1);
        assert_eq!(committed.len(), 1);
    }

    #[tokio::test]
    async fn observed_stale_guard_returns_canonical_v1_rejection_without_current_token() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        std::fs::write(&path, b"current private bytes\n").unwrap();
        let stale = crate::apply_patch::file_mutation::FileVersionToken::from_bytes(
            b"model saw old bytes\n",
        );
        let current = crate::apply_patch::file_mutation::FileVersionToken::from_bytes(
            b"current private bytes\n",
        );
        let patch = format!(
            "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {stale}\n+replacement\n*** End Patch"
        );
        let mut invocation = invocation(root.path(), &patch);
        let (_, preflight) = extract_permission_intent_with_preflight(&invocation);
        invocation.apply_patch_preflight = preflight;
        let identity = InvocationIdentity::new("thread", "turn", "call_stale").unwrap();
        let observer = InMemoryCommitObserver::new();
        let trace =
            crate::events::ToolEventBus::default().start_trace("turn", "call_stale", "apply_patch");

        let output = ApplyPatchHandler
            .handle_with_source_and_observer(
                invocation,
                trace,
                PatchRequestSource::ManagedClaude,
                &identity,
                &observer,
            )
            .await
            .expect("observed stale state is a typed tool result, not a transport failure");
        let payload = output.raw_json();

        assert!(!output.success());
        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["status"], "rejected");
        assert_eq!(payload["exact"], true);
        assert_eq!(payload["changed_files"], serde_json::json!([]));
        assert_eq!(payload["error"]["code"], "stale_file");
        assert_eq!(payload["error"]["operation_index"], 0);
        assert_eq!(payload["error"]["path"], "file.txt");
        assert_eq!(payload["error"]["guard_horizon"], "observed");
        let rendered = output.raw_text();
        assert!(!rendered.contains("current private bytes"));
        assert!(!rendered.contains(&current.to_string()));
        assert!(!rendered.contains("replacement version"));
        assert_eq!(std::fs::read(path).unwrap(), b"current private bytes\n");
        assert!(observer.record(&identity).unwrap().is_none());
    }

    #[tokio::test]
    async fn invalid_preflight_inputs_return_exact_canonical_v1_rejections() {
        let cases = [
            (serde_json::json!({ "patch": "  " }), "patch_empty"),
            (
                serde_json::json!({ "patch": "not a patch envelope" }),
                "patch_syntax_error",
            ),
            (
                serde_json::json!({ "input": "*** Begin Patch\n*** End Patch" }),
                "invalid_payload",
            ),
        ];

        for (index, (arguments, expected_code)) in cases.into_iter().enumerate() {
            let root = tempfile::tempdir().unwrap();
            let mut invocation = invocation(root.path(), "placeholder");
            invocation.call_id = format!("call_invalid_{index}");
            invocation.payload = ToolPayload::Function { arguments };
            let (_, preflight) = extract_permission_intent_with_preflight(&invocation);
            assert!(matches!(
                preflight,
                Some(crate::context::ApplyPatchPreflight::Rejected(_))
            ));
            invocation.apply_patch_preflight = preflight;
            let identity = InvocationIdentity::new(
                "thread_invalid",
                "turn_invalid",
                format!("call_invalid_{index}"),
            )
            .unwrap();
            let observer = InMemoryCommitObserver::new();
            let trace = crate::events::ToolEventBus::default().start_trace(
                "turn_invalid",
                format!("call_invalid_{index}"),
                "apply_patch",
            );

            let output = ApplyPatchHandler
                .handle_with_source_and_observer(
                    invocation,
                    trace,
                    PatchRequestSource::ManagedClaude,
                    &identity,
                    &observer,
                )
                .await
                .expect("invalid patch input should remain a typed tool rejection");
            let payload = output.raw_json();
            assert!(!output.success());
            assert_eq!(payload["schema_version"], 1);
            assert_eq!(payload["status"], "rejected");
            assert_eq!(payload["exact"], true);
            assert_eq!(payload["changed_files"], serde_json::json!([]));
            assert_eq!(payload["error"]["code"], expected_code);
            assert!(observer.record(&identity).unwrap().is_none());
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        }
    }

    #[tokio::test]
    async fn traversal_and_invalid_utf8_use_stable_v1_error_codes() {
        let traversal_root = tempfile::tempdir().unwrap();
        let traversal_patch =
            "*** Begin Patch\n*** Add File: ../escape.txt\n+escape\n*** End Patch";
        let mut traversal = invocation(traversal_root.path(), traversal_patch);
        let (_, preflight) = extract_permission_intent_with_preflight(&traversal);
        traversal.apply_patch_preflight = preflight;
        let traversal_identity =
            InvocationIdentity::new("thread_codes", "turn_codes", "call_traversal").unwrap();
        let traversal_observer = InMemoryCommitObserver::new();
        let traversal_trace = crate::events::ToolEventBus::default().start_trace(
            "turn_codes",
            "call_traversal",
            "apply_patch",
        );
        let traversal_output = ApplyPatchHandler
            .handle_with_source_and_observer(
                traversal,
                traversal_trace,
                PatchRequestSource::ManagedClaude,
                &traversal_identity,
                &traversal_observer,
            )
            .await
            .expect("path escape must be a typed tool rejection");
        assert_eq!(
            traversal_output.raw_json()["error"]["code"],
            "path_outside_allowed_root"
        );
        assert_eq!(traversal_output.raw_json()["schema_version"], 1);
        assert_eq!(traversal_output.raw_json()["exact"], true);
        assert!(
            traversal_observer
                .record(&traversal_identity)
                .unwrap()
                .is_none()
        );

        let utf8_root = tempfile::tempdir().unwrap();
        let invalid_bytes = [0xff, 0xfe, b'\n'];
        std::fs::write(utf8_root.path().join("invalid.txt"), invalid_bytes).unwrap();
        let token = crate::apply_patch::file_mutation::FileVersionToken::from_bytes(&invalid_bytes);
        let utf8_patch = format!(
            "*** Begin Patch\n*** Replace File: invalid.txt\n*** If-Match: {token}\n+replacement\n*** End Patch"
        );
        let mut utf8 = invocation(utf8_root.path(), &utf8_patch);
        utf8.call_id = "call_invalid_utf8".to_owned();
        utf8.idempotency_key = Some("call_invalid_utf8".to_owned());
        let (_, preflight) = extract_permission_intent_with_preflight(&utf8);
        utf8.apply_patch_preflight = preflight;
        let utf8_identity =
            InvocationIdentity::new("thread_codes", "turn_codes", "call_invalid_utf8").unwrap();
        let utf8_observer = InMemoryCommitObserver::new();
        let utf8_trace = crate::events::ToolEventBus::default().start_trace(
            "turn_codes",
            "call_invalid_utf8",
            "apply_patch",
        );
        let utf8_output = ApplyPatchHandler
            .handle_with_source_and_observer(
                utf8,
                utf8_trace,
                PatchRequestSource::ManagedClaude,
                &utf8_identity,
                &utf8_observer,
            )
            .await
            .expect("invalid UTF-8 must be a typed tool rejection");
        assert_eq!(utf8_output.raw_json()["error"]["code"], "invalid_utf8");
        assert_eq!(utf8_output.raw_json()["schema_version"], 1);
        assert_eq!(utf8_output.raw_json()["exact"], true);
        assert_eq!(
            std::fs::read(utf8_root.path().join("invalid.txt")).unwrap(),
            invalid_bytes
        );
        assert!(utf8_observer.record(&utf8_identity).unwrap().is_none());
    }

    #[tokio::test]
    async fn handler_rejects_payload_changed_after_permission_preflight() {
        let root = tempfile::tempdir().unwrap();
        let approved = "*** Begin Patch\n*** Add File: approved.txt\n+approved\n*** End Patch";
        let changed = "*** Begin Patch\n*** Add File: changed.txt\n+changed\n*** End Patch";
        let mut invocation = invocation(root.path(), approved);
        let (_, preflight) = extract_permission_intent_with_preflight(&invocation);
        invocation.apply_patch_preflight = preflight;
        invocation.payload = ToolPayload::Function {
            arguments: serde_json::json!({ "patch": changed }),
        };
        let identity = InvocationIdentity::new("thread", "turn", "call_patch_binding").unwrap();
        let observer = InMemoryCommitObserver::new();
        let trace = crate::events::ToolEventBus::default().start_trace(
            "turn",
            "call_patch_binding",
            "apply_patch",
        );

        let result = ApplyPatchHandler
            .handle_with_source_and_observer(
                invocation,
                trace,
                PatchRequestSource::ManagedClaude,
                &identity,
                &observer,
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("changed payload must be rejected"),
        };

        assert!(
            error
                .to_string()
                .contains("changed after permission preflight")
        );
        assert!(!root.path().join("approved.txt").exists());
        assert!(!root.path().join("changed.txt").exists());
    }

    #[tokio::test]
    async fn handler_fails_closed_without_execution_security_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\n*** Add File: denied.txt\n+denied\n*** End Patch";
        let mut invocation = invocation(root.path(), patch);
        let (_, preflight) = extract_permission_intent_with_preflight(&invocation);
        invocation.apply_patch_preflight = preflight;
        invocation.execution_security_snapshot = None;
        let identity = InvocationIdentity::new("thread", "turn", "call_patch_binding").unwrap();
        let observer = InMemoryCommitObserver::new();
        let trace = crate::events::ToolEventBus::default().start_trace(
            "turn",
            "call_patch_binding",
            "apply_patch",
        );

        let result = ApplyPatchHandler
            .handle_with_source_and_observer(
                invocation,
                trace,
                PatchRequestSource::ManagedClaude,
                &identity,
                &observer,
            )
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("missing security snapshot must be rejected"),
        };

        assert!(error.to_string().contains("security snapshot is missing"));
        assert!(!root.path().join("denied.txt").exists());
    }

    #[tokio::test]
    async fn sandbox_denial_is_an_exact_canonical_v1_rejection() {
        let root = tempfile::tempdir().unwrap();
        let allowed_root = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\n*** Add File: denied.txt\n+denied\n*** End Patch";
        let mut invocation = invocation(root.path(), patch);
        let (_, preflight) = extract_permission_intent_with_preflight(&invocation);
        invocation.apply_patch_preflight = preflight;
        invocation.execution_security_snapshot =
            Some(TurnExecutionSecuritySnapshot::workspace_write(
                TurnPermissionProfileSnapshot::from_mode(
                    TurnPermissionMode::AutoAcceptEdits,
                    TurnPermissionProfileSource::Composer,
                ),
                root.path().to_string_lossy(),
                vec![TurnFilesystemSandboxEntry::workspace_root(
                    TurnFilesystemAccess::Write,
                    allowed_root.path().to_string_lossy(),
                )],
                1,
            ));
        let identity =
            InvocationIdentity::new("thread_policy", "turn_policy", "call_patch_binding").unwrap();
        let observer = InMemoryCommitObserver::new();
        let trace = crate::events::ToolEventBus::default().start_trace(
            "turn_policy",
            "call_patch_binding",
            "apply_patch",
        );

        let output = ApplyPatchHandler
            .handle_with_source_and_observer(
                invocation,
                trace,
                PatchRequestSource::ManagedClaude,
                &identity,
                &observer,
            )
            .await
            .expect("sandbox denial must remain a typed tool rejection");
        let payload = output.raw_json();

        assert!(!output.success());
        assert_eq!(payload["schema_version"], 1);
        assert_eq!(payload["status"], "rejected");
        assert_eq!(payload["exact"], true);
        assert_eq!(payload["changed_files"], serde_json::json!([]));
        assert_eq!(payload["error"]["stage"], "authorize");
        assert_eq!(payload["error"]["code"], "permission_denied");
        assert_eq!(payload["error"]["path"], "denied.txt");
        assert!(!root.path().join("denied.txt").exists());
        assert!(observer.record(&identity).unwrap().is_none());
    }
}
