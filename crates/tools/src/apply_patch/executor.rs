use crate::apply_patch::file_mutation::{
    CasExpectation, FileMutationEngine, FileVersionToken, GuardHorizon, MutationChange,
    MutationError, MutationErrorCode, MutationOutcome, MutationSideEffects, PatchDiagnostic,
    PatchError, PatchErrorCode, PatchStage, PreparedFileStage, Retryability, SnapshotErrorCode,
    StageMetadata, TargetKind, TargetLockGuard, TargetResolutionErrorCode, TextSnapshot,
    supported_mode, version_on_disk,
};
use crate::apply_patch::history::{
    AppliedPatchDelta, ApplyPatchOutcome, ChangeKind, CommitOrdinal, CommittedPatchChange,
    CommittedTextSnapshot, InvocationIdentity, LineEnding, LineEndingMetadata, PatchSideEffects,
    TextEncoding,
};
use crate::apply_patch::observer::{
    CommitAdmission, CommitObserver, ObserverAdmission, ObserverError,
};
use crate::apply_patch::{
    AuthorizedPatch, GuardError, GuardErrorCode, ParseError, ParseErrorCode, PlanError,
    PlanErrorCode, PlannedChange, PlannedPatch, PrepareError, PrepareErrorCode, PreparedPatch,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use std::time::Instant;

use crate::apply_patch::telemetry::{PatchTelemetry, TelemetryStage};

static GLOBAL_PATCH_TELEMETRY: OnceLock<Arc<PatchTelemetry>> = OnceLock::new();

fn patch_telemetry_snapshot() -> pioneer_observability::PatchTelemetrySnapshot {
    GLOBAL_PATCH_TELEMETRY
        .get_or_init(|| Arc::new(PatchTelemetry::default()))
        .snapshot()
}

pub fn patch_telemetry() -> Arc<PatchTelemetry> {
    let telemetry = GLOBAL_PATCH_TELEMETRY.get_or_init(|| Arc::new(PatchTelemetry::default()));
    pioneer_observability::register_patch_telemetry_snapshot_provider(patch_telemetry_snapshot);
    telemetry.clone()
}

pub trait Cancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NeverCancel;

impl Cancellation for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecuteOptions {
    pub lock_timeout: Duration,
}

impl Default for ExecuteOptions {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(2),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Applied,
    Partial,
    Rejected,
    Failed,
    CommitStateUncertain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub status: ExecutionStatus,
    pub delta: AppliedPatchDelta,
    pub failure: Option<PatchDiagnostic>,
}

impl ExecutionReport {
    pub fn rejected_patch_error(error: &PatchError) -> Self {
        Self {
            status: ExecutionStatus::Rejected,
            delta: AppliedPatchDelta::empty(),
            failure: Some(error.diagnostic.clone()),
        }
    }

    pub fn rejected_parse_error(error: &ParseError) -> Self {
        let code = match error.code {
            ParseErrorCode::EmptyInput => PatchErrorCode::PatchEmpty,
            ParseErrorCode::InputTooLarge => PatchErrorCode::InputTooLarge,
            ParseErrorCode::TooManyOperations => PatchErrorCode::TooManyOperations,
            ParseErrorCode::TooManyChunks | ParseErrorCode::TooManyHunks => {
                PatchErrorCode::TooManyHunks
            }
            ParseErrorCode::InvalidPath | ParseErrorCode::PathTooLong => {
                PatchErrorCode::InvalidPath
            }
            ParseErrorCode::MissingBegin
            | ParseErrorCode::MissingEnd
            | ParseErrorCode::TrailingContent
            | ParseErrorCode::UnknownDirective
            | ParseErrorCode::MissingPath
            | ParseErrorCode::InvalidOperationBody
            | ParseErrorCode::MissingHunk
            | ParseErrorCode::InvalidHunkLine
            | ParseErrorCode::EmptyAdd
            | ParseErrorCode::EmptyReplace => PatchErrorCode::PatchSyntaxError,
        };
        Self {
            status: ExecutionStatus::Rejected,
            delta: AppliedPatchDelta::empty(),
            failure: Some(diagnostic_at(
                PatchStage::Parse,
                code,
                &error.to_string(),
                None,
                None,
                None,
            )),
        }
    }

    pub fn rejected_guard_error(error: &GuardError) -> Self {
        let code = match error.code {
            GuardErrorCode::MissingRequiredSourceGuard => PatchErrorCode::PreconditionRequired,
            GuardErrorCode::InvalidSourceGuard | GuardErrorCode::InvalidDestinationGuard => {
                PatchErrorCode::InvalidVersionToken
            }
            GuardErrorCode::InapplicableGuard | GuardErrorCode::DuplicateGuard => {
                PatchErrorCode::InvalidPayload
            }
        };
        Self {
            status: ExecutionStatus::Rejected,
            delta: AppliedPatchDelta::empty(),
            failure: Some(diagnostic_at(
                PatchStage::Parse,
                code,
                &error.to_string(),
                Some(error.operation_index.try_into().unwrap_or(u32::MAX)),
                None,
                None,
            )),
        }
    }

    pub fn rejected_resolve_error(error: &PrepareError) -> Self {
        let code = prepare_error_code(error);
        Self {
            status: ExecutionStatus::Rejected,
            delta: AppliedPatchDelta::empty(),
            failure: Some(diagnostic_at(
                PatchStage::Resolve,
                code,
                &error.to_string(),
                Some(error.operation_index.try_into().unwrap_or(u32::MAX)),
                (!error.path.is_empty()).then(|| error.path.clone()),
                None,
            )),
        }
    }

    /// Convert a post-authorization preparation failure into the same
    /// canonical typed rejection returned by the executor. In particular,
    /// observed guard failures remain distinguishable from under-lock
    /// prepared revalidation and commit-boundary CAS failures.
    pub fn rejected_prepare_error(error: &PrepareError) -> Self {
        prepare_rejection(error)
    }

    pub fn into_outcome(self) -> ApplyPatchOutcome {
        match (self.status, self.failure) {
            (ExecutionStatus::Applied, _) => ApplyPatchOutcome::Applied { delta: self.delta },
            (ExecutionStatus::Partial, Some(failure)) => ApplyPatchOutcome::Partial {
                delta: self.delta,
                failure,
            },
            (ExecutionStatus::Failed, Some(failure)) => ApplyPatchOutcome::Failed {
                delta: self.delta,
                failure,
            },
            (ExecutionStatus::CommitStateUncertain, Some(reason)) => {
                ApplyPatchOutcome::CommitStateUncertain {
                    delta: self.delta,
                    reason,
                }
            }
            (ExecutionStatus::Rejected, Some(failure)) => ApplyPatchOutcome::Rejected { failure },
            (ExecutionStatus::Partial | ExecutionStatus::Failed, None) => {
                ApplyPatchOutcome::Failed {
                    delta: self.delta,
                    failure: diagnostic(
                        PatchStage::Commit,
                        PatchErrorCode::Io,
                        "patch failed without a diagnostic",
                    ),
                }
            }
            (ExecutionStatus::CommitStateUncertain, None) => {
                ApplyPatchOutcome::CommitStateUncertain {
                    delta: self.delta,
                    reason: diagnostic(
                        PatchStage::Commit,
                        PatchErrorCode::CommitStateUncertain,
                        "filesystem commit state is uncertain",
                    ),
                }
            }
            (ExecutionStatus::Rejected, None) => ApplyPatchOutcome::Rejected {
                failure: diagnostic(
                    PatchStage::Authorize,
                    PatchErrorCode::InvalidRequest,
                    "patch was rejected",
                ),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct PatchExecutor {
    engine: FileMutationEngine,
    telemetry: Arc<PatchTelemetry>,
}

type ObserverContext<'a> = (
    &'a dyn CommitObserver,
    &'a InvocationIdentity,
    CommitOrdinal,
);

type PendingObserverContext<'a> = (
    &'a dyn CommitObserver,
    &'a InvocationIdentity,
    &'a CommitAdmission,
);

impl PatchExecutor {
    pub fn new(engine: FileMutationEngine) -> Self {
        Self {
            engine,
            telemetry: patch_telemetry(),
        }
    }

    pub fn with_telemetry(mut self, telemetry: Arc<PatchTelemetry>) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn engine(&self) -> &FileMutationEngine {
        &self.engine
    }

    /// Execute only an already authorized, immutable plan. The executor never
    /// reparses patch text and never shells out. A single complete lock set is
    /// held while prepared versions are revalidated and all ordered primitive
    /// calls run.
    pub fn execute<C: Cancellation>(
        &self,
        authorized: &AuthorizedPatch,
        options: ExecuteOptions,
        cancellation: &C,
    ) -> ExecutionReport {
        let started = Instant::now();
        let report = self.execute_inner(authorized, options, cancellation, None);
        self.telemetry.record_report(&report, started.elapsed());
        report
    }

    /// Execute with a trusted observer identity. Duplicate detection is a
    /// read-only preflight; ordinal reservation and durable intent admission
    /// happen only after the complete target lock set has been acquired and
    /// revalidated, immediately before the first filesystem mutation.
    pub fn execute_with_observer<C: Cancellation>(
        &self,
        authorized: &AuthorizedPatch,
        identity: &InvocationIdentity,
        observer: &dyn CommitObserver,
        options: ExecuteOptions,
        cancellation: &C,
    ) -> ExecutionReport {
        let admission = CommitAdmission::minimal(&authorized.prepared);
        self.execute_with_observer_and_admission(
            authorized,
            identity,
            observer,
            &admission,
            options,
            cancellation,
        )
    }

    pub fn execute_with_observer_and_admission<C: Cancellation>(
        &self,
        authorized: &AuthorizedPatch,
        identity: &InvocationIdentity,
        observer: &dyn CommitObserver,
        admission: &CommitAdmission,
        options: ExecuteOptions,
        cancellation: &C,
    ) -> ExecutionReport {
        let started = Instant::now();
        if admission.plan_fingerprint != authorized.prepared.fingerprint {
            let report = observer_rejection(ObserverError::new(
                crate::apply_patch::observer::ObserverErrorCode::DuplicateInvocation,
                "observer admission fingerprint does not match the authorized patch",
            ));
            self.telemetry.record_report(&report, started.elapsed());
            return report;
        }
        match observer.check(identity, admission.plan_fingerprint) {
            Ok(Some(report)) => {
                self.telemetry.record_duplicate_suppression();
                self.telemetry.record_report(&report, started.elapsed());
                return report;
            }
            Ok(None) => {}
            Err(error) => {
                if matches!(
                    error.code,
                    crate::apply_patch::observer::ObserverErrorCode::InFlight
                ) {
                    self.telemetry.record_duplicate_suppression();
                }
                let report = observer_rejection(error);
                self.telemetry.record_report(&report, started.elapsed());
                return report;
            }
        }
        let report = self.execute_inner(
            authorized,
            options,
            cancellation,
            Some((observer, identity, admission)),
        );
        self.telemetry.record_report(&report, started.elapsed());
        report
    }

    fn execute_inner<C: Cancellation>(
        &self,
        authorized: &AuthorizedPatch,
        options: ExecuteOptions,
        cancellation: &C,
        observer: Option<PendingObserverContext<'_>>,
    ) -> ExecutionReport {
        if cancellation.is_cancelled() {
            return rejected(
                PatchStage::Prepare,
                "patch cancelled before lock acquisition",
            );
        }
        let prepared = &authorized.prepared;
        let lock_started = Instant::now();
        let lock = match self
            .engine
            .lock_registry()
            .acquire(&prepared.target_manifest, options.lock_timeout)
        {
            Ok(lock) => lock,
            Err(_) => {
                self.telemetry
                    .record_stage_latency(TelemetryStage::Lock, lock_started.elapsed());
                return rejected(
                    PatchStage::Lock,
                    "could not acquire the complete target lock set",
                );
            }
        };
        self.telemetry
            .record_stage_latency(TelemetryStage::Lock, lock_started.elapsed());
        if cancellation.is_cancelled() {
            return rejected(
                PatchStage::Lock,
                "patch cancelled while waiting for the lock set",
            );
        }
        let planned = match self.revalidate_and_plan(prepared, &lock) {
            Ok(planned) => planned,
            Err(report) => return report,
        };

        // Admission is deliberately after lock acquisition and prepared
        // revalidation.  The observer reserves bounded durable capacity and a
        // stable ordinal here, never while waiting for a target lock.
        let observer = match observer {
            Some((observer, identity, admission)) => {
                let admission = match admission.for_planned(&planned) {
                    Ok(admission) => admission,
                    Err(error) => return observer_rejection(error),
                };
                let admission = match observer.admit(identity, &admission) {
                    Ok(admission) => admission,
                    Err(error) => return observer_rejection(error),
                };
                let ordinal = match admission {
                    ObserverAdmission::Existing { report } => {
                        self.telemetry.record_duplicate_suppression();
                        return report;
                    }
                    ObserverAdmission::Execute { ordinal } => ordinal,
                };
                Some((observer, identity, ordinal))
            }
            None => None,
        };

        let commit_started = Instant::now();
        let report = (|| {
            let targets = prepared
                .target_manifest
                .targets()
                .iter()
                .map(|target| {
                    (
                        target.relative().to_string_lossy().replace('\\', "/"),
                        target.clone(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            // Every replacement is written, metadata-adjusted and flushed to
            // a private same-directory stage before the first destination is
            // published. A later staging failure therefore cannot leave an
            // otherwise avoidable committed prefix.
            let mut stages = match self.prepare_all_stages(&planned, &targets, &lock) {
                Ok(stages) => stages,
                Err((operation_offset, error)) => {
                    let planned = &planned.operations[operation_offset];
                    let side_effects = patch_side_effects(&error.side_effects);
                    let mut delta = AppliedPatchDelta::empty();
                    delta.exact &= side_effects.exact;
                    merge_delta_side_effects(&mut delta, &side_effects);
                    return ExecutionReport {
                        status: ExecutionStatus::Failed,
                        delta,
                        failure: Some(mutation_diagnostic(PatchStage::Stage, planned, error)),
                    };
                }
            };
            let mut delta = AppliedPatchDelta::empty();
            for planned in &planned.operations {
                let stage = stages
                    .pop_front()
                    .expect("one prepared-stage slot exists per planned operation");
                if cancellation.is_cancelled() {
                    merge_aborted_stages_into_delta(
                        &mut delta,
                        stage.into_iter().chain(stages.into_iter().flatten()),
                    );
                    return partial_or_rejected(
                        delta,
                        PatchStage::Commit,
                        "patch cancelled at a safe commit boundary",
                    );
                }
                if planned_is_noop(planned) {
                    debug_assert!(stage.is_none());
                    continue;
                }
                let outcome = self.execute_change(planned, &targets, stage, &lock);
                match outcome {
                    MutationOutcome::Applied(change) => {
                        let side_effects = patch_side_effects(&change.side_effects);
                        if let Some(mut committed) = committed_change(planned, change) {
                            assign_commit_position(&mut committed, delta.changes.len());
                            merge_delta_side_effects(&mut delta, &side_effects);
                            if let Some(error) = notify_committed(observer, &committed) {
                                self.telemetry.record_tracker_publication_failure();
                                // The filesystem mutation is already committed even when
                                // progress publication fails. Keep the exact change in the
                                // returned delta so the terminal path cannot misclassify this
                                // invocation as an empty no-change failure. The observer's
                                // pending intent remains the recovery authority if its append
                                // did not become durable.
                                delta.changes.push(committed);
                                merge_aborted_stages_into_delta(
                                    &mut delta,
                                    stages.into_iter().flatten(),
                                );
                                return observer_failure(delta, error);
                            }
                            delta.changes.push(committed);
                        } else {
                            merge_delta_side_effects(&mut delta, &side_effects);
                        }
                    }
                    MutationOutcome::Failed {
                        mut error,
                        committed,
                    } => {
                        error.side_effects.merge(&abort_stages(stages));
                        let error_side_effects = patch_side_effects(&error.side_effects);
                        if let Some(change) = committed
                            && let Some(mut committed) = committed_change(planned, change)
                        {
                            assign_commit_position(&mut committed, delta.changes.len());
                            committed.side_effects.merge(&error_side_effects);
                            merge_delta_side_effects(&mut delta, &error_side_effects);
                            if let Some(error) = notify_committed(observer, &committed) {
                                self.telemetry.record_tracker_publication_failure();
                                delta.changes.push(committed);
                                return observer_failure(delta, error);
                            }
                            delta.changes.push(committed);
                        }
                        merge_delta_side_effects(&mut delta, &error_side_effects);
                        let failure = mutation_diagnostic(PatchStage::Commit, planned, error);
                        return if delta.is_empty() {
                            ExecutionReport {
                                status: ExecutionStatus::Failed,
                                delta,
                                failure: Some(failure),
                            }
                        } else {
                            ExecutionReport {
                                status: ExecutionStatus::Partial,
                                delta,
                                failure: Some(failure),
                            }
                        };
                    }
                    MutationOutcome::Uncertain {
                        mut error,
                        committed,
                    } => {
                        error.side_effects.merge(&abort_stages(stages));
                        let error_side_effects = patch_side_effects(&error.side_effects);
                        if let Some(change) = committed
                            && let Some(mut committed) = committed_change(planned, change)
                        {
                            assign_commit_position(&mut committed, delta.changes.len());
                            committed.side_effects.merge(&error_side_effects);
                            merge_delta_side_effects(&mut delta, &error_side_effects);
                            if let Some(error) = notify_committed(observer, &committed) {
                                self.telemetry.record_tracker_publication_failure();
                                delta.changes.push(committed);
                                return observer_failure(delta, error);
                            }
                            delta.changes.push(committed);
                        }
                        merge_delta_side_effects(&mut delta, &error_side_effects);
                        return ExecutionReport {
                            status: ExecutionStatus::CommitStateUncertain,
                            delta: delta.with_exactness(false),
                            failure: Some(mutation_diagnostic(PatchStage::Commit, planned, error)),
                        };
                    }
                }
            }
            ExecutionReport {
                status: ExecutionStatus::Applied,
                delta,
                failure: None,
            }
        })();
        self.telemetry
            .record_stage_latency(TelemetryStage::Commit, commit_started.elapsed());
        if let Some((observer, identity, ordinal)) = observer {
            let persist_started = Instant::now();
            let terminal = observer.on_terminal(identity, ordinal, &report);
            self.telemetry
                .record_stage_latency(TelemetryStage::Persist, persist_started.elapsed());
            if let Err(error) = terminal {
                self.telemetry.record_tracker_publication_failure();
                return observer_failure(report.delta, error);
            }
        }
        report
    }

    fn revalidate_and_plan<'a>(
        &self,
        prepared: &'a PreparedPatch,
        _lock: &TargetLockGuard,
    ) -> Result<Cow<'a, PlannedPatch>, ExecutionReport> {
        for (path, observed) in &prepared.observed_parents {
            let current = match observed.target.metadata_fingerprint() {
                Ok(fingerprint) => fingerprint,
                Err(_) => {
                    return Err(rejected(
                        PatchStage::Prepare,
                        "prepared parent target could not be revalidated",
                    ));
                }
            };
            if current != observed.fingerprint {
                return Err(ExecutionReport {
                    status: ExecutionStatus::Rejected,
                    delta: AppliedPatchDelta::empty(),
                    failure: Some(diagnostic_at(
                        PatchStage::Prepare,
                        PatchErrorCode::StaleFile,
                        "prepared parent directory changed before commit",
                        None,
                        Some(path.clone()),
                        Some(GuardHorizon::Prepared),
                    )),
                });
            }
        }
        let mut changed = false;
        for (_path, observed) in &prepared.observed {
            let current = match version_on_disk(&observed.target, prepared.snapshot_limits) {
                Ok(current) => current,
                Err(_) => {
                    return Err(rejected(
                        PatchStage::Prepare,
                        "prepared target could not be revalidated",
                    ));
                }
            };
            let expected = observed.state.as_ref().map(|state| state.token);
            if current != expected {
                changed = true;
            }
        }
        if !changed {
            return Ok(Cow::Borrowed(&prepared.planned));
        }

        // Ordinary Update operations are optimistic: when a file changed
        // after prepare, match the patch hunk against the current contents
        // while the complete lock set is held. This preserves unrelated
        // external edits and rejects overlapping/ambiguous context. Replace,
        // Delete and Move (and explicitly guarded Update) remain strict because
        // the planner rechecks their If-Match token against this same snapshot.
        let mut current_files = BTreeMap::new();
        let mut current_snapshot_bytes = 0u64;
        for (path, observed) in &prepared.observed {
            match observed.target.inspect_kind() {
                Ok(TargetKind::Missing) => {}
                Ok(TargetKind::RegularFile) => {
                    let snapshot = match TextSnapshot::from_file(
                        observed.target.absolute(),
                        prepared.snapshot_limits,
                    ) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            return Err(ExecutionReport {
                                status: ExecutionStatus::Rejected,
                                delta: AppliedPatchDelta::empty(),
                                failure: Some(diagnostic(
                                    PatchStage::Prepare,
                                    match error.code {
                                        crate::apply_patch::file_mutation::SnapshotErrorCode::BinaryContent
                                        | crate::apply_patch::file_mutation::SnapshotErrorCode::InvalidUtf8
                                        | crate::apply_patch::file_mutation::SnapshotErrorCode::TooLarge => {
                                            PatchErrorCode::UnsupportedContent
                                        }
                                        _ => PatchErrorCode::Io,
                                    },
                                    &format!(
                                        "current target could not be read for contextual replan: {path}"
                                    ),
                                )),
                            });
                        }
                    };
                    let snapshot_bytes = snapshot.version.token.byte_len();
                    let next_snapshot_bytes = match current_snapshot_bytes
                        .checked_add(snapshot_bytes)
                    {
                        Some(total) if total <= prepared.max_total_snapshot_bytes => total,
                        _ => {
                            return Err(ExecutionReport {
                                status: ExecutionStatus::Rejected,
                                delta: AppliedPatchDelta::empty(),
                                failure: Some(diagnostic(
                                    PatchStage::Prepare,
                                    PatchErrorCode::UnsupportedContent,
                                    "current target snapshots exceed the configured aggregate byte limit",
                                )),
                            });
                        }
                    };
                    let bytes = match snapshot.bytes() {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            return Err(ExecutionReport {
                                status: ExecutionStatus::Rejected,
                                delta: AppliedPatchDelta::empty(),
                                failure: Some(diagnostic(
                                    PatchStage::Prepare,
                                    match error.code {
                                        crate::apply_patch::file_mutation::SnapshotErrorCode::BinaryContent
                                        | crate::apply_patch::file_mutation::SnapshotErrorCode::InvalidUtf8
                                        | crate::apply_patch::file_mutation::SnapshotErrorCode::TooLarge => {
                                            PatchErrorCode::UnsupportedContent
                                        }
                                        _ => PatchErrorCode::Io,
                                    },
                                    &format!(
                                        "current target could not be decoded for contextual replan: {path}"
                                    ),
                                )),
                            });
                        }
                    };
                    debug_assert_eq!(bytes.len() as u64, snapshot_bytes);
                    current_snapshot_bytes = next_snapshot_bytes;
                    current_files.insert(path.clone(), bytes);
                }
                Ok(TargetKind::Directory | TargetKind::Symlink | TargetKind::Special) => {
                    return Err(ExecutionReport {
                        status: ExecutionStatus::Rejected,
                        delta: AppliedPatchDelta::empty(),
                        failure: Some(diagnostic(
                            PatchStage::Prepare,
                            PatchErrorCode::UnsupportedContent,
                            &format!("current target is not a regular file: {path}"),
                        )),
                    });
                }
                Err(_) => {
                    return Err(rejected(
                        PatchStage::Prepare,
                        "current target could not be inspected for contextual replan",
                    ));
                }
            }
        }
        crate::apply_patch::planner::plan_with_limits(
            &prepared.document,
            current_files,
            prepared.max_candidate_matches,
            prepared.max_total_output_bytes,
        )
        .map(Cow::Owned)
        .map_err(|error| plan_rejection(error, "patch no longer applies to current files"))
    }

    fn prepare_all_stages(
        &self,
        planned: &PlannedPatch,
        targets: &BTreeMap<String, crate::apply_patch::file_mutation::CanonicalTarget>,
        lock: &TargetLockGuard,
    ) -> Result<VecDeque<Option<PreparedFileStage>>, (usize, MutationError)> {
        let mut stages = VecDeque::with_capacity(planned.operations.len());
        let mut virtual_modes: BTreeMap<String, Option<u32>> = BTreeMap::new();

        for (offset, change) in planned.operations.iter().enumerate() {
            if planned_is_noop(change) {
                stages.push_back(None);
                continue;
            }
            let Some(source) = targets.get(&change.source).cloned() else {
                let mut error = synthetic_error(MutationErrorCode::TargetMissing);
                error.side_effects.merge(&abort_stages(stages));
                return Err((offset, error));
            };
            match change.kind {
                operation_kind::Delete => {
                    virtual_modes.remove(&change.source);
                    stages.push_back(None);
                }
                operation_kind::Add => {
                    let Some(after) = change.after.as_ref() else {
                        let mut error = synthetic_error(MutationErrorCode::Snapshot);
                        error.side_effects.merge(&abort_stages(stages));
                        return Err((offset, error));
                    };
                    let stage = match self.engine.prepare_file_stage_locked(
                        source,
                        after.bytes.clone(),
                        StageMetadata::SafeAdd,
                        lock,
                    ) {
                        Ok(stage) => stage,
                        Err(mut error) => {
                            error.side_effects.merge(&abort_stages(stages));
                            return Err((offset, error));
                        }
                    };
                    virtual_modes.insert(change.source.clone(), stage.resulting_mode());
                    stages.push_back(Some(stage));
                }
                operation_kind::Replace | operation_kind::Update => {
                    let mode =
                        match virtual_source_mode(&mut virtual_modes, &change.source, &source) {
                            Ok(mode) => mode,
                            Err(mut error) => {
                                error.side_effects.merge(&abort_stages(stages));
                                return Err((offset, error));
                            }
                        };
                    let Some(after) = change.after.as_ref() else {
                        let mut error = synthetic_error(MutationErrorCode::Snapshot);
                        error.side_effects.merge(&abort_stages(stages));
                        return Err((offset, error));
                    };
                    let (stage_target, next_path) = match &change.destination {
                        Some(destination_path) => {
                            let Some(destination) = targets.get(destination_path).cloned() else {
                                let mut error = synthetic_error(MutationErrorCode::TargetMissing);
                                error.side_effects.merge(&abort_stages(stages));
                                return Err((offset, error));
                            };
                            (destination, destination_path.clone())
                        }
                        None => (source, change.source.clone()),
                    };
                    let stage = match self.engine.prepare_file_stage_locked(
                        stage_target,
                        after.bytes.clone(),
                        StageMetadata::PreserveSupportedMode(mode),
                        lock,
                    ) {
                        Ok(stage) => stage,
                        Err(mut error) => {
                            error.side_effects.merge(&abort_stages(stages));
                            return Err((offset, error));
                        }
                    };
                    let resulting_mode = stage.resulting_mode();
                    if change.destination.is_some() {
                        virtual_modes.remove(&change.source);
                    }
                    virtual_modes.insert(next_path, resulting_mode);
                    stages.push_back(Some(stage));
                }
            }
        }
        Ok(stages)
    }

    fn execute_change(
        &self,
        planned: &PlannedChange,
        targets: &BTreeMap<String, crate::apply_patch::file_mutation::CanonicalTarget>,
        stage: Option<PreparedFileStage>,
        lock: &TargetLockGuard,
    ) -> MutationOutcome {
        let Some(source) = targets.get(&planned.source).cloned() else {
            return MutationOutcome::Failed {
                error: synthetic_error(MutationErrorCode::TargetMissing),
                committed: None,
            };
        };
        match planned.kind {
            operation_kind::Add => self.engine.create_prepared_locked(
                source,
                match stage {
                    Some(stage) => stage,
                    None => return missing_stage_outcome(),
                },
                lock,
            ),
            operation_kind::Replace => self.engine.replace_prepared_locked(
                source,
                match required_version(planned) {
                    Ok(version) => version,
                    Err(error) => return error,
                },
                match stage {
                    Some(stage) => stage,
                    None => return missing_stage_outcome(),
                },
                lock,
            ),
            operation_kind::Delete => {
                debug_assert!(stage.is_none());
                self.engine.delete_locked(
                    source,
                    match required_version(planned) {
                        Ok(version) => version,
                        Err(error) => return error,
                    },
                    lock,
                )
            }
            operation_kind::Update => {
                if let Some(destination_path) = &planned.destination {
                    let Some(destination) = targets.get(destination_path).cloned() else {
                        return MutationOutcome::Failed {
                            error: synthetic_error(MutationErrorCode::TargetMissing),
                            committed: None,
                        };
                    };
                    let destination_expectation = planned
                        .overwritten_destination
                        .as_ref()
                        .map(|snapshot| CasExpectation::Exact(snapshot.version))
                        .unwrap_or(CasExpectation::MustNotExist);
                    self.engine.move_file_prepared_locked(
                        source,
                        match required_version(planned) {
                            Ok(version) => version,
                            Err(error) => return error,
                        },
                        destination,
                        destination_expectation,
                        match stage {
                            Some(stage) => stage,
                            None => return missing_stage_outcome(),
                        },
                        lock,
                    )
                } else {
                    self.engine.replace_prepared_locked(
                        source,
                        match required_version(planned) {
                            Ok(version) => version,
                            Err(error) => return error,
                        },
                        match stage {
                            Some(stage) => stage,
                            None => return missing_stage_outcome(),
                        },
                        lock,
                    )
                }
            }
        }
    }
}

fn committed_change(
    planned: &PlannedChange,
    change: MutationChange,
) -> Option<CommittedPatchChange> {
    if change
        .before
        .as_ref()
        .zip(change.after.as_ref())
        .is_some_and(|(before, after)| before.version == after.version)
        && change.destination.is_none()
        && change.overwritten_destination.is_none()
    {
        return None;
    }
    // Usually the planned operation and the committed primitive have the same
    // shape.  A move can, however, publish its destination and then discover
    // that the source changed before the source removal.  The mutation engine
    // returns that known destination-only side effect as Add/Replace with
    // inexactness, so do not turn it back into a false completed Move here.
    let kind = match change.kind {
        crate::apply_patch::file_mutation::MutationKind::Add => ChangeKind::Add,
        crate::apply_patch::file_mutation::MutationKind::Replace => {
            if planned.kind == operation_kind::Update {
                ChangeKind::Update
            } else {
                ChangeKind::Replace
            }
        }
        crate::apply_patch::file_mutation::MutationKind::Delete => ChangeKind::Delete,
        crate::apply_patch::file_mutation::MutationKind::Move => ChangeKind::Move,
    };
    let source_path = change
        .source
        .relative()
        .to_string_lossy()
        .replace('\\', "/");
    let destination_path = change
        .destination
        .as_ref()
        .map(|target| target.relative().to_string_lossy().replace('\\', "/"));
    Some(CommittedPatchChange {
        operation_index: planned.operation_index as u32,
        commit_step: 0,
        sequence: 0,
        kind,
        source_path,
        destination_path,
        before: change.before.map(snapshot),
        after: change.after.map(snapshot),
        overwritten_destination: change.overwritten_destination.map(snapshot),
        side_effects: patch_side_effects(&change.side_effects),
    })
}

fn assign_commit_position(change: &mut CommittedPatchChange, committed_count: usize) {
    let sequence = u32::try_from(committed_count).unwrap_or(u32::MAX);
    change.sequence = sequence;
    change.commit_step = u16::try_from(sequence).unwrap_or(u16::MAX);
}

fn patch_side_effects(
    side_effects: &crate::apply_patch::file_mutation::MutationSideEffects,
) -> PatchSideEffects {
    // MutationSideEffects intentionally retains absolute PathBuf values for
    // cleanup and recovery inside the engine.  The patch/history contract must
    // never expose those paths outside the authorized workspace, so the
    // public delta carries bounded typed markers and counts rather than raw
    // host paths.
    PatchSideEffects {
        created_directories: std::iter::repeat_n(
            "<created-parent>".to_owned(),
            side_effects.created_directories.len(),
        )
        .collect(),
        residual_directories: std::iter::repeat_n(
            "<residual-parent>".to_owned(),
            side_effects.residual_directories.len(),
        )
        .collect(),
        metadata_warnings: side_effects
            .metadata_warnings
            .iter()
            .map(|warning| warning.as_str().to_owned())
            .collect(),
        exact: side_effects.exact,
    }
}

fn virtual_source_mode(
    virtual_modes: &mut BTreeMap<String, Option<u32>>,
    path: &str,
    target: &crate::apply_patch::file_mutation::CanonicalTarget,
) -> Result<Option<u32>, MutationError> {
    if let Some(mode) = virtual_modes.get(path) {
        return Ok(*mode);
    }
    let mode = supported_mode(target.absolute()).map_err(|source| MutationError {
        code: MutationErrorCode::Metadata,
        cas: None,
        source: Some(source),
        side_effects: MutationSideEffects::default(),
    })?;
    virtual_modes.insert(path.to_owned(), mode);
    Ok(mode)
}

fn planned_is_noop(planned: &PlannedChange) -> bool {
    planned.destination.is_none()
        && planned
            .before
            .as_ref()
            .zip(planned.after.as_ref())
            .is_some_and(|(before, after)| before.version == after.version)
}

fn abort_stage_iter(stages: impl IntoIterator<Item = PreparedFileStage>) -> MutationSideEffects {
    let mut pending = stages.into_iter().collect::<Vec<_>>();
    let mut side_effects = MutationSideEffects::default();
    // Deep/later stages are removed first so a stage which created their
    // shared parent can remove it only after all private entries are gone.
    while let Some(stage) = pending.pop() {
        side_effects.merge(&stage.abort());
    }
    side_effects
}

fn abort_stages(stages: VecDeque<Option<PreparedFileStage>>) -> MutationSideEffects {
    abort_stage_iter(stages.into_iter().flatten())
}

fn merge_aborted_stages_into_delta(
    delta: &mut AppliedPatchDelta,
    stages: impl IntoIterator<Item = PreparedFileStage>,
) {
    let side_effects = patch_side_effects(&abort_stage_iter(stages));
    merge_delta_side_effects(delta, &side_effects);
}

fn merge_delta_side_effects(delta: &mut AppliedPatchDelta, side_effects: &PatchSideEffects) {
    delta.exact &= side_effects.exact;
    delta.side_effects.merge(side_effects);
}

fn missing_stage_outcome() -> MutationOutcome {
    MutationOutcome::Failed {
        error: synthetic_error(MutationErrorCode::StageWrite),
        committed: None,
    }
}

fn required_version(planned: &PlannedChange) -> Result<FileVersionToken, MutationOutcome> {
    planned
        .before
        .as_ref()
        .map(|snapshot| snapshot.version)
        .ok_or_else(|| MutationOutcome::Failed {
            error: synthetic_error(MutationErrorCode::TargetMissing),
            committed: None,
        })
}

fn notify_committed(
    observer: Option<ObserverContext<'_>>,
    change: &CommittedPatchChange,
) -> Option<ObserverError> {
    let Some((observer, identity, ordinal)) = observer else {
        return None;
    };
    observer.on_committed(identity, ordinal, change).err()
}

fn observer_rejection(error: ObserverError) -> ExecutionReport {
    ExecutionReport {
        status: ExecutionStatus::Rejected,
        delta: AppliedPatchDelta::empty(),
        failure: Some(diagnostic(
            PatchStage::Record,
            PatchErrorCode::InvalidRequest,
            &format!("commit observer rejected execution: {}", error.message),
        )),
    }
}

fn observer_failure(delta: AppliedPatchDelta, error: ObserverError) -> ExecutionReport {
    if delta.is_empty() {
        ExecutionReport {
            status: ExecutionStatus::Failed,
            delta,
            failure: Some(diagnostic(
                PatchStage::Record,
                PatchErrorCode::Io,
                &format!(
                    "commit observer failed before a durable change: {}",
                    error.message
                ),
            )),
        }
    } else {
        ExecutionReport {
            status: ExecutionStatus::CommitStateUncertain,
            delta: delta.with_exactness(false),
            failure: Some(diagnostic(
                PatchStage::Record,
                PatchErrorCode::CommitStateUncertain,
                &format!(
                    "commit observer failed after filesystem mutation: {}",
                    error.message
                ),
            )),
        }
    }
}

fn snapshot(value: crate::apply_patch::file_mutation::MutationSnapshot) -> CommittedTextSnapshot {
    CommittedTextSnapshot {
        version: value.version,
        bytes: value.bytes,
        encoding: match value.encoding {
            crate::apply_patch::file_mutation::SnapshotEncoding::Utf8 => TextEncoding::Utf8,
            crate::apply_patch::file_mutation::SnapshotEncoding::Utf8Bom => TextEncoding::Utf8Bom,
        },
        line_endings: LineEndingMetadata {
            dominant: match value.line_endings.dominant {
                crate::apply_patch::file_mutation::SnapshotLineEnding::Lf => LineEnding::Lf,
                crate::apply_patch::file_mutation::SnapshotLineEnding::Crlf => LineEnding::Crlf,
                crate::apply_patch::file_mutation::SnapshotLineEnding::Mixed => LineEnding::Mixed,
                crate::apply_patch::file_mutation::SnapshotLineEnding::None => LineEnding::None,
            },
            mixed: value.line_endings.mixed,
            final_newline: value.line_endings.final_newline,
        },
    }
}

fn partial_or_rejected(
    delta: AppliedPatchDelta,
    stage: PatchStage,
    message: &str,
) -> ExecutionReport {
    if delta.is_empty() {
        ExecutionReport {
            status: ExecutionStatus::Rejected,
            delta,
            failure: Some(diagnostic(stage, PatchErrorCode::InvalidRequest, message)),
        }
    } else {
        ExecutionReport {
            status: ExecutionStatus::Partial,
            delta,
            failure: Some(diagnostic(stage, PatchErrorCode::InvalidRequest, message)),
        }
    }
}

fn rejected(stage: PatchStage, message: &str) -> ExecutionReport {
    ExecutionReport {
        status: ExecutionStatus::Rejected,
        delta: AppliedPatchDelta::empty(),
        failure: Some(diagnostic(stage, PatchErrorCode::InvalidRequest, message)),
    }
}

fn mutation_diagnostic(
    stage: PatchStage,
    planned: &PlannedChange,
    error: crate::apply_patch::file_mutation::MutationError,
) -> PatchDiagnostic {
    let code = match error.code {
        MutationErrorCode::Cas => PatchErrorCode::StaleFile,
        MutationErrorCode::Lock => PatchErrorCode::LockTimeout,
        MutationErrorCode::Snapshot | MutationErrorCode::NotRegularFile => {
            PatchErrorCode::UnsupportedContent
        }
        // Metadata is applied to the private staging file before the target
        // rename.  A metadata failure therefore leaves the visible target
        // untouched and is an ordinary I/O failure.  Only a failure after the
        // visible filesystem boundary is commit-state-uncertain.
        MutationErrorCode::Uncertain => PatchErrorCode::CommitStateUncertain,
        MutationErrorCode::Metadata => PatchErrorCode::IoWriteFailed,
        MutationErrorCode::TargetMissing | MutationErrorCode::TargetExists => {
            PatchErrorCode::StaleFile
        }
        MutationErrorCode::CrossDevice => PatchErrorCode::CrossDeviceMove,
        MutationErrorCode::ParentCreation | MutationErrorCode::StageCreate => {
            PatchErrorCode::IoCreateFailed
        }
        MutationErrorCode::StageWrite => PatchErrorCode::IoWriteFailed,
        MutationErrorCode::Sync => PatchErrorCode::IoSyncFailed,
        MutationErrorCode::Rename => PatchErrorCode::IoRenameFailed,
        MutationErrorCode::Delete => PatchErrorCode::IoDeleteFailed,
    };
    let side_effect_note = if error.side_effects.residual_directories.is_empty() {
        String::new()
    } else {
        format!(
            "; {} created parent director{} remain after cleanup",
            error.side_effects.residual_directories.len(),
            if error.side_effects.residual_directories.len() == 1 {
                "y"
            } else {
                "ies"
            }
        )
    };
    diagnostic_at(
        stage,
        code,
        &format!(
            "operation {} could not be committed{}",
            planned.operation_index, side_effect_note
        ),
        Some(planned.operation_index.try_into().unwrap_or(u32::MAX)),
        Some(planned.source.clone()),
        (code == PatchErrorCode::StaleFile).then_some(GuardHorizon::Commit),
    )
}

fn synthetic_error(code: MutationErrorCode) -> crate::apply_patch::file_mutation::MutationError {
    // The executor normally receives canonical targets from PreparedPatch; this
    // branch is defensive and never exposes an OS error or model content.
    crate::apply_patch::file_mutation::MutationError {
        code,
        cas: None,
        source: None,
        side_effects: crate::apply_patch::file_mutation::MutationSideEffects::default(),
    }
}

fn diagnostic(stage: PatchStage, code: PatchErrorCode, message: &str) -> PatchDiagnostic {
    diagnostic_at(stage, code, message, None, None, None)
}

fn diagnostic_at(
    stage: PatchStage,
    code: PatchErrorCode,
    message: &str,
    operation_index: Option<u32>,
    path: Option<String>,
    guard_horizon: Option<GuardHorizon>,
) -> PatchDiagnostic {
    PatchDiagnostic {
        code,
        stage,
        message: message.to_owned(),
        retryability: match code {
            PatchErrorCode::StaleFile => Retryability::RetryAfterRead,
            PatchErrorCode::LockTimeout => Retryability::RetryAfterDelay,
            PatchErrorCode::CommitStateUncertain => Retryability::RecoverOnly,
            _ => Retryability::Never,
        },
        operation_index,
        path,
        guard_horizon,
    }
}

fn prepare_rejection(error: &PrepareError) -> ExecutionReport {
    let code = prepare_error_code(error);
    let guard_horizon = (code == PatchErrorCode::StaleFile).then_some(GuardHorizon::Observed);
    let message = if guard_horizon.is_some() {
        format!(
            "observed guard rejected operation {} for `{}`: {}",
            error.operation_index, error.path, error.message
        )
    } else {
        error.to_string()
    };
    ExecutionReport {
        status: ExecutionStatus::Rejected,
        delta: AppliedPatchDelta::empty(),
        failure: Some(diagnostic_at(
            PatchStage::Prepare,
            code,
            &message,
            Some(error.operation_index.try_into().unwrap_or(u32::MAX)),
            (!error.path.is_empty()).then(|| error.path.clone()),
            guard_horizon,
        )),
    }
}

fn prepare_error_code(error: &PrepareError) -> PatchErrorCode {
    if let Some(code) = error.snapshot_code() {
        return match code {
            SnapshotErrorCode::BinaryContent => PatchErrorCode::UnsupportedContent,
            SnapshotErrorCode::InvalidUtf8 => PatchErrorCode::InvalidUtf8,
            SnapshotErrorCode::TooLarge => PatchErrorCode::FileTooLarge,
            SnapshotErrorCode::InvalidLimits => PatchErrorCode::InvalidLimits,
            SnapshotErrorCode::Io
            | SnapshotErrorCode::SpoolUnavailable
            | SnapshotErrorCode::SpoolCorrupt => PatchErrorCode::Io,
        };
    }
    if let Some(code) = error.target_code() {
        return match code {
            TargetResolutionErrorCode::AbsolutePathDenied => PatchErrorCode::PermissionDenied,
            TargetResolutionErrorCode::EscapesRoot => PatchErrorCode::PathOutsideAllowedRoot,
            TargetResolutionErrorCode::SymlinkDenied
            | TargetResolutionErrorCode::ExpectationMismatch => PatchErrorCode::UnsupportedFileType,
            TargetResolutionErrorCode::MetadataUnavailable => PatchErrorCode::Io,
            TargetResolutionErrorCode::InvalidPath
            | TargetResolutionErrorCode::RootMustBeAbsolute
            | TargetResolutionErrorCode::RootTargetDenied
            | TargetResolutionErrorCode::IdentityCollision => PatchErrorCode::InvalidPath,
        };
    }
    match error.plan_code() {
        Some(PlanErrorCode::ContextNotFound) => PatchErrorCode::ContextNotFound,
        Some(PlanErrorCode::AmbiguousContext) => PatchErrorCode::AmbiguousContext,
        Some(PlanErrorCode::UnsupportedContent) => PatchErrorCode::UnsupportedContent,
        Some(PlanErrorCode::OutputTooLarge) => PatchErrorCode::FileTooLarge,
        Some(PlanErrorCode::MissingSourceGuard) => PatchErrorCode::PreconditionRequired,
        Some(PlanErrorCode::StaleSource | PlanErrorCode::StaleDestination) => {
            PatchErrorCode::StaleFile
        }
        Some(PlanErrorCode::SourceMissing) => PatchErrorCode::SourceMissing,
        Some(PlanErrorCode::DestinationExists) => PatchErrorCode::DestinationExists,
        Some(PlanErrorCode::DestinationMissing) => PatchErrorCode::DestinationMissing,
        Some(PlanErrorCode::PathCollision) => PatchErrorCode::InvalidPath,
        Some(_) => PatchErrorCode::InvalidRequest,
        None => match error.code {
            PrepareErrorCode::TargetResolution | PrepareErrorCode::PathTooLong => {
                PatchErrorCode::InvalidPath
            }
            PrepareErrorCode::TargetType | PrepareErrorCode::ParentType => {
                PatchErrorCode::UnsupportedFileType
            }
            PrepareErrorCode::SnapshotTooLarge | PrepareErrorCode::OutputTooLarge => {
                PatchErrorCode::FileTooLarge
            }
            PrepareErrorCode::Read => PatchErrorCode::Io,
            PrepareErrorCode::TooManyFiles => PatchErrorCode::TooManyFiles,
            PrepareErrorCode::TooManyHunks => PatchErrorCode::TooManyHunks,
            PrepareErrorCode::InvalidLimits => PatchErrorCode::InvalidLimits,
            PrepareErrorCode::Planner => PatchErrorCode::InvalidRequest,
        },
    }
}

fn plan_rejection(error: PlanError, fallback: &str) -> ExecutionReport {
    let code = match error.code {
        PlanErrorCode::ContextNotFound => PatchErrorCode::ContextNotFound,
        PlanErrorCode::AmbiguousContext => PatchErrorCode::AmbiguousContext,
        PlanErrorCode::UnsupportedContent => PatchErrorCode::UnsupportedContent,
        PlanErrorCode::OutputTooLarge => PatchErrorCode::InvalidRequest,
        PlanErrorCode::MissingSourceGuard => PatchErrorCode::PreconditionRequired,
        PlanErrorCode::StaleSource
        | PlanErrorCode::SourceMissing
        | PlanErrorCode::DestinationExists
        | PlanErrorCode::DestinationMissing
        | PlanErrorCode::StaleDestination => PatchErrorCode::StaleFile,
        _ => PatchErrorCode::InvalidRequest,
    };
    ExecutionReport {
        status: ExecutionStatus::Rejected,
        delta: AppliedPatchDelta::empty(),
        failure: Some(diagnostic_at(
            PatchStage::Prepare,
            code,
            if error.message.is_empty() {
                fallback
            } else {
                &error.message
            },
            Some(error.operation_index.try_into().unwrap_or(u32::MAX)),
            (!error.path.is_empty()).then(|| error.path.clone()),
            (code == PatchErrorCode::StaleFile).then_some(GuardHorizon::Prepared),
        )),
    }
}

impl fmt::Display for ExecutionReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "patch execution {:?}: {} committed change(s)",
            self.status,
            self.delta.changes.len()
        )
    }
}

// Keep operation-kind matching local to this module without coupling the
// executor to parser internals beyond the public enum.
mod operation_kind {
    pub use crate::apply_patch::OperationKind::{Add, Delete, Replace, Update};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::file_mutation::{
        PatchLimits, PatchRequest, PatchRequestSource, TargetResolver,
    };
    use crate::apply_patch::history::{CommitIntentJournal, InvocationIdentity};
    use crate::apply_patch::{
        AllowAllSandbox, DurableCommitObserver, FullAccessAuthorizer, InMemoryCommitObserver,
        PrepareOptions, authorize, parse, prepare, validate_guards,
    };
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    struct CancelOnCheck {
        check: AtomicUsize,
        cancel_on: usize,
    }

    impl CancelOnCheck {
        const fn new(cancel_on: usize) -> Self {
            Self {
                check: AtomicUsize::new(0),
                cancel_on,
            }
        }
    }

    impl Cancellation for CancelOnCheck {
        fn is_cancelled(&self) -> bool {
            self.check.fetch_add(1, Ordering::SeqCst) + 1 >= self.cancel_on
        }
    }

    fn authorized(root: &std::path::Path, patch: &str) -> AuthorizedPatch {
        let request = PatchRequest::from_provider_text(
            patch,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        let document = validate_guards(parse(&request, PatchLimits::default()).unwrap()).unwrap();
        let prepared = prepare(
            &document,
            &TargetResolver::new(root).unwrap(),
            PrepareOptions::default(),
        )
        .unwrap();
        authorize(prepared, &AllowAllSandbox, &FullAccessAuthorizer).unwrap()
    }

    #[test]
    fn ordered_add_and_update_commit_and_capture_exact_delta() {
        let root = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\n*** Add File: file.txt\n+old\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch";
        let auth = authorized(root.path(), &patch);
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        assert_eq!(report.status, ExecutionStatus::Applied);
        assert_eq!(report.delta.changes.len(), 2);
        assert_eq!(fs::read(root.path().join("file.txt")).unwrap(), b"new");
    }

    #[test]
    fn overlapping_patch_invocations_commit_once_and_reject_stale_work() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"old\n").unwrap();
        let token = FileVersionToken::from_bytes(b"old\n");
        let first = authorized(
            root.path(),
            &format!(
                "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {token}\n+first\n*** End Patch"
            ),
        );
        let second = authorized(
            root.path(),
            &format!(
                "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {token}\n+second\n*** End Patch"
            ),
        );
        let executor = Arc::new(PatchExecutor::new(FileMutationEngine::new(
            Default::default(),
        )));
        let barrier = Arc::new(Barrier::new(3));

        let spawn_patch = |authorized: AuthorizedPatch| {
            let executor = Arc::clone(&executor);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                executor.execute(&authorized, ExecuteOptions::default(), &NeverCancel)
            })
        };
        let first = spawn_patch(first);
        let second = spawn_patch(second);
        barrier.wait();
        let reports = [first.join().unwrap(), second.join().unwrap()];

        assert_eq!(
            reports
                .iter()
                .filter(|report| report.status == ExecutionStatus::Applied)
                .count(),
            1
        );
        let stale = reports
            .iter()
            .find(|report| report.status == ExecutionStatus::Rejected)
            .expect("one stale invocation must be rejected");
        assert!(stale.delta.is_empty());
        assert!(stale.delta.exact);
        assert_eq!(
            stale.failure.as_ref().map(|failure| failure.code),
            Some(PatchErrorCode::StaleFile)
        );
        let final_bytes = fs::read(path).unwrap();
        assert!(final_bytes == b"first\n" || final_bytes == b"second\n");
        assert_eq!(executor.engine().lock_registry().entry_count(), 0);
    }

    #[test]
    fn cancellation_before_lock_rejects_without_filesystem_effects() {
        let root = tempfile::tempdir().unwrap();
        let auth = authorized(
            root.path(),
            "*** Begin Patch\n*** Add File: file.txt\n+new\n*** End Patch",
        );

        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &CancelOnCheck::new(1),
        );

        assert_eq!(report.status, ExecutionStatus::Rejected);
        assert!(report.delta.is_empty());
        assert!(!root.path().join("file.txt").exists());
    }

    #[test]
    fn cancellation_at_commit_boundary_reports_the_exact_prefix() {
        let root = tempfile::tempdir().unwrap();
        let auth = authorized(
            root.path(),
            "*** Begin Patch\n*** Add File: first.txt\n+first\n*** Add File: second.txt\n+second\n*** End Patch",
        );

        // Checks occur before lock acquisition, after lock acquisition and
        // before each ordered publish. Cancel on the second publish boundary.
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &CancelOnCheck::new(4),
        );

        assert_eq!(report.status, ExecutionStatus::Partial);
        assert!(report.delta.exact);
        assert_eq!(report.delta.changes.len(), 1);
        assert_eq!(report.delta.changes[0].source_path, "first.txt");
        assert_eq!(fs::read(root.path().join("first.txt")).unwrap(), b"first");
        assert!(!root.path().join("second.txt").exists());
    }

    #[test]
    fn every_replacement_stage_is_ready_before_the_first_publish() {
        let root = tempfile::tempdir().unwrap();
        let first_path = root.path().join("first.txt");
        fs::write(&first_path, b"old").unwrap();
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: first.txt\n*** If-Match: {token}\n+new\n*** Add File: second.txt\n+later\n*** End Patch"
        );
        let auth = authorized(root.path(), &patch);
        let mut options = crate::apply_patch::file_mutation::MutationOptions::default();
        options.durability.faults =
            crate::apply_patch::file_mutation::FaultPlan::fail_stage_attempt(2);

        let report = PatchExecutor::new(FileMutationEngine::new(options)).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );

        assert_eq!(report.status, ExecutionStatus::Failed);
        assert_eq!(
            report.failure.as_ref().map(|failure| failure.stage),
            Some(PatchStage::Stage)
        );
        assert!(report.delta.changes.is_empty());
        assert_eq!(fs::read(&first_path).unwrap(), b"old");
        assert!(
            fs::read_dir(root.path())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".pioneer-patch-"))
        );
    }

    #[test]
    fn residual_stage_cleanup_is_journaled_without_a_fake_file_change() {
        let root = tempfile::tempdir().unwrap();
        let first_path = root.path().join("first.txt");
        fs::write(&first_path, b"old").unwrap();
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: first.txt\n*** If-Match: {token}\n+new\n*** Add File: second.txt\n+later\n*** End Patch"
        );
        let auth = authorized(root.path(), &patch);
        let mut options = crate::apply_patch::file_mutation::MutationOptions::default();
        options.durability.faults = crate::apply_patch::file_mutation::FaultPlan {
            fail_at: Some(crate::apply_patch::file_mutation::FaultPoint::Cleanup),
            fail_stage_attempt: Some(2),
        };
        let observer = DurableCommitObserver::new(
            CommitIntentJournal::new(),
            crate::apply_patch::history::AppliedPatchLog::new(),
        );
        let identity = InvocationIdentity::new("thread", "turn", "stage-residual").unwrap();

        let report = PatchExecutor::new(FileMutationEngine::new(options)).execute_with_observer(
            &auth,
            &identity,
            &observer,
            ExecuteOptions::default(),
            &NeverCancel,
        );

        assert_eq!(report.status, ExecutionStatus::Failed);
        assert!(report.delta.changes.is_empty());
        assert!(!report.delta.is_empty());
        assert!(!report.delta.exact);
        assert!(
            report
                .delta
                .side_effects
                .metadata_warnings
                .iter()
                .any(|warning| warning == "temporary_file_cleanup_failed")
        );
        assert_eq!(fs::read(&first_path).unwrap(), b"old");

        let stored = observer
            .record_log()
            .get(&identity)
            .unwrap()
            .expect("residual staging effect must have an immutable record");
        assert!(stored.record.changes.is_empty());
        assert_eq!(stored.record.side_effects, report.delta.side_effects);
        assert_eq!(
            stored.record.exactness,
            crate::apply_patch::history::PatchRecordExactness::Uncertain
        );
    }

    #[test]
    fn strictly_guarded_target_is_rejected_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"old").unwrap();
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {token}\n+new\n*** End Patch"
        );
        let auth = authorized(root.path(), &patch);
        fs::write(&path, b"external").unwrap();
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        assert_eq!(report.status, ExecutionStatus::Rejected);
        assert!(report.delta.is_empty());
        assert_eq!(fs::read(&path).unwrap(), b"external");
    }

    #[cfg(unix)]
    #[test]
    fn root_directory_symlink_swap_is_rejected_before_root_level_replace() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"old").unwrap();
        let token = FileVersionToken::from_bytes(b"old");
        let patch = format!(
            "*** Begin Patch\n*** Replace File: file.txt\n*** If-Match: {token}\n+new\n*** End Patch"
        );
        let auth = authorized(root.path(), &patch);

        let original_root = root.path().to_path_buf();
        let moved_root = original_root.with_extension("real");
        let outside = tempfile::tempdir().unwrap();
        let outside_path = outside.path().join("file.txt");
        fs::write(&outside_path, b"outside").unwrap();
        fs::rename(&original_root, &moved_root).unwrap();
        symlink(outside.path(), &original_root).unwrap();

        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );

        assert_eq!(report.status, ExecutionStatus::Rejected);
        assert!(report.delta.is_empty());
        assert_eq!(fs::read(moved_root.join("file.txt")).unwrap(), b"old");
        assert_eq!(fs::read(&outside_path).unwrap(), b"outside");

        fs::remove_file(&original_root).unwrap();
        fs::rename(moved_root, original_root).unwrap();
    }

    #[test]
    fn contextual_update_preserves_unrelated_external_edit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"prefix\nold\nsuffix\n").unwrap();
        let patch = "*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch";
        let auth = authorized(root.path(), &patch);
        fs::write(&path, b"external\nprefix\nold\nsuffix\n").unwrap();
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        assert_eq!(report.status, ExecutionStatus::Applied);
        assert_eq!(fs::read(&path).unwrap(), b"external\nprefix\nnew\nsuffix\n");
    }

    #[test]
    fn contextual_update_rejects_overlapping_external_edit() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file.txt");
        fs::write(&path, b"prefix\nold\nsuffix\n").unwrap();
        let patch = "*** Begin Patch\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch";
        let auth = authorized(root.path(), &patch);
        fs::write(&path, b"prefix\nother\nsuffix\n").unwrap();
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        assert_eq!(report.status, ExecutionStatus::Rejected);
        assert_eq!(
            report.failure.as_ref().map(|failure| failure.code),
            Some(PatchErrorCode::ContextNotFound)
        );
        assert!(report.delta.is_empty());
        assert_eq!(fs::read(&path).unwrap(), b"prefix\nother\nsuffix\n");
    }

    #[test]
    fn contextual_replan_rejects_external_snapshot_growth_over_aggregate_limit() {
        let root = tempfile::tempdir().unwrap();
        let first_path = root.path().join("first.txt");
        let second_path = root.path().join("second.txt");
        fs::write(&first_path, b"old").unwrap();
        fs::write(&second_path, b"old").unwrap();
        let patch = "*** Begin Patch\n*** Update File: first.txt\n@@\n-old\n+new\n*** Update File: second.txt\n@@\n-old\n+new\n*** End Patch";
        let request = PatchRequest::from_provider_text(
            patch,
            PatchRequestSource::NativeFreeform,
            PatchLimits::default(),
        )
        .unwrap();
        let document = validate_guards(parse(&request, PatchLimits::default()).unwrap()).unwrap();
        let mut patch_limits = PatchLimits::default();
        patch_limits.max_total_snapshot_bytes = 6;
        let prepared = prepare(
            &document,
            &TargetResolver::new(root.path()).unwrap(),
            PrepareOptions {
                patch_limits,
                ..PrepareOptions::default()
            },
        )
        .unwrap();
        let auth = authorize(prepared, &AllowAllSandbox, &FullAccessAuthorizer).unwrap();

        fs::write(&first_path, b"external-first").unwrap();
        fs::write(&second_path, b"external-second").unwrap();
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default())).execute(
            &auth,
            ExecuteOptions::default(),
            &NeverCancel,
        );

        assert_eq!(report.status, ExecutionStatus::Rejected);
        assert_eq!(
            report.failure.as_ref().map(|failure| failure.code),
            Some(PatchErrorCode::UnsupportedContent)
        );
        assert_eq!(fs::read(&first_path).unwrap(), b"external-first");
        assert_eq!(fs::read(&second_path).unwrap(), b"external-second");
    }

    #[test]
    fn observer_replay_returns_existing_result_without_second_mutation() {
        let root = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\n*** Add File: file.txt\n+new\n*** End Patch";
        let auth = authorized(root.path(), &patch);
        let executor = PatchExecutor::new(FileMutationEngine::new(Default::default()));
        let observer = InMemoryCommitObserver::new();
        let identity = InvocationIdentity::new("thread", "turn", "call").unwrap();
        let first = executor.execute_with_observer(
            &auth,
            &identity,
            &observer,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        let second = executor.execute_with_observer(
            &auth,
            &identity,
            &observer,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        assert_eq!(first, second);
        assert_eq!(observer.committed_changes(&identity).unwrap().len(), 1);
        assert_eq!(fs::read(root.path().join("file.txt")).unwrap(), b"new");
    }

    #[test]
    fn durable_observer_accepts_every_ordered_change_in_one_invocation() {
        let root = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\n*** Add File: file.txt\n+old\n*** Update File: file.txt\n@@\n-old\n+new\n*** End Patch";
        let auth = authorized(root.path(), &patch);
        let executor = PatchExecutor::new(FileMutationEngine::new(Default::default()));
        let observer = DurableCommitObserver::new(
            CommitIntentJournal::new(),
            crate::apply_patch::history::AppliedPatchLog::new(),
        );
        let identity = InvocationIdentity::new("thread", "turn", "durable-call").unwrap();
        let report = executor.execute_with_observer(
            &auth,
            &identity,
            &observer,
            ExecuteOptions::default(),
            &NeverCancel,
        );
        assert_eq!(report.status, ExecutionStatus::Applied);
        let intent = observer.intent_journal().get(&identity).unwrap().unwrap();
        assert_eq!(
            intent
                .committed_changes
                .iter()
                .map(|change| (change.sequence, change.commit_step))
                .collect::<Vec<_>>(),
            vec![(0, 0), (1, 1)]
        );
        assert_eq!(observer.record_log().len().unwrap(), 1);
    }

    struct FailingCommitObserver;

    impl CommitObserver for FailingCommitObserver {
        fn admit(
            &self,
            _identity: &InvocationIdentity,
            _admission: &CommitAdmission,
        ) -> Result<ObserverAdmission, ObserverError> {
            Ok(ObserverAdmission::Execute {
                ordinal: CommitOrdinal(0),
            })
        }

        fn on_committed(
            &self,
            _identity: &InvocationIdentity,
            _ordinal: CommitOrdinal,
            _change: &CommittedPatchChange,
        ) -> Result<(), ObserverError> {
            Err(ObserverError::new(
                crate::apply_patch::observer::ObserverErrorCode::Storage,
                "injected progress publication failure",
            ))
        }

        fn on_terminal(
            &self,
            _identity: &InvocationIdentity,
            _ordinal: CommitOrdinal,
            _report: &ExecutionReport,
        ) -> Result<(), ObserverError> {
            Ok(())
        }
    }

    #[test]
    fn observer_failure_after_mutation_keeps_committed_delta_for_recovery() {
        let root = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\n*** Add File: file.txt\n+new\n*** End Patch";
        let auth = authorized(root.path(), &patch);
        let identity = InvocationIdentity::new("thread", "turn", "observer-failure").unwrap();
        let report = PatchExecutor::new(FileMutationEngine::new(Default::default()))
            .execute_with_observer(
                &auth,
                &identity,
                &FailingCommitObserver,
                ExecuteOptions::default(),
                &NeverCancel,
            );

        assert_eq!(report.status, ExecutionStatus::CommitStateUncertain);
        assert_eq!(report.delta.changes.len(), 1);
        assert!(!report.delta.exact);
        assert_eq!(fs::read(root.path().join("file.txt")).unwrap(), b"new");
    }

    #[test]
    fn missing_destructive_guard_is_reported_as_precondition_required() {
        let report = plan_rejection(
            PlanError {
                code: PlanErrorCode::MissingSourceGuard,
                operation_index: 0,
                path: "file.txt".to_owned(),
                message: "operation consuming a pre-existing file requires If-Match".to_owned(),
            },
            "fallback",
        );
        assert_eq!(report.status, ExecutionStatus::Rejected);
        assert_eq!(
            report.failure.as_ref().map(|failure| failure.code),
            Some(PatchErrorCode::PreconditionRequired)
        );
    }
}
