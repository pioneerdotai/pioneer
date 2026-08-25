use crate::apply_patch::file_mutation::{
    PatchDiagnostic, PatchErrorCode, PatchStage, Retryability, SnapshotEncoding, SnapshotLimits,
    SnapshotLineEnding, TextSnapshot,
};
use crate::apply_patch::history::{
    AppliedPatchDelta, AppliedPatchLog, AppliedPatchRecordOutcome, CommitIntentJournal,
    CommitOrdinal, CommittedPatchChange, CommittedTextSnapshot, IntentError, InvocationIdentity,
    LineEnding, LineEndingMetadata, PatchRecoveryPlan, PreparedChangeRecovery,
    PreparedDirectoryRecovery, StoredPatchRecord, TextEncoding,
};
use crate::apply_patch::{
    ExecutionReport, ExecutionStatus, OperationKind, PlannedPatch, PlannedSnapshot,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserverAdmission {
    Execute { ordinal: CommitOrdinal },
    Existing { report: ExecutionReport },
}

/// Immutable admission facts supplied by the trusted tool adapter.  The
/// executor replaces the recovery changes with the under-lock re-planned
/// changes before calling the observer, so the durable intent describes the
/// exact plan that is about to touch the workspace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitAdmission {
    pub plan_fingerprint: [u8; 32],
    pub operation_fingerprints: Vec<[u8; 32]>,
    pub recovery_plan: PatchRecoveryPlan,
    pub snapshot_limits: SnapshotLimits,
}

impl CommitAdmission {
    pub fn minimal(prepared: &crate::apply_patch::PreparedPatch) -> Self {
        Self {
            plan_fingerprint: prepared.fingerprint,
            operation_fingerprints: Vec::new(),
            recovery_plan: PatchRecoveryPlan {
                environment_id: String::new(),
                workspace_root: String::new(),
                authority: crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine,
                changes: Vec::new(),
                parent_directories: prepared
                    .observed_parents
                    .iter()
                    .map(|(path, observed)| PreparedDirectoryRecovery {
                        path: path.clone(),
                        existed: observed.existed,
                        fingerprint: observed.fingerprint.clone(),
                    })
                    .collect(),
            },
            snapshot_limits: prepared.snapshot_limits,
        }
    }

    pub fn for_planned(&self, planned: &PlannedPatch) -> Result<Self, ObserverError> {
        let mut operation_fingerprints = Vec::with_capacity(planned.operations.len());
        let mut changes = Vec::with_capacity(planned.operations.len());
        for operation in &planned.operations {
            let encoded = serde_json::to_vec(operation).map_err(|error| {
                ObserverError::new(
                    ObserverErrorCode::BeforeMutation,
                    format!("failed to encode prepared operation for recovery: {error}"),
                )
            })?;
            operation_fingerprints.push(Sha256::digest(encoded).into());
            changes.push(PreparedChangeRecovery {
                operation_index: operation.operation_index as u32,
                kind: match (operation.kind, operation.destination.is_some()) {
                    (OperationKind::Add, _) => crate::apply_patch::history::ChangeKind::Add,
                    (OperationKind::Replace, _) => crate::apply_patch::history::ChangeKind::Replace,
                    (OperationKind::Delete, _) => crate::apply_patch::history::ChangeKind::Delete,
                    (OperationKind::Update, true) => crate::apply_patch::history::ChangeKind::Move,
                    (OperationKind::Update, false) => {
                        crate::apply_patch::history::ChangeKind::Update
                    }
                },
                source_path: operation.source.clone(),
                destination_path: operation.destination.clone(),
                before: operation
                    .before
                    .as_ref()
                    .map(|snapshot| recovery_snapshot(snapshot, self.snapshot_limits))
                    .transpose()?,
                after: operation
                    .after
                    .as_ref()
                    .map(|snapshot| recovery_snapshot(snapshot, self.snapshot_limits))
                    .transpose()?,
                overwritten_destination: operation
                    .overwritten_destination
                    .as_ref()
                    .map(|snapshot| recovery_snapshot(snapshot, self.snapshot_limits))
                    .transpose()?,
                side_effects: Default::default(),
            });
        }
        let mut recovery_plan = self.recovery_plan.clone();
        recovery_plan.changes = changes;
        Ok(Self {
            plan_fingerprint: self.plan_fingerprint,
            operation_fingerprints,
            recovery_plan,
            snapshot_limits: self.snapshot_limits,
        })
    }
}

fn recovery_snapshot(
    snapshot: &PlannedSnapshot,
    limits: SnapshotLimits,
) -> Result<CommittedTextSnapshot, ObserverError> {
    let inspected = TextSnapshot::from_bytes(snapshot.bytes.clone(), limits).map_err(|error| {
        ObserverError::new(
            ObserverErrorCode::BeforeMutation,
            format!("failed to prepare crash recovery snapshot: {error}"),
        )
    })?;
    let bytes = inspected
        .bytes()
        .map_err(|error| {
            ObserverError::new(
                ObserverErrorCode::BeforeMutation,
                format!("failed to read crash recovery snapshot: {error}"),
            )
        })?
        .to_vec();
    Ok(CommittedTextSnapshot {
        version: crate::apply_patch::file_mutation::FileContentVersion::new(snapshot.version),
        bytes,
        encoding: match inspected.encoding {
            SnapshotEncoding::Utf8 => TextEncoding::Utf8,
            SnapshotEncoding::Utf8Bom => TextEncoding::Utf8Bom,
        },
        line_endings: LineEndingMetadata {
            dominant: match inspected.line_endings.dominant {
                SnapshotLineEnding::Lf => LineEnding::Lf,
                SnapshotLineEnding::Crlf => LineEnding::Crlf,
                SnapshotLineEnding::Mixed => LineEnding::Mixed,
                SnapshotLineEnding::None => LineEnding::None,
            },
            mixed: inspected.line_endings.mixed,
            final_newline: inspected.line_endings.final_newline,
        },
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverErrorCode {
    DuplicateInvocation,
    InFlight,
    BeforeMutation,
    AfterMutation,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObserverError {
    pub code: ObserverErrorCode,
    pub message: String,
}

impl ObserverError {
    pub fn new(code: ObserverErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub trait CommitObserver: Send + Sync {
    /// Performs a read-only duplicate/in-flight check.  This hook must not
    /// reserve an ordinal or write an intent: the executor calls `admit` only
    /// after the complete target lock set has been acquired and revalidated.
    /// Keeping the check separate preserves the lock ordering contract while
    /// still allowing a completed replay to return without touching the
    /// workspace.
    fn check(
        &self,
        _identity: &InvocationIdentity,
        _plan_fingerprint: [u8; 32],
    ) -> Result<Option<ExecutionReport>, ObserverError> {
        Ok(None)
    }

    fn admit(
        &self,
        identity: &InvocationIdentity,
        admission: &CommitAdmission,
    ) -> Result<ObserverAdmission, ObserverError>;

    /// Returns the exact in-process record after terminal publication when the
    /// observer keeps one. External durable adapters may return `None`; the
    /// persisted record is still authoritative and replay-safe.
    fn record(
        &self,
        _identity: &InvocationIdentity,
    ) -> Result<Option<(StoredPatchRecord, Vec<CommittedPatchChange>)>, ObserverError> {
        Ok(None)
    }

    /// Returns the durable aggregate revision that includes the invocation's
    /// record, when this observer owns a persisted projection.  A process-local
    /// observer may return `None`; callers must then report tracking as pending
    /// rather than claiming that the aggregate was projected.
    fn projection_revision(
        &self,
        _identity: &InvocationIdentity,
    ) -> Result<Option<u64>, ObserverError> {
        Ok(None)
    }

    fn on_committed(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        change: &CommittedPatchChange,
    ) -> Result<(), ObserverError>;

    fn on_terminal(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        report: &ExecutionReport,
    ) -> Result<(), ObserverError>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryCommitObserver {
    state: Arc<Mutex<ObserverState>>,
}

/// Observer that writes recoverable intent/progress and promotes one immutable
/// record after the executor reports its terminal outcome.  It is deliberately
/// independent of the workspace: replaying `admit` never re-runs a mutation.
#[derive(Clone, Debug, Default)]
pub struct DurableCommitObserver {
    pub journal: CommitIntentJournal,
    pub records: AppliedPatchLog,
    state: Arc<Mutex<HashMap<(String, String, String), ([u8; 32], ExecutionReport)>>>,
    pre_admitted: Arc<Mutex<HashMap<(String, String, String), ([u8; 32], CommitOrdinal)>>>,
    in_flight: Arc<Mutex<HashMap<(String, String, String), ([u8; 32], CommitOrdinal)>>>,
    admission_lock: Arc<Mutex<()>>,
}

impl DurableCommitObserver {
    pub fn new(journal: CommitIntentJournal, records: AppliedPatchLog) -> Self {
        Self {
            journal,
            records,
            state: Arc::new(Mutex::new(HashMap::new())),
            pre_admitted: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            admission_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn record_log(&self) -> &AppliedPatchLog {
        &self.records
    }

    pub fn intent_journal(&self) -> &CommitIntentJournal {
        &self.journal
    }

    /// Seeds the process-local observer after an external durable store has
    /// allocated the ordinal under the complete target lock set. This is only
    /// an admission hand-off; a pending intent discovered without this
    /// in-flight marker is recovery state and must never be executed again.
    pub fn seed_admission(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        plan_fingerprint: [u8; 32],
        operation_fingerprints: Vec<[u8; 32]>,
    ) -> Result<(), ObserverError> {
        let admission = CommitAdmission {
            plan_fingerprint,
            operation_fingerprints,
            recovery_plan: PatchRecoveryPlan {
                environment_id: String::new(),
                workspace_root: String::new(),
                authority: crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine,
                changes: Vec::new(),
                parent_directories: Vec::new(),
            },
            snapshot_limits: SnapshotLimits::default(),
        };
        self.seed_admission_with_admission(identity, ordinal, &admission)
    }

    /// Seeds the process-local journal with the exact recovery plan that was
    /// prepared while the executor held the complete target lock set. The
    /// durable SQLite adapter calls this only after its Pending row has been
    /// committed, so every subsequent filesystem operation has a recoverable
    /// local and durable intent before it starts.
    pub fn seed_admission_with_admission(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        admission: &CommitAdmission,
    ) -> Result<(), ObserverError> {
        let _guard = self.admission_lock.lock().map_err(|_| {
            ObserverError::new(
                ObserverErrorCode::Storage,
                "observer admission lock poisoned",
            )
        })?;
        let key = identity_key(identity);
        if let Some(record) = self.records.get(identity).map_err(intent_observer_error)? {
            if record.plan_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Ok(());
        }
        if let Some((existing_fingerprint, existing_ordinal)) = self
            .in_flight
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .get(&key)
            .copied()
        {
            if existing_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            if existing_ordinal == ordinal {
                return Ok(());
            }
            return Err(ObserverError::new(
                ObserverErrorCode::InFlight,
                "invocation is already executing with a different ordinal",
            ));
        }
        if let Some(intent) = self.journal.get(identity).map_err(intent_error)? {
            if intent.plan_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Err(ObserverError::new(
                ObserverErrorCode::Storage,
                "pending intent requires recovery and cannot be reseeded",
            ));
        }
        self.journal
            .begin(
                identity.clone(),
                ordinal,
                admission.plan_fingerprint,
                admission.operation_fingerprints.clone(),
                Some(admission.recovery_plan.clone()),
            )
            .map_err(intent_error)?;
        self.pre_admitted
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .insert(key, (admission.plan_fingerprint, ordinal));
        Ok(())
    }
}

impl CommitObserver for DurableCommitObserver {
    fn check(
        &self,
        identity: &InvocationIdentity,
        plan_fingerprint: [u8; 32],
    ) -> Result<Option<ExecutionReport>, ObserverError> {
        let _guard = self.admission_lock.lock().map_err(|_| {
            ObserverError::new(
                ObserverErrorCode::Storage,
                "observer admission lock poisoned",
            )
        })?;
        let key = identity_key(identity);
        if let Some(report) = self
            .state
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .get(&identity_key(identity))
            .cloned()
        {
            if report.0 != plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Ok(Some(report.1));
        }
        let pre_admitted = self
            .pre_admitted
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .contains_key(&key);
        if !pre_admitted
            && self
                .in_flight
                .lock()
                .map_err(|_| {
                    ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
                })?
                .contains_key(&key)
        {
            return Err(ObserverError::new(
                ObserverErrorCode::InFlight,
                "invocation is already executing",
            ));
        }
        if pre_admitted {
            // An adapter has already allocated the durable ordinal under the
            // executor's lock set. The following admit call claims it.
            return Ok(None);
        }
        if let Some(record) = self.records.get(identity).map_err(intent_observer_error)? {
            if record.plan_fingerprint != plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Ok(Some(report_from_record(&record.record)));
        }
        if let Some(intent) = self.journal.get(identity).map_err(intent_error)? {
            if intent.plan_fingerprint != plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            match intent.status {
                crate::apply_patch::history::IntentStatus::AppliedNoChange => {
                    return Ok(Some(ExecutionReport {
                        status: ExecutionStatus::Applied,
                        delta: AppliedPatchDelta::empty().with_exactness(true),
                        failure: None,
                    }));
                }
                crate::apply_patch::history::IntentStatus::FailedNoChange => {
                    return Ok(Some(ExecutionReport {
                        status: ExecutionStatus::Failed,
                        delta: AppliedPatchDelta::empty().with_exactness(true),
                        failure: Some(PatchDiagnostic {
                            code: PatchErrorCode::Io,
                            stage: PatchStage::Record,
                            message: "replayed patch failed before any filesystem change"
                                .to_owned(),
                            retryability: Retryability::Never,
                            operation_index: None,
                            path: None,
                            guard_horizon: None,
                        }),
                    }));
                }
                crate::apply_patch::history::IntentStatus::Rejected => {
                    return Ok(Some(ExecutionReport {
                        status: ExecutionStatus::Rejected,
                        delta: AppliedPatchDelta::empty().with_exactness(true),
                        failure: None,
                    }));
                }
                crate::apply_patch::history::IntentStatus::Pending => {}
                crate::apply_patch::history::IntentStatus::Promoted
                | crate::apply_patch::history::IntentStatus::Gap => {
                    return Err(ObserverError::new(
                        ObserverErrorCode::Storage,
                        "terminal intent has no promoted record",
                    ));
                }
            }
            return Err(ObserverError::new(
                ObserverErrorCode::Storage,
                "pending intent requires recovery and cannot be re-executed",
            ));
        }
        Ok(None)
    }

    fn admit(
        &self,
        identity: &InvocationIdentity,
        admission: &CommitAdmission,
    ) -> Result<ObserverAdmission, ObserverError> {
        let _guard = self.admission_lock.lock().map_err(|_| {
            ObserverError::new(
                ObserverErrorCode::Storage,
                "observer admission lock poisoned",
            )
        })?;
        let key = identity_key(identity);
        if let Some(report) = self
            .state
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .get(&identity_key(identity))
            .cloned()
        {
            if report.0 != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Ok(ObserverAdmission::Existing { report: report.1 });
        }
        if let Some((seed_fingerprint, ordinal)) = self
            .pre_admitted
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .remove(&key)
        {
            if seed_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            self.in_flight
                .lock()
                .map_err(|_| {
                    ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
                })?
                .insert(key, (seed_fingerprint, ordinal));
            return Ok(ObserverAdmission::Execute { ordinal });
        }
        if self
            .in_flight
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .contains_key(&key)
        {
            return Err(ObserverError::new(
                ObserverErrorCode::InFlight,
                "invocation is already executing",
            ));
        }
        if let Some(record) = self.records.get(identity).map_err(intent_observer_error)? {
            let report = report_from_record(&record.record);
            self.state
                .lock()
                .map_err(|_| {
                    ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
                })?
                .insert(
                    identity_key(identity),
                    (record.plan_fingerprint, report.clone()),
                );
            return Ok(ObserverAdmission::Existing { report });
        }
        if let Some(intent) = self.journal.get(identity).map_err(intent_error)? {
            if intent.plan_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            match intent.status {
                crate::apply_patch::history::IntentStatus::AppliedNoChange => {
                    return Ok(ObserverAdmission::Existing {
                        report: ExecutionReport {
                            status: ExecutionStatus::Applied,
                            delta: AppliedPatchDelta::empty().with_exactness(true),
                            failure: None,
                        },
                    });
                }
                crate::apply_patch::history::IntentStatus::FailedNoChange => {
                    return Ok(ObserverAdmission::Existing {
                        report: ExecutionReport {
                            status: ExecutionStatus::Failed,
                            delta: AppliedPatchDelta::empty().with_exactness(true),
                            failure: Some(PatchDiagnostic {
                                code: PatchErrorCode::Io,
                                stage: PatchStage::Record,
                                message: "replayed patch failed before any filesystem change"
                                    .to_owned(),
                                retryability: Retryability::Never,
                                operation_index: None,
                                path: None,
                                guard_horizon: None,
                            }),
                        },
                    });
                }
                crate::apply_patch::history::IntentStatus::Rejected => {
                    return Ok(ObserverAdmission::Existing {
                        report: ExecutionReport {
                            status: ExecutionStatus::Rejected,
                            delta: AppliedPatchDelta::empty().with_exactness(true),
                            failure: None,
                        },
                    });
                }
                crate::apply_patch::history::IntentStatus::Pending => {}
                crate::apply_patch::history::IntentStatus::Promoted
                | crate::apply_patch::history::IntentStatus::Gap => {
                    return Err(ObserverError::new(
                        ObserverErrorCode::Storage,
                        "terminal intent has no promoted record",
                    ));
                }
            }
            return Err(ObserverError::new(
                ObserverErrorCode::Storage,
                "pending intent requires recovery and cannot be re-executed",
            ));
        }
        let ordinal = self
            .records
            .allocate_ordinal(&identity.thread_id, &identity.turn_id)
            .map_err(|error| ObserverError::new(ObserverErrorCode::Storage, error.to_string()))?;
        self.journal
            .begin(
                identity.clone(),
                ordinal,
                admission.plan_fingerprint,
                admission.operation_fingerprints.clone(),
                Some(admission.recovery_plan.clone()),
            )
            .map_err(intent_error)?;
        self.in_flight
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .insert(key, (admission.plan_fingerprint, ordinal));
        Ok(ObserverAdmission::Execute { ordinal })
    }

    fn on_committed(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        change: &CommittedPatchChange,
    ) -> Result<(), ObserverError> {
        self.journal
            .append_change(identity, ordinal, change.clone())
            .map(|_| ())
            .map_err(intent_error)
    }

    fn record(
        &self,
        identity: &InvocationIdentity,
    ) -> Result<Option<(StoredPatchRecord, Vec<CommittedPatchChange>)>, ObserverError> {
        let record = self.records.get(identity).map_err(intent_observer_error)?;
        let Some(record) = record else {
            return Ok(None);
        };
        let changes = self
            .journal
            .get(identity)
            .map_err(intent_error)?
            .map(|intent| intent.committed_changes)
            .unwrap_or_default();
        Ok(Some((record, changes)))
    }

    fn on_terminal(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        report: &ExecutionReport,
    ) -> Result<(), ObserverError> {
        let _guard = self.admission_lock.lock().map_err(|_| {
            ObserverError::new(
                ObserverErrorCode::Storage,
                "observer admission lock poisoned",
            )
        })?;
        let key = identity_key(identity);
        let plan_fingerprint = self
            .in_flight
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .get(&key)
            .map(|(fingerprint, _)| *fingerprint);
        let Some(plan_fingerprint) = plan_fingerprint else {
            // Terminal publication can be retried after the durable promotion
            // succeeded but the caller lost its response.  The record (or the
            // process-local terminal state) is authoritative in that case;
            // never append a second record and never ask the executor to
            // repeat the filesystem mutation.
            if let Some(existing) = self
                .state
                .lock()
                .map_err(|_| {
                    ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
                })?
                .get(&key)
            {
                if existing.1 == *report {
                    return Ok(());
                }
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "terminal result was published with a different report",
                ));
            }
            if self
                .records
                .get(identity)
                .map_err(intent_observer_error)?
                .is_some()
            {
                return Ok(());
            }
            return Err(ObserverError::new(
                ObserverErrorCode::AfterMutation,
                "terminal result was published for an unknown invocation",
            ));
        };
        let intent = self
            .journal
            .get(identity)
            .map_err(intent_error)?
            .ok_or_else(|| {
                ObserverError::new(
                    ObserverErrorCode::AfterMutation,
                    "terminal result was published for a missing local intent",
                )
            })?;
        if intent.commit_ordinal != ordinal || intent.committed_changes != report.delta.changes {
            return Err(ObserverError::new(
                ObserverErrorCode::AfterMutation,
                "terminal report does not match journaled committed changes",
            ));
        }
        let terminal_result = if report.delta.is_empty()
            && matches!(
                report.status,
                ExecutionStatus::Applied | ExecutionStatus::Failed | ExecutionStatus::Rejected
            ) {
            match report.status {
                ExecutionStatus::Applied => self
                    .journal
                    .mark_applied_no_change(identity, ordinal)
                    .map(|_| ())
                    .map_err(intent_error),
                ExecutionStatus::Failed => self
                    .journal
                    .mark_failed_no_change(identity, ordinal)
                    .map(|_| ())
                    .map_err(intent_error),
                ExecutionStatus::Rejected => {
                    self.journal.reject(identity, ordinal).map_err(intent_error)
                }
                _ => unreachable!("empty terminal status was checked above"),
            }
        } else {
            let outcome = match report.status {
                ExecutionStatus::Applied => AppliedPatchRecordOutcome::Applied,
                ExecutionStatus::Partial | ExecutionStatus::Failed => {
                    AppliedPatchRecordOutcome::Partial {
                        failed_stage: report
                            .failure
                            .as_ref()
                            .map(|failure| failure.stage)
                            .unwrap_or(crate::apply_patch::file_mutation::PatchStage::Commit),
                        error_code: report
                            .failure
                            .as_ref()
                            .map(|failure| failure.code)
                            .unwrap_or(crate::apply_patch::file_mutation::PatchErrorCode::Io),
                    }
                }
                ExecutionStatus::Rejected => AppliedPatchRecordOutcome::Gap {
                    reason: "rejected after a committed delta".to_owned(),
                },
                ExecutionStatus::CommitStateUncertain => {
                    AppliedPatchRecordOutcome::CommitStateUncertain
                }
            };
            self.journal
                .promote_with_side_effects(
                    identity,
                    ordinal,
                    outcome,
                    report.delta.side_effects.clone(),
                    &self.records,
                )
                .map(|_| ())
                .map_err(intent_error)
        };
        terminal_result?;
        self.state
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .insert(key.clone(), (plan_fingerprint, report.clone()));
        self.in_flight
            .lock()
            .map_err(|_| ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned"))?
            .remove(&key);
        Ok(())
    }
}

fn report_from_record(record: &crate::apply_patch::history::AppliedPatchRecord) -> ExecutionReport {
    let (status, failure) = replay_status_and_failure(&record.outcome);
    let mut delta = crate::apply_patch::history::AppliedPatchDelta::empty()
        .with_exactness(record.exactness.is_exact());
    delta.side_effects = record.side_effects.clone();
    delta.exact &= delta.side_effects.exact;
    ExecutionReport {
        status,
        delta,
        failure,
    }
}

fn replay_status_and_failure(
    outcome: &AppliedPatchRecordOutcome,
) -> (ExecutionStatus, Option<PatchDiagnostic>) {
    match outcome {
        AppliedPatchRecordOutcome::Applied => (ExecutionStatus::Applied, None),
        AppliedPatchRecordOutcome::Partial {
            failed_stage,
            error_code,
        } => (
            ExecutionStatus::Partial,
            Some(replay_diagnostic(
                *failed_stage,
                *error_code,
                "replayed patch has a committed partial outcome",
            )),
        ),
        AppliedPatchRecordOutcome::CommitStateUncertain => (
            ExecutionStatus::CommitStateUncertain,
            Some(replay_diagnostic(
                PatchStage::Recover,
                PatchErrorCode::CommitStateUncertain,
                "replayed patch has an uncertain filesystem commit state",
            )),
        ),
        AppliedPatchRecordOutcome::Gap { .. } => (
            ExecutionStatus::CommitStateUncertain,
            Some(replay_diagnostic(
                PatchStage::Recover,
                PatchErrorCode::CommitStateUncertain,
                "replayed patch history contains an unresolved filesystem gap",
            )),
        ),
    }
}

fn replay_diagnostic(stage: PatchStage, code: PatchErrorCode, message: &str) -> PatchDiagnostic {
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
        operation_index: None,
        path: None,
        guard_horizon: None,
    }
}

fn intent_error(error: IntentError) -> ObserverError {
    ObserverError::new(ObserverErrorCode::Storage, error.to_string())
}

fn intent_observer_error(error: impl std::fmt::Display) -> ObserverError {
    ObserverError::new(ObserverErrorCode::Storage, error.to_string())
}

#[derive(Debug, Default)]
struct ObserverState {
    next_ordinal: u64,
    invocations: HashMap<(String, String, String), InvocationEntry>,
}

#[derive(Debug)]
struct InvocationEntry {
    plan_fingerprint: [u8; 32],
    ordinal: CommitOrdinal,
    changes: Vec<CommittedPatchChange>,
    terminal: Option<ExecutionReport>,
}

impl InMemoryCommitObserver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn committed_changes(
        &self,
        identity: &InvocationIdentity,
    ) -> Option<Vec<CommittedPatchChange>> {
        let state = self.state.lock().ok()?;
        state
            .invocations
            .get(&identity_key(identity))
            .map(|entry| entry.changes.clone())
    }
}

impl CommitObserver for InMemoryCommitObserver {
    fn check(
        &self,
        identity: &InvocationIdentity,
        plan_fingerprint: [u8; 32],
    ) -> Result<Option<ExecutionReport>, ObserverError> {
        let state = self.state.lock().map_err(|_| {
            ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
        })?;
        let Some(entry) = state.invocations.get(&identity_key(identity)) else {
            return Ok(None);
        };
        if entry.plan_fingerprint != plan_fingerprint {
            return Err(ObserverError::new(
                ObserverErrorCode::DuplicateInvocation,
                "invocation identity was reused for a different immutable plan",
            ));
        }
        if let Some(report) = &entry.terminal {
            return Ok(Some(report.clone()));
        }
        Err(ObserverError::new(
            ObserverErrorCode::InFlight,
            "invocation is already executing",
        ))
    }

    fn admit(
        &self,
        identity: &InvocationIdentity,
        admission: &CommitAdmission,
    ) -> Result<ObserverAdmission, ObserverError> {
        let mut state = self.state.lock().map_err(|_| {
            ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
        })?;
        let key = identity_key(identity);
        if let Some(entry) = state.invocations.get(&key) {
            if entry.plan_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            if let Some(report) = &entry.terminal {
                return Ok(ObserverAdmission::Existing {
                    report: report.clone(),
                });
            }
            return Err(ObserverError::new(
                ObserverErrorCode::InFlight,
                "invocation is already executing",
            ));
        }
        let ordinal = CommitOrdinal(state.next_ordinal);
        state.next_ordinal = state.next_ordinal.saturating_add(1);
        state.invocations.insert(
            key,
            InvocationEntry {
                plan_fingerprint: admission.plan_fingerprint,
                ordinal,
                changes: Vec::new(),
                terminal: None,
            },
        );
        Ok(ObserverAdmission::Execute { ordinal })
    }

    fn on_committed(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        change: &CommittedPatchChange,
    ) -> Result<(), ObserverError> {
        let mut state = self.state.lock().map_err(|_| {
            ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
        })?;
        let entry = state
            .invocations
            .get_mut(&identity_key(identity))
            .ok_or_else(|| {
                ObserverError::new(
                    ObserverErrorCode::BeforeMutation,
                    "invocation was not admitted",
                )
            })?;
        if entry.ordinal != ordinal || entry.terminal.is_some() {
            return Err(ObserverError::new(
                ObserverErrorCode::AfterMutation,
                "operation progress arrived for a closed invocation",
            ));
        }
        let mut change = change.clone();
        change.sequence = entry.changes.len() as u32;
        change.commit_step = u16::try_from(change.sequence).unwrap_or(u16::MAX);
        entry.changes.push(change);
        Ok(())
    }

    fn on_terminal(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        report: &ExecutionReport,
    ) -> Result<(), ObserverError> {
        let mut state = self.state.lock().map_err(|_| {
            ObserverError::new(ObserverErrorCode::Storage, "observer state poisoned")
        })?;
        let entry = state
            .invocations
            .get_mut(&identity_key(identity))
            .ok_or_else(|| {
                ObserverError::new(
                    ObserverErrorCode::AfterMutation,
                    "invocation was not admitted",
                )
            })?;
        if entry.ordinal != ordinal {
            return Err(ObserverError::new(
                ObserverErrorCode::AfterMutation,
                "terminal result was already published",
            ));
        }
        if let Some(existing) = &entry.terminal {
            if existing == report {
                return Ok(());
            }
            return Err(ObserverError::new(
                ObserverErrorCode::DuplicateInvocation,
                "terminal result was published with a different report",
            ));
        }
        entry.terminal = Some(report.clone());
        Ok(())
    }
}

fn identity_key(identity: &InvocationIdentity) -> (String, String, String) {
    (
        identity.thread_id.clone(),
        identity.turn_id.clone(),
        identity.invocation_id.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_patch::history::AppliedPatchDelta;
    use crate::apply_patch::{ExecutionReport, ExecutionStatus};

    fn identity() -> InvocationIdentity {
        InvocationIdentity::new("thread", "turn", "call").unwrap()
    }

    fn report() -> ExecutionReport {
        ExecutionReport {
            status: ExecutionStatus::Applied,
            delta: AppliedPatchDelta::empty(),
            failure: None,
        }
    }

    fn failed_without_delta() -> ExecutionReport {
        ExecutionReport {
            status: ExecutionStatus::Failed,
            delta: AppliedPatchDelta::empty(),
            failure: Some(PatchDiagnostic {
                code: PatchErrorCode::Io,
                stage: PatchStage::Commit,
                message: "staging failed before the first filesystem change".to_owned(),
                retryability: Retryability::Never,
                operation_index: None,
                path: None,
                guard_horizon: None,
            }),
        }
    }

    fn admission(fingerprint: [u8; 32]) -> CommitAdmission {
        CommitAdmission {
            plan_fingerprint: fingerprint,
            operation_fingerprints: Vec::new(),
            recovery_plan: PatchRecoveryPlan {
                environment_id: String::new(),
                workspace_root: String::new(),
                authority: crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine,
                changes: Vec::new(),
                parent_directories: Vec::new(),
            },
            snapshot_limits: SnapshotLimits::default(),
        }
    }

    #[test]
    fn duplicate_completed_invocation_returns_existing_report() {
        let observer = InMemoryCommitObserver::new();
        let identity = identity();
        let first = observer.admit(&identity, &admission([7; 32])).unwrap();
        let ordinal = match first {
            ObserverAdmission::Execute { ordinal } => ordinal,
            ObserverAdmission::Existing { .. } => panic!("first admission cannot be replay"),
        };
        observer.on_terminal(&identity, ordinal, &report()).unwrap();
        let second = observer.admit(&identity, &admission([7; 32])).unwrap();
        assert!(matches!(second, ObserverAdmission::Existing { .. }));
    }

    #[test]
    fn duplicate_identity_with_different_plan_is_rejected() {
        let observer = InMemoryCommitObserver::new();
        let identity = identity();
        let _ = observer.admit(&identity, &admission([1; 32])).unwrap();
        let error = observer.admit(&identity, &admission([2; 32])).unwrap_err();
        assert_eq!(error.code, ObserverErrorCode::DuplicateInvocation);
    }

    #[test]
    fn durable_pending_intent_is_not_reexecuted_after_restart() {
        let journal = CommitIntentJournal::new();
        let records = AppliedPatchLog::new();
        let identity = identity();
        journal
            .begin(
                identity.clone(),
                CommitOrdinal(4),
                [3; 32],
                Vec::new(),
                None,
            )
            .unwrap();
        let observer = DurableCommitObserver::new(journal, records);
        assert_eq!(
            observer.check(&identity, [3; 32]).unwrap_err().code,
            ObserverErrorCode::Storage
        );
        assert_eq!(
            observer
                .admit(&identity, &admission([3; 32]))
                .unwrap_err()
                .code,
            ObserverErrorCode::Storage
        );
    }

    #[test]
    fn durable_admission_marks_in_flight_and_clears_on_terminal() {
        let observer =
            DurableCommitObserver::new(CommitIntentJournal::new(), AppliedPatchLog::new());
        let identity = identity();
        let ordinal = match observer.admit(&identity, &admission([9; 32])).unwrap() {
            ObserverAdmission::Execute { ordinal } => ordinal,
            ObserverAdmission::Existing { .. } => panic!("first admission cannot replay"),
        };
        assert_eq!(
            observer.check(&identity, [9; 32]).unwrap_err().code,
            ObserverErrorCode::InFlight
        );
        observer.on_terminal(&identity, ordinal, &report()).unwrap();
        assert!(matches!(
            observer.admit(&identity, &admission([9; 32])).unwrap(),
            ObserverAdmission::Existing { .. }
        ));
        assert_eq!(
            observer
                .admit(&identity, &admission([8; 32]))
                .unwrap_err()
                .code,
            ObserverErrorCode::DuplicateInvocation
        );
    }

    #[test]
    fn durable_terminal_publication_is_idempotent_after_promotion() {
        let observer =
            DurableCommitObserver::new(CommitIntentJournal::new(), AppliedPatchLog::new());
        let identity = identity();
        let ordinal = match observer.admit(&identity, &admission([4; 32])).unwrap() {
            ObserverAdmission::Execute { ordinal } => ordinal,
            ObserverAdmission::Existing { .. } => panic!("first admission cannot replay"),
        };
        observer.on_terminal(&identity, ordinal, &report()).unwrap();
        observer
            .on_terminal(&identity, ordinal, &report())
            .expect("retry after durable promotion must not reapply or append");
        assert_eq!(observer.record_log().len().unwrap(), 0);
    }

    #[test]
    fn failed_without_delta_keeps_replay_status_and_no_file_change() {
        let observer =
            DurableCommitObserver::new(CommitIntentJournal::new(), AppliedPatchLog::new());
        let identity = identity();
        let ordinal = match observer.admit(&identity, &admission([5; 32])).unwrap() {
            ObserverAdmission::Execute { ordinal } => ordinal,
            ObserverAdmission::Existing { .. } => panic!("first admission cannot replay"),
        };
        observer
            .on_terminal(&identity, ordinal, &failed_without_delta())
            .unwrap();

        let replay = observer.admit(&identity, &admission([5; 32])).unwrap();
        let ObserverAdmission::Existing { report } = replay else {
            panic!("failed terminal must be replayable");
        };
        assert_eq!(report.status, ExecutionStatus::Failed);
        assert!(report.delta.is_empty());
        assert!(report.failure.is_some());
        assert_eq!(observer.record_log().len().unwrap(), 0);
        assert!(observer.record_log().get(&identity).unwrap().is_none());
    }
}
