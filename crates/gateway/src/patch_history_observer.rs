//! Synchronous commit-observer adapter for the gateway's asynchronous SQLite
//! history store.
//!
//! The patch executor is intentionally synchronous and invokes its observer
//! while the complete target lock set is held.  This adapter bridges that
//! boundary with a short-lived current-thread Tokio runtime on a dedicated OS
//! thread.  The important ordering is preserved: bounded writer capacity is
//! reserved before locks, while the exact intent and commit ordinal are
//! persisted only after locks and under-lock revalidation, then progress is
//! journaled and the immutable record is promoted before releasing them.

use anyhow::{Context, Result, anyhow, bail};
use pioneer_tools::apply_patch::history::IntentStatus;
use pioneer_tools::apply_patch::history::{
    AppliedPatchDelta, AppliedPatchRecord, AppliedPatchRecordOutcome, CommitOrdinal,
    CommittedPatchChange, InvocationIdentity, PatchHistoryProvenance, SnapshotDomain,
    SqliteAppliedPatchStore, SqliteCommitIntentStore, SqliteTurnDiffStore, StoredPatchRecord,
    TurnDiffAuthority, TurnDiffState,
};
use pioneer_tools::apply_patch::{
    CommitAdmission, CommitObserver, DurableCommitObserver, ExecutionReport, ExecutionStatus,
    ObserverAdmission, ObserverError, ObserverErrorCode, patch_telemetry,
};
use sea_orm::DatabaseConnection;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PATCH_HISTORY_SYNC_TIMEOUT: Duration = Duration::from_secs(5);
const PATCH_HISTORY_ADMISSION_CAPACITY: usize = 256;

static PATCH_HISTORY_ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();

/// Reserve one bounded history-writer slot before the executor acquires any
/// workspace target lock.  The permit is held by the observer until the
/// terminal result has been durably promoted (or the observer is dropped), so
/// a full SQLite/snapshot writer lane fails closed instead of allowing an
/// unbounded number of under-lock database waits.
pub(crate) async fn reserve_patch_history_capacity() -> Result<OwnedSemaphorePermit> {
    let semaphore = PATCH_HISTORY_ADMISSION
        .get_or_init(|| Arc::new(Semaphore::new(PATCH_HISTORY_ADMISSION_CAPACITY)))
        .clone();
    reserve_patch_history_capacity_from(semaphore, PATCH_HISTORY_SYNC_TIMEOUT).await
}

async fn reserve_patch_history_capacity_from(
    semaphore: Arc<Semaphore>,
    timeout: Duration,
) -> Result<OwnedSemaphorePermit> {
    tokio::time::timeout(timeout, semaphore.acquire_owned())
        .await
        .context("patch history admission capacity timed out")?
        .context("patch history admission capacity closed")
}

#[derive(Clone)]
pub(crate) struct SqlitePatchObserver {
    db: DatabaseConnection,
    identity: InvocationIdentity,
    _admission_permit: Arc<OwnedSemaphorePermit>,
    inner: DurableCommitObserver,
}

impl SqlitePatchObserver {
    pub(crate) fn new(
        db: DatabaseConnection,
        identity: InvocationIdentity,
        admission_permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            db,
            identity,
            _admission_permit: Arc::new(admission_permit),
            inner: DurableCommitObserver::new(
                pioneer_tools::apply_patch::history::CommitIntentJournal::new(),
                pioneer_tools::apply_patch::history::AppliedPatchLog::new(),
            ),
        }
    }

    fn db_error(error: anyhow::Error) -> ObserverError {
        ObserverError::new(
            ObserverErrorCode::Storage,
            format!("SQLite patch history: {error:#}"),
        )
    }

    fn report_from_record(
        record: &pioneer_tools::apply_patch::history::AppliedPatchRecord,
        delta: AppliedPatchDelta,
    ) -> ExecutionReport {
        let (status, failure) = replay_status_and_failure(&record.outcome);
        ExecutionReport {
            status,
            delta: delta.with_exactness(record.exactness.is_exact()),
            failure,
        }
    }

    fn report_from_terminal_intent(
        intent: &pioneer_tools::apply_patch::history::PatchCommitIntent,
    ) -> std::result::Result<ExecutionReport, ObserverError> {
        match intent.status {
            IntentStatus::AppliedNoChange => Ok(ExecutionReport {
                status: ExecutionStatus::Applied,
                delta: AppliedPatchDelta::empty().with_exactness(true),
                failure: None,
            }),
            IntentStatus::FailedNoChange => Ok(ExecutionReport {
                status: ExecutionStatus::Failed,
                delta: AppliedPatchDelta::empty().with_exactness(true),
                failure: Some(replay_diagnostic(
                    pioneer_tools::apply_patch::file_mutation::PatchStage::Record,
                    pioneer_tools::apply_patch::file_mutation::PatchErrorCode::Io,
                    "replayed patch failed before any filesystem change",
                )),
            }),
            IntentStatus::Rejected => Ok(ExecutionReport {
                status: ExecutionStatus::Rejected,
                delta: AppliedPatchDelta::empty().with_exactness(true),
                failure: None,
            }),
            IntentStatus::Gap | IntentStatus::Pending => Ok(ExecutionReport {
                status: ExecutionStatus::CommitStateUncertain,
                delta: AppliedPatchDelta::empty().with_exactness(false),
                failure: None,
            }),
            IntentStatus::Promoted => Err(ObserverError::new(
                ObserverErrorCode::Storage,
                "terminal patch marker is promoted but its immutable applied record is missing",
            )),
        }
    }

    pub(crate) async fn project_live(
        db: DatabaseConnection,
        thread_id: String,
        turn_id: String,
        authority: TurnDiffAuthority,
        final_state: bool,
    ) -> Result<TurnDiffState> {
        let records_store = SqliteAppliedPatchStore::new(db.clone());
        let intents = SqliteCommitIntentStore::new(db.clone());
        let replay = pioneer_tools::apply_patch::history::replay_turn_pages(
            &records_store,
            &intents,
            &thread_id,
            &turn_id,
            256,
        )
        .await
        .context("replay bounded patch history for live projection")?;
        for _ in 0..replay.pending_ordinals {
            patch_telemetry().record_pending_ordinal();
        }
        let state = TurnDiffState::from_aggregate(
            replay.aggregate,
            authority,
            replay.revision,
            final_state,
        );
        SqliteTurnDiffStore::new(db)
            .repair_live(&state)
            .await
            .context("persist live patch projection")?;
        Ok(state)
    }
}

impl CommitObserver for SqlitePatchObserver {
    fn check(
        &self,
        identity: &InvocationIdentity,
        plan_fingerprint: [u8; 32],
    ) -> std::result::Result<Option<ExecutionReport>, ObserverError> {
        if identity != &self.identity {
            return Err(ObserverError::new(
                ObserverErrorCode::DuplicateInvocation,
                "commit observer identity does not match the trusted invocation",
            ));
        }
        let identity_for_db = identity.clone();
        let state = run_sync_db(self.db.clone(), move |db| async move {
            let store = SqliteAppliedPatchStore::new(db.clone());
            let record = store.get(&identity_for_db).await?;
            let delta = match &record {
                Some(record) => Some(store.materialize_delta(record).await?),
                None => None,
            };
            let intent = SqliteCommitIntentStore::new(db)
                .get(&identity_for_db)
                .await?;
            Ok((record, intent, delta))
        })
        .map_err(Self::db_error)?;
        if let Some(record) = state.0 {
            if record.plan_fingerprint != plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Ok(Some(Self::report_from_record(
                &record.record,
                state.2.unwrap_or_else(AppliedPatchDelta::empty),
            )));
        }
        if let Some(intent) = state.1 {
            if intent.plan_fingerprint != plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            if matches!(
                intent.status,
                IntentStatus::Promoted
                    | IntentStatus::AppliedNoChange
                    | IntentStatus::FailedNoChange
                    | IntentStatus::Rejected
                    | IntentStatus::Gap
            ) {
                // A terminal marker is an idempotency result. It is never a
                // reason to touch the workspace again. Rejected/gap markers
                // carry their own safe replay outcome; a promoted marker
                // still requires its immutable record below.
                return Self::report_from_terminal_intent(&intent).map(Some);
            }
            return Err(ObserverError::new(
                ObserverErrorCode::Storage,
                format!(
                    "durable patch intent is {:?}; filesystem re-execution is forbidden",
                    intent.status
                ),
            ));
        }
        match self.inner.check(identity, plan_fingerprint) {
            Ok(Some(_)) => Err(ObserverError::new(
                ObserverErrorCode::Storage,
                "process-local terminal exists but durable patch history is missing",
            )),
            other => other,
        }
    }

    fn admit(
        &self,
        identity: &InvocationIdentity,
        admission: &CommitAdmission,
    ) -> std::result::Result<ObserverAdmission, ObserverError> {
        if identity != &self.identity {
            return Err(ObserverError::new(
                ObserverErrorCode::DuplicateInvocation,
                "commit observer identity does not match the trusted invocation",
            ));
        }
        let identity_for_db = identity.clone();
        let admission_for_db = admission.clone();
        let state = run_sync_db(self.db.clone(), move |db| async move {
            let applied = SqliteAppliedPatchStore::new(db.clone())
                .get(&identity_for_db)
                .await?;
            let delta = match &applied {
                Some(record) => Some(
                    SqliteAppliedPatchStore::new(db.clone())
                        .materialize_delta(record)
                        .await?,
                ),
                None => None,
            };
            let intent = SqliteCommitIntentStore::new(db)
                .get(&identity_for_db)
                .await?;
            if let Some(record) = applied {
                return Ok((Some(record), intent, delta));
            }
            if let Some(intent) = intent {
                if intent.plan_fingerprint != admission_for_db.plan_fingerprint
                    || intent.planned_operation_fingerprints
                        != admission_for_db.operation_fingerprints
                {
                    bail!("invocation identity was reused for a different immutable plan");
                }
                return Ok((None, Some(intent), None));
            }
            Ok((None, None, None))
        })
        .map_err(Self::db_error)?;
        if let Some(record) = state.0 {
            if record.plan_fingerprint != admission.plan_fingerprint {
                return Err(ObserverError::new(
                    ObserverErrorCode::DuplicateInvocation,
                    "invocation identity was reused for a different immutable plan",
                ));
            }
            return Ok(ObserverAdmission::Existing {
                report: Self::report_from_record(
                    &record.record,
                    state.2.unwrap_or_else(AppliedPatchDelta::empty),
                ),
            });
        }
        if let Some(intent) = state.1 {
            if matches!(
                intent.status,
                IntentStatus::Promoted
                    | IntentStatus::AppliedNoChange
                    | IntentStatus::FailedNoChange
                    | IntentStatus::Rejected
                    | IntentStatus::Gap
            ) {
                return Ok(ObserverAdmission::Existing {
                    report: Self::report_from_terminal_intent(&intent)?,
                });
            }
            return Err(ObserverError::new(
                ObserverErrorCode::Storage,
                format!(
                    "durable patch intent is {:?}; filesystem re-execution is forbidden",
                    intent.status
                ),
            ));
        }

        // A process-local terminal without a durable row is not a license to
        // allocate a fresh ordinal.  Doing so would leave a new pending intent
        // behind while the executor returns the old in-memory result.
        match self.inner.check(identity, admission.plan_fingerprint) {
            Ok(Some(_)) => {
                return Err(ObserverError::new(
                    ObserverErrorCode::Storage,
                    "process-local terminal exists but durable patch history is missing",
                ));
            }
            Ok(None) => {}
            Err(error) => return Err(error),
        }

        let identity_for_db = identity.clone();
        let admission_for_db = admission.clone();
        let outcome = run_sync_db(self.db.clone(), move |db| async move {
            SqliteCommitIntentStore::new(db)
                .begin_next_owned(
                    identity_for_db,
                    admission_for_db.plan_fingerprint,
                    admission_for_db.operation_fingerprints,
                    admission_for_db.recovery_plan,
                )
                .await
        })
        .map_err(Self::db_error)?;
        let intent = match outcome {
            pioneer_tools::apply_patch::history::BeginNextOutcome::Inserted(intent) => intent,
            pioneer_tools::apply_patch::history::BeginNextOutcome::Existing(intent) => {
                if matches!(
                    intent.status,
                    IntentStatus::Promoted
                        | IntentStatus::AppliedNoChange
                        | IntentStatus::FailedNoChange
                        | IntentStatus::Rejected
                        | IntentStatus::Gap
                ) {
                    return Ok(ObserverAdmission::Existing {
                        report: Self::report_from_terminal_intent(&intent)?,
                    });
                }
                return Err(ObserverError::new(
                    ObserverErrorCode::Storage,
                    format!(
                        "durable patch intent is {:?}; filesystem re-execution is forbidden",
                        intent.status
                    ),
                ));
            }
        };
        let snapshot_reservation = admission.recovery_plan.changes.iter().flat_map(|change| {
            [
                change.before.as_ref(),
                change.after.as_ref(),
                change.overwritten_destination.as_ref(),
            ]
            .into_iter()
            .flatten()
            .cloned()
        });
        let snapshots = snapshot_reservation.collect::<Vec<_>>();
        let domain = SnapshotDomain::new(
            format!("thread:{}", identity.thread_id),
            "pioneer",
            "thread_history",
        );
        if let Err(error) = run_sync_db(self.db.clone(), {
            let identity = identity.clone();
            move |db| async move {
                pioneer_tools::apply_patch::history::SqliteSnapshotStore::new(db)
                    .reserve_for_intent(&identity, &domain, snapshots.as_slice())
                    .await
            }
        }) {
            // The intent was admitted, but no filesystem operation has run.
            // Convert the failed storage reservation into a terminal no-change
            // marker so a later retry cannot mistake it for permission to
            // execute the patch.
            let _ = run_sync_db(self.db.clone(), {
                let identity = identity.clone();
                move |db| async move {
                    let store = SqliteCommitIntentStore::new(db);
                    if store.mark_rejected(&identity).await.is_ok() {
                        let _ = store.compact_terminal(&identity).await;
                    }
                    Ok(())
                }
            });
            return Err(Self::db_error(error));
        }
        if let Err(error) =
            self.inner
                .seed_admission_with_admission(identity, intent.commit_ordinal, admission)
        {
            let _ = run_sync_db(self.db.clone(), {
                let identity = identity.clone();
                move |db| async move {
                    let store = SqliteCommitIntentStore::new(db);
                    if store.mark_rejected(&identity).await.is_ok() {
                        let _ = store.compact_terminal(&identity).await;
                    }
                    Ok(())
                }
            });
            return Err(error);
        }
        match self.inner.admit(identity, admission) {
            Ok(admission) => Ok(admission),
            Err(error) => {
                let _ = run_sync_db(self.db.clone(), {
                    let identity = identity.clone();
                    move |db| async move {
                        let store = SqliteCommitIntentStore::new(db);
                        if store.mark_rejected(&identity).await.is_ok() {
                            let _ = store.compact_terminal(&identity).await;
                        }
                        Ok(())
                    }
                });
                Err(error)
            }
        }
    }

    fn on_committed(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        change: &CommittedPatchChange,
    ) -> std::result::Result<(), ObserverError> {
        self.inner.on_committed(identity, ordinal, change)?;
        let intent = self
            .inner
            .intent_journal()
            .get(identity)
            .map_err(|error| ObserverError::new(ObserverErrorCode::Storage, error.to_string()))?
            .ok_or_else(|| {
                ObserverError::new(
                    ObserverErrorCode::AfterMutation,
                    "local patch intent disappeared after a committed operation",
                )
            })?;
        run_sync_db(self.db.clone(), move |db| async move {
            SqliteCommitIntentStore::new(db)
                .update_progress(&intent)
                .await
                .context("persist committed patch progress")
        })
        .map_err(Self::db_error)
    }

    fn on_terminal(
        &self,
        identity: &InvocationIdentity,
        ordinal: CommitOrdinal,
        report: &ExecutionReport,
    ) -> std::result::Result<(), ObserverError> {
        // The local observer is only a process-local progress buffer.  It can
        // fail after the filesystem has committed (for example when its
        // journal append is interrupted), so the durable adapter must build
        // the record from the executor's exact terminal delta rather than
        // treating a missing local record as a no-op.
        let local_terminal_error = self.inner.on_terminal(identity, ordinal, report).err();
        let identity_for_db = identity.clone();
        let changes_for_db = report.delta.changes.clone();
        let side_effects_for_db = report.delta.side_effects.clone();
        let status = report.status;
        let outcome = record_outcome(report);
        let should_record =
            !report.delta.is_empty() || matches!(status, ExecutionStatus::CommitStateUncertain);
        let persist_started = std::time::Instant::now();
        let persisted = run_sync_db(self.db.clone(), move |db| async move {
            let intent_store = SqliteCommitIntentStore::new(db.clone());
            let applied_store = SqliteAppliedPatchStore::new(db.clone());
            let existing_record = applied_store.get(&identity_for_db).await?;
            let intent = intent_store.get(&identity_for_db).await?;
            if let Some(record) = existing_record {
                if record.record.commit_ordinal != ordinal {
                    bail!(
                        "durable patch record ordinal does not match terminal invocation ordinal"
                    );
                }
                if !outcomes_compatible(&record.record.outcome, &outcome)
                    || record.record.changes
                        != changes_for_db
                            .iter()
                            .map(pioneer_tools::apply_patch::history::DurablePatchChange::from)
                            .collect::<Vec<_>>()
                    || record.record.side_effects != side_effects_for_db
                {
                    bail!("terminal retry does not match the immutable durable patch result");
                }
                // A retry may arrive after record insertion but before the
                // separate intent-progress/compaction transaction committed.
                // Repair only durable bookkeeping and projection; never try to
                // append or execute the filesystem mutation a second time.
                if let Some(intent) = intent {
                    if intent.plan_fingerprint != record.plan_fingerprint {
                        bail!("durable patch record changed immutable plan before retry");
                    }
                    if matches!(intent.status, IntentStatus::Pending) {
                        let mut completed = intent;
                        completed.status = if matches!(
                            &record.record.outcome,
                            AppliedPatchRecordOutcome::Gap { .. }
                        ) {
                            IntentStatus::Gap
                        } else {
                            IntentStatus::Promoted
                        };
                        intent_store
                            .update_progress(&completed)
                            .await
                            .context("repair applied patch intent after durable retry")?;
                    }
                }
                Self::project_live(
                    db.clone(),
                    identity_for_db.thread_id.clone(),
                    identity_for_db.turn_id.clone(),
                    record.record.authority,
                    false,
                )
                .await
                .context("repair applied patch projection after durable retry")?;
                let _ = intent_store.compact_terminal(&identity_for_db).await?;
                return Ok(());
            }
            let intent = intent
                .ok_or_else(|| anyhow!("durable patch intent disappeared before promotion"))?;
            if intent.commit_ordinal != ordinal {
                bail!("durable patch intent changed before promotion");
            }
            if !matches!(intent.status, IntentStatus::Pending) {
                // The terminal marker is already authoritative.  This is the
                // normal path after a persistence retry races with cleanup.
                return Ok(());
            }
            if !should_record {
                // Rejected/preflight/known no-op calls are terminal intent
                // records, not file history.  An uncertain terminal with no
                // captured change is deliberately handled below as a gap.
                match status {
                    ExecutionStatus::Applied => intent_store
                        .mark_applied_no_change(&identity_for_db)
                        .await
                        .context("mark exact no-change patch intent")?,
                    ExecutionStatus::Failed | ExecutionStatus::Partial => intent_store
                        .mark_failed_no_change(&identity_for_db)
                        .await
                        .context("mark failed no-change patch intent")?,
                    ExecutionStatus::Rejected => intent_store
                        .mark_rejected(&identity_for_db)
                        .await
                        .context("reject no-change patch intent")?,
                    ExecutionStatus::CommitStateUncertain => unreachable!(
                        "uncertain empty patch is recorded as an explicit incomplete record"
                    ),
                }
                intent_store
                    .compact_terminal(&identity_for_db)
                    .await
                    .context("compact terminal no-change patch intent")?;
                return Ok(());
            }
            let mut record = AppliedPatchRecord::new(
                identity_for_db.clone(),
                intent.commit_ordinal,
                outcome,
                changes_for_db
                    .iter()
                    .map(pioneer_tools::apply_patch::history::DurablePatchChange::from)
                    .collect(),
            );
            record.side_effects = side_effects_for_db;
            if !record.side_effects.exact {
                record.exactness =
                    pioneer_tools::apply_patch::history::PatchRecordExactness::Uncertain;
            }
            if let Some(plan) = intent.recovery_plan.as_ref() {
                record.environment_id = plan.environment_id.clone();
                record.authority = plan.authority;
                record.provenance = match plan.authority {
                    TurnDiffAuthority::ManagedClaudePatchEngine => {
                        PatchHistoryProvenance::ManagedClaude
                    }
                    _ => PatchHistoryProvenance::NativeEngine,
                };
            }
            record.committed_at_unix_ms = chrono::Utc::now().timestamp_millis();
            let snapshots = changes_for_db
                .iter()
                .flat_map(|change| {
                    [
                        change.before.as_ref(),
                        change.after.as_ref(),
                        change.overwritten_destination.as_ref(),
                    ]
                    .into_iter()
                    .flatten()
                    .cloned()
                })
                .collect::<Vec<_>>();
            let domain = SnapshotDomain::new(
                format!("thread:{}", identity_for_db.thread_id),
                "pioneer",
                "thread_history",
            );
            SqliteAppliedPatchStore::new(db.clone())
                .insert_with_snapshots(record.clone(), intent.plan_fingerprint, &domain, &snapshots)
                .await
                .context("promote applied patch record")?;
            let mut promoted = intent;
            promoted.committed_changes = changes_for_db;
            promoted.status = if matches!(record.outcome, AppliedPatchRecordOutcome::Gap { .. }) {
                IntentStatus::Gap
            } else {
                IntentStatus::Promoted
            };
            intent_store
                .update_progress(&promoted)
                .await
                .context("mark applied patch intent promoted")?;
            Self::project_live(
                db.clone(),
                identity_for_db.thread_id.clone(),
                identity_for_db.turn_id.clone(),
                record.authority,
                false,
            )
            .await
            .context("project applied patch history")?;
            intent_store
                .compact_terminal(&record.identity)
                .await
                .context("compact promoted patch intent")?;
            Ok(())
        });
        match persisted {
            Ok(()) => {
                if should_record {
                    patch_telemetry().record_applied_record_append(persist_started.elapsed());
                    if let Ok(metrics) = run_sync_db(self.db.clone(), |db| async move {
                        pioneer_tools::apply_patch::history::SqliteSnapshotStore::new(db)
                            .metrics()
                            .await
                    }) {
                        patch_telemetry().record_snapshot_metrics(
                            metrics.logical_bytes,
                            metrics.physical_bytes,
                            metrics.references,
                            metrics.referenced_logical_bytes,
                            0,
                            0,
                        );
                    }
                }
            }
            Err(error) => {
                patch_telemetry().record_projection_lag();
                tracing::error!(
                    "apply_patch durable history persistence or projection failed; operator reconciliation is required"
                );
                return Err(Self::db_error(error));
            }
        }
        // A local publication failure is recoverable once the SQLite record
        // and projection are durable.  Do not downgrade the result or invite
        // a re-execution after the authoritative durable path succeeded.
        if local_terminal_error.is_some() {
            tracing::warn!(
                "process-local apply_patch observer publication failed after durable SQLite promotion"
            );
        }
        Ok(())
    }

    fn record(
        &self,
        identity: &InvocationIdentity,
    ) -> std::result::Result<Option<(StoredPatchRecord, Vec<CommittedPatchChange>)>, ObserverError>
    {
        let local = self.inner.record(identity)?;
        let identity_for_db = identity.clone();
        let durable = run_sync_db(self.db.clone(), move |db| async move {
            SqliteAppliedPatchStore::new(db)
                .get(&identity_for_db)
                .await
                .context("load promoted patch record")
        })
        .map_err(Self::db_error)?;
        match durable {
            Some(record) => Ok(Some((
                record,
                local.map(|(_, changes)| changes).unwrap_or_default(),
            ))),
            None => Ok(local),
        }
    }

    fn projection_revision(
        &self,
        identity: &InvocationIdentity,
    ) -> std::result::Result<Option<u64>, ObserverError> {
        if identity != &self.identity {
            return Err(ObserverError::new(
                ObserverErrorCode::DuplicateInvocation,
                "commit observer identity does not match the trusted invocation",
            ));
        }
        let identity_for_db = identity.clone();
        run_sync_db(self.db.clone(), move |db| async move {
            // A projection row by itself is not evidence that this record was
            // included: a record can be durably promoted while projection
            // repair is still pending.  Reconstruct the monotonic stream
            // watermark and record count from the source log/intent tables,
            // then only advertise RecordedAndProjected when the persisted
            // projection covers that exact source state.
            let record = SqliteAppliedPatchStore::new(db.clone())
                .get(&identity_for_db)
                .await?;
            let (record_count, max_record_ordinal) = SqliteAppliedPatchStore::new(db.clone())
                .record_summary_for_turn(&identity_for_db.thread_id, &identity_for_db.turn_id)
                .await?;
            let intents = SqliteCommitIntentStore::new(db.clone());
            let max_status_ordinal = intents
                .max_ordinal_for_turn(&identity_for_db.thread_id, &identity_for_db.turn_id)
                .await?;
            let expected_revision = max_record_ordinal
                .into_iter()
                .chain(max_status_ordinal)
                .map(|ordinal| ordinal.0)
                .max()
                .map(|ordinal| ordinal.saturating_add(1))
                .unwrap_or(0);
            let state = SqliteTurnDiffStore::new(db)
                .get(&identity_for_db.thread_id, &identity_for_db.turn_id)
                .await?;
            Ok(match (record, state) {
                (Some(_record), Some(state))
                    if state.thread_id == identity_for_db.thread_id
                        && state.turn_id == identity_for_db.turn_id
                        && state.revision == expected_revision
                        && state.record_count == record_count =>
                {
                    Some(state.revision)
                }
                _ => None,
            })
        })
        .map_err(Self::db_error)
    }
}

fn record_outcome(report: &ExecutionReport) -> AppliedPatchRecordOutcome {
    match report.status {
        ExecutionStatus::Applied => AppliedPatchRecordOutcome::Applied,
        ExecutionStatus::Partial | ExecutionStatus::Failed => AppliedPatchRecordOutcome::Partial {
            failed_stage: report
                .failure
                .as_ref()
                .map(|failure| failure.stage)
                .unwrap_or(pioneer_tools::apply_patch::file_mutation::PatchStage::Commit),
            error_code: report
                .failure
                .as_ref()
                .map(|failure| failure.code)
                .unwrap_or(pioneer_tools::apply_patch::file_mutation::PatchErrorCode::Io),
        },
        ExecutionStatus::Rejected => AppliedPatchRecordOutcome::Gap {
            reason: "rejected after a committed delta".to_owned(),
        },
        ExecutionStatus::CommitStateUncertain => AppliedPatchRecordOutcome::CommitStateUncertain,
    }
}

fn outcomes_compatible(
    stored: &AppliedPatchRecordOutcome,
    retry: &AppliedPatchRecordOutcome,
) -> bool {
    // A terminal persistence failure is reported back to the executor as
    // `CommitStateUncertain` even when the immutable record was already
    // inserted successfully.  A retry therefore may carry a different
    // wrapper outcome (`Applied`/`Partial` versus `CommitStateUncertain`),
    // while the change list below still has to match byte-for-byte.  Keep the
    // durable outcome authoritative and accept that wrapper rather than
    // turning an idempotent retry into a false immutable-result conflict.
    stored == retry || stored.is_uncertain() || retry.is_uncertain()
}

fn replay_status_and_failure(
    outcome: &AppliedPatchRecordOutcome,
) -> (
    ExecutionStatus,
    Option<pioneer_tools::apply_patch::file_mutation::PatchDiagnostic>,
) {
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
                pioneer_tools::apply_patch::file_mutation::PatchStage::Recover,
                pioneer_tools::apply_patch::file_mutation::PatchErrorCode::CommitStateUncertain,
                "replayed patch has an uncertain filesystem commit state",
            )),
        ),
        AppliedPatchRecordOutcome::Gap { .. } => (
            ExecutionStatus::CommitStateUncertain,
            Some(replay_diagnostic(
                pioneer_tools::apply_patch::file_mutation::PatchStage::Recover,
                pioneer_tools::apply_patch::file_mutation::PatchErrorCode::CommitStateUncertain,
                "replayed patch history contains an unresolved filesystem gap",
            )),
        ),
    }
}

fn replay_diagnostic(
    stage: pioneer_tools::apply_patch::file_mutation::PatchStage,
    code: pioneer_tools::apply_patch::file_mutation::PatchErrorCode,
    message: &str,
) -> pioneer_tools::apply_patch::file_mutation::PatchDiagnostic {
    pioneer_tools::apply_patch::file_mutation::PatchDiagnostic {
        code,
        stage,
        message: message.to_owned(),
        retryability: match code {
            pioneer_tools::apply_patch::file_mutation::PatchErrorCode::StaleFile => {
                pioneer_tools::apply_patch::file_mutation::Retryability::RetryAfterRead
            }
            pioneer_tools::apply_patch::file_mutation::PatchErrorCode::LockTimeout => {
                pioneer_tools::apply_patch::file_mutation::Retryability::RetryAfterDelay
            }
            pioneer_tools::apply_patch::file_mutation::PatchErrorCode::CommitStateUncertain => {
                pioneer_tools::apply_patch::file_mutation::Retryability::RecoverOnly
            }
            _ => pioneer_tools::apply_patch::file_mutation::Retryability::Never,
        },
        operation_index: None,
        path: None,
        guard_horizon: None,
    }
}

fn run_sync_db<T, F, Fut>(db: DatabaseConnection, operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(DatabaseConnection) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    let join = std::thread::Builder::new()
        .name("apply-patch-history".to_owned())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("build synchronous patch history runtime")?;
            runtime.block_on(async move {
                tokio::time::timeout(PATCH_HISTORY_SYNC_TIMEOUT, operation(db))
                    .await
                    .context("synchronous patch history operation timed out")?
            })
        })
        .context("spawn synchronous patch history worker")?;
    join.join()
        .map_err(|_| anyhow!("synchronous patch history worker panicked"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn saturated_history_admission_is_bounded_and_recovers_after_release() {
        let semaphore = Arc::new(Semaphore::new(1));
        let held = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("initial permit");

        let error =
            reserve_patch_history_capacity_from(semaphore.clone(), Duration::from_millis(10))
                .await
                .expect_err("a saturated writer lane must fail closed within its deadline");
        assert!(
            format!("{error:#}").contains("admission capacity timed out"),
            "unexpected admission error: {error:#}"
        );

        drop(held);
        let permit =
            reserve_patch_history_capacity_from(semaphore.clone(), Duration::from_millis(100))
                .await
                .expect("capacity must recover after the terminal observer releases its permit");
        assert_eq!(semaphore.available_permits(), 0);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 1);
    }
}
