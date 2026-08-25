//! Database-backed commit-intent journal.

use crate::apply_patch::file_mutation::{TargetKind, metadata_fingerprint_for_path};
use crate::apply_patch::history::{
    CommitOrdinal, IntentStatus, InvocationIdentity, PatchCommitIntent, PatchRecoveryPlan,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud::patch_history as crud;
use sea_orm::{DatabaseConnection, TransactionTrait};
use serde::{Serialize, de::DeserializeOwned};
use sha2::Digest;
use std::collections::HashMap;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;

const INTENT_ADMISSION_TIMEOUT: Duration = Duration::from_secs(5);
// Recovery is allowed to inspect only the same bounded text-file budget as a
// normal patch.  A process may have replaced a target after the intent was
// written, so the current on-disk size cannot be trusted to remain within the
// plan's original snapshot budget.  Keep both per-file and aggregate reads
// bounded; exceeding either limit makes the outcome uncertain rather than
// allocating an attacker-controlled amount of memory during startup.
const MAX_RECOVERY_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RECOVERY_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
// Startup recovery must never materialize an unbounded number of durable
// intents.  The cursor below is ordered by the timestamp plus immutable
// identity tie-breakers, so terminalizing a page cannot skip later rows.
const MAX_RECOVERY_PAGE_SIZE: i64 = 256;
// A recovery plan may retain bounded source and successor snapshots for one
// patch. The default patch limits permit 128 MiB of observed input and 64 MiB
// of generated output; JSON/base64 framing and move-overwrite lineage require
// a larger, still finite SQLite boundary. The matching encode guard below
// ensures no row is ever written above this decode limit.
const MAX_INTENT_JSON_BYTES: usize = 384 * 1024 * 1024;
const MAX_INTENT_OPERATIONS: usize = 256;
const MAX_INTENT_CHANGES: usize = 256;
const MAX_INTENT_SNAPSHOT_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_INTENT_PARENT_DIRECTORIES: usize = 1024;
const MAX_INTENT_PATH_BYTES: usize = 4096;
const MAX_INTENT_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_INTENT_ID_BYTES: usize = 4096;

// SQLite does not provide a portable per-turn `MAX(ordinal)+1` allocator
// under two deferred write transactions. Serialize only admissions for the
// same logical turn; unrelated turns must not queue behind one stalled
// database transaction. Weak entries keep this process-local registry bounded,
// while the unique database key remains the final foreign-writer guard.
type AdmissionKey = (String, String);
type AdmissionRegistry = StdMutex<HashMap<AdmissionKey, Weak<Mutex<()>>>>;
static INTENT_ORDINAL_ADMISSION: OnceLock<AdmissionRegistry> = OnceLock::new();

fn turn_admission_lock(identity: &InvocationIdentity) -> Result<Arc<Mutex<()>>> {
    let registry = INTENT_ORDINAL_ADMISSION.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .map_err(|_| anyhow!("patch intent admission registry is poisoned"))?;
    registry.retain(|_, lock| lock.strong_count() > 0);
    let key = (identity.thread_id.clone(), identity.turn_id.clone());
    if let Some(lock) = registry.get(&key).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    registry.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}

/// Result of an under-lock intent admission.  `Inserted` is the only result
/// that may be handed to the filesystem executor.  `Existing` is an
/// idempotency hit and must never cause a second filesystem execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BeginNextOutcome {
    Inserted(PatchCommitIntent),
    Existing(PatchCommitIntent),
}

/// SQLite-backed operational intent state. AppliedPatchRecord remains the
/// immutable source log; this table only closes the crash window around the
/// filesystem executor and is safe to replay or terminalize as a gap.
#[derive(Clone)]
pub struct SqliteCommitIntentStore {
    db: DatabaseConnection,
}

#[derive(Clone, Debug)]
struct PendingCursor {
    updated_at: chrono::DateTime<chrono::FixedOffset>,
    thread_id: String,
    turn_id: String,
    invocation_id: String,
}

#[derive(Clone, Debug)]
struct PendingKey {
    identity: InvocationIdentity,
}

impl SqliteCommitIntentStore {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Allocate the ordinal and persist the exact under-lock intent.  Unlike
    /// the historical reservation API, this method is called only after the
    /// executor has acquired and revalidated its complete target lock set.
    /// The boolean outcome is explicit so a racing retry cannot mistake an
    /// existing durable intent for permission to execute again.
    pub async fn begin_next_owned(
        &self,
        identity: InvocationIdentity,
        plan_fingerprint: [u8; 32],
        planned_operation_fingerprints: Vec<[u8; 32]>,
        recovery_plan: PatchRecoveryPlan,
    ) -> Result<BeginNextOutcome> {
        let (intent, inserted) = self
            .begin_next_internal(
                identity,
                plan_fingerprint,
                planned_operation_fingerprints,
                Some(recovery_plan),
            )
            .await?;
        Ok(if inserted {
            BeginNextOutcome::Inserted(intent)
        } else {
            BeginNextOutcome::Existing(intent)
        })
    }

    async fn begin_next_internal(
        &self,
        identity: InvocationIdentity,
        plan_fingerprint: [u8; 32],
        planned_operation_fingerprints: Vec<[u8; 32]>,
        recovery_plan: Option<PatchRecoveryPlan>,
    ) -> Result<(PatchCommitIntent, bool)> {
        validate_identity(&identity)?;
        if plan_fingerprint.iter().all(|byte| *byte == 0) {
            bail!("patch intent plan fingerprint must not be all zeroes");
        }
        validate_operation_fingerprints(&planned_operation_fingerprints)?;
        if let Some(plan) = recovery_plan.as_ref() {
            validate_recovery_plan(plan)?;
        }
        let ordinal_lock = turn_admission_lock(&identity)?;
        let _ordinal_guard = timeout(INTENT_ADMISSION_TIMEOUT, ordinal_lock.lock())
            .await
            .context("next patch intent turn admission timed out")?;
        let transaction = timeout(INTENT_ADMISSION_TIMEOUT, self.db.begin())
            .await
            .context("next patch intent database admission timed out")?
            .context("begin next patch intent")?;
        let existing = crud::find_patch_commit_intent(
            &transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("query existing next patch intent")?;
        if let Some(row) = existing {
            let current = decode_row(&identity, &row)?;
            if current.plan_fingerprint != plan_fingerprint
                || current.planned_operation_fingerprints != planned_operation_fingerprints
                || current.recovery_plan != recovery_plan
            {
                transaction.rollback().await.ok();
                bail!("patch intent identity is bound to a different immutable plan");
            }
            transaction
                .commit()
                .await
                .context("commit existing next patch intent")?;
            return Ok((current, false));
        }
        if let Some(row) = crud::find_patch_commit_terminal(
            &transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("query compacted next patch intent")?
        {
            let current = decode_terminal_row(&identity, &row)?;
            if current.plan_fingerprint != plan_fingerprint
                || current.planned_operation_fingerprints != planned_operation_fingerprints
            {
                transaction.rollback().await.ok();
                bail!("patch intent identity is bound to a different immutable plan");
            }
            transaction
                .commit()
                .await
                .context("commit existing compacted next patch intent")?;
            return Ok((current, false));
        }
        let next_ordinal =
            crud::next_patch_commit_ordinal(&transaction, &identity.thread_id, &identity.turn_id)
                .await
                .context("allocate next patch intent ordinal")?;
        let commit_ordinal = CommitOrdinal(sqlite_decode_ordinal(next_ordinal)?);
        let intent = PatchCommitIntent {
            identity,
            commit_ordinal,
            plan_fingerprint,
            planned_operation_fingerprints,
            recovery_plan,
            committed_changes: Vec::new(),
            status: IntentStatus::Pending,
        };
        let operations_json = encode_bounded_json(
            &intent.planned_operation_fingerprints,
            "patch intent operation fingerprints",
        )?;
        let recovery_json =
            encode_bounded_json(&intent.recovery_plan, "patch intent recovery plan")?;
        crud::insert_patch_commit_intent(
            &transaction,
            crud::PatchCommitIntentWrite {
                thread_id: intent.identity.thread_id.clone(),
                turn_id: intent.identity.turn_id.clone(),
                invocation_id: intent.identity.invocation_id.clone(),
                commit_ordinal: sqlite_ordinal(intent.commit_ordinal)?,
                plan_fingerprint: intent.plan_fingerprint.to_vec(),
                operations_json,
                recovery_json,
                progress_json: "[]".to_owned(),
                status: status_name(IntentStatus::Pending).to_owned(),
            },
        )
        .await
        .context("insert next patch intent")?;
        transaction
            .commit()
            .await
            .context("commit next patch intent")?;
        Ok((intent, true))
    }

    pub async fn update_progress(&self, intent: &PatchCommitIntent) -> Result<()> {
        validate_identity(&intent.identity)?;
        if intent.plan_fingerprint.iter().all(|byte| *byte == 0) {
            bail!("patch intent plan fingerprint must not be all zeroes");
        }
        let transaction = timeout(INTENT_ADMISSION_TIMEOUT, self.db.begin())
            .await
            .context("patch intent progress database admission timed out")?
            .context("begin patch intent progress")?;
        let expected_ordinal = sqlite_ordinal(intent.commit_ordinal)?;
        let current = crud::find_patch_commit_intent(
            &transaction,
            &intent.identity.thread_id,
            &intent.identity.turn_id,
            &intent.identity.invocation_id,
        )
        .await
        .context("load patch intent before progress update")?;
        let Some(current) = current.filter(|row| row.commit_ordinal == expected_ordinal) else {
            transaction.rollback().await.ok();
            bail!("patch intent disappeared before progress update");
        };
        let stored_fingerprint: [u8; 32] = current
            .plan_fingerprint
            .try_into()
            .map_err(|_| anyhow!("patch intent plan fingerprint must be 32 bytes"))?;
        let stored_operations: Vec<[u8; 32]> = decode_bounded_json(
            &current.operations_json,
            "stored patch intent operation fingerprints",
        )?;
        validate_operation_fingerprints(&stored_operations)?;
        let stored_recovery: Option<PatchRecoveryPlan> =
            decode_bounded_json(&current.recovery_json, "stored patch intent recovery plan")?;
        if let Some(plan) = stored_recovery.as_ref() {
            validate_recovery_plan(plan)?;
        }
        if stored_fingerprint != intent.plan_fingerprint
            || stored_operations != intent.planned_operation_fingerprints
            || stored_recovery != intent.recovery_plan
        {
            transaction.rollback().await.ok();
            bail!("patch intent immutable plan does not match the durable record");
        }
        let current_progress: Vec<crate::apply_patch::history::CommittedPatchChange> =
            decode_bounded_json(&current.progress_json, "existing patch intent progress")?;
        validate_progress(&current_progress)?;
        validate_progress(&intent.committed_changes)?;
        if intent.committed_changes.len() < current_progress.len()
            || intent.committed_changes[..current_progress.len()] != current_progress[..]
        {
            transaction.rollback().await.ok();
            bail!("patch intent progress must extend the already durable committed prefix");
        }
        let progress_json =
            encode_bounded_json(&intent.committed_changes, "patch intent committed progress")?;
        let current_status = parse_status(&current.status)?;
        if current_status != IntentStatus::Pending
            && (current_status != intent.status || current_progress != intent.committed_changes)
        {
            transaction.rollback().await.ok();
            bail!(
                "patch intent status transition {:?} -> {:?} or progress replacement is not allowed",
                current_status,
                intent.status
            );
        }
        let updated = crud::update_patch_commit_intent_progress(
            &transaction,
            &intent.identity.thread_id,
            &intent.identity.turn_id,
            &intent.identity.invocation_id,
            expected_ordinal,
            status_name(current_status),
            progress_json,
            status_name(intent.status).to_owned(),
        )
        .await
        .context("update patch intent progress")?;
        if updated == 0 {
            transaction.rollback().await.ok();
            bail!("patch intent progress update lost a concurrent transition");
        }
        transaction
            .commit()
            .await
            .context("commit patch intent progress")?;
        Ok(())
    }

    pub async fn mark_rejected(&self, identity: &InvocationIdentity) -> Result<()> {
        let intent = self
            .get(identity)
            .await?
            .ok_or_else(|| anyhow!("patch intent is missing"))?;
        if matches!(intent.status, IntentStatus::Pending) {
            let mut rejected = intent;
            rejected.status = IntentStatus::Rejected;
            self.update_progress(&rejected).await?;
        }
        Ok(())
    }

    pub async fn mark_applied_no_change(&self, identity: &InvocationIdentity) -> Result<()> {
        let intent = self
            .get(identity)
            .await?
            .ok_or_else(|| anyhow!("patch intent is missing"))?;
        if matches!(intent.status, IntentStatus::Pending) {
            let mut completed = intent;
            completed.status = IntentStatus::AppliedNoChange;
            self.update_progress(&completed).await?;
        }
        Ok(())
    }

    pub async fn mark_failed_no_change(&self, identity: &InvocationIdentity) -> Result<()> {
        let intent = self
            .get(identity)
            .await?
            .ok_or_else(|| anyhow!("patch intent is missing"))?;
        if matches!(intent.status, IntentStatus::Pending) {
            let mut failed = intent;
            failed.status = IntentStatus::FailedNoChange;
            self.update_progress(&failed).await?;
        }
        Ok(())
    }

    pub async fn get(&self, identity: &InvocationIdentity) -> Result<Option<PatchCommitIntent>> {
        validate_identity(identity)?;
        let row = crud::find_patch_commit_intent(
            &self.db,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("query patch intent")?;
        if let Some(row) = row {
            return decode_row(identity, &row).map(Some);
        }
        crud::find_patch_commit_terminal(
            &self.db,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("query compacted patch intent")?
        .map(|row| decode_terminal_row(identity, &row))
        .transpose()
    }

    async fn pending_page(
        &self,
        cursor: Option<&PendingCursor>,
    ) -> Result<(Vec<PendingKey>, Option<PendingCursor>)> {
        let cursor = cursor.map(|cursor| crud::PendingPatchIntentCursor {
            updated_at: cursor.updated_at,
            thread_id: cursor.thread_id.clone(),
            turn_id: cursor.turn_id.clone(),
            invocation_id: cursor.invocation_id.clone(),
        });
        let rows = crud::list_pending_patch_commit_intents(
            &self.db,
            cursor.as_ref(),
            MAX_RECOVERY_PAGE_SIZE as u64,
        )
        .await
        .context("query a bounded page of pending patch intents")?;
        let next_cursor = rows.last().map(|row| PendingCursor {
            updated_at: row.updated_at,
            thread_id: row.thread_id.clone(),
            turn_id: row.turn_id.clone(),
            invocation_id: row.invocation_id.clone(),
        });
        let keys = rows
            .iter()
            .map(|row| {
                Ok(PendingKey {
                    identity: InvocationIdentity::new(
                        row.thread_id.clone(),
                        row.turn_id.clone(),
                        row.invocation_id.clone(),
                    )
                    .map_err(|error| anyhow!("invalid pending patch identity: {error:?}"))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((keys, next_cursor))
    }

    pub async fn authority_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<crate::apply_patch::history::TurnDiffAuthority>> {
        let mut selected_authority = None;
        let mut intent_cursor = None;
        loop {
            let rows = crud::list_patch_commit_intents_for_turn(
                &self.db,
                thread_id,
                turn_id,
                intent_cursor,
                MAX_RECOVERY_PAGE_SIZE as u64,
            )
            .await
            .context("query bounded patch turn authority page")?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let identity = InvocationIdentity::new(
                    thread_id.to_owned(),
                    turn_id.to_owned(),
                    row.invocation_id.clone(),
                )
                .map_err(|error| anyhow!("invalid patch authority identity: {error:?}"))?;
                if let Some(plan) = self
                    .get(&identity)
                    .await?
                    .and_then(|intent| intent.recovery_plan)
                {
                    if let Some(existing) = selected_authority
                        && existing != plan.authority
                    {
                        bail!("patch turn contains conflicting durable authorities");
                    }
                    selected_authority = Some(plan.authority);
                }
            }
            let next_cursor = rows
                .last()
                .expect("non-empty authority page")
                .commit_ordinal;
            if rows.len() < usize::try_from(MAX_RECOVERY_PAGE_SIZE).unwrap_or(usize::MAX) {
                break;
            }
            if intent_cursor == Some(next_cursor) {
                bail!("patch turn authority cursor did not advance");
            }
            intent_cursor = Some(next_cursor);
        }
        // Terminal intents may already be compacted.  Applied records retain
        // the authoritative runtime profile, so use them as the durable
        // source once the operational intent row is gone.  DISTINCT keeps
        // this fallback bounded even for a very large turn.
        let rows = crud::distinct_applied_patch_authorities_for_turn(&self.db, thread_id, turn_id)
            .await
            .context("query applied patch turn authority")?;
        for authority_name in rows {
            let parsed = parse_authority(&authority_name)?;
            if let Some(existing) = selected_authority
                && existing != parsed
            {
                bail!("patch turn contains conflicting durable authorities");
            }
            selected_authority = Some(parsed);
        }
        let rows = crud::distinct_patch_terminal_authorities_for_turn(&self.db, thread_id, turn_id)
            .await
            .context("query compacted patch turn authority")?;
        for authority_name in rows {
            let parsed = parse_authority(&authority_name)?;
            if let Some(existing) = selected_authority
                && existing != parsed
            {
                bail!("patch turn contains conflicting durable authorities");
            }
            selected_authority = Some(parsed);
        }
        Ok(selected_authority)
    }

    /// Move a terminal operational intent into a compact marker.  The marker
    /// preserves invocation/ordinal idempotency and history coverage while
    /// dropping the recovery plan and committed snapshot progress.  This must
    /// be called only after the immutable record (or no-change projection) is
    /// durable; a crash before this call simply leaves the full intent for
    /// startup recovery.
    pub async fn compact_terminal(&self, identity: &InvocationIdentity) -> Result<bool> {
        validate_identity(identity)?;
        let transaction = timeout(INTENT_ADMISSION_TIMEOUT, self.db.begin())
            .await
            .context("patch intent compaction database admission timed out")?
            .context("begin patch intent compaction")?;
        let Some(row) = crud::find_patch_commit_intent(
            &transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("load terminal patch intent for compaction")?
        else {
            transaction.rollback().await.ok();
            return Ok(false);
        };
        let ordinal = row.commit_ordinal;
        let fingerprint = row.plan_fingerprint;
        let operations_json = row.operations_json;
        let recovery_json = row.recovery_json;
        let progress_json = row.progress_json;
        let status = row.status;
        let recovery_plan: Option<PatchRecoveryPlan> =
            decode_bounded_json(&recovery_json, "terminal patch intent recovery plan")?;
        if let Some(plan) = recovery_plan.as_ref() {
            validate_recovery_plan(plan)?;
        }
        let authority = recovery_plan
            .as_ref()
            .map(|plan| plan.authority)
            .unwrap_or(crate::apply_patch::history::TurnDiffAuthority::Unsupported);
        let progress: Vec<crate::apply_patch::history::CommittedPatchChange> = decode_bounded_json(
            &progress_json,
            "terminal patch intent progress during compaction",
        )?;
        validate_progress(&progress)?;
        let operations: Vec<[u8; 32]> =
            decode_bounded_json(&operations_json, "terminal patch intent operations")?;
        validate_operation_fingerprints(&operations)?;
        if !matches!(
            status.as_str(),
            "promoted" | "applied_no_change" | "failed_no_change" | "rejected" | "gap"
        ) {
            transaction.rollback().await.ok();
            bail!("only terminal patch intents may be compacted");
        }
        let record_id = crud::find_applied_patch_record_by_invocation(
            &transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("lookup patch record for intent compaction")?
        .map(|row| row.id);
        crud::upsert_patch_commit_terminal(
            &transaction,
            crud::PatchCommitTerminalWrite {
                thread_id: identity.thread_id.clone(),
                turn_id: identity.turn_id.clone(),
                invocation_id: identity.invocation_id.clone(),
                commit_ordinal: ordinal,
                plan_fingerprint: fingerprint,
                operations_json,
                authority: authority_name(authority).to_owned(),
                status,
                record_id,
            },
        )
        .await
        .context("insert compacted patch intent marker")?;
        crud::delete_patch_commit_intent(
            &transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
            ordinal,
        )
        .await
        .context("delete compacted patch intent")?;
        crud::delete_patch_snapshot_reservation(
            &transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("release compacted patch snapshot reservation")?;
        transaction
            .commit()
            .await
            .context("commit patch intent compaction")?;
        Ok(true)
    }

    /// Read one bounded keyset page of non-promoted ordinal statuses. The
    /// projection/recovery callers merge this stream with bounded applied
    /// record pages, so a turn containing a large number of rejected/no-op
    /// invocations never materializes every compact terminal marker at once.
    pub async fn ordinal_status_page_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        after_ordinal: Option<CommitOrdinal>,
        limit: usize,
    ) -> Result<Vec<(CommitOrdinal, IntentStatus)>> {
        if limit == 0 || limit > usize::try_from(MAX_RECOVERY_PAGE_SIZE).unwrap_or(0) {
            bail!("invalid patch ordinal-status page size");
        }
        let rows = crud::list_non_promoted_patch_statuses_for_turn(
            &self.db,
            thread_id,
            turn_id,
            after_ordinal.map(sqlite_ordinal).transpose()?,
            limit as u64,
        )
        .await
        .context("query bounded patch ordinal-status page")?;
        rows.into_iter()
            .map(|row| {
                let ordinal = row.commit_ordinal;
                if ordinal < 0 {
                    bail!("patch ordinal status cannot be negative");
                }
                let status = parse_status(&row.status)?;
                Ok((CommitOrdinal(sqlite_decode_ordinal(ordinal)?), status))
            })
            .collect()
    }

    /// Return only the ordinal watermark for a turn.  Projection admission
    /// uses this aggregate query to compare a stored projection revision with
    /// the durable source stream without loading every terminal marker.
    pub async fn max_ordinal_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<CommitOrdinal>> {
        let ordinal = crud::max_patch_commit_state_ordinal(&self.db, thread_id, turn_id)
            .await
            .context("query patch turn ordinal watermark")?;
        let Some(ordinal) = ordinal else {
            return Ok(None);
        };
        if ordinal < 0 {
            bail!("patch turn commit ordinal cannot be negative");
        }
        Ok(Some(CommitOrdinal(sqlite_decode_ordinal(ordinal)?)))
    }

    /// Read a bounded keyset page of non-promoted statuses across a thread.
    /// The turn id is part of the cursor because commit ordinals restart for
    /// every turn.
    pub async fn ordinal_status_page_for_thread(
        &self,
        thread_id: &str,
        after: Option<(&str, CommitOrdinal)>,
        limit: usize,
    ) -> Result<Vec<(String, CommitOrdinal, IntentStatus)>> {
        if limit == 0 || limit > usize::try_from(MAX_RECOVERY_PAGE_SIZE).unwrap_or(0) {
            bail!("invalid patch thread ordinal-status page size");
        }
        let after = match after {
            Some((turn_id, ordinal)) => Some((turn_id, sqlite_ordinal(ordinal)?)),
            None => None,
        };
        let rows = crud::list_non_promoted_patch_statuses_for_thread(
            &self.db,
            thread_id,
            after,
            limit as u64,
        )
        .await
        .context("query bounded patch thread ordinal-status page")?;
        rows.into_iter()
            .map(|row| {
                let turn_id = row.turn_id;
                let ordinal = row.commit_ordinal;
                if ordinal < 0 {
                    bail!("patch ordinal status cannot be negative");
                }
                let status = parse_status(&row.status)?;
                Ok((
                    turn_id,
                    CommitOrdinal(sqlite_decode_ordinal(ordinal)?),
                    status,
                ))
            })
            .collect()
    }

    pub async fn mark_gap(&self, identity: &InvocationIdentity, reason: &str) -> Result<()> {
        let intent = self
            .get(identity)
            .await?
            .ok_or_else(|| anyhow!("patch intent is missing"))?;
        let mut gap = intent;
        // The gap status and its bounded reason identify an unresolved
        // ordinal. Never manufacture a pseudo-path to make the gap look like
        // a file change: downstream lineage and file-history queries must
        // contain only filesystem effects that can actually be named.
        let _ = reason;
        gap.status = IntentStatus::Gap;
        self.update_progress(&gap).await
    }

    /// Reconcile intents left by a process exit. The prepared plan is compared
    /// with the current workspace; no patch is ever executed again. An exact
    /// committed prefix is promoted to the normal immutable record, while an
    /// unresolvable filesystem state remains an explicit gap.
    pub async fn terminalize_pending_gaps(
        &self,
        records: &crate::apply_patch::history::SqliteAppliedPatchStore,
    ) -> Result<u64> {
        let mut terminalized = 0u64;
        let mut cursor = None;
        loop {
            let (pending, next_cursor) = self.pending_page(cursor.as_ref()).await?;
            if pending.is_empty() {
                break;
            }
            for pending_key in pending {
                // Fetch and decode one full recovery plan at a time. The page
                // query above intentionally carries only bounded identity
                // keys; retaining all recovery JSON/snapshots in a page would
                // defeat the startup memory ceiling.
                let Some(intent) = self.get(&pending_key.identity).await? else {
                    continue;
                };
                if intent.status != IntentStatus::Pending {
                    continue;
                }
                if let Some(existing) = records.get(&intent.identity).await? {
                    if existing.plan_fingerprint != intent.plan_fingerprint
                        || existing.record.commit_ordinal != intent.commit_ordinal
                    {
                        bail!(
                            "durable patch record does not match the pending intent immutable identity"
                        );
                    }
                    let mut completed = intent;
                    completed.status = if matches!(
                        existing.record.outcome,
                        crate::apply_patch::history::AppliedPatchRecordOutcome::Gap { .. }
                            | crate::apply_patch::history::AppliedPatchRecordOutcome::CommitStateUncertain
                    ) {
                        IntentStatus::Gap
                    } else {
                        IntentStatus::Promoted
                    };
                    self.update_progress(&completed).await?;
                    self.project_recovered_turn(records, &existing.record)
                        .await?;
                    self.compact_terminal(&existing.record.identity).await?;
                    terminalized = terminalized.saturating_add(1);
                    continue;
                }
                let recovered = intent
                    .recovery_plan
                    .as_ref()
                    .map(recover_filesystem_outcome)
                    .transpose()?;
                let (outcome, mut changes, side_effects) = match recovered {
                    Some(RecoveryOutcome::Exact { changes, complete }) => {
                        // An empty recovered change set means that no effective
                        // filesystem mutation can be attributed to this
                        // intent.  This is true both for an all-no-op plan
                        // (`complete`) and for a crash/failure before the
                        // first effective operation (`!complete`).  Never
                        // manufacture an unresolved AppliedPatchRecord for
                        // the latter: the commit-intent recovery invariant is
                        // that a pre-commit failure has no applied record.
                        if changes.is_empty() {
                            let mut completed = intent;
                            completed.status = if complete {
                                IntentStatus::AppliedNoChange
                            } else {
                                IntentStatus::FailedNoChange
                            };
                            self.update_progress(&completed).await?;
                            self.compact_terminal(&completed.identity).await?;
                            terminalized = terminalized.saturating_add(1);
                            continue;
                        }
                        let outcome = if complete {
                            crate::apply_patch::history::AppliedPatchRecordOutcome::Applied
                        } else {
                            crate::apply_patch::history::AppliedPatchRecordOutcome::Partial {
                                failed_stage: crate::apply_patch::history::PatchStage::Recover,
                                error_code: crate::apply_patch::history::PatchErrorCode::Io,
                            }
                        };
                        (
                            outcome,
                            changes,
                            crate::apply_patch::history::PatchSideEffects::default(),
                        )
                    }
                    Some(RecoveryOutcome::Uncertain {
                        changes,
                        reason,
                        side_effects,
                    }) => (
                        crate::apply_patch::history::AppliedPatchRecordOutcome::Gap { reason },
                        changes,
                        side_effects,
                    ),
                    None if !intent.committed_changes.is_empty() => (
                        crate::apply_patch::history::AppliedPatchRecordOutcome::Gap {
                            reason:
                                "process stopped before the prepared recovery plan was available"
                                    .to_owned(),
                        },
                        intent.committed_changes.clone(),
                        crate::apply_patch::history::PatchSideEffects::default(),
                    ),
                    None => (
                        crate::apply_patch::history::AppliedPatchRecordOutcome::Gap {
                            reason: "process stopped while patch commit intent was pending"
                                .to_owned(),
                        },
                        Vec::new(),
                        crate::apply_patch::history::PatchSideEffects::default(),
                    ),
                };
                for (sequence, change) in changes.iter_mut().enumerate() {
                    let sequence = u32::try_from(sequence).unwrap_or(u32::MAX);
                    change.sequence = sequence;
                    change.commit_step = u16::try_from(sequence).unwrap_or(u16::MAX);
                }
                let record = crate::apply_patch::history::AppliedPatchRecord::new(
                    intent.identity.clone(),
                    intent.commit_ordinal,
                    outcome,
                    changes
                        .clone()
                        .iter()
                        .map(crate::apply_patch::history::DurablePatchChange::from)
                        .collect(),
                );
                let mut record = record;
                record.side_effects = side_effects;
                if !record.side_effects.exact {
                    record.exactness = crate::apply_patch::history::PatchRecordExactness::Uncertain;
                }
                if let Some(plan) = intent.recovery_plan.as_ref() {
                    record.environment_id = plan.environment_id.clone();
                    record.authority = plan.authority;
                }
                record.provenance = crate::apply_patch::history::PatchHistoryProvenance::Recovery;
                record.committed_at_unix_ms = chrono::Utc::now().timestamp_millis();
                let terminal_status = if matches!(
                    record.outcome,
                    crate::apply_patch::history::AppliedPatchRecordOutcome::Gap { .. }
                ) {
                    IntentStatus::Gap
                } else {
                    IntentStatus::Promoted
                };
                let domain = crate::apply_patch::history::SnapshotDomain::new(
                    format!("thread:{}", intent.identity.thread_id),
                    "pioneer",
                    "thread_history",
                );
                let snapshots = changes
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
                records
                    .insert_with_snapshots(
                        record,
                        intent.plan_fingerprint,
                        &domain,
                        snapshots.as_slice(),
                    )
                    .await?;
                let mut completed = intent;
                completed.committed_changes = changes;
                completed.status = terminal_status;
                self.update_progress(&completed).await?;
                let stored = records
                    .get(&completed.identity)
                    .await?
                    .ok_or_else(|| anyhow!("recovered patch record disappeared after insert"))?;
                self.project_recovered_turn(records, &stored.record).await?;
                self.compact_terminal(&stored.record.identity).await?;
                terminalized = terminalized.saturating_add(1);
            }
            cursor = next_cursor;
        }
        Ok(terminalized)
    }

    async fn project_recovered_turn(
        &self,
        records: &crate::apply_patch::history::SqliteAppliedPatchStore,
        record: &crate::apply_patch::history::AppliedPatchRecord,
    ) -> Result<()> {
        let replay = crate::apply_patch::history::replay_turn_pages(
            records,
            self,
            record.identity.thread_id.as_str(),
            record.identity.turn_id.as_str(),
            256,
        )
        .await
        .context("replay recovered patch history")?;
        let authority = self
            .get(&record.identity)
            .await?
            .and_then(|intent| intent.recovery_plan.map(|plan| plan.authority))
            .unwrap_or(crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine);
        let state = crate::apply_patch::history::TurnDiffState::from_aggregate(
            replay.aggregate,
            authority,
            replay.revision,
            false,
        );
        crate::apply_patch::history::SqliteTurnDiffStore::new(self.db.clone())
            .repair_live(&state)
            .await?;
        Ok(())
    }
}

enum RecoveryOutcome {
    Exact {
        changes: Vec<crate::apply_patch::history::CommittedPatchChange>,
        complete: bool,
    },
    Uncertain {
        changes: Vec<crate::apply_patch::history::CommittedPatchChange>,
        reason: String,
        side_effects: crate::apply_patch::history::PatchSideEffects,
    },
}

fn recover_filesystem_outcome(plan: &PatchRecoveryPlan) -> Result<RecoveryOutcome> {
    if plan.workspace_root.trim().is_empty() {
        return Ok(RecoveryOutcome::Uncertain {
            changes: Vec::new(),
            reason: "prepared patch recovery plan has no workspace root".to_owned(),
            side_effects: crate::apply_patch::history::PatchSideEffects::default(),
        });
    }
    let root = PathBuf::from(&plan.workspace_root);
    if !root.is_absolute() {
        return Ok(RecoveryOutcome::Uncertain {
            changes: Vec::new(),
            reason: "prepared patch recovery workspace root is not absolute".to_owned(),
            side_effects: crate::apply_patch::history::PatchSideEffects::default(),
        });
    }
    let mut state = HashMap::<PathBuf, Option<Vec<u8>>>::new();
    let mut recovery_bytes = 0u64;
    for change in &plan.changes {
        for path in [
            &change.source_path,
            change.destination_path.as_deref().unwrap_or(""),
        ] {
            if path.is_empty() {
                continue;
            }
            let Some(absolute) = safe_recovery_path(&root, path) else {
                return Ok(RecoveryOutcome::Uncertain {
                    changes: Vec::new(),
                    reason: "prepared patch recovery target escapes workspace root".to_owned(),
                    side_effects: crate::apply_patch::history::PatchSideEffects::default(),
                });
            };
            if state.contains_key(&absolute) {
                continue;
            }
            // `safe_recovery_path` validates the ancestor chain, but a
            // process may replace the final component after that check.  Do
            // not follow a newly introduced final symlink (or classify a
            // directory as file content) while reconstructing a pending
            // intent; that would turn a path-substitution race into a false
            // exact history record.
            let current = match std::fs::symlink_metadata(&absolute) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    return Ok(RecoveryOutcome::Uncertain {
                        changes: Vec::new(),
                        reason: format!(
                            "pending patch recovery target `{}` is not a regular file",
                            safe_recovery_display_path(&root, &absolute)
                        ),
                        side_effects: crate::apply_patch::history::PatchSideEffects::default(),
                    });
                }
                Ok(_) => match read_recovery_file(&absolute) {
                    Ok(bytes) => {
                        recovery_bytes = match recovery_bytes.checked_add(bytes.len() as u64) {
                            Some(total) if total <= MAX_RECOVERY_TOTAL_BYTES => total,
                            _ => {
                                return Ok(RecoveryOutcome::Uncertain {
                                    changes: Vec::new(),
                                    reason: format!(
                                        "pending patch recovery exceeds the {} byte aggregate read limit",
                                        MAX_RECOVERY_TOTAL_BYTES
                                    ),
                                    side_effects:
                                        crate::apply_patch::history::PatchSideEffects::default(),
                                });
                            }
                        };
                        Some(bytes)
                    }
                    Err(error) => {
                        return Ok(RecoveryOutcome::Uncertain {
                            changes: Vec::new(),
                            reason: format!(
                                "cannot read pending patch recovery target `{}`: {error}",
                                safe_recovery_display_path(&root, &absolute)
                            ),
                            side_effects: crate::apply_patch::history::PatchSideEffects::default(),
                        });
                    }
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Ok(RecoveryOutcome::Uncertain {
                        changes: Vec::new(),
                        reason: format!(
                            "cannot inspect pending patch recovery target `{}`: {error}",
                            safe_recovery_display_path(&root, &absolute)
                        ),
                        side_effects: crate::apply_patch::history::PatchSideEffects::default(),
                    });
                }
            };
            state.insert(absolute, current);
        }
    }

    let mut committed = vec![false; plan.changes.len()];
    for (index, change) in plan.changes.iter().enumerate().rev() {
        if state_matches_after_map(&root, change, &state) {
            committed[index] = true;
            apply_inverse(change, &root, &mut state);
        } else if state_matches_before_map(&root, change, &state) {
            // The operation did not reach the filesystem. The reverse walk can
            // still classify an earlier committed prefix safely.
        } else {
            let changes = plan
                .changes
                .iter()
                .enumerate()
                .filter(|(index, _)| committed[*index])
                .map(|(_, change)| to_committed_change(change))
                .collect();
            return Ok(RecoveryOutcome::Uncertain {
                changes,
                reason: format!(
                    "filesystem state for prepared operation {} matches neither before nor after",
                    change.operation_index
                ),
                side_effects: crate::apply_patch::history::PatchSideEffects::default(),
            });
        }
    }

    let mut saw_uncommitted = false;
    for is_committed in &committed {
        if !*is_committed {
            saw_uncommitted = true;
        } else if saw_uncommitted {
            return Ok(RecoveryOutcome::Uncertain {
                changes: Vec::new(),
                reason: "recovered operation outcomes are not an ordered prefix".to_owned(),
                side_effects: crate::apply_patch::history::PatchSideEffects::default(),
            });
        }
    }
    let mut changes: Vec<crate::apply_patch::history::CommittedPatchChange> = plan
        .changes
        .iter()
        .enumerate()
        .filter(|(index, change)| committed[*index] && effective_change(change))
        .map(|(_, change)| to_committed_change(change))
        .collect();
    let mut side_effects = crate::apply_patch::history::PatchSideEffects::default();
    if let Some(reason) =
        reconcile_parent_directories(plan, &committed, &mut changes, &mut side_effects)?
    {
        return Ok(RecoveryOutcome::Uncertain {
            changes,
            reason,
            side_effects,
        });
    }
    Ok(RecoveryOutcome::Exact {
        changes,
        complete: committed.iter().all(|committed| *committed),
    })
}

fn read_recovery_file(path: &Path) -> io::Result<Vec<u8>> {
    let file = crate::apply_patch::file_mutation::open_regular_file(path)?;
    let mut limited = file.take(MAX_RECOVERY_FILE_BYTES.saturating_add(1));
    let mut bytes = Vec::new();
    limited.read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECOVERY_FILE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "recovery target exceeds the {} byte per-file read limit",
                MAX_RECOVERY_FILE_BYTES
            ),
        ));
    }
    Ok(bytes)
}

/// Reconcile parent-directory side effects without ever following a path that
/// was not part of the prepared, symlink-free manifest.  Existing parents must
/// retain their captured identity.  A parent that was missing at prepare time
/// is reported as a created side effect only when a committed operation under
/// that directory proves the patch reached it; otherwise its presence is a
/// residual/ambiguous side effect and recovery remains incomplete.
fn reconcile_parent_directories(
    plan: &PatchRecoveryPlan,
    committed: &[bool],
    changes: &mut [crate::apply_patch::history::CommittedPatchChange],
    side_effects: &mut crate::apply_patch::history::PatchSideEffects,
) -> Result<Option<String>> {
    for parent in &plan.parent_directories {
        let Some(absolute) = safe_recovery_path(Path::new(&plan.workspace_root), &parent.path)
        else {
            return Ok(Some(format!(
                "prepared parent directory `{}` escapes the workspace or has an unsafe ancestor",
                parent.path
            )));
        };
        let current = match metadata_fingerprint_for_path(&absolute) {
            Ok(current) => current,
            Err(error) => {
                return Ok(Some(format!(
                    "cannot inspect prepared parent directory `{}` during recovery: {error}",
                    parent.path
                )));
            }
        };
        if parent.existed {
            if current != parent.fingerprint {
                return Ok(Some(format!(
                    "prepared parent directory `{}` changed during recovery",
                    parent.path
                )));
            }
            continue;
        }
        match current.kind {
            TargetKind::Missing => {}
            TargetKind::Directory => {
                let touching_operation = plan
                    .changes
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| committed[*index])
                    .find(|(_, change)| {
                        path_is_under(&change.source_path, &parent.path)
                            || change
                                .destination_path
                                .as_deref()
                                .is_some_and(|path| path_is_under(path, &parent.path))
                    })
                    .map(|(_, change)| change.operation_index);
                let Some(operation_index) = touching_operation else {
                    side_effects.residual_directories.push(parent.path.clone());
                    side_effects.residual_directories.sort();
                    side_effects.residual_directories.dedup();
                    side_effects.exact = false;
                    return Ok(Some(format!(
                        "parent directory `{}` exists after an uncommitted patch",
                        parent.path
                    )));
                };
                if let Some(change) = changes
                    .iter_mut()
                    .find(|change| change.operation_index == operation_index)
                {
                    change
                        .side_effects
                        .created_directories
                        .push(parent.path.clone());
                    change.side_effects.created_directories.sort();
                    change.side_effects.created_directories.dedup();
                } else {
                    side_effects.residual_directories.push(parent.path.clone());
                    side_effects.residual_directories.sort();
                    side_effects.residual_directories.dedup();
                    side_effects.exact = false;
                    return Ok(Some(format!(
                        "created parent directory `{}` has no durable committed change",
                        parent.path
                    )));
                }
            }
            TargetKind::Symlink => {
                return Ok(Some(format!(
                    "prepared parent directory `{}` became a symlink during recovery",
                    parent.path
                )));
            }
            _ => {
                return Ok(Some(format!(
                    "prepared parent directory `{}` is no longer a directory",
                    parent.path
                )));
            }
        }
    }
    Ok(None)
}

fn path_is_under(path: &str, parent: &str) -> bool {
    path == parent
        || path
            .strip_prefix(parent)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn safe_recovery_path(root: &Path, relative: &str) -> Option<PathBuf> {
    // The workspace root is part of the trusted recovery plan, but it is a
    // pathname rather than an open directory handle.  A process restart may
    // therefore observe the root (or one of its ancestors) replaced by a
    // symlink.  Refuse to follow that substitution before inspecting any
    // planned target; otherwise recovery could read bytes outside the
    // authorized workspace and classify them as an exact commit.
    if !safe_recovery_root(root) {
        return None;
    }
    let path = Path::new(relative);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return None;
    }
    let absolute = root.join(path);
    let mut current = root.to_path_buf();
    for component in path.components() {
        let Component::Normal(value) = component else {
            continue;
        };
        current.push(value);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return None,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(_) => return None,
        }
    }
    absolute.starts_with(root).then_some(absolute)
}

fn safe_recovery_root(root: &Path) -> bool {
    if !root.is_absolute() {
        return false;
    }
    let mut current = PathBuf::new();
    for component in root.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return false,
            Ok(metadata) if !metadata.is_dir() => return false,
            Ok(_) => {}
            Err(_) => return false,
        }
    }
    true
}

fn safe_recovery_display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .ok()
        .filter(|relative| !relative.as_os_str().is_empty())
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|| "<outside-workspace>".to_owned())
}

fn effective_change(change: &crate::apply_patch::history::PreparedChangeRecovery) -> bool {
    if change.kind == crate::apply_patch::history::ChangeKind::Move {
        return true;
    }
    match (&change.before, &change.after) {
        (Some(before), Some(after)) => before.version != after.version,
        (None, Some(_)) | (Some(_), None) => true,
        (None, None) => false,
    }
}

fn to_committed_change(
    change: &crate::apply_patch::history::PreparedChangeRecovery,
) -> crate::apply_patch::history::CommittedPatchChange {
    crate::apply_patch::history::CommittedPatchChange {
        operation_index: change.operation_index,
        commit_step: 0,
        sequence: 0,
        kind: change.kind,
        source_path: change.source_path.clone(),
        destination_path: change.destination_path.clone(),
        before: change.before.clone(),
        after: change.after.clone(),
        overwritten_destination: change.overwritten_destination.clone(),
        side_effects: change.side_effects.clone(),
    }
}

fn state_matches_before_map(
    root: &Path,
    change: &crate::apply_patch::history::PreparedChangeRecovery,
    state: &HashMap<PathBuf, Option<Vec<u8>>>,
) -> bool {
    matches_state(
        state,
        &root.join(&change.source_path),
        change.before.as_ref(),
    ) && (change.kind != crate::apply_patch::history::ChangeKind::Move
        || matches_state(
            state,
            &root.join(change.destination_path.as_deref().unwrap_or_default()),
            change.overwritten_destination.as_ref(),
        ))
}

fn state_matches_after_map(
    root: &Path,
    change: &crate::apply_patch::history::PreparedChangeRecovery,
    state: &HashMap<PathBuf, Option<Vec<u8>>>,
) -> bool {
    let source_matches = if change.kind == crate::apply_patch::history::ChangeKind::Move {
        matches_state(state, &root.join(&change.source_path), None)
    } else {
        matches_state(
            state,
            &root.join(&change.source_path),
            change.after.as_ref(),
        )
    };
    let destination_matches = if change.kind == crate::apply_patch::history::ChangeKind::Move {
        matches_state(
            state,
            &root.join(change.destination_path.as_deref().unwrap_or_default()),
            change.after.as_ref(),
        )
    } else {
        true
    };
    source_matches && destination_matches
}

fn matches_state(
    state: &HashMap<PathBuf, Option<Vec<u8>>>,
    path: &Path,
    expected: Option<&crate::apply_patch::history::CommittedTextSnapshot>,
) -> bool {
    match (state.get(path).cloned().flatten(), expected) {
        (None, None) => true,
        (Some(bytes), Some(snapshot)) => {
            crate::apply_patch::file_mutation::FileVersionToken::from_bytes(&bytes)
                == snapshot.version.token
        }
        _ => false,
    }
}

fn apply_inverse(
    change: &crate::apply_patch::history::PreparedChangeRecovery,
    root: &Path,
    state: &mut HashMap<PathBuf, Option<Vec<u8>>>,
) {
    let source = root.join(&change.source_path);
    if change.kind == crate::apply_patch::history::ChangeKind::Move {
        state.insert(
            source,
            change
                .before
                .as_ref()
                .map(|snapshot| snapshot.bytes.clone()),
        );
        if let Some(destination) = change.destination_path.as_deref() {
            state.insert(
                root.join(destination),
                change
                    .overwritten_destination
                    .as_ref()
                    .map(|snapshot| snapshot.bytes.clone()),
            );
        }
    } else {
        state.insert(
            source,
            change
                .before
                .as_ref()
                .map(|snapshot| snapshot.bytes.clone()),
        );
    }
}

fn decode_row(
    identity: &InvocationIdentity,
    row: &crud::PatchCommitIntentRow,
) -> Result<PatchCommitIntent> {
    validate_identity(identity)?;
    let plan_fingerprint: [u8; 32] = row
        .plan_fingerprint
        .clone()
        .try_into()
        .map_err(|_| anyhow!("patch intent plan fingerprint must be 32 bytes"))?;
    if plan_fingerprint.iter().all(|byte| *byte == 0) {
        bail!("stored patch intent plan fingerprint must not be all zeroes");
    }
    let planned_operation_fingerprints: Vec<[u8; 32]> =
        decode_bounded_json(&row.operations_json, "patch intent operation fingerprints")?;
    validate_operation_fingerprints(&planned_operation_fingerprints)?;
    let recovery_plan: Option<PatchRecoveryPlan> =
        decode_bounded_json(&row.recovery_json, "patch intent recovery plan")?;
    if let Some(plan) = recovery_plan.as_ref() {
        validate_recovery_plan(plan)?;
    }
    let committed_changes: Vec<crate::apply_patch::history::CommittedPatchChange> =
        decode_bounded_json(&row.progress_json, "patch intent committed progress")?;
    validate_progress(&committed_changes)?;
    Ok(PatchCommitIntent {
        identity: identity.clone(),
        commit_ordinal: CommitOrdinal(sqlite_decode_ordinal(row.commit_ordinal)?),
        plan_fingerprint,
        planned_operation_fingerprints,
        recovery_plan,
        committed_changes,
        status: parse_status(&row.status)?,
    })
}

fn decode_terminal_row(
    identity: &InvocationIdentity,
    row: &crud::PatchCommitTerminalRow,
) -> Result<PatchCommitIntent> {
    validate_identity(identity)?;
    let plan_fingerprint: [u8; 32] = row
        .plan_fingerprint
        .clone()
        .try_into()
        .map_err(|_| anyhow!("patch terminal plan fingerprint must be 32 bytes"))?;
    if plan_fingerprint.iter().all(|byte| *byte == 0) {
        bail!("stored patch terminal plan fingerprint must not be all zeroes");
    }
    let planned_operation_fingerprints: Vec<[u8; 32]> = decode_bounded_json(
        &row.operations_json,
        "terminal patch intent operation fingerprints",
    )?;
    validate_operation_fingerprints(&planned_operation_fingerprints)?;
    Ok(PatchCommitIntent {
        identity: identity.clone(),
        commit_ordinal: CommitOrdinal(sqlite_decode_ordinal(row.commit_ordinal)?),
        plan_fingerprint,
        planned_operation_fingerprints,
        recovery_plan: None,
        committed_changes: Vec::new(),
        status: parse_status(&row.status)?,
    })
}

fn parse_authority(value: &str) -> Result<crate::apply_patch::history::TurnDiffAuthority> {
    match value {
        "native_patch_engine" => {
            Ok(crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine)
        }
        "managed_claude_patch_engine" => {
            Ok(crate::apply_patch::history::TurnDiffAuthority::ManagedClaudePatchEngine)
        }
        "codex_aggregate_event" => {
            Ok(crate::apply_patch::history::TurnDiffAuthority::CodexAggregateEvent)
        }
        "unsupported" => Ok(crate::apply_patch::history::TurnDiffAuthority::Unsupported),
        other => bail!("unknown patch intent authority `{other}`"),
    }
}

fn authority_name(value: crate::apply_patch::history::TurnDiffAuthority) -> &'static str {
    match value {
        crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine => "native_patch_engine",
        crate::apply_patch::history::TurnDiffAuthority::CodexAggregateEvent => {
            "codex_aggregate_event"
        }
        crate::apply_patch::history::TurnDiffAuthority::ManagedClaudePatchEngine => {
            "managed_claude_patch_engine"
        }
        crate::apply_patch::history::TurnDiffAuthority::Unsupported => "unsupported",
    }
}

fn status_name(status: IntentStatus) -> &'static str {
    match status {
        IntentStatus::Pending => "pending",
        IntentStatus::Promoted => "promoted",
        IntentStatus::AppliedNoChange => "applied_no_change",
        IntentStatus::FailedNoChange => "failed_no_change",
        IntentStatus::Rejected => "rejected",
        IntentStatus::Gap => "gap",
    }
}

fn parse_status(value: &str) -> Result<IntentStatus> {
    match value {
        "pending" => Ok(IntentStatus::Pending),
        "promoted" => Ok(IntentStatus::Promoted),
        "applied_no_change" => Ok(IntentStatus::AppliedNoChange),
        "failed_no_change" => Ok(IntentStatus::FailedNoChange),
        "rejected" => Ok(IntentStatus::Rejected),
        "gap" => Ok(IntentStatus::Gap),
        other => bail!("unknown patch intent status `{other}`"),
    }
}

fn validate_progress(changes: &[crate::apply_patch::history::CommittedPatchChange]) -> Result<()> {
    if changes.len() > MAX_INTENT_CHANGES {
        bail!("patch intent committed progress exceeds the persisted bound");
    }
    let mut previous_operation_index = None;
    let mut snapshot_bytes = 0u64;
    for (index, change) in changes.iter().enumerate() {
        let expected_sequence = index as u32;
        let expected_commit_step = u16::try_from(expected_sequence).unwrap_or(u16::MAX);
        if previous_operation_index.is_some_and(|previous| change.operation_index < previous) {
            bail!(
                "patch intent committed progress operation order regressed at index {}",
                index
            );
        }
        previous_operation_index = Some(change.operation_index);
        if change.sequence != expected_sequence || change.commit_step != expected_commit_step {
            bail!(
                "patch intent committed progress order is not contiguous at index {}",
                index
            );
        }
        validate_committed_change(change, &mut snapshot_bytes)?;
    }
    Ok(())
}

fn decode_bounded_json<T: DeserializeOwned>(value: &str, label: &str) -> Result<T> {
    if value.len() > MAX_INTENT_JSON_BYTES {
        bail!("{label} exceeds the persisted JSON decode bound");
    }
    serde_json::from_str(value).with_context(|| format!("decode {label}"))
}

fn encode_bounded_json<T: Serialize>(value: &T, label: &str) -> Result<String> {
    let encoded = serde_json::to_string(value).with_context(|| format!("encode {label}"))?;
    if encoded.len() > MAX_INTENT_JSON_BYTES {
        bail!("{label} exceeds the persisted JSON bound");
    }
    Ok(encoded)
}

fn validate_operation_fingerprints(operations: &[[u8; 32]]) -> Result<()> {
    if operations.len() > MAX_INTENT_OPERATIONS {
        bail!("patch intent operation count exceeds the persisted bound");
    }
    Ok(())
}

fn validate_recovery_plan(plan: &PatchRecoveryPlan) -> Result<()> {
    if plan.workspace_root.trim().is_empty()
        || plan.environment_id.len() > MAX_INTENT_PATH_BYTES
        || plan.workspace_root.len() > MAX_INTENT_PATH_BYTES
    {
        bail!("patch recovery plan identity exceeds the persisted bound");
    }
    if plan.changes.len() > MAX_INTENT_CHANGES {
        bail!("patch recovery plan change count exceeds the persisted bound");
    }
    if plan.parent_directories.len() > MAX_INTENT_PARENT_DIRECTORIES {
        bail!("patch recovery plan parent count exceeds the persisted bound");
    }
    let mut snapshot_bytes = 0u64;
    for change in &plan.changes {
        if change.source_path.len() > MAX_INTENT_PATH_BYTES
            || change
                .destination_path
                .as_deref()
                .is_some_and(|path| path.len() > MAX_INTENT_PATH_BYTES)
        {
            bail!("patch recovery plan path exceeds the persisted bound");
        }
        for snapshot in [
            change.before.as_ref(),
            change.after.as_ref(),
            change.overwritten_destination.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_committed_snapshot(snapshot, &mut snapshot_bytes)?;
        }
        validate_side_effects(&change.side_effects)?;
    }
    for parent in &plan.parent_directories {
        if parent.path.len() > MAX_INTENT_PATH_BYTES
            || parent.fingerprint.identity.len() > MAX_INTENT_PATH_BYTES
        {
            bail!("patch recovery parent identity exceeds the persisted bound");
        }
    }
    Ok(())
}

fn validate_committed_change(
    change: &crate::apply_patch::history::CommittedPatchChange,
    snapshot_bytes: &mut u64,
) -> Result<()> {
    if change.source_path.trim().is_empty()
        || change.source_path.len() > MAX_INTENT_PATH_BYTES
        || change
            .destination_path
            .as_deref()
            .is_some_and(|path| path.trim().is_empty() || path.len() > MAX_INTENT_PATH_BYTES)
    {
        bail!("patch intent change path exceeds the persisted bound");
    }
    if (change.kind == crate::apply_patch::history::ChangeKind::Move)
        != change.destination_path.is_some()
    {
        bail!(
            "patch intent move changes must have a destination and non-move changes must not have one"
        );
    }
    for snapshot in [
        change.before.as_ref(),
        change.after.as_ref(),
        change.overwritten_destination.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        validate_committed_snapshot(snapshot, snapshot_bytes)?;
    }
    validate_side_effects(&change.side_effects)
}

fn validate_committed_snapshot(
    snapshot: &crate::apply_patch::history::CommittedTextSnapshot,
    total_bytes: &mut u64,
) -> Result<()> {
    if snapshot.bytes.len() > MAX_INTENT_SNAPSHOT_BYTES
        || snapshot.version.token.byte_len() != snapshot.bytes.len() as u64
        || sha2::Sha256::digest(snapshot.bytes.as_slice()).as_slice()
            != snapshot.version.token.digest()
    {
        bail!("patch intent snapshot is oversized or has an invalid content token");
    }
    *total_bytes = (*total_bytes)
        .checked_add(snapshot.bytes.len() as u64)
        .ok_or_else(|| anyhow!("patch intent snapshot byte count overflow"))?;
    if *total_bytes > MAX_INTENT_SNAPSHOT_TOTAL_BYTES {
        bail!("patch intent snapshots exceed the persisted aggregate byte bound");
    }
    Ok(())
}

fn validate_identity(identity: &InvocationIdentity) -> Result<()> {
    if identity.thread_id.trim().is_empty()
        || identity.turn_id.trim().is_empty()
        || identity.invocation_id.trim().is_empty()
        || identity.thread_id.len() > MAX_INTENT_ID_BYTES
        || identity.turn_id.len() > MAX_INTENT_ID_BYTES
        || identity.invocation_id.len() > MAX_INTENT_ID_BYTES
    {
        bail!("patch intent identity is empty or exceeds the persisted bound");
    }
    Ok(())
}

fn validate_side_effects(
    side_effects: &crate::apply_patch::history::PatchSideEffects,
) -> Result<()> {
    let count = side_effects
        .created_directories
        .len()
        .saturating_add(side_effects.residual_directories.len())
        .saturating_add(side_effects.metadata_warnings.len());
    if count > 1024
        || side_effects
            .created_directories
            .iter()
            .chain(side_effects.residual_directories.iter())
            .chain(side_effects.metadata_warnings.iter())
            .any(|value| value.len() > MAX_INTENT_PATH_BYTES)
    {
        bail!("patch intent side effects exceed the persisted bound");
    }
    Ok(())
}

fn sqlite_ordinal(ordinal: CommitOrdinal) -> Result<i64> {
    i64::try_from(ordinal.0).map_err(|_| anyhow!("patch intent ordinal exceeds SQLite range"))
}

fn sqlite_decode_ordinal(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("patch intent commit ordinal cannot be negative"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordinal_admission_serializes_only_the_same_turn() {
        let first = InvocationIdentity::new("thread", "turn-a", "call-a").unwrap();
        let same_turn = InvocationIdentity::new("thread", "turn-a", "call-b").unwrap();
        let other_turn = InvocationIdentity::new("thread", "turn-b", "call-c").unwrap();
        let first_lock = turn_admission_lock(&first).unwrap();
        let _first_guard = first_lock.lock().await;

        let same_lock = turn_admission_lock(&same_turn).unwrap();
        assert!(
            timeout(Duration::from_millis(20), same_lock.lock())
                .await
                .is_err(),
            "same-turn ordinal allocators must serialize"
        );

        let other_lock = turn_admission_lock(&other_turn).unwrap();
        assert!(
            timeout(Duration::from_millis(20), other_lock.lock())
                .await
                .is_ok(),
            "an unrelated turn must not queue behind a stalled admission"
        );
    }
}
