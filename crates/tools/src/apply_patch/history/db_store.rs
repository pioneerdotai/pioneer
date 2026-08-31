//! Database adapter for the immutable Apply Patch record log.
//!
//! The adapter keeps persistence deliberately boring: one row contains one
//! complete schema-versioned record, and a unique invocation key makes replay
//! idempotent.  Snapshot bytes and projections have separate adapters so a
//! failed projection can never mutate the source record.

use crate::apply_patch::history::InsertedPatchRecord;
use crate::apply_patch::history::queries::{
    HistoryCoverageAccumulator, validate_optional_query_id, validate_query_id, validate_query_path,
};
use crate::apply_patch::history::store::validate_record;
use crate::apply_patch::history::{
    AggregateFileChange, AppliedPatchDelta, AppliedPatchRecord, AppliedStep, CommittedPatchChange,
    CommittedTextSnapshot, ContentAddressedSnapshotRef, FileHistoryEntry, HistoryCoverage,
    HistoryPage, HistoryQueryError, HistoryQueryLimits, HistoryRenderedDiff, IntentStatus,
    InvocationIdentity, PatchHistoryCoverage, RecordStoreError, SnapshotDomain,
    SqliteCommitIntentStore, SqliteSnapshotStore, StoredPatchRecord, ThreadHistoryCursor,
    TurnDiffExactness, TurnRecordProjector,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud::patch_history as crud;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{DatabaseTransaction, TransactionTrait};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, VecDeque};
#[cfg(test)]
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

const MAX_FILE_HISTORY_ALIASES: usize = 256;
const MAX_FILE_HISTORY_ENVIRONMENTS: usize = 256;
const FILE_HISTORY_INDEX_BATCH: usize = 512;
const MAX_REPLAY_PAGE_SIZE: usize = 256;
const MAX_INDEX_ID_BYTES: usize = 4096;
const MAX_INDEX_PATH_BYTES: usize = 4096;
const MAX_HISTORY_DIFF_RECORDS: usize = 4096;
const MAX_HISTORY_DIFF_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_HISTORY_DIFF_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Serialize)]
struct PersistedRecordDelta<'a> {
    changes: &'a [crate::apply_patch::history::DurablePatchChange],
    side_effects: &'a crate::apply_patch::history::PatchSideEffects,
}

#[derive(Deserialize)]
struct DecodedRecordDelta {
    changes: Vec<crate::apply_patch::history::DurablePatchChange>,
    side_effects: crate::apply_patch::history::PatchSideEffects,
}

pub(crate) fn decode_persisted_record_delta(
    changes_json: &str,
) -> Result<(
    Vec<crate::apply_patch::history::DurablePatchChange>,
    crate::apply_patch::history::PatchSideEffects,
)> {
    let delta: DecodedRecordDelta =
        serde_json::from_str(changes_json).context("decode stored patch delta")?;
    Ok((delta.changes, delta.side_effects))
}

#[derive(Clone)]
pub struct SqliteAppliedPatchStore {
    db: SqliteDatabase,
    #[cfg(test)]
    fail_next_record_insert: Arc<AtomicBool>,
}

impl SqliteAppliedPatchStore {
    pub fn new(db: impl Into<SqliteDatabase>) -> Self {
        Self {
            db: db.into(),
            #[cfg(test)]
            fail_next_record_insert: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn inject_next_record_insert_failure(&self) {
        self.fail_next_record_insert.store(true, Ordering::SeqCst);
    }

    pub fn database(&self) -> &SqliteDatabase {
        &self.db
    }

    /// Atomically admits all snapshot blobs and their immutable applied record.
    /// A crash can therefore expose neither a record without its blobs nor a
    /// ref-counted blob without an owning record. Replays repair a missing or
    /// corrupt blob for an existing record without incrementing its reference.
    pub async fn insert_with_snapshots(
        &self,
        record: AppliedPatchRecord,
        plan_fingerprint: [u8; 32],
        domain: &SnapshotDomain,
        snapshots: &[CommittedTextSnapshot],
    ) -> Result<InsertedPatchRecord> {
        validate_record(&record).map_err(|error| anyhow!(error))?;
        validate_fingerprint(plan_fingerprint)?;
        validate_snapshot_inputs(&record, snapshots)?;
        let transaction = self
            .db
            .begin()
            .await
            .context("begin patch record and snapshot insert")?;
        // The admission reservation covers the gap before the first
        // filesystem write.  Remove it inside the same transaction that
        // interns the record's actual blobs: a rollback restores the
        // reservation and keeps the tracked invocation fail-closed.
        SqliteSnapshotStore::release_reservation_in_transaction(&transaction, &record.identity)
            .await?;
        let existing = self
            .lookup_in_transaction(&transaction, &record.identity)
            .await?;
        if let Some(existing) = existing {
            if existing.plan_fingerprint != plan_fingerprint || existing.record != record {
                let _ = transaction.rollback().await;
                return Err(anyhow!(RecordStoreError::ConflictingDuplicate {
                    identity: record.identity,
                }));
            }
            let snapshot_store =
                crate::apply_patch::history::SqliteSnapshotStore::new(self.db.clone());
            let mut required_references = HashMap::<([u8; 32], u64), i64>::new();
            for snapshot in snapshots {
                snapshot_store
                    .put_in_transaction(&transaction, domain, snapshot, false)
                    .await?;
                *required_references
                    .entry((
                        *snapshot.version.token.digest(),
                        snapshot.bytes.len() as u64,
                    ))
                    .or_default() += 1;
            }
            for ((content_hash, byte_len), required) in required_references {
                crud::ensure_patch_snapshot_reference_count(
                    &transaction,
                    &domain.id(),
                    &content_hash,
                    sqlite_u64(byte_len, "snapshot byte length")?,
                    required,
                )
                .await
                .context("repair existing patch snapshot references")?;
            }
            ensure_change_index(&transaction, &existing.record).await?;
            transaction
                .commit()
                .await
                .context("commit idempotent patch record and snapshot repair")?;
            return Ok(InsertedPatchRecord::Existing(existing));
        }

        let snapshot_store = crate::apply_patch::history::SqliteSnapshotStore::new(self.db.clone());
        for snapshot in snapshots {
            snapshot_store
                .put_in_transaction(&transaction, domain, snapshot, true)
                .await?;
        }
        let result = self
            .insert_in_transaction(&transaction, record, plan_fingerprint)
            .await;
        match result {
            Ok(inserted) => {
                transaction
                    .commit()
                    .await
                    .context("commit patch record and snapshots")?;
                Ok(inserted)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn insert_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        record: AppliedPatchRecord,
        plan_fingerprint: [u8; 32],
    ) -> Result<InsertedPatchRecord> {
        validate_record(&record).map_err(|error| anyhow!(error))?;
        validate_fingerprint(plan_fingerprint)?;
        let existing = crud::find_applied_patch_record_by_invocation(
            transaction,
            &record.identity.thread_id,
            &record.identity.turn_id,
            &record.identity.invocation_id,
        )
        .await
        .context("look up patch record idempotency key")?;
        if let Some(row) = existing {
            let stored = decode_row(&row)?;
            if stored.plan_fingerprint == plan_fingerprint && stored.record == record {
                ensure_change_index(transaction, &stored.record).await?;
                return Ok(InsertedPatchRecord::Existing(stored));
            }
            return Err(anyhow!(RecordStoreError::ConflictingDuplicate {
                identity: record.identity,
            }));
        }

        let ordinal_conflict = crud::applied_patch_ordinal_exists(
            transaction,
            &record.identity.thread_id,
            &record.identity.turn_id,
            sqlite_ordinal(record.commit_ordinal)?,
        )
        .await
        .context("check patch record ordinal uniqueness")?;
        if ordinal_conflict {
            bail!(
                "commit ordinal {} is already occupied",
                record.commit_ordinal.0
            );
        }

        let id = record_id(&record.identity, record.commit_ordinal.0);
        let outcome_json =
            serde_json::to_string(&record.outcome).context("encode patch outcome")?;
        let changes_json = serde_json::to_string(&PersistedRecordDelta {
            changes: &record.changes,
            side_effects: &record.side_effects,
        })
        .context("encode patch delta")?;
        #[cfg(test)]
        if self.fail_next_record_insert.swap(false, Ordering::SeqCst) {
            bail!("injected patch record failure");
        }
        crud::insert_applied_patch_record(
            transaction,
            crud::AppliedPatchRecordWrite {
                id,
                schema_version: i64::from(record.schema_version),
                thread_id: record.identity.thread_id.clone(),
                turn_id: record.identity.turn_id.clone(),
                invocation_id: record.identity.invocation_id.clone(),
                environment_id: record.environment_id.clone(),
                commit_ordinal: sqlite_ordinal(record.commit_ordinal)?,
                authority: authority_name(record.authority).to_owned(),
                provenance: provenance_name(record.provenance).to_owned(),
                exactness: exactness_name(record.exactness).to_owned(),
                committed_at_unix_ms: record.committed_at_unix_ms,
                plan_fingerprint: plan_fingerprint.to_vec(),
                outcome_json,
                changes_json,
            },
        )
        .await
        .context("insert applied patch record")?;
        ensure_change_index(transaction, &record).await?;
        Ok(InsertedPatchRecord::Inserted(StoredPatchRecord {
            record,
            plan_fingerprint,
        }))
    }

    async fn lookup_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        identity: &InvocationIdentity,
    ) -> Result<Option<StoredPatchRecord>> {
        crud::find_applied_patch_record_by_invocation(
            transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("look up patch record in transaction")?
        .map(|row| decode_row(&row))
        .transpose()
    }

    pub async fn get(&self, identity: &InvocationIdentity) -> Result<Option<StoredPatchRecord>> {
        let row = crud::find_applied_patch_record_by_invocation(
            &self.db,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("query applied patch record")?;
        row.map(|row| decode_row(&row)).transpose()
    }

    /// Resolve one immutable record by its stable content-free identifier.
    /// The owning turn is part of the lookup so callers cannot use a record ID
    /// to bypass their already-authorized history scope.
    pub async fn get_by_record_id(
        &self,
        thread_id: &str,
        turn_id: &str,
        record_id: &str,
    ) -> Result<Option<StoredPatchRecord>> {
        validate_query_id(thread_id).map_err(|error| anyhow!(error))?;
        validate_query_id(turn_id).map_err(|error| anyhow!(error))?;
        if record_id.len() != 64 || !record_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("patch record id must be a 64-character hexadecimal digest");
        }
        let row = crud::find_applied_patch_record_by_scoped_id(
            &self.db,
            thread_id,
            turn_id,
            &record_id.to_ascii_lowercase(),
        )
        .await
        .context("query applied patch record by record id")?;
        row.map(|row| decode_row(&row)).transpose()
    }

    /// Materialize one durable record's exact delta from its content-addressed
    /// snapshot references.  This is used only by lifecycle repair paths when
    /// the process-local observer still has a record but its SQLite promotion
    /// was interrupted.  It never reads the live workspace or substitutes the
    /// pre-mutation recovery plan, which would be incorrect for a contextual
    /// Update that was replanned against unrelated external edits.
    pub async fn materialize_delta(&self, stored: &StoredPatchRecord) -> Result<AppliedPatchDelta> {
        let domain = SnapshotDomain::new(
            format!("thread:{}", stored.record.identity.thread_id),
            "pioneer",
            "thread_history",
        );
        let snapshots = SqliteSnapshotStore::new(self.db.clone());
        let load = |reference: Option<&crate::apply_patch::history::TextSnapshotRef>| {
            let snapshots = snapshots.clone();
            let domain_id = domain.id();
            let reference = reference.cloned();
            async move {
                match reference.as_ref() {
                    Some(reference) => snapshots
                        .get(&ContentAddressedSnapshotRef {
                            domain_id,
                            snapshot: reference.clone(),
                        })
                        .await
                        .map(Some),
                    None => Ok(None),
                }
            }
        };
        let mut changes = Vec::with_capacity(stored.record.changes.len());
        let mut side_effects = stored.record.side_effects.clone();
        side_effects.exact &= stored.record.exactness.is_exact();
        for change in &stored.record.changes {
            side_effects.merge(&change.side_effects);
            changes.push(CommittedPatchChange {
                operation_index: change.operation_index,
                commit_step: change.commit_step,
                sequence: change.sequence,
                kind: change.kind,
                source_path: change.source_path.clone(),
                destination_path: change.destination_path.clone(),
                before: load(change.before.as_ref()).await?,
                after: load(change.after.as_ref()).await?,
                overwritten_destination: load(change.overwritten_destination.as_ref()).await?,
                side_effects: change.side_effects.clone(),
            });
        }
        Ok(AppliedPatchDelta {
            changes,
            exact: stored.record.exactness.is_exact(),
            side_effects,
        })
    }

    /// Render one immutable invocation solely from retained snapshot refs.
    pub async fn render_record_diff(
        &self,
        stored: &StoredPatchRecord,
        max_output_bytes: usize,
    ) -> Result<HistoryRenderedDiff> {
        validate_diff_limits(max_output_bytes)?;
        validate_record(&stored.record).map_err(|error| anyhow!(error))?;
        let unified_patch = self
            .render_changes(
                stored.record.identity.thread_id.as_str(),
                stored
                    .record
                    .changes
                    .iter()
                    .map(|change| AggregateFileChange {
                        environment_id: stored.record.environment_id.clone(),
                        kind: change.kind,
                        source_path: change.source_path.clone(),
                        destination_path: change.destination_path.clone(),
                        before: change.before.clone(),
                        after: change.after.clone(),
                        overwritten_destination: change.overwritten_destination.clone(),
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                max_output_bytes,
            )
            .await?;
        let coverage = if stored.record.exactness.is_exact() {
            PatchHistoryCoverage::EngineVerifiedSteps
        } else {
            PatchHistoryCoverage::Incomplete {
                reason: "record commit state is uncertain".to_owned(),
            }
        };
        Ok(HistoryRenderedDiff {
            unified_patch,
            exactness: TurnDiffExactness::from_coverage(
                stored.record.exactness.is_exact(),
                &coverage,
            ),
            coverage,
            records_rendered: 1,
            after_ordinal: stored
                .record
                .commit_ordinal
                .0
                .checked_sub(1)
                .map(crate::apply_patch::history::CommitOrdinal),
            through_ordinal: Some(stored.record.commit_ordinal),
        })
    }

    /// Render the net effect of an exclusive/inclusive ordinal boundary from
    /// durable records and terminal intent markers. No workspace or Git read
    /// participates in either reconstruction or rendering.
    pub async fn render_turn_diff_between(
        &self,
        thread_id: &str,
        turn_id: &str,
        after_ordinal: Option<crate::apply_patch::history::CommitOrdinal>,
        through_ordinal: Option<crate::apply_patch::history::CommitOrdinal>,
        max_output_bytes: usize,
    ) -> Result<HistoryRenderedDiff> {
        validate_query_id(thread_id).map_err(|error| anyhow!(error))?;
        validate_query_id(turn_id).map_err(|error| anyhow!(error))?;
        validate_diff_limits(max_output_bytes)?;
        if after_ordinal.is_some_and(|after| after.0 == u64::MAX)
            || through_ordinal.is_some_and(|through| through.0 > i64::MAX as u64)
            || after_ordinal.is_some_and(|after| after.0 > i64::MAX as u64)
            || after_ordinal
                .zip(through_ordinal)
                .is_some_and(|(after, through)| through <= after)
        {
            bail!("invalid patch history diff boundary");
        }

        let first_ordinal = crate::apply_patch::history::CommitOrdinal(
            after_ordinal
                .map(|ordinal| ordinal.0.saturating_add(1))
                .unwrap_or(0),
        );
        let mut records = Vec::new();
        let mut record_cursor = after_ordinal;
        let mut reached_record_end = false;
        while !reached_record_end {
            let page = self
                .records_for_turn_page(thread_id, turn_id, record_cursor, MAX_REPLAY_PAGE_SIZE)
                .await?;
            if page.is_empty() {
                break;
            }
            let fetched = page.len();
            for stored in page {
                if through_ordinal.is_some_and(|through| stored.record.commit_ordinal > through) {
                    reached_record_end = true;
                    break;
                }
                record_cursor = Some(stored.record.commit_ordinal);
                records.push(stored);
                if records.len() > MAX_HISTORY_DIFF_RECORDS {
                    bail!("patch history diff exceeds the record limit");
                }
            }
            if fetched < MAX_REPLAY_PAGE_SIZE {
                break;
            }
        }

        let intents = SqliteCommitIntentStore::new(self.db.clone());
        let mut statuses = Vec::new();
        let mut status_cursor = after_ordinal;
        let mut reached_status_end = false;
        while !reached_status_end {
            let page = intents
                .ordinal_status_page_for_turn(
                    thread_id,
                    turn_id,
                    status_cursor,
                    MAX_REPLAY_PAGE_SIZE,
                )
                .await?;
            if page.is_empty() {
                break;
            }
            let fetched = page.len();
            for (ordinal, status) in page {
                if through_ordinal.is_some_and(|through| ordinal > through) {
                    reached_status_end = true;
                    break;
                }
                status_cursor = Some(ordinal);
                statuses.push((ordinal, status));
                if records.len().saturating_add(statuses.len()) > MAX_HISTORY_DIFF_RECORDS {
                    bail!("patch history diff exceeds the record/status limit");
                }
            }
            if fetched < MAX_REPLAY_PAGE_SIZE {
                break;
            }
        }

        let empty = BTreeSet::new();
        let mut projector =
            TurnRecordProjector::new_at_ordinal(thread_id, turn_id, &empty, &empty, first_ordinal);
        let mut record_index = 0usize;
        let mut status_index = 0usize;
        while record_index < records.len() || status_index < statuses.len() {
            match (records.get(record_index), statuses.get(status_index)) {
                (Some(record), Some((status_ordinal, status)))
                    if *status_ordinal < record.record.commit_ordinal =>
                {
                    projector.push_ordinal_status(*status_ordinal, *status);
                    status_index += 1;
                }
                (Some(record), Some((status_ordinal, status)))
                    if *status_ordinal == record.record.commit_ordinal =>
                {
                    if !matches!(status, IntentStatus::Pending | IntentStatus::Gap) {
                        bail!(
                            "patch record and terminal no-change status share commit ordinal {}",
                            status_ordinal.0
                        );
                    }
                    projector
                        .push(record)
                        .map_err(|error| anyhow!("project history diff boundary: {error}"))?;
                    projector.push_ordinal_status(*status_ordinal, *status);
                    record_index += 1;
                    status_index += 1;
                }
                (Some(record), _) => {
                    projector
                        .push(record)
                        .map_err(|error| anyhow!("project history diff boundary: {error}"))?;
                    record_index += 1;
                }
                (None, Some((ordinal, status))) => {
                    projector.push_ordinal_status(*ordinal, *status);
                    status_index += 1;
                }
                (None, None) => break,
            }
        }
        let aggregate = projector
            .finish()
            .map_err(|error| anyhow!("finish history diff boundary: {error}"))?;
        let unified_patch = self
            .render_changes(thread_id, aggregate.changes.as_slice(), max_output_bytes)
            .await?;
        let last_observed = records
            .last()
            .map(|record| record.record.commit_ordinal)
            .into_iter()
            .chain(statuses.last().map(|(ordinal, _)| *ordinal))
            .max();
        Ok(HistoryRenderedDiff {
            exactness: TurnDiffExactness::from_coverage(aggregate.exact, &aggregate.coverage),
            coverage: aggregate.coverage,
            unified_patch,
            records_rendered: u32::try_from(records.len())
                .map_err(|_| anyhow!("patch history diff record count is out of range"))?,
            after_ordinal,
            through_ordinal: through_ordinal.or(last_observed),
        })
    }

    async fn render_changes(
        &self,
        thread_id: &str,
        changes: &[AggregateFileChange],
        max_output_bytes: usize,
    ) -> Result<String> {
        let domain =
            SnapshotDomain::new(format!("thread:{thread_id}"), "pioneer", "thread_history");
        let snapshots = SqliteSnapshotStore::new(self.db.clone());
        let mut resolved = HashMap::<([u8; 32], u64), Vec<u8>>::new();
        let mut decompressed_bytes = 0usize;
        for reference in changes.iter().flat_map(|change| {
            [
                change.before.as_ref(),
                change.after.as_ref(),
                change.overwritten_destination.as_ref(),
            ]
            .into_iter()
            .flatten()
        }) {
            let key = (reference.content_hash, reference.byte_len);
            if resolved.contains_key(&key) {
                continue;
            }
            let snapshot = snapshots
                .get(&ContentAddressedSnapshotRef {
                    domain_id: domain.id(),
                    snapshot: reference.clone(),
                })
                .await?;
            decompressed_bytes = decompressed_bytes
                .checked_add(snapshot.bytes.len())
                .ok_or_else(|| anyhow!("patch history diff snapshot byte count overflow"))?;
            if decompressed_bytes > MAX_HISTORY_DIFF_DECOMPRESSED_BYTES {
                bail!("patch history diff exceeds the decompressed snapshot limit");
            }
            resolved.insert(key, snapshot.bytes);
        }

        let aggregate = crate::apply_patch::history::TurnAggregate {
            thread_id: thread_id.to_owned(),
            turn_id: "history-diff-render".to_owned(),
            changes: changes.to_vec(),
            exact: true,
            coverage: PatchHistoryCoverage::EngineVerifiedSteps,
            applied_through: None,
            record_count: 0,
        };
        let output = aggregate
            .render_unified_diff(|reference| {
                resolved
                    .get(&(reference.content_hash, reference.byte_len))
                    .cloned()
                    .ok_or_else(|| {
                        crate::apply_patch::history::AggregateProjectionError::Snapshot(
                            "authorized retained snapshot is missing".to_owned(),
                        )
                    })
            })
            .map_err(|error| anyhow!("render patch history diff: {error}"))?;
        if output.len() > max_output_bytes {
            bail!("patch history diff exceeds the output byte limit");
        }
        Ok(output)
    }

    /// Read one bounded keyset page of records for deterministic projection
    /// replay.  Callers must advance `after_ordinal` to the last returned
    /// ordinal; SQLite never materializes the remainder of the turn here.
    pub async fn records_for_turn_page(
        &self,
        thread_id: &str,
        turn_id: &str,
        after_ordinal: Option<crate::apply_patch::history::CommitOrdinal>,
        limit: usize,
    ) -> Result<Vec<StoredPatchRecord>> {
        if limit == 0 || limit > MAX_REPLAY_PAGE_SIZE {
            bail!("invalid patch replay page size")
        }
        let rows = crud::list_applied_patch_records_for_turn(
            &self.db,
            thread_id,
            turn_id,
            after_ordinal.map(sqlite_ordinal).transpose()?,
            u64::try_from(limit).map_err(|_| anyhow!("patch replay page size is out of range"))?,
        )
        .await
        .context("query bounded applied patch replay page")?;
        rows.iter().map(decode_row).collect()
    }

    /// Read one bounded keyset page of records across a thread.  The cursor
    /// includes the turn because commit ordinals restart for every turn.
    pub async fn records_for_thread_page(
        &self,
        thread_id: &str,
        after: Option<&ThreadHistoryCursor>,
        limit: usize,
    ) -> Result<Vec<StoredPatchRecord>> {
        if limit == 0 || limit > MAX_REPLAY_PAGE_SIZE {
            bail!("invalid patch replay page size")
        }
        let after = match after {
            Some(cursor) => Some((cursor.turn_id.as_str(), sqlite_ordinal(cursor.ordinal)?)),
            None => None,
        };
        let rows = crud::list_applied_patch_records_for_thread(
            &self.db,
            thread_id,
            after,
            u64::try_from(limit).map_err(|_| anyhow!("patch replay page size is out of range"))?,
        )
        .await
        .context("query bounded applied patch thread replay page")?;
        rows.iter().map(decode_row).collect()
    }

    pub async fn records_for_threads_page(
        &self,
        thread_ids: &[String],
        after: Option<&crate::apply_patch::history::ExecutionHistoryCursor>,
        limit: usize,
    ) -> Result<Vec<StoredPatchRecord>> {
        if thread_ids.is_empty() || limit == 0 || limit > MAX_REPLAY_PAGE_SIZE {
            bail!("invalid patch execution-scope replay page")
        }
        let after = match after {
            Some(cursor) => Some((
                cursor.committed_at_unix_ms,
                cursor.thread_id.as_str(),
                cursor.turn_id.as_str(),
                sqlite_ordinal(cursor.ordinal)?,
            )),
            None => None,
        };
        let rows = crud::list_applied_patch_records_for_threads(
            &self.db,
            thread_ids,
            after,
            u64::try_from(limit).map_err(|_| anyhow!("patch replay page size is out of range"))?,
        )
        .await
        .context("query bounded applied patch execution-scope page")?;
        rows.iter().map(decode_row).collect()
    }

    /// Return only the immutable record count and highest ordinal for a turn.
    /// Projection-status checks use this summary instead of deserializing all
    /// change payloads merely to validate a live aggregate watermark.
    pub async fn record_summary_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<(u64, Option<crate::apply_patch::history::CommitOrdinal>)> {
        let (count, max_ordinal) =
            crud::summarize_applied_patch_records_for_turn(&self.db, thread_id, turn_id)
                .await
                .context("query applied patch turn record summary")?;
        Ok((
            count,
            max_ordinal
                .map(sqlite_decode_ordinal)
                .transpose()?
                .map(crate::apply_patch::history::CommitOrdinal),
        ))
    }

    pub async fn record_count_for_thread(&self, thread_id: &str) -> Result<u64> {
        validate_query_id(thread_id).map_err(|error| anyhow!(error))?;
        crud::count_applied_patch_records_for_thread(&self.db, thread_id)
            .await
            .context("query applied patch thread record count")
    }

    async fn has_turn_records_after(
        &self,
        thread_id: &str,
        turn_id: &str,
        after_ordinal: crate::apply_patch::history::CommitOrdinal,
    ) -> Result<bool> {
        crud::has_applied_patch_records_for_turn_after(
            &self.db,
            thread_id,
            turn_id,
            sqlite_ordinal(after_ordinal)?,
        )
        .await
        .context("check for later applied patch turn records")
    }

    async fn has_thread_records_after(
        &self,
        thread_id: &str,
        after: &ThreadHistoryCursor,
    ) -> Result<bool> {
        crud::has_applied_patch_records_for_thread_after(
            &self.db,
            thread_id,
            &after.turn_id,
            sqlite_ordinal(after.ordinal)?,
        )
        .await
        .context("check for later applied patch thread records")
    }

    /// Read distinct turn keys in deterministic keyset pages.  Projection
    /// repair uses this instead of loading the complete journal before it can
    /// start folding the first turn.
    pub async fn turn_keys_page(
        &self,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<Vec<(String, String)>> {
        if limit == 0 || limit > MAX_REPLAY_PAGE_SIZE {
            bail!("invalid patch turn-key page size")
        }
        crud::list_applied_patch_turn_keys(
            &self.db,
            after,
            u64::try_from(limit)
                .map_err(|_| anyhow!("patch turn-key page size is out of range"))?,
        )
        .await
        .context("query bounded applied patch turn-key page")
    }

    /// Delete one owning thread without materializing its complete history.
    ///
    /// Records and their snapshot references are processed one at a time in a
    /// single transaction.  This keeps explicit thread deletion bounded even
    /// when a long-lived thread owns a large history.  Shared blobs remain
    /// alive because each decrement is guarded by the durable reference count.
    pub async fn delete_thread(&self, thread_id: &str) -> Result<u64> {
        let transaction = self
            .db
            .begin()
            .await
            .context("begin patch thread deletion")?;
        let domain =
            SnapshotDomain::new(format!("thread:{thread_id}"), "pioneer", "thread_history");
        let domain_id = domain.id();
        let mut last_turn_id: Option<String> = None;
        let mut last_ordinal: Option<crate::apply_patch::history::CommitOrdinal> = None;
        let mut records_deleted = 0_u64;
        loop {
            let after = match last_turn_id.as_deref().zip(last_ordinal) {
                Some((turn_id, ordinal)) => Some((turn_id, sqlite_ordinal(ordinal)?)),
                None => None,
            };
            let rows =
                crud::list_applied_patch_records_for_thread(&transaction, thread_id, after, 1)
                    .await
                    .context("load one patch record for thread deletion")?;
            let Some(row) = rows.into_iter().next() else {
                break;
            };
            let stored = decode_row(&row)?;
            let current_turn_id = stored.record.identity.turn_id.clone();
            let current_ordinal = stored.record.commit_ordinal;

            for snapshot in stored.record.changes.iter().flat_map(|change| {
                [
                    change.before.as_ref(),
                    change.after.as_ref(),
                    change.overwritten_destination.as_ref(),
                ]
                .into_iter()
                .flatten()
            }) {
                let byte_len = sqlite_u64(snapshot.byte_len, "snapshot byte length")?;
                let updated = crud::decrement_patch_snapshot_reference(
                    &transaction,
                    &domain_id,
                    &snapshot.content_hash,
                    byte_len,
                )
                .await
                .context("release one patch snapshot during thread deletion")?;
                if updated != 1 {
                    bail!(
                        "snapshot reference accounting is inconsistent for deleted thread `{thread_id}`"
                    );
                }
                crud::delete_unreferenced_patch_snapshot(
                    &transaction,
                    &domain_id,
                    &snapshot.content_hash,
                    byte_len,
                )
                .await
                .context("collect one deleted-thread snapshot")?;
            }

            let deleted = crud::delete_applied_patch_record_by_ordinal(
                &transaction,
                thread_id,
                &current_turn_id,
                sqlite_ordinal(current_ordinal)?,
            )
            .await
            .context("delete one patch record for thread")?;
            if deleted != 1 {
                bail!("patch record disappeared during thread deletion");
            }
            crud::delete_applied_patch_change_index_by_ordinal(
                &transaction,
                thread_id,
                &current_turn_id,
                sqlite_ordinal(current_ordinal)?,
            )
            .await
            .context("delete one patch change index for thread")?;
            records_deleted = records_deleted
                .checked_add(1)
                .ok_or_else(|| anyhow!("patch deletion record count overflow"))?;
            last_turn_id = Some(current_turn_id);
            last_ordinal = Some(current_ordinal);
        }

        crud::delete_patch_history_auxiliary_rows_for_thread(&transaction, thread_id)
            .await
            .context("delete auxiliary patch history rows for thread")?;
        transaction
            .commit()
            .await
            .context("commit patch thread deletion")?;
        Ok(records_deleted)
    }

    /// Durable adapters expose the same bounded query contract as the pure
    /// in-memory query layer. Records are loaded from SQLite and folded by the
    /// canonical query implementation; no workspace or Git read is involved.
    pub async fn query_turn_steps(
        &self,
        thread_id: &str,
        turn_id: &str,
        cursor: Option<crate::apply_patch::history::CommitOrdinal>,
        limits: HistoryQueryLimits,
    ) -> Result<HistoryPage<AppliedStep>, HistoryQueryError> {
        validate_history_limits(limits)?;
        validate_query_id(thread_id)?;
        validate_query_id(turn_id)?;
        if let Some(cursor) = cursor {
            if cursor.0 > i64::MAX as u64 {
                return Err(HistoryQueryError::InvalidArgument);
            }
        }
        let coverage = self.coverage_for_turn_pages(thread_id, turn_id).await?;
        let fetch_limit = limits
            .max_page_records
            .saturating_add(1)
            .min(MAX_REPLAY_PAGE_SIZE)
            .max(1);
        let mut records = self
            .records_for_turn_page(thread_id, turn_id, cursor, fetch_limit)
            .await
            .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
        let mut items = Vec::new();
        let mut page_bytes = 0usize;
        let mut next_cursor = None;
        while let Some(stored) = records.first().cloned() {
            records.remove(0);
            if items.len() >= limits.max_page_records {
                next_cursor = items
                    .last()
                    .map(|item: &AppliedStep| item.record.record.commit_ordinal);
                break;
            }
            let item = AppliedStep {
                record: stored,
                coverage: coverage.clone(),
            };
            let item_bytes = serde_json::to_vec(&item)
                .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
            if item_bytes.len() > limits.max_page_bytes {
                return Err(HistoryQueryError::PageTooLarge);
            }
            if page_bytes.saturating_add(item_bytes.len()) > limits.max_page_bytes {
                if items.is_empty() {
                    return Err(HistoryQueryError::PageTooLarge);
                }
                next_cursor = items
                    .last()
                    .map(|item: &AppliedStep| item.record.record.commit_ordinal);
                break;
            }
            page_bytes = page_bytes.saturating_add(item_bytes.len());
            items.push(item);
        }
        if next_cursor.is_none() && records.is_empty() {
            if let Some(last) = items
                .last()
                .map(|item: &AppliedStep| item.record.record.commit_ordinal)
            {
                if self
                    .has_turn_records_after(thread_id, turn_id, last)
                    .await
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?
                {
                    next_cursor = Some(last);
                }
            }
        }
        Ok(HistoryPage {
            items,
            next_cursor,
            next_thread_cursor: None,
            next_file_cursor: None,
            coverage,
        })
    }

    pub async fn query_thread_steps(
        &self,
        thread_id: &str,
        cursor: Option<crate::apply_patch::history::ThreadHistoryCursor>,
        limits: HistoryQueryLimits,
    ) -> Result<HistoryPage<AppliedStep>, HistoryQueryError> {
        validate_history_limits(limits)?;
        validate_query_id(thread_id)?;
        if let Some(cursor) = cursor.as_ref() {
            validate_query_id(&cursor.turn_id)?;
            if cursor.ordinal.0 > i64::MAX as u64 {
                return Err(HistoryQueryError::InvalidArgument);
            }
        }
        let coverage = self.coverage_for_thread_pages(thread_id).await?;
        let fetch_limit = limits
            .max_page_records
            .saturating_add(1)
            .min(MAX_REPLAY_PAGE_SIZE)
            .max(1);
        let mut records = self
            .records_for_thread_page(thread_id, cursor.as_ref(), fetch_limit)
            .await
            .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
        let mut items = Vec::new();
        let mut page_bytes = 0usize;
        let mut next_thread_cursor = None;
        while let Some(stored) = records.first().cloned() {
            records.remove(0);
            if items.len() >= limits.max_page_records {
                next_thread_cursor = items.last().map(|item: &AppliedStep| ThreadHistoryCursor {
                    turn_id: item.record.record.identity.turn_id.clone(),
                    ordinal: item.record.record.commit_ordinal,
                });
                break;
            }
            let item = AppliedStep {
                record: stored,
                coverage: coverage.clone(),
            };
            let item_bytes = serde_json::to_vec(&item)
                .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
            if item_bytes.len() > limits.max_page_bytes {
                return Err(HistoryQueryError::PageTooLarge);
            }
            if page_bytes.saturating_add(item_bytes.len()) > limits.max_page_bytes {
                if items.is_empty() {
                    return Err(HistoryQueryError::PageTooLarge);
                }
                next_thread_cursor = items.last().map(|item: &AppliedStep| ThreadHistoryCursor {
                    turn_id: item.record.record.identity.turn_id.clone(),
                    ordinal: item.record.record.commit_ordinal,
                });
                break;
            }
            page_bytes = page_bytes.saturating_add(item_bytes.len());
            items.push(item);
        }
        if next_thread_cursor.is_none() && records.is_empty() {
            if let Some(last) = items.last().map(|item: &AppliedStep| ThreadHistoryCursor {
                turn_id: item.record.record.identity.turn_id.clone(),
                ordinal: item.record.record.commit_ordinal,
            }) {
                if self
                    .has_thread_records_after(thread_id, &last)
                    .await
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?
                {
                    next_thread_cursor = Some(last);
                }
            }
        }
        Ok(HistoryPage {
            items,
            next_cursor: None,
            next_thread_cursor,
            next_file_cursor: None,
            coverage,
        })
    }

    pub async fn query_file_history(
        &self,
        thread_id: &str,
        path: &str,
        cursor: Option<crate::apply_patch::history::FileHistoryCursor>,
        limits: HistoryQueryLimits,
    ) -> Result<HistoryPage<FileHistoryEntry>, HistoryQueryError> {
        validate_query_id(thread_id)?;
        validate_query_path(path)?;
        if limits.max_page_records == 0
            || limits.max_page_bytes == 0
            || limits.max_decompressed_bytes == 0
        {
            return Err(HistoryQueryError::InvalidLimit);
        }
        if let Some(cursor) = cursor.as_ref() {
            validate_optional_query_id(&cursor.environment_id)?;
            validate_query_id(&cursor.turn_id)?;
            if cursor.ordinal.0 > i64::MAX as u64 {
                return Err(HistoryQueryError::InvalidArgument);
            }
        }
        // Resolve rename lineage through bounded keyset pages.  The closure
        // pass is repeated only when a newly discovered alias must be queried;
        // no page, record or change JSON is retained across a pass.  The final
        // pass streams the matching rows in display order and stops at the
        // requested page limits.
        let mut aliases = BTreeSet::from([path.to_owned()]);
        let mut aliases_by_environment = HashMap::<String, BTreeSet<String>>::new();
        loop {
            let mut index_cursor = None;
            let mut discovered_alias = false;
            loop {
                let rows = query_index_rows(
                    &self.db,
                    thread_id,
                    &aliases,
                    index_cursor.as_ref(),
                    FILE_HISTORY_INDEX_BATCH,
                    true,
                )
                .await
                .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
                if rows.is_empty() {
                    break;
                }
                let fetched = rows.len();
                for row in rows {
                    index_cursor = Some(IndexCursor::from_row(&row));
                    if !aliases_by_environment.contains_key(&row.environment_id)
                        && aliases_by_environment.len() >= MAX_FILE_HISTORY_ENVIRONMENTS
                    {
                        return Err(HistoryQueryError::Store(
                            "file history spans more environments than the configured query limit"
                                .to_owned(),
                        ));
                    }
                    let environment_aliases = aliases_by_environment
                        .entry(row.environment_id.clone())
                        .or_insert_with(|| BTreeSet::from([path.to_owned()]));
                    let source_matches = environment_aliases.contains(&row.source_path);
                    let destination_matches = row
                        .destination_path
                        .as_ref()
                        .is_some_and(|destination| environment_aliases.contains(destination));
                    let discovered = destination_matches
                        .then_some(row.source_path.clone())
                        .into_iter()
                        .chain(source_matches.then_some(row.destination_path).flatten());
                    for discovered_path in discovered {
                        if environment_aliases.insert(discovered_path.clone()) {
                            discovered_alias = true;
                            aliases.insert(discovered_path);
                            if aliases.len() > MAX_FILE_HISTORY_ALIASES {
                                return Err(HistoryQueryError::Store(
                                    "file history rename lineage exceeds the configured alias limit"
                                        .to_owned(),
                                ));
                            }
                        }
                    }
                }
                if fetched < FILE_HISTORY_INDEX_BATCH {
                    break;
                }
            }
            if !discovered_alias {
                break;
            }
        }
        // Coverage is derived from the immutable record outcomes rather than
        // assumed from the existence of index rows.  A partial/uncertain
        // record or an ordinal gap must remain visible on a per-file page.
        // The scan is paginated and touches only record metadata; snapshot
        // blobs are not read by the history query.
        let coverage = self.coverage_for_thread_pages(thread_id).await?;
        let mut items = Vec::new();
        let mut page_bytes = 0usize;
        let mut has_more = false;
        let mut index_cursor = cursor.as_ref().map(|cursor| IndexCursor {
            environment_id: cursor.environment_id.clone(),
            turn_id: cursor.turn_id.clone(),
            ordinal: i64::try_from(cursor.ordinal.0).unwrap_or(i64::MAX),
            sequence: i64::from(cursor.sequence),
        });
        'pages: loop {
            let rows = query_index_rows(
                &self.db,
                thread_id,
                &aliases,
                index_cursor.as_ref(),
                FILE_HISTORY_INDEX_BATCH,
                false,
            )
            .await
            .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
            if rows.is_empty() {
                break;
            }
            let fetched = rows.len();
            for row in rows {
                index_cursor = Some(IndexCursor::from_row(&row));
                let Some(environment_aliases) = aliases_by_environment.get(&row.environment_id)
                else {
                    continue;
                };
                if !(environment_aliases.contains(&row.source_path)
                    || row
                        .destination_path
                        .as_ref()
                        .is_some_and(|destination| environment_aliases.contains(destination)))
                {
                    continue;
                }
                if items.len() >= limits.max_page_records {
                    has_more = true;
                    break 'pages;
                }
                let entry = FileHistoryEntry {
                    environment_id: row.environment_id.clone(),
                    turn_id: row.turn_id,
                    ordinal: row.ordinal,
                    invocation_id: row.invocation_id,
                    before: row.change.before.clone(),
                    after: row.change.after.clone(),
                    overwritten_destination: row.change.overwritten_destination.clone(),
                    change: row.change,
                };
                let item_bytes = serde_json::to_vec(&entry)
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
                if item_bytes.len() > limits.max_page_bytes {
                    return Err(HistoryQueryError::PageTooLarge);
                }
                if page_bytes.saturating_add(item_bytes.len()) > limits.max_page_bytes {
                    if items.is_empty() {
                        return Err(HistoryQueryError::PageTooLarge);
                    }
                    has_more = true;
                    break 'pages;
                }
                page_bytes = page_bytes.saturating_add(item_bytes.len());
                items.push(entry);
            }
            if fetched < FILE_HISTORY_INDEX_BATCH {
                break;
            }
        }
        let next_file_cursor = if has_more {
            items
                .last()
                .map(|entry| crate::apply_patch::history::FileHistoryCursor {
                    environment_id: entry.environment_id.clone(),
                    turn_id: entry.turn_id.clone(),
                    ordinal: entry.ordinal,
                    sequence: entry.change.sequence,
                })
        } else {
            None
        };
        Ok(HistoryPage {
            items,
            next_cursor: None,
            next_thread_cursor: None,
            next_file_cursor,
            coverage,
        })
    }

    async fn coverage_for_turn_pages(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<HistoryCoverage, HistoryQueryError> {
        let intents = crate::apply_patch::history::SqliteCommitIntentStore::new(self.db.clone());
        let mut accumulator = HistoryCoverageAccumulator::new();
        let mut record_cursor = None;
        let mut record_page = VecDeque::new();
        let mut records_exhausted = false;
        let mut status_cursor = None;
        let mut status_page = VecDeque::new();
        let mut statuses_exhausted = false;
        loop {
            if record_page.is_empty() && !records_exhausted {
                let page = self
                    .records_for_turn_page(thread_id, turn_id, record_cursor, MAX_REPLAY_PAGE_SIZE)
                    .await
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
                records_exhausted = page.len() < MAX_REPLAY_PAGE_SIZE;
                record_cursor = page.last().map(|stored| stored.record.commit_ordinal);
                record_page.extend(page);
            }
            if status_page.is_empty() && !statuses_exhausted {
                let page = intents
                    .ordinal_status_page_for_turn(
                        thread_id,
                        turn_id,
                        status_cursor,
                        MAX_REPLAY_PAGE_SIZE,
                    )
                    .await
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
                statuses_exhausted = page.len() < MAX_REPLAY_PAGE_SIZE;
                status_cursor = page.last().map(|(ordinal, _)| *ordinal);
                status_page.extend(page);
            }

            match (record_page.front(), status_page.front()) {
                (Some(record), Some((status_ordinal, _)))
                    if *status_ordinal < record.record.commit_ordinal =>
                {
                    let (ordinal, status) = status_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("status page front disappeared".to_owned())
                    })?;
                    accumulator.push_status(ordinal, status);
                }
                (Some(record), Some((status_ordinal, _)))
                    if *status_ordinal == record.record.commit_ordinal =>
                {
                    let record = record_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("record page front disappeared".to_owned())
                    })?;
                    let (ordinal, status) = status_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("status page front disappeared".to_owned())
                    })?;
                    if !matches!(
                        status,
                        crate::apply_patch::history::IntentStatus::Pending
                            | crate::apply_patch::history::IntentStatus::Gap
                    ) {
                        return Err(HistoryQueryError::Store(format!(
                            "patch record and non-promoted ordinal status share commit ordinal {}",
                            ordinal.0
                        )));
                    }
                    accumulator.push(&record);
                    accumulator.push_status(ordinal, status);
                }
                (Some(_), _) => {
                    let record = record_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("record page front disappeared".to_owned())
                    })?;
                    accumulator.push(&record);
                }
                (None, Some(_)) => {
                    let (ordinal, status) = status_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("status page front disappeared".to_owned())
                    })?;
                    accumulator.push_status(ordinal, status);
                }
                (None, None) if records_exhausted && statuses_exhausted => break,
                (None, None) => continue,
            }
        }
        Ok(accumulator.finish())
    }

    async fn coverage_for_thread_pages(
        &self,
        thread_id: &str,
    ) -> Result<HistoryCoverage, HistoryQueryError> {
        let intents = crate::apply_patch::history::SqliteCommitIntentStore::new(self.db.clone());
        // Both source streams are ordered by `(turn_id, commit_ordinal)`, so
        // only the current turn's ordinal watermark is needed.  Keeping one
        // accumulator per historical turn would make a thread-wide coverage
        // query grow with the number of turns even though the result is only
        // one aggregate coverage value.
        let mut current_turn_id = None::<String>;
        let mut current_accumulator = HistoryCoverageAccumulator::new();
        let mut combined_exact = true;
        let mut combined_first_missing_ordinal = None;

        let finish_turn = |turn_id: &str,
                           current_turn_id: &mut Option<String>,
                           current_accumulator: &mut HistoryCoverageAccumulator,
                           combined_exact: &mut bool,
                           combined_first_missing_ordinal: &mut Option<
            crate::apply_patch::history::CommitOrdinal,
        >| {
            if current_turn_id.as_deref() == Some(turn_id) {
                return;
            }
            if current_turn_id.is_some() {
                let coverage =
                    std::mem::replace(current_accumulator, HistoryCoverageAccumulator::new())
                        .finish();
                if !coverage.exact {
                    *combined_exact = false;
                    if combined_first_missing_ordinal.is_none() {
                        *combined_first_missing_ordinal = coverage.first_missing_ordinal;
                    }
                }
            }
            *current_turn_id = Some(turn_id.to_owned());
        };

        let mut record_cursor = None;
        let mut record_page = VecDeque::new();
        let mut records_exhausted = false;
        let mut status_cursor = None;
        let mut status_page = VecDeque::new();
        let mut statuses_exhausted = false;
        loop {
            if record_page.is_empty() && !records_exhausted {
                let page = self
                    .records_for_thread_page(
                        thread_id,
                        record_cursor.as_ref(),
                        MAX_REPLAY_PAGE_SIZE,
                    )
                    .await
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
                records_exhausted = page.len() < MAX_REPLAY_PAGE_SIZE;
                record_cursor = page.last().map(|stored| ThreadHistoryCursor {
                    turn_id: stored.record.identity.turn_id.clone(),
                    ordinal: stored.record.commit_ordinal,
                });
                record_page.extend(page);
            }
            if status_page.is_empty() && !statuses_exhausted {
                let page = intents
                    .ordinal_status_page_for_thread(
                        thread_id,
                        status_cursor.as_ref().map(
                            |cursor: &(String, crate::apply_patch::history::CommitOrdinal)| {
                                (cursor.0.as_str(), cursor.1)
                            },
                        ),
                        MAX_REPLAY_PAGE_SIZE,
                    )
                    .await
                    .map_err(|error| HistoryQueryError::Store(error.to_string()))?;
                statuses_exhausted = page.len() < MAX_REPLAY_PAGE_SIZE;
                status_cursor = page
                    .last()
                    .map(|(turn_id, ordinal, _)| (turn_id.clone(), *ordinal));
                status_page.extend(page);
            }

            match (record_page.front(), status_page.front()) {
                (Some(record), Some((status_turn, status_ordinal, _)))
                    if (status_turn.as_str(), *status_ordinal)
                        < (
                            record.record.identity.turn_id.as_str(),
                            record.record.commit_ordinal,
                        ) =>
                {
                    let (turn_id, ordinal, status) = status_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("status page front disappeared".to_owned())
                    })?;
                    finish_turn(
                        turn_id.as_str(),
                        &mut current_turn_id,
                        &mut current_accumulator,
                        &mut combined_exact,
                        &mut combined_first_missing_ordinal,
                    );
                    current_accumulator.push_status(ordinal, status);
                }
                (Some(record), Some((status_turn, status_ordinal, _)))
                    if (status_turn.as_str(), *status_ordinal)
                        == (
                            record.record.identity.turn_id.as_str(),
                            record.record.commit_ordinal,
                        ) =>
                {
                    let record = record_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("record page front disappeared".to_owned())
                    })?;
                    let (turn_id, ordinal, status) = status_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("status page front disappeared".to_owned())
                    })?;
                    if !matches!(
                        status,
                        crate::apply_patch::history::IntentStatus::Pending
                            | crate::apply_patch::history::IntentStatus::Gap
                    ) {
                        return Err(HistoryQueryError::Store(format!(
                            "patch record and non-promoted ordinal status share commit ordinal {}",
                            ordinal.0
                        )));
                    }
                    finish_turn(
                        turn_id.as_str(),
                        &mut current_turn_id,
                        &mut current_accumulator,
                        &mut combined_exact,
                        &mut combined_first_missing_ordinal,
                    );
                    current_accumulator.push(&record);
                    current_accumulator.push_status(ordinal, status);
                }
                (Some(_), _) => {
                    let record = record_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("record page front disappeared".to_owned())
                    })?;
                    let turn_id = record.record.identity.turn_id.clone();
                    finish_turn(
                        turn_id.as_str(),
                        &mut current_turn_id,
                        &mut current_accumulator,
                        &mut combined_exact,
                        &mut combined_first_missing_ordinal,
                    );
                    current_accumulator.push(&record);
                }
                (None, Some(_)) => {
                    let (turn_id, ordinal, status) = status_page.pop_front().ok_or_else(|| {
                        HistoryQueryError::Store("status page front disappeared".to_owned())
                    })?;
                    finish_turn(
                        turn_id.as_str(),
                        &mut current_turn_id,
                        &mut current_accumulator,
                        &mut combined_exact,
                        &mut combined_first_missing_ordinal,
                    );
                    current_accumulator.push_status(ordinal, status);
                }
                (None, None) if records_exhausted && statuses_exhausted => break,
                (None, None) => continue,
            }
        }
        if current_turn_id.is_some() {
            let coverage = current_accumulator.finish();
            if !coverage.exact {
                combined_exact = false;
                if combined_first_missing_ordinal.is_none() {
                    combined_first_missing_ordinal = coverage.first_missing_ordinal;
                }
            }
        }
        Ok(combined_history_coverage(
            combined_exact,
            combined_first_missing_ordinal,
        ))
    }

    pub async fn coverage_for_threads(
        &self,
        thread_ids: &[String],
    ) -> Result<HistoryCoverage, HistoryQueryError> {
        if thread_ids.is_empty() {
            return Err(HistoryQueryError::InvalidArgument);
        }
        let mut exact = true;
        let mut first_missing_ordinal = None;
        for thread_id in thread_ids {
            let coverage = self.coverage_for_thread_pages(thread_id).await?;
            if !coverage.exact {
                exact = false;
                if first_missing_ordinal.is_none() {
                    first_missing_ordinal = coverage.first_missing_ordinal;
                }
            }
        }
        Ok(combined_history_coverage(exact, first_missing_ordinal))
    }
}

fn validate_history_limits(limits: HistoryQueryLimits) -> Result<(), HistoryQueryError> {
    if limits.max_page_records == 0
        || limits.max_page_bytes == 0
        || limits.max_decompressed_bytes == 0
    {
        return Err(HistoryQueryError::InvalidLimit);
    }
    Ok(())
}

fn combined_history_coverage(
    exact: bool,
    first_missing_ordinal: Option<crate::apply_patch::history::CommitOrdinal>,
) -> HistoryCoverage {
    let coverage = if exact {
        PatchHistoryCoverage::EngineVerifiedSteps
    } else {
        PatchHistoryCoverage::Incomplete {
            reason: first_missing_ordinal
                .map(|ordinal| format!("missing commit ordinal {}", ordinal.0))
                .unwrap_or_else(|| "one or more records are partial or uncertain".to_owned()),
        }
    };
    HistoryCoverage {
        exact,
        coverage,
        first_missing_ordinal,
    }
}

async fn query_index_rows(
    db: &SqliteDatabase,
    thread_id: &str,
    aliases: &BTreeSet<String>,
    cursor: Option<&IndexCursor>,
    limit: usize,
    descending: bool,
) -> Result<Vec<IndexedFileChange>> {
    if aliases.is_empty()
        || aliases.len() > MAX_FILE_HISTORY_ALIASES
        || limit == 0
        || limit > FILE_HISTORY_INDEX_BATCH
    {
        bail!("invalid file-history alias batch")
    }
    let aliases = aliases.iter().cloned().collect::<Vec<_>>();
    let cursor = cursor.map(|cursor| crud::PatchChangeIndexCursor {
        environment_id: cursor.environment_id.clone(),
        turn_id: cursor.turn_id.clone(),
        commit_ordinal: cursor.ordinal,
        sequence: cursor.sequence,
    });
    // Two bounded indexed queries avoid the broad OR plan. A move can match
    // both branches, so merge and deduplicate the entity rows before decoding.
    let mut rows = crud::list_patch_change_index_by_source_paths(
        db,
        thread_id,
        &aliases,
        cursor.as_ref(),
        limit as u64,
        descending,
    )
    .await
    .context("query source-path patch history index")?;
    rows.extend(
        crud::list_patch_change_index_by_destination_paths(
            db,
            thread_id,
            &aliases,
            cursor.as_ref(),
            limit as u64,
            descending,
        )
        .await
        .context("query destination-path patch history index")?,
    );
    rows.sort_by(|left, right| {
        let ordering = (
            &left.environment_id,
            &left.turn_id,
            left.commit_ordinal,
            left.sequence,
        )
            .cmp(&(
                &right.environment_id,
                &right.turn_id,
                right.commit_ordinal,
                right.sequence,
            ));
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
    rows.dedup_by(|left, right| {
        left.record_id == right.record_id && left.sequence == right.sequence
    });
    rows.truncate(limit);
    rows.into_iter().map(decode_index_row).collect()
}

#[derive(Clone, Debug)]
struct IndexCursor {
    environment_id: String,
    turn_id: String,
    ordinal: i64,
    sequence: i64,
}

impl IndexCursor {
    fn from_row(row: &IndexedFileChange) -> Self {
        Self {
            environment_id: row.environment_id.clone(),
            turn_id: row.turn_id.clone(),
            ordinal: i64::try_from(row.ordinal.0).unwrap_or(i64::MAX),
            sequence: i64::from(row.sequence),
        }
    }
}

fn decode_index_row(row: crud::AppliedPatchChangeIndexRow) -> Result<IndexedFileChange> {
    let environment_id = row.environment_id;
    let turn_id = row.turn_id;
    let invocation_id = row.invocation_id;
    if environment_id.len() > MAX_INDEX_ID_BYTES
        || turn_id.trim().is_empty()
        || turn_id.len() > MAX_INDEX_ID_BYTES
        || invocation_id.trim().is_empty()
        || invocation_id.len() > MAX_INDEX_ID_BYTES
    {
        bail!("indexed patch identity exceeds its bound");
    }
    let ordinal = row.commit_ordinal;
    if ordinal < 0 {
        bail!("patch change index commit ordinal cannot be negative")
    }
    let sequence = row.sequence;
    if sequence < 0 || sequence > u32::MAX as i64 {
        bail!("patch change index sequence is out of range")
    }
    let source_path = row.source_path;
    let destination_path = row.destination_path;
    if source_path.trim().is_empty()
        || source_path.len() > MAX_INDEX_PATH_BYTES
        || destination_path
            .as_deref()
            .is_some_and(|path| path.trim().is_empty() || path.len() > MAX_INDEX_PATH_BYTES)
    {
        bail!("indexed patch path exceeds its bound");
    }
    let change_json = row.change_json;
    if change_json.len() > crate::apply_patch::history::store::MAX_PERSISTED_RECORD_JSON_BYTES {
        bail!("indexed patch change JSON exceeds the persisted decode bound");
    }
    let change =
        serde_json::from_str::<crate::apply_patch::history::DurablePatchChange>(&change_json)
            .context("decode indexed patch change")?;
    if change.sequence != sequence as u32
        || change.source_path != source_path
        || change.destination_path != destination_path
    {
        bail!("indexed patch row disagrees with its durable change payload");
    }
    if (change.kind == crate::apply_patch::history::ChangeKind::Move)
        != change.destination_path.is_some()
    {
        bail!("indexed patch change has an invalid destination for its operation kind");
    }
    Ok(IndexedFileChange {
        environment_id,
        turn_id,
        ordinal: crate::apply_patch::history::CommitOrdinal(sqlite_decode_ordinal(ordinal)?),
        sequence: sequence as u32,
        invocation_id,
        source_path,
        destination_path,
        change,
    })
}

struct IndexedFileChange {
    environment_id: String,
    turn_id: String,
    ordinal: crate::apply_patch::history::CommitOrdinal,
    sequence: u32,
    invocation_id: String,
    source_path: String,
    destination_path: Option<String>,
    change: crate::apply_patch::history::DurablePatchChange,
}

fn decode_row(row: &crud::AppliedPatchRecordRow) -> Result<StoredPatchRecord> {
    let stored_record_id = row.id.clone();
    let schema_version = row.schema_version;
    if schema_version < 0 || schema_version > u16::MAX as i64 {
        bail!("stored patch schema version is out of range");
    }
    let thread_id = row.thread_id.clone();
    let turn_id = row.turn_id.clone();
    let invocation_id = row.invocation_id.clone();
    let environment_id = row.environment_id.clone();
    let ordinal = row.commit_ordinal;
    if ordinal < 0 {
        bail!("stored patch commit ordinal cannot be negative");
    }
    let authority = parse_authority(&row.authority)?;
    let provenance = parse_provenance(&row.provenance)?;
    let exactness = parse_exactness(&row.exactness)?;
    let committed_at_unix_ms = row.committed_at_unix_ms;
    let fingerprint = row.plan_fingerprint.clone();
    let plan_fingerprint: [u8; 32] = fingerprint
        .try_into()
        .map_err(|_| anyhow!("stored patch plan fingerprint must be 32 bytes"))?;
    let outcome_json = row.outcome_json.clone();
    if outcome_json.len() > crate::apply_patch::history::store::MAX_PERSISTED_RECORD_JSON_BYTES {
        bail!("stored patch outcome JSON exceeds the persisted decode bound");
    }
    let outcome = serde_json::from_str(&outcome_json).context("decode stored patch outcome")?;
    let changes_json = row.changes_json.clone();
    if changes_json.len() > crate::apply_patch::history::store::MAX_PERSISTED_RECORD_JSON_BYTES {
        bail!("stored patch changes JSON exceeds the persisted decode bound");
    }
    let (changes, side_effects) = decode_persisted_record_delta(&changes_json)?;
    let identity = InvocationIdentity::new(thread_id, turn_id, invocation_id)
        .map_err(|error| anyhow!(error))?;
    let record = AppliedPatchRecord {
        schema_version: schema_version as u16,
        identity,
        environment_id,
        commit_ordinal: crate::apply_patch::history::CommitOrdinal(sqlite_decode_ordinal(ordinal)?),
        authority,
        provenance,
        exactness,
        committed_at_unix_ms,
        outcome,
        changes,
        side_effects,
    };
    let expected_record_id = record_id(&record.identity, record.commit_ordinal.0);
    if stored_record_id != expected_record_id {
        bail!("stored patch record id disagrees with its immutable identity");
    }
    validate_record(&record).map_err(|error| anyhow!(error))?;
    Ok(StoredPatchRecord {
        record,
        plan_fingerprint,
    })
}

fn record_id(identity: &InvocationIdentity, ordinal: u64) -> String {
    crate::apply_patch::history::applied_patch_record_id(identity, ordinal)
}

fn validate_fingerprint(fingerprint: [u8; 32]) -> Result<()> {
    if fingerprint.iter().all(|byte| *byte == 0) {
        bail!("plan fingerprint must not be all zeroes")
    }
    Ok(())
}

fn validate_diff_limits(max_output_bytes: usize) -> Result<()> {
    if max_output_bytes == 0 || max_output_bytes > MAX_HISTORY_DIFF_OUTPUT_BYTES {
        bail!(
            "patch history diff output limit must be between 1 and {} bytes",
            MAX_HISTORY_DIFF_OUTPUT_BYTES
        );
    }
    Ok(())
}

/// The immutable record and the in-memory snapshot list form one atomic
/// promotion unit. Require an exact multiset match before opening the
/// transaction: otherwise an extra input snapshot could gain a reference
/// without an owning record, or a missing input could leave a durable record
/// that cannot be materialized after restart.
fn validate_snapshot_inputs(
    record: &AppliedPatchRecord,
    snapshots: &[crate::apply_patch::history::CommittedTextSnapshot],
) -> Result<()> {
    let mut expected = Vec::new();
    for change in &record.changes {
        for snapshot in [
            change.before.as_ref(),
            change.after.as_ref(),
            change.overwritten_destination.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            expected.push(snapshot_key_ref(snapshot));
        }
    }
    if snapshots.len() != expected.len() {
        bail!("snapshot inputs do not exactly match the applied record references");
    }
    let mut actual = snapshots.iter().map(snapshot_key_value).collect::<Vec<_>>();
    expected.sort_unstable();
    actual.sort_unstable();
    if expected != actual {
        bail!("snapshot inputs do not exactly match the applied record references");
    }
    Ok(())
}

fn snapshot_key_ref(snapshot: &crate::apply_patch::history::TextSnapshotRef) -> String {
    format!(
        "{}:{}:{:?}:{:?}",
        hex::encode(snapshot.content_hash),
        snapshot.byte_len,
        snapshot.encoding,
        snapshot.line_endings
    )
}

fn snapshot_key_value(snapshot: &crate::apply_patch::history::CommittedTextSnapshot) -> String {
    format!(
        "{}:{}:{:?}:{:?}",
        hex::encode(snapshot.version.token.digest()),
        snapshot.version.token.byte_len(),
        snapshot.encoding,
        snapshot.line_endings
    )
}

fn authority_name(authority: crate::apply_patch::history::TurnDiffAuthority) -> &'static str {
    match authority {
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

fn parse_authority(value: &str) -> Result<crate::apply_patch::history::TurnDiffAuthority> {
    match value {
        "native_patch_engine" => {
            Ok(crate::apply_patch::history::TurnDiffAuthority::NativePatchEngine)
        }
        "codex_aggregate_event" => {
            Ok(crate::apply_patch::history::TurnDiffAuthority::CodexAggregateEvent)
        }
        "managed_claude_patch_engine" => {
            Ok(crate::apply_patch::history::TurnDiffAuthority::ManagedClaudePatchEngine)
        }
        "unsupported" => Ok(crate::apply_patch::history::TurnDiffAuthority::Unsupported),
        other => bail!("unknown patch history authority `{other}`"),
    }
}

fn provenance_name(
    provenance: crate::apply_patch::history::PatchHistoryProvenance,
) -> &'static str {
    match provenance {
        crate::apply_patch::history::PatchHistoryProvenance::NativeEngine => "native_engine",
        crate::apply_patch::history::PatchHistoryProvenance::ManagedClaude => "managed_claude",
        crate::apply_patch::history::PatchHistoryProvenance::Recovery => "recovery",
        crate::apply_patch::history::PatchHistoryProvenance::ProviderAggregate => {
            "provider_aggregate"
        }
        crate::apply_patch::history::PatchHistoryProvenance::Unknown => "unknown",
    }
}

fn parse_provenance(value: &str) -> Result<crate::apply_patch::history::PatchHistoryProvenance> {
    match value {
        "native_engine" => Ok(crate::apply_patch::history::PatchHistoryProvenance::NativeEngine),
        "managed_claude" => Ok(crate::apply_patch::history::PatchHistoryProvenance::ManagedClaude),
        "recovery" => Ok(crate::apply_patch::history::PatchHistoryProvenance::Recovery),
        "provider_aggregate" => {
            Ok(crate::apply_patch::history::PatchHistoryProvenance::ProviderAggregate)
        }
        "unknown" => Ok(crate::apply_patch::history::PatchHistoryProvenance::Unknown),
        other => bail!("unknown patch history provenance `{other}`"),
    }
}

fn exactness_name(exactness: crate::apply_patch::history::PatchRecordExactness) -> &'static str {
    match exactness {
        crate::apply_patch::history::PatchRecordExactness::Exact => "exact",
        crate::apply_patch::history::PatchRecordExactness::Partial => "partial",
        crate::apply_patch::history::PatchRecordExactness::Uncertain => "uncertain",
    }
}

fn parse_exactness(value: &str) -> Result<crate::apply_patch::history::PatchRecordExactness> {
    match value {
        "exact" => Ok(crate::apply_patch::history::PatchRecordExactness::Exact),
        "partial" => Ok(crate::apply_patch::history::PatchRecordExactness::Partial),
        "uncertain" => Ok(crate::apply_patch::history::PatchRecordExactness::Uncertain),
        other => bail!("unknown patch record exactness `{other}`"),
    }
}

async fn ensure_change_index(
    transaction: &DatabaseTransaction,
    record: &AppliedPatchRecord,
) -> Result<()> {
    let record_id = record_id(&record.identity, record.commit_ordinal.0);
    let mut rows = Vec::with_capacity(record.changes.len());
    for change in &record.changes {
        let change_json = serde_json::to_string(change).context("encode patch change index")?;
        rows.push(crud::AppliedPatchChangeIndexWrite {
            record_id: record_id.clone(),
            thread_id: record.identity.thread_id.clone(),
            turn_id: record.identity.turn_id.clone(),
            invocation_id: record.identity.invocation_id.clone(),
            environment_id: record.environment_id.clone(),
            commit_ordinal: sqlite_ordinal(record.commit_ordinal)?,
            sequence: i64::from(change.sequence),
            source_path: change.source_path.clone(),
            destination_path: change.destination_path.clone(),
            change_json,
        });
    }
    crud::replace_applied_patch_change_index(transaction, &record_id, rows)
        .await
        .context("replace applied patch change index rows")?;
    Ok(())
}

fn sqlite_ordinal(ordinal: crate::apply_patch::history::CommitOrdinal) -> Result<i64> {
    i64::try_from(ordinal.0).map_err(|_| anyhow!("patch record ordinal exceeds SQLite range"))
}

fn sqlite_u64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{label} exceeds SQLite integer range"))
}

fn sqlite_decode_ordinal(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("stored patch commit ordinal cannot be negative"))
}
