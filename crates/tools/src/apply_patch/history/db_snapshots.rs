//! Database-backed content-addressed snapshot store.

use crate::apply_patch::history::snapshots::{BoundedSnapshotDecodeError, decode_zstd_bounded};
use crate::apply_patch::history::{
    CommittedTextSnapshot, ContentAddressedSnapshotRef, InvocationIdentity, SnapshotDomain,
    SnapshotReservation, SnapshotStoreLimits, SnapshotStoreMetrics, TextSnapshotRef,
};
use anyhow::{Context, Result, anyhow, bail};
use pioneer_crud::patch_history as crud;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::{ConnectionTrait, DatabaseTransaction, TransactionTrait};
use sha2::Digest;
const RECONCILIATION_PAGE_SIZE: u64 = 32;
const RECONCILIATION_REFERENCE_BATCH_SIZE: usize = 32;
const MAX_RECORD_JSON_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORD_CHANGES: usize = 256;
const MAX_RESERVATION_SNAPSHOTS: usize = MAX_RECORD_CHANGES * 3;
const MAX_SNAPSHOT_METADATA_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotReconciliationReport {
    pub repaired_references: u64,
    pub collected_blobs: u64,
    pub collected_bytes: u64,
}

#[derive(Clone)]
pub struct SqliteSnapshotStore {
    db: SqliteDatabase,
    limits: SnapshotStoreLimits,
}

#[derive(Clone)]
pub(crate) struct PreparedSnapshot {
    pub(crate) reference: ContentAddressedSnapshotRef,
    byte_len_sql: i64,
    encoding_json: String,
    line_endings_json: String,
    compressed_bytes: Vec<u8>,
}

impl SqliteSnapshotStore {
    pub fn new(db: impl Into<SqliteDatabase>) -> Self {
        Self {
            db: db.into(),
            limits: SnapshotStoreLimits::default(),
        }
    }

    pub fn with_limits(db: impl Into<SqliteDatabase>, limits: SnapshotStoreLimits) -> Self {
        Self {
            db: db.into(),
            limits,
        }
    }

    pub fn limits(&self) -> SnapshotStoreLimits {
        self.limits
    }

    pub(crate) fn prepare_snapshots(
        &self,
        domain: &SnapshotDomain,
        snapshots: &[CommittedTextSnapshot],
    ) -> Result<Vec<PreparedSnapshot>> {
        if snapshots.len() > MAX_RESERVATION_SNAPSHOTS {
            bail!("snapshot set contains too many snapshots");
        }
        let domain_id = domain.id();
        let mut total_bytes = 0u64;
        snapshots
            .iter()
            .map(|snapshot| {
                total_bytes = total_bytes
                    .checked_add(snapshot.bytes.len() as u64)
                    .ok_or_else(|| anyhow!("snapshot set logical-byte overflow"))?;
                if total_bytes > self.limits.max_logical_bytes {
                    bail!("snapshot set logical-byte limit exceeded");
                }
                self.prepare_snapshot(domain_id.as_str(), snapshot)
            })
            .collect()
    }

    fn prepare_snapshot(
        &self,
        domain_id: &str,
        snapshot: &CommittedTextSnapshot,
    ) -> Result<PreparedSnapshot> {
        let byte_len = snapshot.bytes.len() as u64;
        if snapshot.version.token.byte_len() != byte_len {
            bail!("snapshot version byte length does not match snapshot bytes");
        }
        if byte_len > self.limits.max_single_bytes || byte_len > self.limits.max_decompressed_bytes
        {
            bail!("snapshot single-blob limit exceeded");
        }
        let content_hash: [u8; 32] = sha2::Sha256::digest(snapshot.bytes.as_slice()).into();
        if content_hash != *snapshot.version.token.digest() {
            bail!("snapshot content hash does not match snapshot bytes");
        }
        let compressed_bytes =
            zstd::stream::encode_all(snapshot.bytes.as_slice(), 3).context("compress snapshot")?;
        let compressed_len = u64::try_from(compressed_bytes.len())
            .map_err(|_| anyhow!("compressed snapshot length exceeds u64"))?;
        if compressed_len > self.limits.max_physical_bytes {
            bail!("snapshot compressed-byte limit exceeded");
        }
        Ok(PreparedSnapshot {
            reference: ContentAddressedSnapshotRef {
                domain_id: domain_id.to_owned(),
                snapshot: TextSnapshotRef::from_snapshot(snapshot),
            },
            byte_len_sql: sqlite_i64(byte_len, "snapshot byte length")?,
            encoding_json: serde_json::to_string(&snapshot.encoding)?,
            line_endings_json: serde_json::to_string(&snapshot.line_endings)?,
            compressed_bytes,
        })
    }

    /// Reserve the storage needed by a tracked invocation before its first
    /// filesystem mutation.  The reservation is atomic with respect to other
    /// admissions and is conservative for content that is not interned yet:
    /// concurrent invocations may reserve the same bytes, but they can never
    /// collectively exceed the configured logical/physical quota.
    pub async fn reserve_for_intent(
        &self,
        identity: &InvocationIdentity,
        domain: &SnapshotDomain,
        snapshots: &[CommittedTextSnapshot],
    ) -> Result<SnapshotReservation> {
        let prepared = self.prepare_snapshots(domain, snapshots)?;
        let transaction = self
            .db
            .begin()
            .await
            .context("begin patch snapshot admission")?;
        let result = self
            .reserve_for_intent_in_transaction(&transaction, identity, &prepared)
            .await;
        match result {
            Ok(reservation) => {
                transaction
                    .commit()
                    .await
                    .context("commit patch snapshot admission")?;
                Ok(reservation)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn reserve_for_intent_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        identity: &InvocationIdentity,
        snapshots: &[PreparedSnapshot],
    ) -> Result<SnapshotReservation> {
        if snapshots.len() > MAX_RESERVATION_SNAPSHOTS {
            bail!("snapshot reservation contains too many snapshots");
        }
        let existing = crud::find_patch_snapshot_reservation(
            transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("look up patch snapshot admission")?;
        if let Some(row) = existing {
            let logical_bytes = decode_nonnegative_i64(row.logical_bytes, "logical reservation")?;
            let physical_bytes =
                decode_nonnegative_i64(row.physical_bytes, "physical reservation")?;
            if logical_bytes > self.limits.max_logical_bytes
                || physical_bytes > self.limits.max_physical_bytes
            {
                bail!("stored snapshot reservation exceeds configured quota");
            }
            return Ok(SnapshotReservation {
                logical_bytes,
                physical_bytes,
            });
        }

        let mut unique = Vec::<([u8; 32], u64, String, String)>::new();
        let mut logical_bytes = 0u64;
        let mut physical_bytes = 0u64;
        for snapshot in snapshots {
            let content_hash = snapshot.reference.snapshot.content_hash;
            let byte_len = snapshot.reference.snapshot.byte_len;
            if let Some((_, _, existing_encoding, existing_line_endings)) =
                unique.iter().find(|(hash, existing_len, _, _)| {
                    *hash == content_hash && *existing_len == byte_len
                })
            {
                if existing_encoding != &snapshot.encoding_json
                    || existing_line_endings != &snapshot.line_endings_json
                {
                    bail!("snapshot content hash collision or metadata mismatch during admission");
                }
                continue;
            }
            unique.push((
                content_hash,
                byte_len,
                snapshot.encoding_json.clone(),
                snapshot.line_endings_json.clone(),
            ));
            let existing = load_snapshot_bounded(
                transaction,
                &snapshot.reference.domain_id,
                &content_hash,
                snapshot.byte_len_sql,
                self.limits.max_physical_bytes,
            )
            .await
            .context("look up patch snapshot during admission")?;
            let needs_storage = match existing {
                None => true,
                Some(row) => {
                    let existing_compressed_len = row.compressed_bytes.len() as u64;
                    if existing_compressed_len > self.limits.max_physical_bytes {
                        bail!("stored compressed snapshot exceeds the physical-byte limit");
                    }
                    let existing_raw_len = row.raw_byte_len;
                    if existing_raw_len < 0
                        || existing_raw_len as u64 != byte_len
                        || row.encoding != snapshot.encoding_json
                        || row.line_endings_json != snapshot.line_endings_json
                    {
                        bail!(
                            "snapshot content hash collision or metadata mismatch during admission"
                        );
                    }
                    row.compressed_bytes != snapshot.compressed_bytes
                }
            };
            if needs_storage {
                logical_bytes = logical_bytes
                    .checked_add(byte_len)
                    .ok_or_else(|| anyhow!("snapshot reservation logical-byte overflow"))?;
                let compressed_len = u64::try_from(snapshot.compressed_bytes.len())
                    .map_err(|_| anyhow!("compressed snapshot length exceeds u64"))?;
                physical_bytes = physical_bytes
                    .checked_add(compressed_len)
                    .ok_or_else(|| anyhow!("snapshot reservation physical-byte overflow"))?;
                if logical_bytes > self.limits.max_logical_bytes {
                    bail!("snapshot reservation logical-byte limit exceeded");
                }
                if physical_bytes > self.limits.max_physical_bytes {
                    bail!("snapshot reservation physical-byte limit exceeded");
                }
            }
        }
        let totals = crud::patch_snapshot_totals(transaction)
            .await
            .context("read patch snapshot admission totals")?;
        let existing_logical =
            decode_nonnegative_i64(totals.logical_bytes, "stored logical snapshot bytes")?;
        let existing_physical =
            decode_nonnegative_i64(totals.physical_bytes, "stored physical snapshot bytes")?;
        let reserved = crud::patch_snapshot_reservation_totals_excluding(
            transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("read active patch snapshot reservations")?;
        let active_logical = decode_nonnegative_i64(
            reserved.logical_bytes,
            "active logical snapshot reservations",
        )?;
        let active_physical = decode_nonnegative_i64(
            reserved.physical_bytes,
            "active physical snapshot reservations",
        )?;
        if existing_logical
            .checked_add(active_logical)
            .and_then(|value| value.checked_add(logical_bytes))
            .is_none_or(|value| value > self.limits.max_logical_bytes)
        {
            bail!("snapshot logical-byte admission limit exceeded");
        }
        if existing_physical
            .checked_add(active_physical)
            .and_then(|value| value.checked_add(physical_bytes))
            .is_none_or(|value| value > self.limits.max_physical_bytes)
        {
            bail!("snapshot physical-byte admission limit exceeded");
        }
        crud::insert_patch_snapshot_reservation(
            transaction,
            identity.thread_id.clone(),
            identity.turn_id.clone(),
            identity.invocation_id.clone(),
            sqlite_i64(logical_bytes, "logical reservation")?,
            sqlite_i64(physical_bytes, "physical reservation")?,
        )
        .await
        .context("insert patch snapshot admission")?;
        Ok(SnapshotReservation {
            logical_bytes,
            physical_bytes,
        })
    }

    /// Release one invocation's reservation inside an existing transaction.
    /// Record promotion deletes it before interning blobs; rollback restores
    /// the reservation if promotion fails.
    pub(crate) async fn release_reservation_in_transaction(
        transaction: &DatabaseTransaction,
        identity: &InvocationIdentity,
    ) -> Result<()> {
        crud::delete_patch_snapshot_reservation(
            transaction,
            &identity.thread_id,
            &identity.turn_id,
            &identity.invocation_id,
        )
        .await
        .context("release patch snapshot admission")?;
        Ok(())
    }

    pub async fn release_reservation(&self, identity: &InvocationIdentity) -> Result<()> {
        let transaction = self
            .db
            .begin()
            .await
            .context("begin snapshot admission release")?;
        let result = Self::release_reservation_in_transaction(&transaction, identity).await;
        match result {
            Ok(()) => {
                transaction
                    .commit()
                    .await
                    .context("commit snapshot admission release")?;
                Ok(())
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Returns bounded storage counters without loading any snapshot blob.
    /// These counters are safe to expose as operational telemetry because
    /// they contain no paths, hashes or source content.
    pub async fn metrics(&self) -> Result<SnapshotStoreMetrics> {
        let row = crud::patch_snapshot_metrics(&self.db)
            .await
            .context("query snapshot storage metrics")?;
        let blobs =
            i64::try_from(row.blobs).map_err(|_| anyhow!("snapshot blob count overflow"))?;
        let logical_bytes = row.logical_bytes;
        let physical_bytes = row.physical_bytes;
        let references = row.references;
        let referenced_logical_bytes = row.referenced_logical_bytes;
        if [
            blobs,
            logical_bytes,
            physical_bytes,
            references,
            referenced_logical_bytes,
        ]
        .iter()
        .any(|value| *value < 0)
        {
            bail!("snapshot storage metrics contain a negative value");
        }
        Ok(SnapshotStoreMetrics {
            blobs: blobs as u64,
            logical_bytes: logical_bytes as u64,
            physical_bytes: physical_bytes as u64,
            references: references as u64,
            referenced_logical_bytes: referenced_logical_bytes as u64,
        })
    }

    /// Reconcile ref counts from the immutable record log. This is a bounded
    /// startup repair for blobs left by older writers or an interrupted
    /// pre-transaction promotion; it never invents bytes for a missing blob.
    ///
    /// Every scan and mutation is cursor-paginated. Expected counts live in a
    /// connection-local TEMP table, while record decoding runs before a short
    /// writer transaction. A crash can leave only a partially repaired count;
    /// the next startup rebuilds the scratch table and deterministically
    /// completes the remaining idempotent repairs.
    pub async fn reconcile_references(&self) -> Result<SnapshotReconciliationReport> {
        crud::prepare_expected_patch_snapshot_references(&self.db)
            .await
            .context("prepare snapshot reference repair scratch state")?;

        let mut reservation_cursor = None;
        loop {
            let reservations = crud::list_patch_snapshot_reservations(
                &self.db,
                reservation_cursor.as_ref(),
                RECONCILIATION_PAGE_SIZE,
            )
            .await
            .context("load patch snapshot reservation page")?;
            if reservations.is_empty() {
                break;
            }
            let transaction = self
                .db
                .begin()
                .await
                .context("begin stale snapshot reservation batch")?;
            for reservation in &reservations {
                if crud::find_patch_commit_intent(
                    &transaction,
                    &reservation.thread_id,
                    &reservation.turn_id,
                    &reservation.invocation_id,
                )
                .await?
                .is_none()
                {
                    crud::delete_patch_snapshot_reservation(
                        &transaction,
                        &reservation.thread_id,
                        &reservation.turn_id,
                        &reservation.invocation_id,
                    )
                    .await?;
                }
            }
            transaction
                .commit()
                .await
                .context("commit stale snapshot reservation batch")?;
            let last = reservations.last().expect("non-empty reservation page");
            reservation_cursor = Some(crud::PatchSnapshotReservationCursor {
                thread_id: last.thread_id.clone(),
                turn_id: last.turn_id.clone(),
                invocation_id: last.invocation_id.clone(),
            });
            if reservations.len() < RECONCILIATION_PAGE_SIZE as usize {
                break;
            }
        }

        let mut last_id = String::new();
        loop {
            let rows = crud::list_applied_patch_records_after_id(
                &self.db,
                &last_id,
                RECONCILIATION_PAGE_SIZE,
            )
            .await
            .context("load patch record page for snapshot reference repair")?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                last_id = row.id;
                if row.changes_json.len() > MAX_RECORD_JSON_BYTES {
                    bail!(
                        "patch record changes JSON exceeds the snapshot reconciliation decode bound"
                    );
                }
                let (changes, _) =
                    crate::apply_patch::history::db_store::decode_persisted_record_delta(
                        &row.changes_json,
                    )
                    .context("decode patch changes for snapshot reference repair")?;
                if changes.len() > MAX_RECORD_CHANGES {
                    bail!("patch record contains too many changes for snapshot reconciliation");
                }
                let domain_id = SnapshotDomain::new(
                    format!("thread:{}", row.thread_id),
                    "pioneer",
                    "thread_history",
                )
                .id();
                let references = changes
                    .iter()
                    .flat_map(|change| {
                        [
                            change.before.as_ref(),
                            change.after.as_ref(),
                            change.overwritten_destination.as_ref(),
                        ]
                        .into_iter()
                        .flatten()
                    })
                    .map(|snapshot| {
                        Ok((
                            snapshot.content_hash,
                            sqlite_i64(snapshot.byte_len, "snapshot byte length")?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if references.is_empty() {
                    continue;
                }
                for reference_batch in references.chunks(RECONCILIATION_REFERENCE_BATCH_SIZE) {
                    let transaction = self
                        .db
                        .begin()
                        .await
                        .context("begin expected snapshot reference batch")?;
                    for (content_hash, byte_len) in reference_batch {
                        crud::increment_expected_patch_snapshot_reference(
                            &transaction,
                            &domain_id,
                            content_hash,
                            *byte_len,
                        )
                        .await
                        .context("accumulate snapshot reference count")?;
                    }
                    transaction
                        .commit()
                        .await
                        .context("commit expected snapshot reference batch")?;
                    tokio::task::yield_now().await;
                }
            }
        }

        // Validate the complete scratch projection before changing persistent
        // ref counts. A missing blob therefore fails closed without exposing
        // a partially applied repair.
        let mut missing_references = 0_u64;
        let mut expected_cursor = None;
        loop {
            let transaction = self
                .db
                .begin()
                .await
                .context("begin expected snapshot validation batch")?;
            let expected = crud::list_expected_patch_snapshot_references(
                &transaction,
                expected_cursor.as_ref(),
                RECONCILIATION_PAGE_SIZE,
            )
            .await
            .context("load expected snapshot reference page")?;
            if expected.is_empty() {
                transaction
                    .commit()
                    .await
                    .context("commit empty expected snapshot validation batch")?;
                break;
            }
            for expected_row in &expected {
                if crud::find_patch_snapshot_sizes(
                    &transaction,
                    &expected_row.domain_id,
                    &expected_row.content_hash,
                    expected_row.byte_len,
                )
                .await?
                .is_none()
                {
                    missing_references = missing_references.saturating_add(1);
                }
            }
            let last = expected.last().expect("non-empty expected reference page");
            expected_cursor = Some(crud::PatchSnapshotKeyCursor {
                domain_id: last.domain_id.clone(),
                content_hash: last.content_hash.clone(),
                byte_len: last.byte_len,
            });
            transaction
                .commit()
                .await
                .context("commit expected snapshot validation batch")?;
            if expected.len() < RECONCILIATION_PAGE_SIZE as usize {
                break;
            }
        }
        if missing_references > 0 {
            bail!(
                "{} retained patch snapshot reference(s) have no stored blob",
                missing_references
            );
        }

        let mut repaired_references = 0_u64;
        expected_cursor = None;
        loop {
            let transaction = self
                .db
                .begin()
                .await
                .context("begin snapshot reference repair batch")?;
            let expected = crud::list_expected_patch_snapshot_references(
                &transaction,
                expected_cursor.as_ref(),
                RECONCILIATION_PAGE_SIZE,
            )
            .await
            .context("load snapshot reference repair page")?;
            if expected.is_empty() {
                transaction
                    .commit()
                    .await
                    .context("commit empty snapshot reference repair batch")?;
                break;
            }
            for expected_row in &expected {
                let stored = crud::find_patch_snapshot_sizes(
                    &transaction,
                    &expected_row.domain_id,
                    &expected_row.content_hash,
                    expected_row.byte_len,
                )
                .await?
                .context("validated patch snapshot disappeared during repair")?;
                if stored.ref_count != expected_row.ref_count {
                    let updated = crud::set_patch_snapshot_reference_count(
                        &transaction,
                        &expected_row.domain_id,
                        &expected_row.content_hash,
                        expected_row.byte_len,
                        stored.ref_count,
                        expected_row.ref_count,
                    )
                    .await?;
                    if updated != 1 {
                        bail!("snapshot reference repair lost a concurrent update");
                    }
                    repaired_references = repaired_references.saturating_add(1);
                }
            }
            let last = expected.last().expect("non-empty reference repair page");
            expected_cursor = Some(crud::PatchSnapshotKeyCursor {
                domain_id: last.domain_id.clone(),
                content_hash: last.content_hash.clone(),
                byte_len: last.byte_len,
            });
            transaction
                .commit()
                .await
                .context("commit snapshot reference repair batch")?;
            if expected.len() < RECONCILIATION_PAGE_SIZE as usize {
                break;
            }
        }

        let mut collected_blobs = 0_u64;
        let mut collected_bytes = 0_u64;
        let mut snapshot_cursor = None;
        loop {
            let snapshots = crud::list_patch_snapshot_keys(
                &self.db,
                snapshot_cursor.as_ref(),
                RECONCILIATION_PAGE_SIZE,
            )
            .await
            .context("load stored snapshot key page")?;
            if snapshots.is_empty() {
                break;
            }
            let transaction = self
                .db
                .begin()
                .await
                .context("begin orphan snapshot collection batch")?;
            for snapshot in &snapshots {
                if !crud::expected_patch_snapshot_reference_exists(
                    &transaction,
                    &snapshot.domain_id,
                    &snapshot.content_hash,
                    snapshot.byte_len,
                )
                .await?
                {
                    if snapshot.ref_count != 0 {
                        repaired_references = repaired_references.saturating_add(1);
                    }
                    collected_blobs = collected_blobs.saturating_add(1);
                    collected_bytes = collected_bytes
                        .checked_add(decode_nonnegative_i64(
                            snapshot.physical_bytes,
                            "orphan snapshot physical bytes",
                        )?)
                        .ok_or_else(|| anyhow!("orphan snapshot byte count overflow"))?;
                    crud::delete_patch_snapshot(
                        &transaction,
                        &snapshot.domain_id,
                        &snapshot.content_hash,
                        snapshot.byte_len,
                    )
                    .await?;
                }
            }
            let last = snapshots.last().expect("non-empty snapshot key page");
            snapshot_cursor = Some(crud::PatchSnapshotKeyCursor {
                domain_id: last.domain_id.clone(),
                content_hash: last.content_hash.clone(),
                byte_len: last.byte_len,
            });
            transaction
                .commit()
                .await
                .context("commit orphan snapshot collection batch")?;
            if snapshots.len() < RECONCILIATION_PAGE_SIZE as usize {
                break;
            }
        }
        crud::drop_expected_patch_snapshot_references(&self.db)
            .await
            .context("drop snapshot reference repair scratch state")?;
        Ok(SnapshotReconciliationReport {
            repaired_references,
            collected_blobs,
            collected_bytes,
        })
    }

    pub async fn put(
        &self,
        domain: &SnapshotDomain,
        snapshot: &CommittedTextSnapshot,
    ) -> Result<ContentAddressedSnapshotRef> {
        let prepared = self.prepare_snapshot(domain.id().as_str(), snapshot)?;
        let transaction = self.db.begin().await.context("begin snapshot insert")?;
        let result = self
            .put_prepared_in_transaction(&transaction, prepared, true)
            .await;
        match result {
            Ok(reference) => {
                transaction
                    .commit()
                    .await
                    .context("commit snapshot insert")?;
                Ok(reference)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Intern one snapshot while the caller owns the surrounding SQLite
    /// transaction. `add_reference` is true for a newly inserted history
    /// record and false when repairing a pre-existing record's missing or
    /// corrupt blob. Keeping blob admission and record insertion in one
    /// transaction prevents an orphan ref-counted blob after a crash.
    pub(crate) async fn put_prepared_in_transaction(
        &self,
        transaction: &DatabaseTransaction,
        snapshot: PreparedSnapshot,
        add_reference: bool,
    ) -> Result<ContentAddressedSnapshotRef> {
        let domain_id = snapshot.reference.domain_id.as_str();
        let content_hash = snapshot.reference.snapshot.content_hash;
        let byte_len = snapshot.reference.snapshot.byte_len;
        let existing = load_snapshot_bounded(
            transaction,
            domain_id,
            &content_hash,
            snapshot.byte_len_sql,
            self.limits.max_physical_bytes,
        )
        .await
        .context("look up content addressed snapshot")?;
        if let Some(existing) = existing {
            let compressed_len = existing.compressed_bytes.len() as u64;
            if compressed_len > self.limits.max_physical_bytes {
                bail!("stored compressed snapshot exceeds the physical-byte limit");
            }
            if existing.raw_byte_len < 0
                || existing.raw_byte_len as u64 != byte_len
                || existing.encoding != snapshot.encoding_json
                || existing.line_endings_json != snapshot.line_endings_json
            {
                bail!("snapshot content hash collision or metadata mismatch");
            }
            let existing_ref_count = existing.ref_count;
            if existing_ref_count < 0 {
                bail!("snapshot reference count is negative");
            }
            if add_reference && existing_ref_count == i64::MAX {
                bail!("snapshot reference count is exhausted");
            }
            let next_ref_count = if add_reference {
                existing_ref_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("snapshot reference count is exhausted"))?
            } else {
                existing_ref_count.max(1)
            };
            if existing.compressed_bytes != snapshot.compressed_bytes {
                let updated = crud::replace_patch_snapshot(
                    transaction,
                    crud::PatchSnapshotWrite {
                        domain_id: domain_id.to_owned(),
                        content_hash: content_hash.to_vec(),
                        byte_len: snapshot.byte_len_sql,
                        encoding: snapshot.encoding_json,
                        line_endings_json: snapshot.line_endings_json,
                        compressed_bytes: snapshot.compressed_bytes,
                        raw_byte_len: snapshot.byte_len_sql,
                        ref_count: next_ref_count,
                    },
                    existing_ref_count,
                )
                .await
                .context("repair corrupted content addressed snapshot")?;
                if updated != 1 {
                    bail!("snapshot repair lost a concurrent update");
                }
            } else {
                if next_ref_count != existing_ref_count {
                    let updated = crud::set_patch_snapshot_reference_count(
                        transaction,
                        domain_id,
                        &content_hash,
                        snapshot.byte_len_sql,
                        existing_ref_count,
                        next_ref_count,
                    )
                    .await
                    .context("update snapshot reference")?;
                    if updated != 1 {
                        bail!("snapshot reference update lost a concurrent transition");
                    }
                }
            }
        } else {
            let stored_totals = crud::patch_snapshot_totals(transaction)
                .await
                .context("read stored snapshot totals")?;
            let reservation_totals = crud::patch_snapshot_reservation_totals(transaction)
                .await
                .context("read reserved snapshot totals")?;
            let logical_bytes = decode_nonnegative_i64(
                stored_totals
                    .logical_bytes
                    .checked_add(reservation_totals.logical_bytes)
                    .ok_or_else(|| anyhow!("snapshot logical total overflow"))?,
                "stored and reserved logical snapshot bytes",
            )?;
            let physical_bytes = decode_nonnegative_i64(
                stored_totals
                    .physical_bytes
                    .checked_add(reservation_totals.physical_bytes)
                    .ok_or_else(|| anyhow!("snapshot physical total overflow"))?,
                "stored and reserved physical snapshot bytes",
            )?;
            if logical_bytes
                .checked_add(byte_len)
                .is_none_or(|value| value > self.limits.max_logical_bytes)
            {
                bail!("snapshot logical-byte limit exceeded");
            }
            let compressed_len = u64::try_from(snapshot.compressed_bytes.len())
                .map_err(|_| anyhow!("compressed snapshot length exceeds u64"))?;
            if physical_bytes
                .checked_add(compressed_len)
                .is_none_or(|value| value > self.limits.max_physical_bytes)
            {
                bail!("snapshot physical-byte limit exceeded");
            }
            crud::insert_patch_snapshot(
                transaction,
                crud::PatchSnapshotWrite {
                    domain_id: domain_id.to_owned(),
                    content_hash: content_hash.to_vec(),
                    byte_len: snapshot.byte_len_sql,
                    encoding: snapshot.encoding_json,
                    line_endings_json: snapshot.line_endings_json,
                    compressed_bytes: snapshot.compressed_bytes,
                    raw_byte_len: snapshot.byte_len_sql,
                    ref_count: 1,
                },
            )
            .await
            .context("insert content addressed snapshot")?;
        }
        Ok(snapshot.reference.clone())
    }

    pub async fn get(
        &self,
        reference: &ContentAddressedSnapshotRef,
    ) -> Result<CommittedTextSnapshot> {
        if reference.snapshot.schema_version
            != crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION
        {
            bail!("snapshot reference schema version is unsupported");
        }
        if reference.snapshot.byte_len > self.limits.max_single_bytes
            || reference.snapshot.byte_len > self.limits.max_decompressed_bytes
        {
            bail!("snapshot reference exceeds the configured byte limit");
        }
        let row = load_snapshot_bounded(
            &self.db,
            &reference.domain_id,
            &reference.snapshot.content_hash,
            sqlite_i64(reference.snapshot.byte_len, "snapshot byte length")?,
            self.limits.max_physical_bytes,
        )
        .await
        .context("query content addressed snapshot")?
        .ok_or_else(|| anyhow!("snapshot is missing"))?;
        let compressed_len = row.compressed_bytes.len() as u64;
        if compressed_len > self.limits.max_physical_bytes {
            bail!("stored compressed snapshot exceeds the physical-byte limit");
        }
        let raw_len = row.raw_byte_len;
        if raw_len < 0 || raw_len as u64 > self.limits.max_decompressed_bytes {
            bail!("snapshot decompressed-byte limit exceeded");
        }
        let encoding: crate::apply_patch::history::TextEncoding =
            serde_json::from_str(&row.encoding)?;
        let line_endings: crate::apply_patch::history::LineEndingMetadata =
            serde_json::from_str(&row.line_endings_json)?;
        if encoding != reference.snapshot.encoding
            || line_endings != reference.snapshot.line_endings
        {
            bail!("snapshot reference metadata does not match stored content");
        }
        let bytes = match decode_zstd_bounded(
            row.compressed_bytes.as_slice(),
            self.limits.max_decompressed_bytes,
        ) {
            Ok(bytes) => bytes,
            Err(BoundedSnapshotDecodeError::LimitExceeded) => {
                bail!("snapshot decompressed-byte limit exceeded");
            }
            Err(BoundedSnapshotDecodeError::Io(error)) => {
                return Err(anyhow!(error).context("decompress snapshot"));
            }
        };
        let actual_hash: [u8; 32] = sha2::Sha256::digest(bytes.as_slice()).into();
        if bytes.len() as i64 != raw_len
            || bytes.len() as u64 != reference.snapshot.byte_len
            || actual_hash != reference.snapshot.content_hash
        {
            bail!("snapshot content verification failed");
        }
        Ok(CommittedTextSnapshot::from_bytes(
            bytes,
            encoding,
            line_endings,
        ))
    }

    pub async fn release(&self, reference: &ContentAddressedSnapshotRef) -> Result<bool> {
        if reference.snapshot.schema_version
            != crate::apply_patch::history::SNAPSHOT_REF_SCHEMA_VERSION
        {
            bail!("snapshot reference schema version is unsupported");
        }
        if reference.snapshot.byte_len > self.limits.max_single_bytes
            || reference.snapshot.byte_len > self.limits.max_decompressed_bytes
        {
            bail!("snapshot reference exceeds the configured byte limit");
        }
        let byte_len = sqlite_i64(reference.snapshot.byte_len, "snapshot byte length")?;
        let encoding_json = serde_json::to_string(&reference.snapshot.encoding)?;
        let line_endings_json = serde_json::to_string(&reference.snapshot.line_endings)?;
        let transaction = self.db.begin().await.context("begin snapshot release")?;
        let existing = load_snapshot_bounded(
            &transaction,
            &reference.domain_id,
            &reference.snapshot.content_hash,
            byte_len,
            self.limits.max_physical_bytes,
        )
        .await
        .context("look up snapshot before release")?;
        let Some(existing) = existing else {
            transaction.rollback().await.ok();
            bail!("snapshot is missing");
        };
        let ref_count = existing.ref_count;
        if existing.raw_byte_len < 0
            || existing.raw_byte_len as u64 != reference.snapshot.byte_len
            || ref_count <= 0
            || existing.encoding != encoding_json
            || existing.line_endings_json != line_endings_json
        {
            transaction.rollback().await.ok();
            bail!("snapshot reference metadata or count is corrupt");
        }
        let next_ref_count = ref_count - 1;
        let updated = crud::set_patch_snapshot_reference_count(
            &transaction,
            &reference.domain_id,
            &reference.snapshot.content_hash,
            byte_len,
            ref_count,
            next_ref_count,
        )
        .await
        .context("decrement snapshot reference")?;
        if updated == 0 {
            transaction.rollback().await.ok();
            bail!("snapshot reference count underflow");
        }
        if next_ref_count == 0 {
            crud::delete_unreferenced_patch_snapshot(
                &transaction,
                &reference.domain_id,
                &reference.snapshot.content_hash,
                byte_len,
            )
            .await
            .context("collect unreferenced snapshot")?;
        }
        transaction
            .commit()
            .await
            .context("commit snapshot release")?;
        Ok(next_ref_count == 0)
    }

    pub async fn release_record(
        &self,
        domain: &SnapshotDomain,
        record: &crate::apply_patch::history::AppliedPatchRecord,
    ) -> Result<u64> {
        let mut released = 0u64;
        for change in &record.changes {
            for snapshot in [
                change.before.as_ref(),
                change.after.as_ref(),
                change.overwritten_destination.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                let reference = ContentAddressedSnapshotRef {
                    domain_id: domain.id(),
                    snapshot: snapshot.clone(),
                };
                if self.release(&reference).await? {
                    released = released.saturating_add(1);
                }
            }
        }
        Ok(released)
    }
}

async fn load_snapshot_bounded<C: ConnectionTrait>(
    db: &C,
    domain_id: &str,
    content_hash: &[u8; 32],
    byte_len: i64,
    max_physical_bytes: u64,
) -> Result<Option<crud::PatchSnapshotRow>> {
    let Some(sizes) =
        crud::find_patch_snapshot_sizes(db, domain_id, content_hash, byte_len).await?
    else {
        return Ok(None);
    };
    let compressed_bytes =
        decode_nonnegative_i64(sizes.compressed_bytes, "stored compressed snapshot length")?;
    let encoding_bytes =
        decode_nonnegative_i64(sizes.encoding_bytes, "stored snapshot encoding length")?;
    let line_endings_bytes = decode_nonnegative_i64(
        sizes.line_endings_bytes,
        "stored snapshot line-ending metadata length",
    )?;
    if compressed_bytes > max_physical_bytes {
        bail!("stored compressed snapshot exceeds the physical-byte limit");
    }
    if encoding_bytes > MAX_SNAPSHOT_METADATA_BYTES as u64
        || line_endings_bytes > MAX_SNAPSHOT_METADATA_BYTES as u64
    {
        bail!("stored snapshot metadata exceeds the decode bound");
    }
    let row = crud::find_patch_snapshot_with_bounds(
        db,
        domain_id,
        content_hash,
        byte_len,
        sqlite_i64(max_physical_bytes, "snapshot physical-byte limit")?,
        MAX_SNAPSHOT_METADATA_BYTES as i64,
    )
    .await?
    .ok_or_else(|| anyhow!("stored snapshot changed while enforcing decode bounds"))?;
    if row.compressed_bytes.len() as u64 != compressed_bytes
        || row.encoding.len() as u64 != encoding_bytes
        || row.line_endings_json.len() as u64 != line_endings_bytes
        || row.raw_byte_len != sizes.raw_byte_len
        || row.ref_count != sizes.ref_count
    {
        bail!("stored snapshot changed while being read");
    }
    Ok(Some(row))
}

fn sqlite_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{label} exceeds SQLite integer range"))
}

fn decode_nonnegative_i64(value: i64, label: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow!("{label} is negative"))
}
