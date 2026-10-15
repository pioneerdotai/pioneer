//! Bounded replay of durable Apply Patch record and status streams.
//!
//! The applied-record log and the intent terminal markers are independent
//! SQLite streams.  A projection must merge them by commit ordinal without
//! materializing a whole (potentially very large) turn in memory.  This
//! module owns that merge so live publication, bootstrap repair, and crash
//! recovery cannot drift into subtly different coverage semantics.

use crate::apply_patch::history::{
    CommitOrdinal, IntentStatus, SqliteAppliedPatchStore, SqliteCommitIntentStore,
    StoredPatchRecord, TurnAggregate, TurnRecordProjector,
};
use anyhow::{Result, anyhow, bail, ensure};
use std::collections::{BTreeSet, VecDeque};

const DEFAULT_REPLAY_PAGE_SIZE: usize = 256;

/// Result of one bounded turn replay.  `pending_ordinals` counts unresolved
/// status rows for telemetry; it does not affect the aggregate itself.
#[derive(Debug)]
pub struct TurnProjectionReplay {
    pub aggregate: TurnAggregate,
    pub revision: u64,
    pub pending_ordinals: u64,
}

/// Merge applied records and non-promoted intent statuses in commit order.
///
/// The function keeps at most one page from each source plus the projector's
/// current lineage map.  A pending or gap marker may legitimately share an
/// ordinal with an immutable record during the small transaction window
/// between record promotion and intent terminalization: the record preserves
/// the committed delta while the marker preserves incomplete coverage until
/// bookkeeping repair commits. Other equal ordinals are a durable consistency
/// error.
pub async fn replay_turn_pages(
    records: &SqliteAppliedPatchStore,
    intents: &SqliteCommitIntentStore,
    thread_id: &str,
    turn_id: &str,
    page_size: usize,
) -> Result<TurnProjectionReplay> {
    ensure!(
        (1..=DEFAULT_REPLAY_PAGE_SIZE).contains(&page_size),
        "invalid patch replay page size"
    );

    let empty_statuses = BTreeSet::new();
    let mut projector =
        TurnRecordProjector::new(thread_id, turn_id, &empty_statuses, &empty_statuses);
    let mut record_cursor = None;
    let mut record_page = VecDeque::<StoredPatchRecord>::new();
    let mut records_exhausted = false;
    let mut status_cursor = None;
    let mut status_page = VecDeque::<(CommitOrdinal, IntentStatus)>::new();
    let mut statuses_exhausted = false;
    let mut max_record_ordinal = None;
    let mut max_status_ordinal = None;
    let mut pending_ordinals = 0u64;

    loop {
        if record_page.is_empty() && !records_exhausted {
            let page = records
                .records_for_turn_page(thread_id, turn_id, record_cursor, page_size)
                .await?;
            records_exhausted = page.len() < page_size;
            record_cursor = page.last().map(|record| record.record.commit_ordinal);
            record_page.extend(page);
        }
        if status_page.is_empty() && !statuses_exhausted {
            let page = intents
                .ordinal_status_page_for_turn(thread_id, turn_id, status_cursor, page_size)
                .await?;
            statuses_exhausted = page.len() < page_size;
            status_cursor = page.last().map(|(ordinal, _)| *ordinal);
            status_page.extend(page);
        }

        match (record_page.front(), status_page.front()) {
            (Some(record), Some((status_ordinal, _)))
                if *status_ordinal < record.record.commit_ordinal =>
            {
                let (ordinal, status) = status_page
                    .pop_front()
                    .ok_or_else(|| anyhow!("status page front disappeared during replay"))?;
                if matches!(status, IntentStatus::Pending | IntentStatus::Gap) {
                    pending_ordinals = pending_ordinals.saturating_add(1);
                }
                max_status_ordinal = Some(ordinal);
                projector.push_ordinal_status(ordinal, status);
            }
            (Some(record), Some((status_ordinal, _)))
                if *status_ordinal == record.record.commit_ordinal =>
            {
                let record = record_page
                    .pop_front()
                    .ok_or_else(|| anyhow!("record page front disappeared during replay"))?;
                let (ordinal, status) = status_page
                    .pop_front()
                    .ok_or_else(|| anyhow!("status page front disappeared during replay"))?;
                if !matches!(status, IntentStatus::Pending | IntentStatus::Gap) {
                    bail!(
                        "patch record and non-promoted ordinal status share commit ordinal {}",
                        ordinal.0
                    );
                }
                pending_ordinals = pending_ordinals.saturating_add(1);
                max_record_ordinal = Some(record.record.commit_ordinal);
                max_status_ordinal = Some(ordinal);
                projector
                    .push(&record)
                    .map_err(|error| anyhow!("project patch history: {error}"))?;
                projector.push_ordinal_status(ordinal, status);
            }
            (Some(_), _) => {
                let record = record_page
                    .pop_front()
                    .ok_or_else(|| anyhow!("record page front disappeared during replay"))?;
                max_record_ordinal = Some(record.record.commit_ordinal);
                projector
                    .push(&record)
                    .map_err(|error| anyhow!("project patch history: {error}"))?;
            }
            (None, Some(_)) => {
                let (ordinal, status) = status_page
                    .pop_front()
                    .ok_or_else(|| anyhow!("status page front disappeared during replay"))?;
                if matches!(status, IntentStatus::Pending | IntentStatus::Gap) {
                    pending_ordinals = pending_ordinals.saturating_add(1);
                }
                max_status_ordinal = Some(ordinal);
                projector.push_ordinal_status(ordinal, status);
            }
            (None, None) if records_exhausted && statuses_exhausted => break,
            (None, None) => continue,
        }
    }

    let aggregate = projector
        .finish()
        .map_err(|error| anyhow!("project patch history: {error}"))?;
    let revision = max_record_ordinal
        .into_iter()
        .chain(max_status_ordinal)
        .map(|ordinal| ordinal.0)
        .max()
        .map(|ordinal| ordinal.saturating_add(1))
        .unwrap_or(0);
    Ok(TurnProjectionReplay {
        aggregate,
        revision,
        pending_ordinals,
    })
}
