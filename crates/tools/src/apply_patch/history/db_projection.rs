//! Database-backed turn-diff projection store.

use crate::apply_patch::history::TurnDiffState;
use crate::apply_patch::history::projection::validate_turn_diff_state;
use anyhow::{Context, Result, anyhow};
use pioneer_crud::{
    TurnDiffStateRow, TurnDiffStateWrite, find_turn_diff_state, list_turn_diff_states_for_threads,
    upsert_turn_diff_state,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::TransactionTrait;

const MAX_PROJECTION_JSON_BYTES: usize = 8 * 1024 * 1024;
const MAX_PROJECTION_ID_BYTES: usize = 4096;

#[derive(Clone)]
pub struct SqliteTurnDiffStore {
    db: SqliteDatabase,
}

impl SqliteTurnDiffStore {
    pub fn new(db: impl Into<SqliteDatabase>) -> Self {
        Self { db: db.into() }
    }

    pub async fn upsert(&self, state: &TurnDiffState) -> Result<bool> {
        let payload = encode_state(state)?;
        let transaction = self
            .db
            .begin()
            .await
            .context("begin turn diff projection")?;
        let existing = find_turn_diff_state(&transaction, &state.thread_id, &state.turn_id)
            .await
            .context("query turn diff projection")?;
        if let Some(existing) = existing {
            let existing_json = existing.state_json.clone();
            let existing_state = decode_state(&existing_json, &state.thread_id, &state.turn_id)?;
            let (revision, final_state) = validate_projection_row(&existing, &existing_state)?;
            if final_state != 0 {
                if existing_json == payload {
                    transaction
                        .commit()
                        .await
                        .context("commit idempotent final projection")?;
                    return Ok(false);
                }
                transaction.rollback().await.ok();
                return Err(anyhow!("final turn diff state is immutable"));
            }
            let revision = u64::try_from(revision)
                .map_err(|_| anyhow!("stored turn diff revision is negative"))?;
            if state.revision < revision {
                transaction.rollback().await.ok();
                return Err(anyhow!("turn diff projection revision is stale"));
            }
            if state.revision == revision {
                if existing_json == payload {
                    transaction
                        .commit()
                        .await
                        .context("commit idempotent projection")?;
                    return Ok(false);
                }
                let finalization_only = state.final_state
                    && final_state == 0
                    && same_projection_ignoring_terminal(&existing_state, state);
                if !finalization_only {
                    transaction.rollback().await.ok();
                    return Err(anyhow!("turn diff projection revision conflicts"));
                }
            }
        }
        upsert_turn_diff_state(&transaction, projection_write(state, payload)?)
            .await
            .context("upsert turn diff projection")?;
        transaction
            .commit()
            .await
            .context("commit turn diff projection")?;
        Ok(true)
    }

    /// Rebuild a missing or corrupt live projection from the immutable record
    /// log. A final projection remains immutable. A non-final row may be
    /// replaced when its rebuilt revision is newer; the same revision is
    /// accepted only for an idempotent write or the explicit live-to-final
    /// transition. A stale rebuild must never regress a projection that was
    /// published concurrently by the live path.
    pub async fn repair_live(&self, state: &TurnDiffState) -> Result<bool> {
        let payload = encode_state(state)?;
        let transaction = self
            .db
            .begin()
            .await
            .context("begin turn diff projection repair")?;
        if let Some(existing) = find_turn_diff_state(&transaction, &state.thread_id, &state.turn_id)
            .await
            .context("query projection before repair")?
        {
            let existing_json = existing.state_json.clone();
            let existing_state = decode_state(&existing_json, &state.thread_id, &state.turn_id)?;
            let (existing_revision, final_state) =
                validate_projection_row(&existing, &existing_state)?;
            let existing_revision = u64::try_from(existing_revision)
                .map_err(|_| anyhow!("stored turn diff revision is negative"))?;
            if final_state != 0 {
                if existing_json == payload {
                    transaction
                        .commit()
                        .await
                        .context("commit idempotent projection repair")?;
                    return Ok(false);
                }
                transaction.rollback().await.ok();
                return Err(anyhow::anyhow!(
                    "final turn diff state is immutable during repair"
                ));
            }
            if state.revision < existing_revision {
                transaction.rollback().await.ok();
                return Ok(false);
            }
            // Finalization intentionally changes only the final_state bit at
            // the current source revision; that transition is allowed after a
            // live projection has already published the same aggregate.
            if state.revision == existing_revision && existing_json != payload {
                let finalization_only = state.final_state
                    && final_state == 0
                    && same_projection_ignoring_terminal(&existing_state, state);
                if !finalization_only {
                    transaction.rollback().await.ok();
                    return Err(anyhow!(
                        "turn diff repair conflicts at an existing projection revision"
                    ));
                }
            }
        }
        upsert_turn_diff_state(&transaction, projection_write(state, payload)?)
            .await
            .context("repair turn diff projection")?;
        transaction
            .commit()
            .await
            .context("commit turn diff projection repair")?;
        Ok(true)
    }

    pub async fn get(&self, thread_id: &str, turn_id: &str) -> Result<Option<TurnDiffState>> {
        validate_lookup_identity(thread_id, "thread_id")?;
        validate_lookup_identity(turn_id, "turn_id")?;
        let row = find_turn_diff_state(&self.db, thread_id, turn_id)
            .await
            .context("query turn diff projection state")?;
        row.map(|row| {
            let payload = row.state_json.clone();
            let state = decode_state(&payload, thread_id, turn_id)?;
            validate_projection_row(&row, &state)?;
            Ok(state)
        })
        .transpose()
    }

    pub async fn list_for_threads(&self, thread_ids: &[String]) -> Result<Vec<TurnDiffState>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }
        for thread_id in thread_ids {
            validate_lookup_identity(thread_id, "thread_id")?;
        }
        list_turn_diff_states_for_threads(&self.db, thread_ids)
            .await
            .context("query turn diff projection states for execution threads")?
            .into_iter()
            .map(|row| {
                let state = decode_state(&row.state_json, &row.thread_id, &row.turn_id)?;
                validate_projection_row(&row, &state)?;
                Ok(state)
            })
            .collect()
    }
}

fn validate_lookup_identity(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_PROJECTION_ID_BYTES {
        return Err(anyhow!(
            "turn diff {label} is empty or exceeds its persisted bound"
        ));
    }
    Ok(())
}

fn encode_state(state: &TurnDiffState) -> Result<String> {
    validate_turn_diff_state(state).map_err(|error| anyhow!("invalid turn diff state: {error}"))?;
    let payload = serde_json::to_string(state)?;
    if payload.len() > MAX_PROJECTION_JSON_BYTES {
        return Err(anyhow!(
            "turn diff projection exceeds the persisted size bound"
        ));
    }
    Ok(payload)
}

fn decode_state(payload: &str, thread_id: &str, turn_id: &str) -> Result<TurnDiffState> {
    if payload.len() > MAX_PROJECTION_JSON_BYTES {
        return Err(anyhow!(
            "stored turn diff projection exceeds the decode bound"
        ));
    }
    let state: TurnDiffState = serde_json::from_str(payload)
        .map_err(|error| anyhow!("decode stored turn diff projection: {error}"))?;
    validate_turn_diff_state(&state)
        .map_err(|error| anyhow!("invalid stored turn diff state: {error}"))?;
    if state.thread_id != thread_id || state.turn_id != turn_id {
        return Err(anyhow!(
            "stored turn diff identity disagrees with projection key"
        ));
    }
    Ok(state)
}

fn validate_projection_row(row: &TurnDiffStateRow, state: &TurnDiffState) -> Result<(i64, i64)> {
    let revision = row.revision;
    if revision < 0 {
        return Err(anyhow!("stored turn diff revision is negative"));
    }
    let final_state = row.final_state;
    if !matches!(final_state, 0 | 1) {
        return Err(anyhow!("turn diff final_state is not boolean"));
    }
    let expected_authority = serde_json::to_string(&state.authority)?;
    if row.authority != expected_authority {
        return Err(anyhow!(
            "stored turn diff authority disagrees with state payload"
        ));
    }
    if row.exact != if state.exact { 1 } else { 0 } {
        return Err(anyhow!(
            "stored turn diff exactness disagrees with state payload"
        ));
    }
    if row.coverage_json != serde_json::to_string(&state.coverage)? {
        return Err(anyhow!(
            "stored turn diff coverage disagrees with state payload"
        ));
    }
    let expected_applied_through = state
        .applied_through_ordinal
        .map(|ordinal| sqlite_i64(ordinal.0, "turn diff applied ordinal"))
        .transpose()?;
    if row.applied_through_ordinal != expected_applied_through {
        return Err(anyhow!(
            "stored turn diff applied ordinal disagrees with state payload"
        ));
    }
    if row.record_count < 0 || row.record_count as u64 != state.record_count {
        return Err(anyhow!(
            "stored turn diff record count disagrees with state payload"
        ));
    }
    if final_state != if state.final_state { 1 } else { 0 } {
        return Err(anyhow!(
            "stored turn diff final state disagrees with state payload"
        ));
    }
    Ok((revision, final_state))
}

fn projection_write(state: &TurnDiffState, payload: String) -> Result<TurnDiffStateWrite> {
    Ok(TurnDiffStateWrite {
        thread_id: state.thread_id.clone(),
        turn_id: state.turn_id.clone(),
        revision: sqlite_i64(state.revision, "turn diff revision")?,
        authority: serde_json::to_string(&state.authority)?,
        exact: i64::from(state.exact),
        coverage_json: serde_json::to_string(&state.coverage)?,
        applied_through_ordinal: state
            .applied_through_ordinal
            .map(|ordinal| sqlite_i64(ordinal.0, "turn diff applied ordinal"))
            .transpose()?,
        record_count: sqlite_i64(state.record_count, "turn diff record count")?,
        final_state: i64::from(state.final_state),
        state_json: payload,
    })
}

fn sqlite_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{label} exceeds SQLite integer range"))
}

fn same_projection_ignoring_terminal(existing: &TurnDiffState, candidate: &TurnDiffState) -> bool {
    let mut existing = existing.clone();
    existing.final_state = candidate.final_state;
    existing.filesystem_coverage = candidate.filesystem_coverage.clone();
    existing == *candidate
}
