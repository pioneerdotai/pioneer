//! Database-backed Codex aggregate-state adapter.

use crate::apply_patch::history::codex::{
    CODEX_MAX_CONTEXT_FIELD_BYTES, CODEX_MAX_EVENT_IDS, CODEX_MAX_STATE_JSON_BYTES,
    CodexAggregateEvent, CodexAggregateState, event_payload_digest, validate_event, validate_state,
};
use anyhow::{Context, Result, anyhow};
use pioneer_crud::{
    CodexAggregateStateRow, CodexAggregateStateWrite, finalize_codex_aggregate_state,
    find_codex_aggregate_state, find_first_codex_aggregate_state_for_thread,
    upsert_codex_aggregate_state,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::TransactionTrait;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;

static CODEX_PROJECTION_LOCK: OnceLock<Arc<Mutex<()>>> = OnceLock::new();

/// SQLite projection for Codex aggregate events.  Applied patch records are
/// intentionally never written here: Codex remains an AggregateOnly source.
#[derive(Clone)]
pub struct SqliteCodexAggregateStore {
    db: SqliteDatabase,
}

impl SqliteCodexAggregateStore {
    pub fn new(db: impl Into<SqliteDatabase>) -> Self {
        Self { db: db.into() }
    }

    /// Ingest one normalized `turn/diff/updated` event into the durable
    /// aggregate projection. Codex owns filesystem execution, so this path
    /// never creates an AppliedPatchRecord or reads the workspace.
    pub async fn ingest_event(&self, event: &CodexAggregateEvent) -> Result<bool> {
        validate_event(event).map_err(|error| anyhow!(error))?;
        let _guard = CODEX_PROJECTION_LOCK
            .get_or_init(|| Arc::new(Mutex::new(())))
            .lock()
            .await;
        let existing = self
            .get(event.thread_id.as_str(), event.turn_id.as_str())
            .await?;
        let Some(mut state) = existing else {
            return self
                .upsert(&CodexAggregateState::from_event(
                    event,
                    event.revision.max(1),
                ))
                .await;
        };
        if state.workspace_id != event.workspace_id
            || state.protocol_revision != event.protocol_revision
        {
            return Err(anyhow!(
                "Codex aggregate event changes the trusted workspace or protocol identity"
            ));
        }
        if state.final_state {
            if let Some(index) = state.event_ids.iter().position(|id| id == &event.event_id) {
                if state.event_payload_digests[index] != event_payload_digest(event) {
                    return Err(anyhow!(
                        "Codex aggregate event ID was replayed with a different payload"
                    ));
                }
                return Ok(false);
            }
            return Err(anyhow!("final Codex aggregate state is immutable"));
        }
        if let Some(index) = state.event_ids.iter().position(|id| id == &event.event_id) {
            if state.event_payload_digests[index] != event_payload_digest(event) {
                return Err(anyhow!(
                    "Codex aggregate event ID was replayed with a different payload"
                ));
            }
            return Ok(false);
        }
        if state.event_ids.len() >= CODEX_MAX_EVENT_IDS {
            return Err(anyhow!(
                "Codex aggregate event history exceeds maximum {CODEX_MAX_EVENT_IDS}"
            ));
        }
        let incoming_revision = if event.revision == 0 {
            state.revision.saturating_add(1)
        } else {
            event.revision
        };
        if incoming_revision > state.revision {
            let mut next = CodexAggregateState::from_event(event, incoming_revision);
            next.event_count = state.event_count.saturating_add(1);
            next.event_ids = state.event_ids;
            next.event_ids.push(event.event_id.clone());
            next.event_payload_digests = state.event_payload_digests;
            next.event_payload_digests.push(event_payload_digest(event));
            return self.upsert(&next).await;
        }
        if incoming_revision < state.revision {
            // Keep the latest aggregate bytes while retaining the event id so
            // a reconnect/replay cannot count the same provider event twice.
            state.event_count = state.event_count.saturating_add(1);
            state.event_ids.push(event.event_id.clone());
            state
                .event_payload_digests
                .push(event_payload_digest(event));
            return self.upsert(&state).await;
        }
        Err(anyhow!(
            "Codex aggregate revision {} conflicts with a different event",
            incoming_revision
        ))
    }

    /// Freeze the provider aggregate at terminal turn lifecycle. Repeated
    /// finalization is idempotent; a late new event is rejected by
    /// `ingest_event`.
    pub async fn finalize(&self, thread_id: &str, turn_id: &str) -> Result<bool> {
        validate_lookup_identity(thread_id, "thread_id")?;
        validate_lookup_identity(turn_id, "turn_id")?;
        let _guard = CODEX_PROJECTION_LOCK
            .get_or_init(|| Arc::new(Mutex::new(())))
            .lock()
            .await;
        let Some(mut state) = self.get(thread_id, turn_id).await? else {
            // A turn can complete without emitting a diff event.  There is
            // then no aggregate row to freeze, but finalization is still a
            // successful idempotent lifecycle operation for the caller.
            return Ok(true);
        };
        if state.final_state {
            return Ok(true);
        }
        state.final_state = true;
        validate_state(&state).map_err(|error| anyhow!(error))?;
        let payload = bounded_state_json(&state)?;
        let revision = sqlite_i64(state.revision, "Codex aggregate revision")?;
        let updated =
            finalize_codex_aggregate_state(&self.db, thread_id, turn_id, revision, payload)
                .await
                .context("finalize Codex aggregate projection")?;
        Ok(updated)
    }

    pub async fn upsert(&self, state: &CodexAggregateState) -> Result<bool> {
        validate_state(state).map_err(|error| anyhow!(error))?;
        let revision_sql = sqlite_i64(state.revision, "Codex aggregate revision")?;
        let existing = find_codex_aggregate_state(&self.db, &state.thread_id, &state.turn_id)
            .await
            .context("query Codex aggregate projection")?;
        if let Some(existing) = existing.as_ref() {
            let revision = existing.revision;
            let final_state = existing.final_state;
            if !matches!(final_state, 0 | 1) {
                return Err(anyhow!("Codex aggregate final_state is not boolean"));
            }
            let existing_json = existing.state_json.clone();
            let existing_state = decode_state_json(&existing_json)?;
            if existing_state.thread_id != state.thread_id
                || existing_state.turn_id != state.turn_id
            {
                return Err(anyhow!(
                    "stored Codex aggregate identity disagrees with projection key"
                ));
            }
            if revision < 0 || existing_state.revision != revision as u64 {
                return Err(anyhow!(
                    "stored Codex aggregate revision disagrees with state payload"
                ));
            }
            if existing_state.final_state != (final_state != 0) {
                return Err(anyhow!(
                    "stored Codex aggregate final_state disagrees with state payload"
                ));
            }
            let payload = bounded_state_json(state)?;
            if final_state != 0 {
                if existing_json == payload {
                    return Ok(false);
                }
                return Err(anyhow!("final Codex aggregate state is immutable"));
            }
            let revision = u64::try_from(revision)
                .map_err(|_| anyhow!("stored Codex aggregate revision is negative"))?;
            if state.revision < revision {
                return Ok(false);
            }
            if state.revision == revision {
                if existing_json == payload {
                    return Ok(false);
                }
                if !same_aggregate_payload(&existing_state, state) {
                    return Err(anyhow!("Codex aggregate revision conflicts"));
                }
                if state.event_count < existing_state.event_count
                    || !existing_state
                        .event_ids
                        .iter()
                        .all(|event_id| state.event_ids.contains(event_id))
                {
                    return Err(anyhow!("Codex aggregate event history regressed"));
                }
                for (index, event_id) in existing_state.event_ids.iter().enumerate() {
                    let Some(candidate_index) =
                        state.event_ids.iter().position(|id| id == event_id)
                    else {
                        return Err(anyhow!("Codex aggregate event history regressed"));
                    };
                    if existing_state.event_payload_digests[index]
                        != state.event_payload_digests[candidate_index]
                    {
                        return Err(anyhow!(
                            "Codex aggregate event history contains a conflicting payload"
                        ));
                    }
                }
            }
        }
        let payload = bounded_state_json(state)?;
        let transaction = self
            .db
            .begin()
            .await
            .context("begin Codex aggregate projection")?;
        let current = find_codex_aggregate_state(&transaction, &state.thread_id, &state.turn_id)
            .await
            .context("revalidate Codex aggregate projection")?;
        if current != existing {
            transaction.rollback().await.ok();
            return Err(anyhow!(
                "Codex aggregate projection changed while its write was preparing"
            ));
        }
        upsert_codex_aggregate_state(
            &transaction,
            CodexAggregateStateWrite {
                thread_id: state.thread_id.clone(),
                turn_id: state.turn_id.clone(),
                revision: revision_sql,
                final_state: i64::from(state.final_state),
                state_json: payload,
            },
        )
        .await
        .context("upsert Codex aggregate projection")?;
        transaction
            .commit()
            .await
            .context("commit Codex aggregate projection")?;
        Ok(true)
    }

    pub async fn get(&self, thread_id: &str, turn_id: &str) -> Result<Option<CodexAggregateState>> {
        validate_lookup_identity(thread_id, "thread_id")?;
        validate_lookup_identity(turn_id, "turn_id")?;
        let row = find_codex_aggregate_state(&self.db, thread_id, turn_id)
            .await
            .context("query Codex aggregate state")?;
        row.map(|row| decode_projected_state(&row, thread_id, Some(turn_id)))
            .transpose()
    }

    /// Return one validated aggregate-only state for thread-level history
    /// coverage decisions without materializing every provider aggregate.
    pub async fn first_for_thread(&self, thread_id: &str) -> Result<Option<CodexAggregateState>> {
        validate_lookup_identity(thread_id, "thread_id")?;
        let row = find_first_codex_aggregate_state_for_thread(&self.db, thread_id)
            .await
            .context("query first Codex aggregate state for thread")?;
        row.map(|row| decode_projected_state(&row, thread_id, None))
            .transpose()
    }
}

fn decode_projected_state(
    row: &CodexAggregateStateRow,
    thread_id: &str,
    turn_id: Option<&str>,
) -> Result<CodexAggregateState> {
    if row.revision < 0 || !matches!(row.final_state, 0 | 1) {
        return Err(anyhow!(
            "Codex aggregate row contains invalid scalar metadata"
        ));
    }
    let state = decode_state_json(&row.state_json)?;
    if state.thread_id != thread_id
        || turn_id.is_some_and(|turn_id| state.turn_id != turn_id)
        || state.revision != row.revision as u64
        || state.final_state != (row.final_state != 0)
    {
        return Err(anyhow!(
            "stored Codex aggregate disagrees with its projection row"
        ));
    }
    Ok(state)
}

fn validate_lookup_identity(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > CODEX_MAX_CONTEXT_FIELD_BYTES {
        return Err(anyhow!("Codex {label} is empty or exceeds its bound"));
    }
    Ok(())
}

fn bounded_state_json(state: &CodexAggregateState) -> Result<String> {
    let payload = serde_json::to_string(state).context("encode Codex aggregate state")?;
    if payload.len() > CODEX_MAX_STATE_JSON_BYTES {
        return Err(anyhow!(
            "Codex aggregate state exceeds maximum {} bytes",
            CODEX_MAX_STATE_JSON_BYTES
        ));
    }
    Ok(payload)
}

fn decode_state_json(payload: &str) -> Result<CodexAggregateState> {
    if payload.len() > CODEX_MAX_STATE_JSON_BYTES {
        return Err(anyhow!(
            "stored Codex aggregate state exceeds maximum {} bytes",
            CODEX_MAX_STATE_JSON_BYTES
        ));
    }
    let state: CodexAggregateState =
        serde_json::from_str(payload).context("decode stored Codex aggregate state")?;
    validate_state(&state).map_err(|error| anyhow!(error))?;
    Ok(state)
}

fn same_aggregate_payload(left: &CodexAggregateState, right: &CodexAggregateState) -> bool {
    left.schema_version == right.schema_version
        && left.workspace_id == right.workspace_id
        && left.thread_id == right.thread_id
        && left.turn_id == right.turn_id
        && left.protocol_revision == right.protocol_revision
        && left.authority == right.authority
        && left.exactness == right.exactness
        && left.exact == right.exact
        && left.coverage == right.coverage
        && left.revision == right.revision
        && left.final_state == right.final_state
        && left.diff == right.diff
        && left.files == right.files
}

fn sqlite_i64(value: u64, label: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{label} exceeds SQLite integer range"))
}
