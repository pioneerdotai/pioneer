use anyhow::{Context, Result, bail};
use pioneer_entity::{native_terminal_effect_outbox, task_result_candidate, thread, turn};
use pioneer_protocol::{
    NativeTerminalEffectGate, NativeTerminalEffectKind, NativeTerminalEffectPayload,
    NativeTerminalEffectPreparation, NativeTerminalEffectSpec,
};
use pioneer_sqlite::SqliteDatabase;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, EntityTrait,
    FromQueryResult, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    Statement, TransactionTrait,
};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub const STATUS_PREPARED: &str = "prepared";
pub const STATUS_WAITING_ACCEPTANCE: &str = "waiting_acceptance";
pub const STATUS_READY: &str = "ready";
pub const STATUS_RUNNING: &str = "running";
pub const STATUS_RETRY_WAIT: &str = "retry_wait";
pub const STATUS_SUCCEEDED: &str = "succeeded";
pub const STATUS_UNRESOLVED: &str = "unresolved";
pub const STATUS_DISCARDED: &str = "discarded";
pub const STATUS_SUPERSEDED: &str = "superseded";

pub const MAX_EFFECTS_PER_TURN: usize = 2;
pub const EFFECT_INPUT_BUDGET: u64 = 8;
const MAX_GATE_PROBE_ATTEMPTS: i64 = 16;
pub const MAX_EFFECT_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_EFFECT_HANDLER_CHECKPOINT_BYTES: usize = 128 * 1024;
pub const MAX_EFFECT_ATTEMPTS: u16 = 20;
pub const MAX_PURGE_BATCH_SIZE: u64 = 1_000;
pub const MAX_RETRYABLE_UNRESOLVED_REQUEUE_BATCH_SIZE: u64 = 100;
const MAX_ERROR_CODE_CHARS: usize = 64;
const MAX_ERROR_MESSAGE_CHARS: usize = 2_048;
const COMPACTED_PAYLOAD_JSON: &str = r#"{"compacted":true}"#;

#[derive(Debug, Clone)]
pub struct ClaimedNativeTerminalEffect {
    pub row: native_terminal_effect_outbox::Model,
    pub claim_token: String,
}

/// Independent confirmed claims plus a low-cardinality failure signal.
#[derive(Debug, Default)]
pub struct NativeTerminalEffectClaimBatch {
    pub claimed: Vec<ClaimedNativeTerminalEffect>,
    pub storage_failed: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeTerminalEffectStats {
    pub prepared: u64,
    pub waiting_acceptance: u64,
    pub ready: u64,
    pub running: u64,
    pub retry_wait: u64,
    pub succeeded: u64,
    pub unresolved: u64,
}

#[derive(Debug, Clone)]
pub struct PreparedNativeTerminalEffectPreparation {
    preparation: NativeTerminalEffectPreparation,
    runtime_generation: i64,
    effects: Vec<PreparedNativeTerminalEffect>,
    compacted_payload_sha256: String,
}

#[derive(Debug, Clone)]
struct PreparedNativeTerminalEffect {
    payload_json: String,
    payload_sha256: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedNativeTerminalEffectActivation {
    turn_id: String,
    rows: Vec<PreparedNativeTerminalEffectActivationRow>,
}

#[derive(Debug, Clone)]
struct PreparedNativeTerminalEffectActivationRow {
    effect_id: String,
    thread_id: String,
    effect_kind: String,
    gate_kind: String,
    // Terminal repair preparation decoded these exact bytes. Hash columns
    // and updated_at alone cannot fence a same-timestamp physical SQL edit.
    expected_payload_json: String,
    payload_sha256: String,
    payload_identity_sha256: String,
    updated_at: DateTimeWithTimeZone,
    candidate_state: Option<CandidateGateState>,
    status: &'static str,
    candidate_id: Option<String>,
    run_on_commit: bool,
    complete_on_commit: bool,
    error_code: Option<String>,
    error_message: Option<String>,
    compact_payload: bool,
    compacted_payload_sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedCandidateGateResolution {
    candidate_id: String,
    thread_id: String,
    turn_id: String,
    resolved_at: DateTimeWithTimeZone,
    requires_fence: bool,
    latest: Option<CandidateGateMetadata>,
    probe_token: Option<String>,
    rows: Vec<PreparedCandidateGateResolutionRow>,
}

#[derive(Debug, Clone)]
struct PreparedCandidateGateResolutionRow {
    effect_id: String,
    status_before: String,
    updated_at_before: DateTimeWithTimeZone,
    payload_json_before: String,
    payload_sha256_before: String,
    payload_identity_sha256_before: String,
    terminal_committed_at_before: Option<DateTimeWithTimeZone>,
    status_after: &'static str,
    accepted_candidate_id: Option<String>,
    next_run_at: Option<DateTimeWithTimeZone>,
    completed_at: Option<DateTimeWithTimeZone>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    compact_payload: bool,
    compacted_payload_sha256: Option<String>,
}

pub fn prepare_input(
    preparation: NativeTerminalEffectPreparation,
) -> Result<PreparedNativeTerminalEffectPreparation> {
    validate_preparation(&preparation)?;
    let runtime_generation = i64::try_from(preparation.runtime_generation)
        .context("terminal-effect runtime generation exceeds database range")?;
    let mut effects = Vec::with_capacity(preparation.effects.len());
    for effect in &preparation.effects {
        let payload_json = serde_json::to_string(&effect.payload)
            .context("failed to serialize native terminal-effect payload")?;
        let payload_sha256 = payload_sha256_hex(payload_json.as_str());
        effects.push(PreparedNativeTerminalEffect {
            payload_json,
            payload_sha256,
        });
    }
    Ok(PreparedNativeTerminalEffectPreparation {
        preparation,
        runtime_generation,
        effects,
        compacted_payload_sha256: payload_sha256_hex(COMPACTED_PAYLOAD_JSON),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CandidateGateState {
    Waiting,
    Accepted(CandidateGateMetadata),
    Rejected(CandidateGateMetadata),
}

pub async fn prepare<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedNativeTerminalEffectPreparation,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    prepare_with_policy(db, prepared, now, true).await
}

/// A terminal adapter retries the same observed completion after reconnect.
/// Its first durable hook snapshot wins; never refresh it from live settings.
pub async fn post_turn_batch_exists<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    batch_id: &str,
) -> Result<bool> {
    Ok(native_terminal_effect_outbox::Entity::find_by_id(format!(
        "{turn_id}:terminal-effect:post-turn"
    ))
    .filter(native_terminal_effect_outbox::Column::TurnId.eq(turn_id))
    .filter(native_terminal_effect_outbox::Column::BatchId.eq(batch_id))
    .filter(native_terminal_effect_outbox::Column::Status.ne(STATUS_SUPERSEDED))
    .count(db)
    .await?
        != 0)
}

/// Called inside the writer transaction. Preserve unrelated cleanup obligations
/// and recheck the read-side existence hint under the serialized writer.
pub async fn prepare_post_turn_once<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedNativeTerminalEffectPreparation,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    let input = &prepared.preparation;
    if input.effects.len() != 1
        || input.effects[0].effect_kind != NativeTerminalEffectKind::PostTurnHook
        || input.effects[0].effect_id != format!("{}:terminal-effect:post-turn", input.turn_id)
    {
        bail!("post-turn preparation requires exactly one canonical hook obligation");
    }
    if let Some(existing) =
        native_terminal_effect_outbox::Entity::find_by_id(input.effects[0].effect_id.clone())
            .one(db)
            .await?
    {
        validate_existing_identity(&existing, input, &input.effects[0])?;
        if existing.batch_id == input.batch_id && existing.status != STATUS_SUPERSEDED {
            return Ok(());
        }
    }
    prepare_with_policy(db, prepared, now, false).await
}

/// Merge a recovery-owned obligation without superseding a hook/cleanup plan
/// already prepared by the live actor. This is used only inside the canonical
/// recovery terminalization transaction.
pub async fn prepare_supplemental<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedNativeTerminalEffectPreparation,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    let PreparedNativeTerminalEffectPreparation {
        preparation,
        runtime_generation,
        effects,
        compacted_payload_sha256,
    } = prepared;
    if super::native_cancellation_context::has_accepted(db, &preparation.turn_id).await? {
        bail!("recovery supplemental preparation was superseded by accepted native cancellation");
    }
    if preparation.effects.iter().any(|effect| {
        effect.gate != NativeTerminalEffectGate::TerminalCommit
            || effect.effect_kind != NativeTerminalEffectKind::AttachedTaskCleanup
    }) {
        bail!("supplemental recovery effects must be terminal-commit task cleanup obligations");
    }

    let turn_row = turn::Entity::find_by_id(preparation.turn_id.clone())
        .one(db)
        .await
        .context("failed to load supplemental terminal-effect Turn")?
        .with_context(|| {
            format!(
                "supplemental terminal-effect Turn `{}` does not exist",
                preparation.turn_id
            )
        })?;
    if turn_row.status == "in_progress" {
        return prepare_with_policy(
            db,
            PreparedNativeTerminalEffectPreparation {
                preparation,
                runtime_generation,
                effects,
                compacted_payload_sha256,
            },
            now,
            false,
        )
        .await;
    }
    if turn_row.thread_id != preparation.thread_id {
        bail!("supplemental terminal-effect preparation has a mismatched thread scope");
    }
    let thread_row = thread::Entity::find_by_id(preparation.thread_id.clone())
        .one(db)
        .await
        .context("failed to load supplemental terminal-effect thread")?
        .with_context(|| {
            format!(
                "supplemental terminal-effect thread `{}` does not exist",
                preparation.thread_id
            )
        })?;
    if thread_row.workspace_id != preparation.workspace_id {
        bail!("supplemental terminal-effect preparation has a mismatched workspace scope");
    }

    // Rolling upgrades can discover a recovery terminalization only after an
    // older Gateway has committed the terminal Turn. That canonical event can
    // no longer activate a newly reconstructed cleanup row, so this recovery
    // transaction acts as the terminal fence and inserts (or repairs) the
    // missing obligation directly in `ready`. A previously committed row is
    // immutable authority and is never rewritten.
    for (effect, prepared_effect) in preparation.effects.iter().zip(effects) {
        let payload_json = prepared_effect.payload_json;
        let payload_sha256 = prepared_effect.payload_sha256;
        if let Some(existing) =
            native_terminal_effect_outbox::Entity::find_by_id(effect.effect_id.clone())
                .one(db)
                .await
                .context("failed to query supplemental native terminal effect")?
        {
            validate_existing_identity(&existing, &preparation, effect)?;
            if existing.terminal_committed_at.is_some() {
                if existing.gate_kind != gate_to_db(NativeTerminalEffectGate::TerminalCommit) {
                    bail!(
                        "committed supplemental terminal effect `{}` has a conflicting gate",
                        effect.effect_id
                    );
                }
                // The effect already activated by the canonical terminal
                // commit is immutable authority. Recovery may reconstruct a
                // different explanatory reason or runtime generation after a
                // rolling upgrade; that must neither rewrite the committed
                // obligation nor poison the recovery outbox forever.
                continue;
            }
            if !matches!(
                existing.status.as_str(),
                STATUS_PREPARED | STATUS_SUPERSEDED
            ) {
                bail!(
                    "uncommitted supplemental terminal effect `{}` has invalid status `{}`",
                    effect.effect_id,
                    existing.status
                );
            }
            let mut active = existing.into_active_model();
            active.batch_id = Set(preparation.batch_id.clone());
            active.runtime_generation = Set(runtime_generation);
            active.gate_kind = Set(gate_to_db(effect.gate).to_owned());
            active.payload_json = Set(payload_json);
            active.payload_sha256 = Set(payload_sha256.clone());
            active.payload_identity_sha256 = Set(payload_sha256);
            active.handler_checkpoint_json = Set(None);
            active.handler_checkpoint_sha256 = Set(None);
            active.status = Set(STATUS_READY.to_owned());
            active.accepted_candidate_id = Set(None);
            active.attempt_count = Set(0);
            active.max_attempts = Set(i64::from(effect.max_attempts));
            active.last_error_code = Set(None);
            active.last_error_message = Set(None);
            active.next_run_at = Set(Some(now));
            active.gate_probe_at = Set(0);
            active.gate_probe_attempts = Set(0);
            active.gate_probe_token = Set(None);
            active.claim_token = Set(None);
            active.claim_expires_at = Set(None);
            active.terminal_committed_at = Set(Some(now));
            active.completed_at = Set(None);
            active.prepared_at = Set(now);
            active.updated_at = Set(now);
            active
                .update(db)
                .await
                .context("failed to repair supplemental native terminal effect")?;
        } else {
            native_terminal_effect_outbox::ActiveModel {
                effect_id: Set(effect.effect_id.clone()),
                batch_id: Set(preparation.batch_id.clone()),
                workspace_id: Set(preparation.workspace_id.clone()),
                thread_id: Set(preparation.thread_id.clone()),
                turn_id: Set(preparation.turn_id.clone()),
                runtime_generation: Set(runtime_generation),
                effect_kind: Set(kind_to_db(effect.effect_kind).to_owned()),
                gate_kind: Set(gate_to_db(effect.gate).to_owned()),
                payload_json: Set(payload_json),
                payload_sha256: Set(payload_sha256.clone()),
                payload_identity_sha256: Set(payload_sha256),
                handler_checkpoint_json: Set(None),
                handler_checkpoint_sha256: Set(None),
                status: Set(STATUS_READY.to_owned()),
                accepted_candidate_id: Set(None),
                attempt_count: Set(0),
                max_attempts: Set(i64::from(effect.max_attempts)),
                last_error_code: Set(None),
                last_error_message: Set(None),
                next_run_at: Set(Some(now)),
                claim_token: Set(None),
                claim_expires_at: Set(None),
                terminal_committed_at: Set(Some(now)),
                completed_at: Set(None),
                prepared_at: Set(now),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(db)
            .await
            .context("failed to insert supplemental native terminal effect")?;
        }
    }
    Ok(())
}

async fn prepare_with_policy<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedNativeTerminalEffectPreparation,
    now: DateTimeWithTimeZone,
    supersede_omitted_effects: bool,
) -> Result<()> {
    let PreparedNativeTerminalEffectPreparation {
        preparation,
        runtime_generation,
        effects,
        compacted_payload_sha256,
    } = prepared;

    let turn_row = turn::Entity::find_by_id(preparation.turn_id.clone())
        .one(db)
        .await
        .context("failed to load terminal-effect Turn")?
        .with_context(|| {
            format!(
                "terminal-effect Turn `{}` does not exist",
                preparation.turn_id
            )
        })?;
    if turn_row.thread_id != preparation.thread_id {
        bail!("terminal-effect preparation has a mismatched thread scope");
    }
    let thread_row = thread::Entity::find_by_id(preparation.thread_id.clone())
        .one(db)
        .await
        .context("failed to load terminal-effect thread")?
        .with_context(|| {
            format!(
                "terminal-effect thread `{}` does not exist",
                preparation.thread_id
            )
        })?;
    if thread_row.workspace_id != preparation.workspace_id {
        bail!("terminal-effect preparation has a mismatched workspace scope");
    }

    let terminal = turn_row.status != "in_progress";
    if !terminal
        && super::native_cancellation_context::has_accepted(db, &preparation.turn_id).await?
    {
        bail!("terminal-effect preparation was superseded by accepted native cancellation");
    }
    if !terminal && supersede_omitted_effects {
        native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                native_terminal_effect_outbox::Column::Status,
                Expr::value(STATUS_SUPERSEDED.to_owned()),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::PayloadJson,
                Expr::value(COMPACTED_PAYLOAD_JSON.to_owned()),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::PayloadSha256,
                Expr::value(compacted_payload_sha256),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::HandlerCheckpointJson,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::HandlerCheckpointSha256,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::CompletedAt,
                Expr::value(Some(now)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::UpdatedAt,
                Expr::value(now),
            )
            .filter(native_terminal_effect_outbox::Column::TurnId.eq(preparation.turn_id.clone()))
            .filter(native_terminal_effect_outbox::Column::TerminalCommittedAt.is_null())
            .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_PREPARED))
            .exec(db)
            .await
            .context("failed to supersede prior terminal-effect preparation")?;
    }

    for (effect, prepared_effect) in preparation.effects.iter().zip(effects) {
        let payload_json = prepared_effect.payload_json;
        let payload_sha256 = prepared_effect.payload_sha256;

        if let Some(existing) =
            native_terminal_effect_outbox::Entity::find_by_id(effect.effect_id.clone())
                .one(db)
                .await
                .context("failed to query existing native terminal effect")?
        {
            validate_existing_identity(&existing, &preparation, effect)?;
            if terminal {
                if existing.terminal_committed_at.is_none() {
                    bail!(
                        "terminal effect `{}` was not activated by its canonical Turn commit",
                        effect.effect_id
                    );
                }
                if existing.batch_id != preparation.batch_id
                    || existing.payload_identity_sha256 != payload_sha256
                    || existing.gate_kind != gate_to_db(effect.gate)
                {
                    bail!(
                        "terminal effect `{}` is already committed with a conflicting immutable payload",
                        effect.effect_id
                    );
                }
                continue;
            }
            if existing.terminal_committed_at.is_some() {
                bail!(
                    "terminal effect `{}` cannot be rewritten by an in-progress Turn",
                    effect.effect_id
                );
            }
            let accepted_candidate_id =
                if effect.gate == NativeTerminalEffectGate::AcceptedTaskResult {
                    match candidate_gate_state(
                        db,
                        preparation.thread_id.as_str(),
                        preparation.turn_id.as_str(),
                    )
                    .await?
                    {
                        CandidateGateState::Accepted(candidate) => Some(candidate.id),
                        CandidateGateState::Waiting | CandidateGateState::Rejected(_) => None,
                    }
                } else {
                    None
                };
            let mut active = existing.into_active_model();
            active.batch_id = Set(preparation.batch_id.clone());
            active.runtime_generation = Set(runtime_generation);
            active.gate_kind = Set(gate_to_db(effect.gate).to_owned());
            active.payload_json = Set(payload_json);
            active.payload_sha256 = Set(payload_sha256.clone());
            active.payload_identity_sha256 = Set(payload_sha256);
            active.handler_checkpoint_json = Set(None);
            active.handler_checkpoint_sha256 = Set(None);
            active.status = Set(STATUS_PREPARED.to_owned());
            active.accepted_candidate_id = Set(accepted_candidate_id);
            active.attempt_count = Set(0);
            active.max_attempts = Set(i64::from(effect.max_attempts));
            active.last_error_code = Set(None);
            active.last_error_message = Set(None);
            active.next_run_at = Set(None);
            active.gate_probe_at = Set(0);
            active.gate_probe_attempts = Set(0);
            active.gate_probe_token = Set(None);
            active.claim_token = Set(None);
            active.claim_expires_at = Set(None);
            active.terminal_committed_at = Set(None);
            active.completed_at = Set(None);
            active.prepared_at = Set(now);
            active.updated_at = Set(now);
            active
                .update(db)
                .await
                .context("failed to refresh native terminal effect")?;
        } else {
            if terminal {
                bail!(
                    "terminal effect `{}` cannot be created after the canonical Turn commit",
                    effect.effect_id
                );
            }
            let accepted_candidate_id =
                if effect.gate == NativeTerminalEffectGate::AcceptedTaskResult {
                    match candidate_gate_state(
                        db,
                        preparation.thread_id.as_str(),
                        preparation.turn_id.as_str(),
                    )
                    .await?
                    {
                        CandidateGateState::Accepted(candidate) => Some(candidate.id),
                        CandidateGateState::Waiting | CandidateGateState::Rejected(_) => None,
                    }
                } else {
                    None
                };
            native_terminal_effect_outbox::ActiveModel {
                effect_id: Set(effect.effect_id.clone()),
                batch_id: Set(preparation.batch_id.clone()),
                workspace_id: Set(preparation.workspace_id.clone()),
                thread_id: Set(preparation.thread_id.clone()),
                turn_id: Set(preparation.turn_id.clone()),
                runtime_generation: Set(runtime_generation),
                effect_kind: Set(kind_to_db(effect.effect_kind).to_owned()),
                gate_kind: Set(gate_to_db(effect.gate).to_owned()),
                payload_json: Set(payload_json),
                payload_sha256: Set(payload_sha256.clone()),
                payload_identity_sha256: Set(payload_sha256),
                handler_checkpoint_json: Set(None),
                handler_checkpoint_sha256: Set(None),
                status: Set(STATUS_PREPARED.to_owned()),
                accepted_candidate_id: Set(accepted_candidate_id),
                attempt_count: Set(0),
                max_attempts: Set(i64::from(effect.max_attempts)),
                last_error_code: Set(None),
                last_error_message: Set(None),
                next_run_at: Set(None),
                claim_token: Set(None),
                claim_expires_at: Set(None),
                terminal_committed_at: Set(None),
                completed_at: Set(None),
                prepared_at: Set(now),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(db)
            .await
            .context("failed to insert native terminal effect")?;
        }
    }
    Ok(())
}

/// Prepares payload validation, hashing, decoding, and error formatting before
/// the canonical terminal projection obtains writer admission.
pub(crate) async fn prepare_activation_for_terminal<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<PreparedNativeTerminalEffectActivation> {
    let rows = native_terminal_effect_outbox::Entity::find()
        .filter(native_terminal_effect_outbox::Column::TurnId.eq(turn_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_PREPARED))
        .filter(native_terminal_effect_outbox::Column::TerminalCommittedAt.is_null())
        .order_by_asc(native_terminal_effect_outbox::Column::EffectId)
        .limit((MAX_EFFECTS_PER_TURN + 1) as u64)
        .all(db)
        .await
        .context("failed to load prepared native terminal effects")?;
    if rows.len() > MAX_EFFECTS_PER_TURN {
        bail!(
            "Turn `{turn_id}` has more than {MAX_EFFECTS_PER_TURN} prepared native terminal effects"
        );
    }

    let mut prepared_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let decoded = if row.payload_json.len() <= MAX_EFFECT_PAYLOAD_BYTES
            && payload_integrity_matches(
                row.payload_json.as_str(),
                row.payload_sha256.as_str(),
                row.payload_identity_sha256.as_str(),
            ) {
            serde_json::from_str::<NativeTerminalEffectPayload>(row.payload_json.as_str()).ok()
        } else {
            None
        };
        let gate = gate_from_db(row.gate_kind.as_str());
        let candidate_state = match gate.as_ref() {
            Ok(NativeTerminalEffectGate::AcceptedTaskResult) => {
                Some(candidate_gate_state(db, row.thread_id.as_str(), turn_id).await?)
            }
            _ => None,
        };
        let (status, candidate_id, run_on_commit, complete_on_commit, error_code, error_message) =
            match decoded {
                Some(payload) if !payload_matches_db_kind(row.effect_kind.as_str(), &payload) => (
                    STATUS_UNRESOLVED,
                    None,
                    false,
                    true,
                    Some("invalid_persisted_kind".to_owned()),
                    Some(
                        "persisted native terminal-effect kind does not match its payload"
                            .to_owned(),
                    ),
                ),
                Some(NativeTerminalEffectPayload::PostTurnHookPreparationFailed { failure }) => {
                    match gate.as_ref() {
                        Ok(NativeTerminalEffectGate::TerminalCommit) => (
                            STATUS_UNRESOLVED,
                            None,
                            false,
                            true,
                            Some("terminal_effect_preparation_failed".to_owned()),
                            Some(format!("post-turn hook preparation failed: {failure:?}")),
                        ),
                        Ok(NativeTerminalEffectGate::AcceptedTaskResult) => {
                            match candidate_state
                                .as_ref()
                                .expect("accepted-result gate has a prepared candidate state")
                            {
                                CandidateGateState::Accepted(candidate_id) => (
                                    STATUS_UNRESOLVED,
                                    Some(candidate_id.id.clone()),
                                    false,
                                    true,
                                    Some("terminal_effect_preparation_failed".to_owned()),
                                    Some(format!("post-turn hook preparation failed: {failure:?}")),
                                ),
                                CandidateGateState::Rejected(_) => {
                                    (STATUS_DISCARDED, None, false, true, None, None)
                                }
                                CandidateGateState::Waiting => {
                                    (STATUS_WAITING_ACCEPTANCE, None, false, false, None, None)
                                }
                            }
                        }
                        Err(_) => (
                            STATUS_UNRESOLVED,
                            None,
                            false,
                            true,
                            Some("invalid_persisted_gate".to_owned()),
                            Some("persisted native terminal-effect gate is invalid".to_owned()),
                        ),
                    }
                }
                Some(_) => match gate.as_ref() {
                    Ok(gate) => {
                        let candidate = match gate {
                            NativeTerminalEffectGate::TerminalCommit => CandidateGateState::Waiting,
                            NativeTerminalEffectGate::AcceptedTaskResult => candidate_state
                                .clone()
                                .expect("accepted-result gate has a prepared candidate state"),
                        };
                        let (status, candidate_id, run_on_commit) =
                            prepared_activated_state(*gate, candidate);
                        (
                            status,
                            candidate_id,
                            run_on_commit,
                            status == STATUS_DISCARDED,
                            None,
                            None,
                        )
                    }
                    Err(_) => (
                        STATUS_UNRESOLVED,
                        None,
                        false,
                        true,
                        Some("invalid_persisted_gate".to_owned()),
                        Some("persisted native terminal-effect gate is invalid".to_owned()),
                    ),
                },
                None => (
                    STATUS_UNRESOLVED,
                    None,
                    false,
                    true,
                    Some("invalid_persisted_payload".to_owned()),
                    Some(
                        "persisted native terminal-effect payload failed integrity validation"
                            .to_owned(),
                    ),
                ),
            };
        let compact_payload = status == STATUS_DISCARDED;
        prepared_rows.push(PreparedNativeTerminalEffectActivationRow {
            effect_id: row.effect_id,
            thread_id: row.thread_id,
            effect_kind: row.effect_kind,
            gate_kind: row.gate_kind,
            expected_payload_json: row.payload_json,
            payload_sha256: row.payload_sha256,
            payload_identity_sha256: row.payload_identity_sha256,
            updated_at: row.updated_at,
            candidate_state,
            status,
            candidate_id,
            run_on_commit,
            complete_on_commit,
            error_code,
            error_message,
            compact_payload,
            compacted_payload_sha256: compact_payload
                .then(|| payload_sha256_hex(COMPACTED_PAYLOAD_JSON)),
        });
    }
    Ok(PreparedNativeTerminalEffectActivation {
        turn_id: turn_id.to_owned(),
        rows: prepared_rows,
    })
}

/// Applies a prevalidated plan inside the canonical terminal projection. This
/// path performs only bounded SQLite work: a small identity fence, an optional
/// candidate fence, and at most two updates.
pub(crate) async fn activate_prepared_for_terminal<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedNativeTerminalEffectActivation,
    committed_at: DateTimeWithTimeZone,
) -> Result<u64> {
    let current_effect_ids = native_terminal_effect_outbox::Entity::find()
        .select_only()
        .column(native_terminal_effect_outbox::Column::EffectId)
        .filter(native_terminal_effect_outbox::Column::TurnId.eq(prepared.turn_id.clone()))
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_PREPARED))
        .filter(native_terminal_effect_outbox::Column::TerminalCommittedAt.is_null())
        .order_by_asc(native_terminal_effect_outbox::Column::EffectId)
        .limit((MAX_EFFECTS_PER_TURN + 1) as u64)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to fence prepared native terminal effects")?;
    let prepared_effect_ids = prepared
        .rows
        .iter()
        .map(|row| row.effect_id.clone())
        .collect::<Vec<_>>();
    if current_effect_ids.len() > MAX_EFFECTS_PER_TURN || current_effect_ids != prepared_effect_ids
    {
        bail!(
            "prepared native terminal effects changed before terminal projection for Turn `{}`",
            prepared.turn_id
        );
    }

    let mut activated = 0_u64;
    for row in prepared.rows {
        if let Some(expected_candidate_state) = row.candidate_state.as_ref() {
            let current_candidate_state =
                candidate_gate_state(db, row.thread_id.as_str(), prepared.turn_id.as_str()).await?;
            if &current_candidate_state != expected_candidate_state {
                bail!(
                    "terminal-effect candidate gate changed before terminal projection for Turn `{}`",
                    prepared.turn_id
                );
            }
        }

        let mut update = native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                native_terminal_effect_outbox::Column::Status,
                Expr::value(row.status.to_owned()),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::AcceptedCandidateId,
                Expr::value(row.candidate_id),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::TerminalCommittedAt,
                Expr::value(Some(committed_at)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::NextRunAt,
                Expr::value(row.run_on_commit.then_some(committed_at)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::CompletedAt,
                Expr::value(row.complete_on_commit.then_some(committed_at)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::LastErrorCode,
                Expr::value(row.error_code),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::LastErrorMessage,
                Expr::value(row.error_message),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::UpdatedAt,
                Expr::value(committed_at),
            );
        if row.compact_payload {
            update = update
                .col_expr(
                    native_terminal_effect_outbox::Column::PayloadJson,
                    Expr::value(COMPACTED_PAYLOAD_JSON.to_owned()),
                )
                .col_expr(
                    native_terminal_effect_outbox::Column::PayloadSha256,
                    Expr::value(row.compacted_payload_sha256),
                );
        }
        let updated = update
            .filter(native_terminal_effect_outbox::Column::EffectId.eq(row.effect_id))
            .filter(native_terminal_effect_outbox::Column::TurnId.eq(prepared.turn_id.clone()))
            .filter(native_terminal_effect_outbox::Column::ThreadId.eq(row.thread_id))
            .filter(native_terminal_effect_outbox::Column::EffectKind.eq(row.effect_kind))
            .filter(native_terminal_effect_outbox::Column::GateKind.eq(row.gate_kind))
            .filter(
                native_terminal_effect_outbox::Column::PayloadJson.eq(row.expected_payload_json),
            )
            .filter(native_terminal_effect_outbox::Column::PayloadSha256.eq(row.payload_sha256))
            .filter(
                native_terminal_effect_outbox::Column::PayloadIdentitySha256
                    .eq(row.payload_identity_sha256),
            )
            .filter(native_terminal_effect_outbox::Column::UpdatedAt.eq(row.updated_at))
            .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_PREPARED))
            .filter(native_terminal_effect_outbox::Column::TerminalCommittedAt.is_null())
            .exec(db)
            .await
            .context("failed to activate prepared native terminal effect")?
            .rows_affected;
        if updated != 1 {
            bail!(
                "prepared native terminal effect changed before terminal projection for Turn `{}`",
                prepared.turn_id
            );
        }
        activated = activated.saturating_add(updated);
    }
    Ok(activated)
}

fn prepared_activated_state(
    gate: NativeTerminalEffectGate,
    candidate: CandidateGateState,
) -> (&'static str, Option<String>, bool) {
    match gate {
        NativeTerminalEffectGate::TerminalCommit => (STATUS_READY, None, true),
        NativeTerminalEffectGate::AcceptedTaskResult => match candidate {
            CandidateGateState::Accepted(candidate) => (STATUS_READY, Some(candidate.id), true),
            CandidateGateState::Rejected(_) => (STATUS_DISCARDED, None, false),
            CandidateGateState::Waiting => (STATUS_WAITING_ACCEPTANCE, None, false),
        },
    }
}

/// Immutable payload facts prepared before Task-batch writer admission. The
/// sequential projector still selects current state inside the transaction.
#[derive(Debug, Clone)]
pub(crate) struct PreparedGatePayloads {
    rows: Vec<(String, String, String, String, bool)>,
    compacted_sha256: String,
}

pub(crate) async fn prepare_gate_payloads<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<PreparedGatePayloads> {
    let rows = candidate_gated_rows(db, thread_id, turn_id).await?;
    Ok(PreparedGatePayloads {
        rows: rows
            .into_iter()
            .map(|row| {
                let failure = row.payload_json.len() <= MAX_EFFECT_PAYLOAD_BYTES
                    && payload_integrity_matches(
                        &row.payload_json,
                        &row.payload_sha256,
                        &row.payload_identity_sha256,
                    )
                    && matches!(
                        serde_json::from_str::<NativeTerminalEffectPayload>(&row.payload_json),
                        Ok(NativeTerminalEffectPayload::PostTurnHookPreparationFailed { .. })
                    );
                (
                    row.effect_id,
                    row.payload_json,
                    row.payload_sha256,
                    row.payload_identity_sha256,
                    failure,
                )
            })
            .collect(),
        compacted_sha256: payload_sha256_hex(COMPACTED_PAYLOAD_JSON),
    })
}

async fn candidate_gated_rows<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Vec<native_terminal_effect_outbox::Model>> {
    let rows = native_terminal_effect_outbox::Entity::find()
        .filter(native_terminal_effect_outbox::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::TurnId.eq(turn_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::GateKind.eq("accepted_task_result"))
        .filter(
            native_terminal_effect_outbox::Column::Status
                .is_in([STATUS_PREPARED, STATUS_WAITING_ACCEPTANCE]),
        )
        .order_by_asc(native_terminal_effect_outbox::Column::EffectId)
        .limit((MAX_EFFECTS_PER_TURN + 1) as u64)
        .all(db)
        .await?;
    if rows.len() > MAX_EFFECTS_PER_TURN {
        bail!("Turn exceeds candidate-gated effect bound");
    }
    Ok(rows)
}

/// Loads and validates the bounded acceptance-gate write set before an
/// authoritative candidate transaction obtains writer admission.
pub(crate) async fn prepare_gate_resolution_for_candidate<C: ConnectionTrait>(
    db: &C,
    candidate: Option<CandidateGateMetadata>,
    thread_id: &str,
    turn_id: &str,
    now: DateTimeWithTimeZone,
    payloads: Option<&PreparedGatePayloads>,
) -> Result<PreparedCandidateGateResolution> {
    let terminal = candidate
        .as_ref()
        .is_none_or(|candidate| terminal_candidate_statuses().contains(&candidate.status.as_str()));
    let latest = if terminal {
        latest_candidate_metadata(db, thread_id, turn_id, candidate.clone()).await?
    } else {
        None
    };
    if latest.is_none() {
        return Ok(PreparedCandidateGateResolution {
            candidate_id: candidate.map(|candidate| candidate.id).unwrap_or_default(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            resolved_at: now,
            requires_fence: false,
            latest: None,
            probe_token: None,
            rows: Vec::new(),
        });
    }
    let rows = candidate_gated_rows(db, thread_id, turn_id).await?;
    let candidate_id = latest
        .as_ref()
        .expect("terminal metadata was selected")
        .id
        .clone();
    prepare_gate_resolution_rows(
        &candidate_id,
        thread_id,
        turn_id,
        latest,
        rows,
        now,
        None,
        payloads,
    )
}

fn prepare_gate_resolution_rows(
    candidate_id: &str,
    thread_id: &str,
    turn_id: &str,
    latest: Option<CandidateGateMetadata>,
    rows: Vec<native_terminal_effect_outbox::Model>,
    now: DateTimeWithTimeZone,
    probe_token: Option<String>,
    payloads: Option<&PreparedGatePayloads>,
) -> Result<PreparedCandidateGateResolution> {
    let accepted = latest
        .as_ref()
        .is_some_and(|candidate| candidate.status == "accepted");
    let compacted_payload_sha256 = payloads
        .map(|facts| facts.compacted_sha256.clone())
        .unwrap_or_else(|| payload_sha256_hex(COMPACTED_PAYLOAD_JSON));
    let mut prepared_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let committed = row.terminal_committed_at.is_some();
        let payload_failure = if let Some(payloads) = payloads {
            let facts = payloads
                .rows
                .iter()
                .find(|facts| facts.0 == row.effect_id)
                .context("candidate gate payload was not prepared before writer admission")?;
            if facts.1 != row.payload_json
                || facts.2 != row.payload_sha256
                || facts.3 != row.payload_identity_sha256
            {
                bail!("candidate gate payload changed after preparation");
            }
            facts.4
        } else {
            row.payload_json.len() <= MAX_EFFECT_PAYLOAD_BYTES
                && payload_integrity_matches(
                    &row.payload_json,
                    &row.payload_sha256,
                    &row.payload_identity_sha256,
                )
                && matches!(
                    serde_json::from_str::<NativeTerminalEffectPayload>(&row.payload_json),
                    Ok(NativeTerminalEffectPayload::PostTurnHookPreparationFailed { .. })
                )
        };
        let preparation_failure = committed && accepted && payload_failure;
        let status_after = if !committed {
            STATUS_PREPARED
        } else if preparation_failure {
            STATUS_UNRESOLVED
        } else if accepted {
            STATUS_READY
        } else {
            STATUS_DISCARDED
        };
        prepared_rows.push(PreparedCandidateGateResolutionRow {
            effect_id: row.effect_id,
            status_before: row.status,
            updated_at_before: row.updated_at,
            payload_json_before: row.payload_json,
            payload_sha256_before: row.payload_sha256,
            payload_identity_sha256_before: row.payload_identity_sha256,
            terminal_committed_at_before: row.terminal_committed_at,
            status_after,
            accepted_candidate_id: latest
                .as_ref()
                .filter(|_| accepted)
                .map(|candidate| candidate.id.clone()),
            next_run_at: (committed && accepted && !preparation_failure).then_some(now),
            completed_at: (committed && (!accepted || preparation_failure)).then_some(now),
            last_error_code: preparation_failure
                .then_some("terminal_effect_preparation_failed".to_owned()),
            last_error_message: preparation_failure
                .then_some("post-turn hook preparation failed before durable execution".to_owned()),
            compact_payload: committed && !accepted,
            compacted_payload_sha256: (committed && !accepted)
                .then(|| compacted_payload_sha256.clone()),
        });
    }
    Ok(PreparedCandidateGateResolution {
        candidate_id: candidate_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        resolved_at: now,
        requires_fence: true,
        latest,
        probe_token,
        rows: prepared_rows,
    })
}

/// Applies a prevalidated acceptance-gate plan in the same transaction which
/// persists the authoritative task-result candidate state. Only bounded
/// SQLite reads and updates execute while the writer is held.
pub(crate) async fn apply_prepared_gate_resolution<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedCandidateGateResolution,
) -> Result<u64> {
    // Non-terminal candidate updates do not resolve the acceptance gate and
    // therefore must not fence or mutate the current waiting effect set.
    if !prepared.requires_fence {
        return Ok(0);
    }
    // Validate the exact latest id/status/version after the candidate mutation,
    // using the same four seeks as preparation. Unknown freshness never ACKs.
    if latest_candidate_metadata(db, &prepared.thread_id, &prepared.turn_id, None).await?
        != prepared.latest
    {
        bail!("latest terminal candidate changed before gate resolution");
    }
    if prepared.probe_token.is_none() {
        let current_effect_ids = native_terminal_effect_outbox::Entity::find()
            .select_only()
            .column(native_terminal_effect_outbox::Column::EffectId)
            .filter(native_terminal_effect_outbox::Column::ThreadId.eq(prepared.thread_id.clone()))
            .filter(native_terminal_effect_outbox::Column::TurnId.eq(prepared.turn_id.clone()))
            .filter(
                native_terminal_effect_outbox::Column::GateKind
                    .eq(gate_to_db(NativeTerminalEffectGate::AcceptedTaskResult)),
            )
            .filter(
                native_terminal_effect_outbox::Column::Status
                    .is_in([STATUS_PREPARED, STATUS_WAITING_ACCEPTANCE]),
            )
            .order_by_asc(native_terminal_effect_outbox::Column::EffectId)
            .limit((MAX_EFFECTS_PER_TURN + 1) as u64)
            .into_tuple::<String>()
            .all(db)
            .await
            .context("failed to fence candidate-gated terminal effects")?;
        let prepared_effect_ids = prepared
            .rows
            .iter()
            .map(|row| row.effect_id.clone())
            .collect::<Vec<_>>();
        if current_effect_ids.len() > MAX_EFFECTS_PER_TURN
            || current_effect_ids != prepared_effect_ids
        {
            bail!(
                "candidate-gated native terminal effects changed before resolving candidate `{}`",
                prepared.candidate_id
            );
        }
    }
    let mut resolved = 0_u64;
    for row in prepared.rows {
        let mut update = native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                native_terminal_effect_outbox::Column::GateProbeToken,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::Status,
                Expr::value(row.status_after.to_owned()),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::AcceptedCandidateId,
                Expr::value(row.accepted_candidate_id),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::NextRunAt,
                Expr::value(row.next_run_at),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::CompletedAt,
                Expr::value(row.completed_at),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::LastErrorCode,
                Expr::value(row.last_error_code),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::LastErrorMessage,
                Expr::value(row.last_error_message),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::UpdatedAt,
                Expr::value(prepared.resolved_at),
            )
            .filter(native_terminal_effect_outbox::Column::EffectId.eq(row.effect_id))
            .filter(native_terminal_effect_outbox::Column::ThreadId.eq(prepared.thread_id.clone()))
            .filter(native_terminal_effect_outbox::Column::TurnId.eq(prepared.turn_id.clone()))
            .filter(
                native_terminal_effect_outbox::Column::GateKind
                    .eq(gate_to_db(NativeTerminalEffectGate::AcceptedTaskResult)),
            )
            .filter(native_terminal_effect_outbox::Column::Status.eq(row.status_before))
            .filter(native_terminal_effect_outbox::Column::UpdatedAt.eq(row.updated_at_before))
            .filter(
                native_terminal_effect_outbox::Column::PayloadSha256.eq(row.payload_sha256_before),
            )
            .filter(
                native_terminal_effect_outbox::Column::PayloadIdentitySha256
                    .eq(row.payload_identity_sha256_before),
            );
        update = update
            .filter(native_terminal_effect_outbox::Column::PayloadJson.eq(row.payload_json_before));
        if let Some(token) = &prepared.probe_token {
            update = update
                .filter(native_terminal_effect_outbox::Column::GateProbeToken.eq(token.clone()));
        }
        update = match row.terminal_committed_at_before {
            Some(committed_at) => update.filter(
                native_terminal_effect_outbox::Column::TerminalCommittedAt.eq(committed_at),
            ),
            None => {
                update.filter(native_terminal_effect_outbox::Column::TerminalCommittedAt.is_null())
            }
        };
        if row.compact_payload {
            update = update
                .col_expr(
                    native_terminal_effect_outbox::Column::PayloadJson,
                    Expr::value(COMPACTED_PAYLOAD_JSON.to_owned()),
                )
                .col_expr(
                    native_terminal_effect_outbox::Column::PayloadSha256,
                    Expr::value(row.compacted_payload_sha256),
                );
        }
        let updated = update
            .exec(db)
            .await
            .context("failed to resolve candidate-gated terminal effect")?
            .rows_affected;
        if updated != 1 && prepared.probe_token.is_none() {
            bail!(
                "candidate-gated native terminal effect changed before resolving candidate `{}`",
                prepared.candidate_id
            );
        }
        resolved = resolved.saturating_add(updated);
    }
    Ok(resolved)
}

// SeaQuery has no SQLite INDEXED BY support. The literal predicate makes the
// partial index usable; INDEXED BY prevents a competing status index and sort
// from visiting the entire waiting set before LIMIT.
pub(crate) fn gate_due_page(now: i64, limit: u64) -> Statement {
    Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT native_terminal_effect_outbox.* FROM native_terminal_effect_outbox \
         INDEXED BY idx_native_terminal_effect_gate_due \
         WHERE status = 'waiting_acceptance' AND gate_probe_at <= ? \
         ORDER BY gate_probe_at, prepared_at, effect_id LIMIT ?",
        [
            now.into(),
            (std::cmp::min(limit, EFFECT_INPUT_BUDGET) as i64).into(),
        ],
    )
}

fn probe_delay(attempts: i64) -> i64 {
    std::cmp::min(
        5_i64 << (attempts.clamp(1, MAX_GATE_PROBE_ATTEMPTS) - 1),
        300,
    )
}

fn probe_snapshot(row: &native_terminal_effect_outbox::Model) -> Condition {
    let mut guard = Condition::all()
        .add(native_terminal_effect_outbox::Column::EffectId.eq(row.effect_id.clone()))
        .add(native_terminal_effect_outbox::Column::ThreadId.eq(row.thread_id.clone()))
        .add(native_terminal_effect_outbox::Column::TurnId.eq(row.turn_id.clone()))
        .add(native_terminal_effect_outbox::Column::GateKind.eq(row.gate_kind.clone()))
        .add(native_terminal_effect_outbox::Column::Status.eq(STATUS_WAITING_ACCEPTANCE))
        .add(native_terminal_effect_outbox::Column::UpdatedAt.eq(row.updated_at))
        .add(native_terminal_effect_outbox::Column::PayloadJson.eq(row.payload_json.clone()))
        .add(native_terminal_effect_outbox::Column::PayloadSha256.eq(row.payload_sha256.clone()))
        .add(
            native_terminal_effect_outbox::Column::PayloadIdentitySha256
                .eq(row.payload_identity_sha256.clone()),
        )
        .add(native_terminal_effect_outbox::Column::GateProbeAt.eq(row.gate_probe_at))
        .add(native_terminal_effect_outbox::Column::GateProbeAttempts.eq(row.gate_probe_attempts));
    guard = match &row.gate_probe_token {
        Some(token) => {
            guard.add(native_terminal_effect_outbox::Column::GateProbeToken.eq(token.clone()))
        }
        None => guard.add(native_terminal_effect_outbox::Column::GateProbeToken.is_null()),
    };
    guard = match row.terminal_committed_at {
        Some(at) => guard.add(native_terminal_effect_outbox::Column::TerminalCommittedAt.eq(at)),
        None => guard.add(native_terminal_effect_outbox::Column::TerminalCommittedAt.is_null()),
    };
    guard
}

/// Also used after failed reservation/commit: only the original snapshot may
/// be deferred, including NULL token. A committed reservation won't match it.
pub(crate) async fn reserve_gate_probe(
    db: &SqliteDatabase,
    row: &native_terminal_effect_outbox::Model,
    token: Option<String>,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<Option<native_terminal_effect_outbox::Model>> {
    write_gate_probe(
        db,
        row,
        token,
        row.gate_probe_attempts
            .saturating_add(1)
            .clamp(1, MAX_GATE_PROBE_ATTEMPTS),
        clock,
        true,
    )
    .await
}

pub(crate) async fn defer_gate_probe(
    db: &SqliteDatabase,
    row: &native_terminal_effect_outbox::Model,
    reservation_committed: bool,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<()> {
    let attempts = if reservation_committed {
        row.gate_probe_attempts
    } else {
        row.gate_probe_attempts.saturating_add(1)
    }
    .clamp(1, MAX_GATE_PROBE_ATTEMPTS);
    write_gate_probe(
        db,
        row,
        row.gate_probe_token.clone(),
        attempts,
        clock,
        false,
    )
    .await?;
    Ok(())
}

async fn write_gate_probe(
    db: &SqliteDatabase,
    row: &native_terminal_effect_outbox::Model,
    token: Option<String>,
    attempts: i64,
    clock: &(dyn Fn() -> i64 + Send + Sync),
    require_due: bool,
) -> Result<Option<native_terminal_effect_outbox::Model>> {
    let delay = probe_delay(attempts);
    let mut guard = probe_snapshot(row);
    let tx = db.begin().await?;
    let now = clock();
    let next = std::cmp::max(row.gate_probe_at, now.saturating_add(delay));
    if require_due {
        guard = guard.add(native_terminal_effect_outbox::Column::GateProbeAt.lte(now));
    }
    let result = native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            native_terminal_effect_outbox::Column::GateProbeAt,
            Expr::value(next),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::GateProbeAttempts,
            Expr::value(attempts),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::GateProbeToken,
            Expr::value(token.clone()),
        )
        // Bookkeeping deliberately does not touch domain updated_at. A probe
        // must not invalidate a concurrently prepared authoritative resolution.
        .filter(guard)
        .exec(&tx)
        .await;
    let affected = match result {
        Ok(result) => result.rows_affected,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(error.into());
        }
    };
    tx.commit().await?;
    Ok((affected == 1).then(|| {
        let mut reserved = row.clone();
        reserved.gate_probe_at = next;
        reserved.gate_probe_attempts = attempts;
        reserved.gate_probe_token = token;
        reserved
    }))
}

pub(crate) async fn probe_waiting_gate(
    db: &SqliteDatabase,
    row: &native_terminal_effect_outbox::Model,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<u64> {
    if row.gate_kind != "accepted_task_result" {
        bail!("waiting effect has an invalid gate");
    }
    let token = row
        .gate_probe_token
        .clone()
        .context("gate probe has no durable reservation")?;
    let latest = latest_candidate_metadata(db, &row.thread_id, &row.turn_id, None).await?;
    if latest.is_none() {
        return Ok(0);
    } // Durable reservation already deferred Waiting.
    let ids = native_terminal_effect_outbox::Entity::find()
        .select_only()
        .column(native_terminal_effect_outbox::Column::EffectId)
        .filter(native_terminal_effect_outbox::Column::TurnId.eq(row.turn_id.clone()))
        .limit((MAX_EFFECTS_PER_TURN + 1) as u64)
        .into_tuple::<String>()
        .all(db)
        .await?;
    if ids.len() > MAX_EFFECTS_PER_TURN {
        bail!("Turn exceeds terminal-effect bound");
    }
    let prepared = prepare_gate_resolution_rows(
        "probe",
        &row.thread_id,
        &row.turn_id,
        latest,
        vec![row.clone()],
        crate::util::unix_to_datetime(clock()),
        Some(token),
        None,
    )?;
    let tx = db.begin().await?;
    let result = apply_prepared_gate_resolution(&tx, prepared).await;
    match result {
        Ok(resolved) => {
            tx.commit().await?;
            Ok(resolved)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn discover_gate_probes(
    db: &SqliteDatabase,
    now: i64,
    limit: u64,
) -> Result<Vec<native_terminal_effect_outbox::Model>> {
    Ok(native_terminal_effect_outbox::Entity::find()
        .from_raw_sql(gate_due_page(now, limit))
        .all(db)
        .await?)
}

pub(crate) fn claim_page(
    status: &str,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> sea_orm::Select<native_terminal_effect_outbox::Entity> {
    let due_column = if status == STATUS_RUNNING {
        native_terminal_effect_outbox::Column::ClaimExpiresAt
    } else {
        native_terminal_effect_outbox::Column::NextRunAt
    };
    native_terminal_effect_outbox::Entity::find()
        .filter(native_terminal_effect_outbox::Column::Status.eq(status.to_owned()))
        .filter(due_column.lte(now))
        .order_by_asc(due_column)
        .order_by_asc(native_terminal_effect_outbox::Column::PreparedAt)
        .order_by_asc(native_terminal_effect_outbox::Column::EffectId)
        .limit(limit)
}

pub async fn claim_due<F: FnMut() -> String>(
    db: &SqliteDatabase,
    discovery_now: DateTimeWithTimeZone,
    claim_lease_secs: u64,
    limit: u64,
    mut claim_token_factory: F,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<NativeTerminalEffectClaimBatch> {
    let mut remaining = std::cmp::min(limit, EFFECT_INPUT_BUDGET);
    let statuses = [STATUS_READY, STATUS_RETRY_WAIT, STATUS_RUNNING];
    // Three pages share eight INPUT slots, including exhausted rows. Rotate
    // which status receives the extra slots; empty pages donate unused quota.
    let rotation = discovery_now.timestamp().div_euclid(2).rem_euclid(3) as usize;
    let mut candidates = Vec::new();
    let mut outcome = NativeTerminalEffectClaimBatch::default();
    for page in 0..3 {
        if remaining == 0 {
            break;
        }
        let quota = remaining.div_ceil(3 - page);
        let rows = claim_page(
            statuses[(rotation + page as usize) % 3],
            discovery_now,
            quota,
        )
        .all(db)
        .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(_error) => {
                // The failed read may have consumed its whole LIMIT before
                // decoding failed. Unknown pages cannot donate input slots.
                remaining -= quota;
                outcome.storage_failed = true;
                tracing::warn!("terminal-effect claim page failed");
                continue;
            }
        };
        remaining -= rows.len() as u64;
        candidates.extend(rows);
    }
    // Discover once. Every selected row, including exhausted/failed rows,
    // consumes one of these <=8 input slots. No whole-quantum retry.
    for row in candidates {
        let token = claim_token_factory(); // outside writer admission
        match claim_one(db, &row, token, claim_lease_secs, clock).await {
            Ok(Some(claim)) => outcome.claimed.push(claim),
            Ok(None) => {}
            Err(_error) => {
                outcome.storage_failed = true;
                tracing::warn!("terminal-effect point claim failed");
                // A commit may have succeeded. The original snapshot then no
                // longer matches: never dispatch or retry this row here.
                if let Err(_error) = defer_execution_claim(db, &row, clock).await {
                    tracing::warn!("terminal-effect claim failure deferral failed");
                }
            }
        }
    }
    Ok(outcome)
}

fn execution_due_column(
    row: &native_terminal_effect_outbox::Model,
) -> native_terminal_effect_outbox::Column {
    if row.status == STATUS_RUNNING {
        native_terminal_effect_outbox::Column::ClaimExpiresAt
    } else {
        native_terminal_effect_outbox::Column::NextRunAt
    }
}

// All scheduling/ownership facts used by the point claim and failure deferral.
// NULLs must match explicitly, particularly a not-yet-claimed ready row.
fn execution_snapshot(row: &native_terminal_effect_outbox::Model) -> Condition {
    let mut guard = Condition::all()
        .add(native_terminal_effect_outbox::Column::EffectId.eq(row.effect_id.clone()))
        .add(native_terminal_effect_outbox::Column::Status.eq(row.status.clone()))
        .add(native_terminal_effect_outbox::Column::AttemptCount.eq(row.attempt_count))
        .add(native_terminal_effect_outbox::Column::MaxAttempts.eq(row.max_attempts))
        .add(native_terminal_effect_outbox::Column::UpdatedAt.eq(row.updated_at));
    guard = match &row.claim_token {
        Some(token) => {
            guard.add(native_terminal_effect_outbox::Column::ClaimToken.eq(token.clone()))
        }
        None => guard.add(native_terminal_effect_outbox::Column::ClaimToken.is_null()),
    };
    for (column, value) in [
        (
            native_terminal_effect_outbox::Column::NextRunAt,
            row.next_run_at,
        ),
        (
            native_terminal_effect_outbox::Column::ClaimExpiresAt,
            row.claim_expires_at,
        ),
    ] {
        guard = match value {
            Some(value) => guard.add(column.eq(value)),
            None => guard.add(column.is_null()),
        };
    }
    guard
}

async fn claim_one(
    db: &SqliteDatabase,
    row: &native_terminal_effect_outbox::Model,
    token: String,
    claim_lease_secs: u64,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<Option<ClaimedNativeTerminalEffect>> {
    // Only this row's mutation, reload and commit are atomic. Independent
    // effects do not share a domain transition or a claim transaction.
    let tx = db.begin().await?;
    let result = async {
        let now_unix = clock(); // after writer admission, for each input
        let now = crate::util::unix_to_datetime(now_unix);
        let expires =
            crate::util::unix_to_datetime(now_unix.saturating_add(
                i64::try_from(std::cmp::max(claim_lease_secs, 1)).unwrap_or(i64::MAX),
            ));
        let guard = execution_snapshot(row).add(execution_due_column(row).lte(now));
        let exhausted = row.attempt_count >= row.max_attempts;
        let mut update = native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                native_terminal_effect_outbox::Column::Status,
                Expr::value(if exhausted {
                    STATUS_UNRESOLVED
                } else {
                    STATUS_RUNNING
                }),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::ClaimToken,
                Expr::value((!exhausted).then(|| token.clone())),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::ClaimExpiresAt,
                Expr::value((!exhausted).then_some(expires)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::UpdatedAt,
                Expr::value(now),
            )
            .filter(guard);
        if exhausted {
            update = update
                .col_expr(
                    native_terminal_effect_outbox::Column::CompletedAt,
                    Expr::value(Some(now)),
                )
                .col_expr(
                    native_terminal_effect_outbox::Column::NextRunAt,
                    Expr::value(Option::<DateTimeWithTimeZone>::None),
                )
                .col_expr(
                    native_terminal_effect_outbox::Column::LastErrorCode,
                    Expr::value(Some("retry_exhausted".to_owned())),
                )
                .col_expr(
                    native_terminal_effect_outbox::Column::LastErrorMessage,
                    Expr::value(Some(
                        "native terminal effect exhausted its retry budget".to_owned(),
                    )),
                );
        } else {
            update = update.col_expr(
                native_terminal_effect_outbox::Column::AttemptCount,
                Expr::col(native_terminal_effect_outbox::Column::AttemptCount).add(1),
            );
        }
        if update.exec(&tx).await?.rows_affected == 1 && !exhausted {
            let row = native_terminal_effect_outbox::Entity::find_by_id(row.effect_id.clone())
                .one(&tx)
                .await?
                .context("claimed effect disappeared")?;
            return Ok(Some(ClaimedNativeTerminalEffect {
                row,
                claim_token: token,
            }));
        }
        Ok(None)
    }
    .await;
    match result {
        Ok(claim) => {
            tx.commit().await?;
            Ok(claim)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn defer_execution_claim(
    db: &SqliteDatabase,
    row: &native_terminal_effect_outbox::Model,
    clock: &(dyn Fn() -> i64 + Send + Sync),
) -> Result<bool> {
    let guard = execution_snapshot(row);
    let column = execution_due_column(row);
    let old_due = if row.status == STATUS_RUNNING {
        row.claim_expires_at
    } else {
        row.next_run_at
    };
    let tx = db.begin().await?;
    let next = crate::util::unix_to_datetime(clock().saturating_add(5));
    let next = old_due.map_or(next, |due| std::cmp::max(due, next));
    // Bookkeeping changes only the due field. Preserve the old owner's token,
    // attempt_count and domain updated_at; a concurrent claim/completion wins.
    let result = native_terminal_effect_outbox::Entity::update_many()
        .col_expr(column, Expr::value(Some(next)))
        .filter(guard)
        .exec(&tx)
        .await;
    match result {
        Ok(result) => {
            tx.commit().await?;
            Ok(result.rows_affected == 1)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error.into())
        }
    }
}

pub async fn mark_succeeded<C: ConnectionTrait>(
    db: &C,
    effect_id: &str,
    claim_token: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let updated = native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            native_terminal_effect_outbox::Column::Status,
            Expr::value(STATUS_SUCCEEDED.to_owned()),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::PayloadJson,
            Expr::value(COMPACTED_PAYLOAD_JSON.to_owned()),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::PayloadSha256,
            Expr::value(payload_sha256_hex(COMPACTED_PAYLOAD_JSON)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::HandlerCheckpointJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::HandlerCheckpointSha256,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::LastErrorCode,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::LastErrorMessage,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::NextRunAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::ClaimExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::CompletedAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(native_terminal_effect_outbox::Column::EffectId.eq(effect_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_RUNNING))
        .filter(native_terminal_effect_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to complete native terminal effect")?
        .rows_affected;
    Ok(updated == 1)
}

#[derive(Debug)]
pub struct HandlerCheckpointInvalid {
    pub class: &'static str,
}

impl std::fmt::Display for HandlerCheckpointInvalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "native terminal-effect checkpoint is invalid ({})",
            self.class
        )
    }
}

impl std::error::Error for HandlerCheckpointInvalid {}

/// Load the immutable handler checkpoint owned by the current delivery lease.
/// A missing/expired claim is an error rather than an empty checkpoint so a
/// stale worker can never continue provider or memory side effects.
pub async fn load_handler_checkpoint<C: ConnectionTrait>(
    db: &C,
    effect_id: &str,
    claim_token: &str,
) -> Result<Option<String>> {
    let row = native_terminal_effect_outbox::Entity::find_by_id(effect_id.to_owned())
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_RUNNING))
        .filter(native_terminal_effect_outbox::Column::EffectKind.eq("post_turn_hook"))
        .filter(native_terminal_effect_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .one(db)
        .await
        .context("failed to load native terminal-effect handler checkpoint")?
        .with_context(|| {
            format!("native terminal effect `{effect_id}` is not owned by the supplied claim")
        })?;
    match (row.handler_checkpoint_json, row.handler_checkpoint_sha256) {
        (None, None) => Ok(None),
        (Some(checkpoint), Some(expected_sha256)) => {
            if checkpoint.len() > MAX_EFFECT_HANDLER_CHECKPOINT_BYTES {
                return Err(HandlerCheckpointInvalid {
                    class: "checkpoint_size",
                }
                .into());
            }
            if payload_sha256_hex(checkpoint.as_str()) != expected_sha256 {
                return Err(HandlerCheckpointInvalid {
                    class: "checkpoint_hash",
                }
                .into());
            }
            Ok(Some(checkpoint))
        }
        _ => Err(HandlerCheckpointInvalid {
            class: "checkpoint_incomplete",
        }
        .into()),
    }
}

/// Publish the first successful handler checkpoint under the active claim.
/// Checkpoints are immutable: retrying the same value is accepted, while a
/// second different provider result fails closed instead of changing replay.
pub async fn store_handler_checkpoint<C: ConnectionTrait>(
    db: &C,
    effect_id: &str,
    claim_token: &str,
    checkpoint_json: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    if checkpoint_json.len() > MAX_EFFECT_HANDLER_CHECKPOINT_BYTES {
        bail!("native terminal-effect handler checkpoint exceeds its durable byte limit");
    }
    // Pure preparation precedes SqliteDatabase's statement-scoped writer.
    // CrudStore's run_serialized_write is a lock-retry wrapper, not a reservation.
    let checkpoint_sha256 = payload_sha256_hex(checkpoint_json);
    let updated = native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            native_terminal_effect_outbox::Column::HandlerCheckpointJson,
            Expr::value(Some(checkpoint_json.to_owned())),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::HandlerCheckpointSha256,
            Expr::value(Some(checkpoint_sha256)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(native_terminal_effect_outbox::Column::EffectId.eq(effect_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_RUNNING))
        .filter(native_terminal_effect_outbox::Column::EffectKind.eq("post_turn_hook"))
        .filter(native_terminal_effect_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .filter(native_terminal_effect_outbox::Column::HandlerCheckpointJson.is_null())
        .filter(native_terminal_effect_outbox::Column::HandlerCheckpointSha256.is_null())
        .exec(db)
        .await
        .context("failed to store native terminal-effect handler checkpoint")?
        .rows_affected;
    if updated == 1 {
        return Ok(());
    }
    match load_handler_checkpoint(db, effect_id, claim_token).await? {
        Some(existing) if existing == checkpoint_json => Ok(()),
        Some(_) => bail!(
            "native terminal effect `{effect_id}` already has a conflicting handler checkpoint"
        ),
        None => bail!("native terminal effect `{effect_id}` did not accept its handler checkpoint"),
    }
}

pub async fn mark_failed<C: ConnectionTrait>(
    db: &C,
    effect_id: &str,
    claim_token: &str,
    error_code: &str,
    error_message: &str,
    retryable: bool,
    retry_at: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let code = bounded_chars(error_code, MAX_ERROR_CODE_CHARS);
    let message = bounded_chars(error_message, MAX_ERROR_MESSAGE_CHARS);
    if retryable {
        let updated = native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                native_terminal_effect_outbox::Column::Status,
                Expr::value(STATUS_RETRY_WAIT.to_owned()),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::LastErrorCode,
                Expr::value(Some(code.clone())),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::LastErrorMessage,
                Expr::value(Some(message.clone())),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::NextRunAt,
                Expr::value(Some(retry_at)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::ClaimToken,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::ClaimExpiresAt,
                Expr::value(Option::<DateTimeWithTimeZone>::None),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::UpdatedAt,
                Expr::value(now),
            )
            .filter(native_terminal_effect_outbox::Column::EffectId.eq(effect_id.to_owned()))
            .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_RUNNING))
            .filter(native_terminal_effect_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
            .filter(
                Expr::col(native_terminal_effect_outbox::Column::AttemptCount).lt(Expr::col(
                    native_terminal_effect_outbox::Column::MaxAttempts,
                )),
            )
            .exec(db)
            .await
            .context("failed to retry native terminal effect")?
            .rows_affected;
        if updated == 1 {
            return Ok(true);
        }
    }
    let updated = native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            native_terminal_effect_outbox::Column::Status,
            Expr::value(STATUS_UNRESOLVED.to_owned()),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::LastErrorCode,
            Expr::value(Some(code)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::LastErrorMessage,
            Expr::value(Some(message)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::NextRunAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::ClaimExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::CompletedAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(native_terminal_effect_outbox::Column::EffectId.eq(effect_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_RUNNING))
        .filter(native_terminal_effect_outbox::Column::ClaimToken.eq(claim_token.to_owned()))
        .exec(db)
        .await
        .context("failed to terminalize native terminal effect")?
        .rows_affected;
    Ok(updated == 1)
}

/// Reopens a bounded set of recently exhausted post-turn obligations whose
/// typed failure can make progress after an external provider or storage
/// outage clears. A legacy write_failed with a checkpoint gets one bounded
/// revalidation attempt, not a fresh retry budget. The marker is committed with
/// requeue so recovery cannot repeatedly reopen the legacy code. Legacy
/// manifest_failed gets the same single attempt without requiring a checkpoint.
/// Its distinct durable marker is excluded from discovery, including after restart.
pub async fn requeue_retryable_unresolved<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    completed_before: DateTimeWithTimeZone,
    prepared_after: DateTimeWithTimeZone,
    limit: u64,
) -> Result<u64> {
    let limit = std::cmp::Ord::min(limit, MAX_RETRYABLE_UNRESOLVED_REQUEUE_BATCH_SIZE);
    if limit == 0 {
        return Ok(0);
    }
    let retryable_codes = [
        "effect_timeout",
        "memory.post_turn_extractor.runtime_unavailable",
        "memory.post_turn_extractor.checkpoint_load_failed",
        "memory.post_turn_extractor.checkpoint_store_failed",
        "memory.post_turn_extractor.manifest_storage_transient",
        "memory.post_turn_extractor.write_storage_transient",
        "memory.post_turn_extractor.provider_network_transient",
        "memory.post_turn_extractor.provider_rate_limited",
        "memory.post_turn_extractor.provider_5xx",
        "memory.post_turn_extractor.provider_stream_stall",
        "memory.post_turn_extractor.provider_stream_truncated",
    ];
    let eligible_failures = Condition::any()
        .add(native_terminal_effect_outbox::Column::LastErrorCode.is_in(retryable_codes))
        .add(
            Condition::all()
                .add(
                    native_terminal_effect_outbox::Column::LastErrorCode
                        .eq("memory.post_turn_extractor.write_failed"),
                )
                .add(native_terminal_effect_outbox::Column::HandlerCheckpointJson.is_not_null()),
        )
        // No checkpoint is required: legacy manifest failures preceded extraction.
        .add(
            native_terminal_effect_outbox::Column::LastErrorCode
                .eq("memory.post_turn_extractor.manifest_failed"),
        );
    let effect_ids = native_terminal_effect_outbox::Entity::find()
        .select_only()
        .column(native_terminal_effect_outbox::Column::EffectId)
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_UNRESOLVED))
        .filter(native_terminal_effect_outbox::Column::EffectKind.eq("post_turn_hook"))
        .filter(eligible_failures.clone())
        .filter(native_terminal_effect_outbox::Column::CompletedAt.lte(completed_before))
        .filter(native_terminal_effect_outbox::Column::PreparedAt.gte(prepared_after))
        .order_by_asc(native_terminal_effect_outbox::Column::CompletedAt)
        .limit(limit)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to list retryable unresolved native terminal effects")?;
    if effect_ids.is_empty() {
        return Ok(0);
    }
    let updated = native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            native_terminal_effect_outbox::Column::Status,
            Expr::value(STATUS_RETRY_WAIT.to_owned()),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::AttemptCount,
            Expr::cust("CASE WHEN last_error_code IN ('memory.post_turn_extractor.write_failed', 'memory.post_turn_extractor.manifest_failed') THEN max_attempts - 1 ELSE 0 END"),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::MaxAttempts,
            Expr::cust("CASE WHEN last_error_code IN ('memory.post_turn_extractor.write_failed', 'memory.post_turn_extractor.manifest_failed') THEN max_attempts ELSE MAX(max_attempts, 8) END"),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::NextRunAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::CompletedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::UpdatedAt,
            Expr::value(now),
        )
        .col_expr(
            native_terminal_effect_outbox::Column::LastErrorCode,
            Expr::cust("CASE WHEN last_error_code = 'memory.post_turn_extractor.write_failed' THEN 'memory.post_turn_extractor.legacy_write_revalidate' WHEN last_error_code = 'memory.post_turn_extractor.manifest_failed' THEN 'memory.post_turn_extractor.legacy_manifest_revalidate' ELSE last_error_code END"),
        )
        .filter(native_terminal_effect_outbox::Column::EffectId.is_in(effect_ids))
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_UNRESOLVED))
        .filter(eligible_failures)
        .filter(native_terminal_effect_outbox::Column::CompletedAt.lte(completed_before))
        .filter(native_terminal_effect_outbox::Column::PreparedAt.gte(prepared_after))
        .exec(db)
        .await
        .context("failed to requeue retryable unresolved native terminal effects")?
        .rows_affected;
    Ok(updated)
}

pub async fn load_stats<C: ConnectionTrait>(db: &C) -> Result<NativeTerminalEffectStats> {
    Ok(NativeTerminalEffectStats {
        prepared: count_status(db, STATUS_PREPARED).await?,
        waiting_acceptance: count_status(db, STATUS_WAITING_ACCEPTANCE).await?,
        ready: count_status(db, STATUS_READY).await?,
        running: count_status(db, STATUS_RUNNING).await?,
        retry_wait: count_status(db, STATUS_RETRY_WAIT).await?,
        succeeded: count_status(db, STATUS_SUCCEEDED).await?,
        unresolved: count_status(db, STATUS_UNRESOLVED).await?,
    })
}

/// Delete a bounded batch of old, fully resolved obligations.
///
/// Pending and unresolved rows are durable recovery authority and are never
/// eligible. The second status/cutoff fence on the delete makes the operation
/// safe if another database connection observes the candidates concurrently.
pub async fn purge_resolved_before<C: ConnectionTrait>(
    db: &C,
    cutoff: DateTimeWithTimeZone,
    limit: u64,
) -> Result<u64> {
    let limit = std::cmp::Ord::min(limit, MAX_PURGE_BATCH_SIZE);
    if limit == 0 {
        return Ok(0);
    }
    let resolved_statuses = [STATUS_SUCCEEDED, STATUS_DISCARDED, STATUS_SUPERSEDED];
    let effect_ids = native_terminal_effect_outbox::Entity::find()
        .select_only()
        .column(native_terminal_effect_outbox::Column::EffectId)
        .filter(native_terminal_effect_outbox::Column::Status.is_in(resolved_statuses))
        .filter(native_terminal_effect_outbox::Column::CompletedAt.lte(cutoff))
        .order_by_asc(native_terminal_effect_outbox::Column::CompletedAt)
        .order_by_asc(native_terminal_effect_outbox::Column::EffectId)
        .limit(limit)
        .into_tuple::<String>()
        .all(db)
        .await
        .context("failed to select resolved native terminal effects for retention")?;
    if effect_ids.is_empty() {
        return Ok(0);
    }
    Ok(native_terminal_effect_outbox::Entity::delete_many()
        .filter(native_terminal_effect_outbox::Column::EffectId.is_in(effect_ids))
        .filter(native_terminal_effect_outbox::Column::Status.is_in(resolved_statuses))
        .filter(native_terminal_effect_outbox::Column::CompletedAt.lte(cutoff))
        .exec(db)
        .await
        .context("failed to purge resolved native terminal effects")?
        .rows_affected)
}

async fn count_status<C: ConnectionTrait>(db: &C, status: &'static str) -> Result<u64> {
    native_terminal_effect_outbox::Entity::find()
        .filter(native_terminal_effect_outbox::Column::Status.eq(status))
        .count(db)
        .await
        .with_context(|| format!("failed to count `{status}` native terminal effects"))
}

fn validate_preparation(preparation: &NativeTerminalEffectPreparation) -> Result<()> {
    if preparation.runtime_generation == 0 {
        bail!("terminal-effect runtime generation must be positive");
    }
    if preparation.effects.len() > MAX_EFFECTS_PER_TURN {
        bail!("terminal-effect batch exceeds the per-Turn effect limit");
    }
    if preparation.batch_id.is_empty() || preparation.batch_id.chars().count() > 128 {
        bail!("terminal-effect batch id is invalid");
    }
    let mut kinds = HashSet::new();
    let mut ids = HashSet::new();
    for effect in &preparation.effects {
        if effect.effect_id.is_empty() || effect.effect_id.chars().count() > 128 {
            bail!("terminal-effect id is invalid");
        }
        if !ids.insert(effect.effect_id.as_str()) || !kinds.insert(effect.effect_kind) {
            bail!("terminal-effect batch contains duplicate identity");
        }
        if effect.max_attempts == 0 || effect.max_attempts > MAX_EFFECT_ATTEMPTS {
            bail!("terminal-effect retry budget is outside the supported range");
        }
        let encoded = serde_json::to_vec(&effect.payload)
            .context("failed to encode terminal-effect payload for admission")?;
        if encoded.len() > MAX_EFFECT_PAYLOAD_BYTES {
            bail!("terminal-effect payload exceeds the durable byte limit");
        }
        match (&effect.effect_kind, &effect.payload) {
            (
                NativeTerminalEffectKind::PostTurnHook,
                pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook { .. },
            )
            | (
                NativeTerminalEffectKind::PostTurnHook,
                pioneer_protocol::NativeTerminalEffectPayload::PostTurnHookPreparationFailed {
                    ..
                },
            )
            | (
                NativeTerminalEffectKind::AttachedTaskCleanup,
                pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup { .. },
            ) => {}
            _ => bail!("terminal-effect kind does not match its payload"),
        }
        if let pioneer_protocol::NativeTerminalEffectPayload::AttachedTaskCleanup {
            reason,
            runtime_contract,
        } = &effect.payload
        {
            if reason.chars().count() > 4_096 {
                bail!("attached-task cleanup reason exceeds its durable character limit");
            }
            if runtime_contract.trim().is_empty()
                || runtime_contract.len() > 128
                || !runtime_contract.is_ascii()
            {
                bail!("attached-task cleanup runtime contract is invalid");
            }
        }
        if effect.effect_kind == NativeTerminalEffectKind::AttachedTaskCleanup
            && effect.gate != NativeTerminalEffectGate::TerminalCommit
        {
            bail!("attached-task cleanup must use the terminal-commit gate");
        }
    }
    Ok(())
}

fn validate_existing_identity(
    existing: &native_terminal_effect_outbox::Model,
    preparation: &NativeTerminalEffectPreparation,
    effect: &NativeTerminalEffectSpec,
) -> Result<()> {
    if existing.workspace_id != preparation.workspace_id
        || existing.thread_id != preparation.thread_id
        || existing.turn_id != preparation.turn_id
        || existing.effect_kind != kind_to_db(effect.effect_kind)
    {
        bail!(
            "terminal effect `{}` conflicts with an existing authority scope",
            effect.effect_id
        );
    }
    Ok(())
}

/// The covering index serves four equality seeks, returning no candidate JSON.
#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub(crate) struct CandidateGateMetadata {
    pub id: String,
    pub status: String,
    pub updated_at: DateTimeWithTimeZone,
}

pub(crate) fn candidate_metadata_seek(
    thread_id: &str,
    turn_id: &str,
    status: &str,
) -> sea_orm::Select<task_result_candidate::Entity> {
    task_result_candidate::Entity::find()
        .select_only()
        .columns([
            task_result_candidate::Column::Id,
            task_result_candidate::Column::Status,
            task_result_candidate::Column::UpdatedAt,
        ])
        .filter(task_result_candidate::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(task_result_candidate::Column::TurnId.eq(turn_id.to_owned()))
        .filter(task_result_candidate::Column::Status.eq(status.to_owned()))
        .order_by_desc(task_result_candidate::Column::UpdatedAt)
        .order_by_desc(task_result_candidate::Column::Id)
        .limit(1)
}

async fn latest_candidate_metadata<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    replacement: Option<CandidateGateMetadata>,
) -> Result<Option<CandidateGateMetadata>> {
    let mut latest = replacement.clone();
    for status in terminal_candidate_statuses() {
        let mut query = candidate_metadata_seek(thread_id, turn_id, status);
        // Predict the state after one authoritative candidate write. At most
        // one index entry can be excluded (id is unique), not a history scan.
        if let Some(replacement) = &replacement {
            query = query.filter(task_result_candidate::Column::Id.ne(replacement.id.clone()));
        }
        let row = query.into_model::<CandidateGateMetadata>().one(db).await?;
        if let Some(row) = row {
            if latest
                .as_ref()
                .is_none_or(|current| (row.updated_at, &row.id) > (current.updated_at, &current.id))
            {
                latest = Some(row);
            }
        }
    }
    Ok(latest)
}

async fn candidate_gate_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<CandidateGateState> {
    Ok(
        match latest_candidate_metadata(db, thread_id, turn_id, None).await? {
            Some(row) if row.status == "accepted" => CandidateGateState::Accepted(row),
            Some(row) => CandidateGateState::Rejected(row),
            None => CandidateGateState::Waiting,
        },
    )
}

fn terminal_candidate_statuses() -> [&'static str; 4] {
    ["accepted", "rejected", "superseded", "cancelled"]
}

fn kind_to_db(kind: NativeTerminalEffectKind) -> &'static str {
    match kind {
        NativeTerminalEffectKind::PostTurnHook => "post_turn_hook",
        NativeTerminalEffectKind::AttachedTaskCleanup => "attached_task_cleanup",
    }
}

fn gate_to_db(gate: NativeTerminalEffectGate) -> &'static str {
    match gate {
        NativeTerminalEffectGate::TerminalCommit => "terminal_commit",
        NativeTerminalEffectGate::AcceptedTaskResult => "accepted_task_result",
    }
}

fn gate_from_db(value: &str) -> Result<NativeTerminalEffectGate> {
    match value {
        "terminal_commit" => Ok(NativeTerminalEffectGate::TerminalCommit),
        "accepted_task_result" => Ok(NativeTerminalEffectGate::AcceptedTaskResult),
        _ => bail!("unknown terminal-effect gate `{value}`"),
    }
}

pub(crate) fn payload_matches_db_kind(
    effect_kind: &str,
    payload: &NativeTerminalEffectPayload,
) -> bool {
    matches!(
        (effect_kind, payload),
        (
            "post_turn_hook",
            NativeTerminalEffectPayload::PostTurnHook { .. }
                | NativeTerminalEffectPayload::PostTurnHookPreparationFailed { .. }
        ) | (
            "attached_task_cleanup",
            NativeTerminalEffectPayload::AttachedTaskCleanup { .. }
        )
    )
}

fn bounded_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

pub(crate) fn payload_sha256_hex(payload_json: &str) -> String {
    hex::encode(Sha256::digest(payload_json.as_bytes()))
}

pub(crate) fn payload_integrity_matches(
    payload_json: &str,
    payload_sha256: &str,
    payload_identity_sha256: &str,
) -> bool {
    let actual_sha256 = payload_sha256_hex(payload_json);
    actual_sha256 == payload_sha256 && actual_sha256 == payload_identity_sha256
}
