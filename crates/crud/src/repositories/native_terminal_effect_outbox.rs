use anyhow::{Context, Result, bail};
use pioneer_entity::{native_terminal_effect_outbox, task_result_candidate, thread, turn};
use pioneer_protocol::{
    NativeTerminalEffectGate, NativeTerminalEffectKind, NativeTerminalEffectPayload,
    NativeTerminalEffectPreparation, NativeTerminalEffectSpec,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, EntityTrait, IntoActiveModel,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set,
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
pub const MAX_EFFECT_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_EFFECT_HANDLER_CHECKPOINT_BYTES: usize = 128 * 1024;
pub const MAX_EFFECT_ATTEMPTS: u16 = 20;
pub const MAX_PURGE_BATCH_SIZE: u64 = 1_000;
const MAX_ERROR_CODE_CHARS: usize = 64;
const MAX_ERROR_MESSAGE_CHARS: usize = 2_048;
const COMPACTED_PAYLOAD_JSON: &str = r#"{"compacted":true}"#;

#[derive(Debug, Clone)]
pub struct ClaimedNativeTerminalEffect {
    pub row: native_terminal_effect_outbox::Model,
    pub claim_token: String,
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
    rows: Vec<PreparedCandidateGateResolutionRow>,
}

#[derive(Debug, Clone)]
struct PreparedCandidateGateResolutionRow {
    effect_id: String,
    status_before: String,
    updated_at_before: DateTimeWithTimeZone,
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
    Accepted(String),
    Rejected,
}

pub async fn prepare<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedNativeTerminalEffectPreparation,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    prepare_with_policy(db, prepared, now, true).await
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
                        CandidateGateState::Accepted(id) => Some(id),
                        CandidateGateState::Waiting | CandidateGateState::Rejected => None,
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
                        CandidateGateState::Accepted(id) => Some(id),
                        CandidateGateState::Waiting | CandidateGateState::Rejected => None,
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
                                    Some(candidate_id.clone()),
                                    false,
                                    true,
                                    Some("terminal_effect_preparation_failed".to_owned()),
                                    Some(format!("post-turn hook preparation failed: {failure:?}")),
                                ),
                                CandidateGateState::Rejected => {
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
            CandidateGateState::Accepted(id) => (STATUS_READY, Some(id), true),
            CandidateGateState::Rejected => (STATUS_DISCARDED, None, false),
            CandidateGateState::Waiting => (STATUS_WAITING_ACCEPTANCE, None, false),
        },
    }
}

/// Loads and validates the bounded acceptance-gate write set before an
/// authoritative candidate transaction obtains writer admission.
pub(crate) async fn prepare_gate_resolution_for_candidate<C: ConnectionTrait>(
    db: &C,
    candidate_id: &str,
    thread_id: &str,
    turn_id: &str,
    candidate_status: &str,
    now: DateTimeWithTimeZone,
) -> Result<PreparedCandidateGateResolution> {
    let terminal_status = matches!(
        candidate_status,
        "accepted" | "rejected" | "superseded" | "cancelled"
    );
    if !terminal_status {
        return Ok(PreparedCandidateGateResolution {
            candidate_id: candidate_id.to_owned(),
            thread_id: thread_id.to_owned(),
            turn_id: turn_id.to_owned(),
            resolved_at: now,
            requires_fence: false,
            rows: Vec::new(),
        });
    }
    let rows = native_terminal_effect_outbox::Entity::find()
        .filter(native_terminal_effect_outbox::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(native_terminal_effect_outbox::Column::TurnId.eq(turn_id.to_owned()))
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
        .all(db)
        .await
        .context("failed to load candidate-gated terminal effects")?;
    if rows.len() > MAX_EFFECTS_PER_TURN {
        bail!(
            "Turn `{turn_id}` has more than {MAX_EFFECTS_PER_TURN} candidate-gated native terminal effects"
        );
    }

    let compacted_payload_sha256 = payload_sha256_hex(COMPACTED_PAYLOAD_JSON);
    let mut prepared_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let committed = row.terminal_committed_at.is_some();
        let preparation_failure = committed
            && candidate_status == "accepted"
            && payload_integrity_matches(
                row.payload_json.as_str(),
                row.payload_sha256.as_str(),
                row.payload_identity_sha256.as_str(),
            )
            && matches!(
                serde_json::from_str::<NativeTerminalEffectPayload>(row.payload_json.as_str()),
                Ok(NativeTerminalEffectPayload::PostTurnHookPreparationFailed { .. })
            );
        let status_after = if committed {
            if preparation_failure {
                STATUS_UNRESOLVED
            } else if candidate_status == "accepted" {
                STATUS_READY
            } else {
                STATUS_DISCARDED
            }
        } else {
            STATUS_PREPARED
        };
        let accepted_candidate_id =
            (candidate_status == "accepted").then(|| candidate_id.to_owned());
        let next_run_at =
            (committed && candidate_status == "accepted" && !preparation_failure).then_some(now);
        let completed_at =
            (committed && (candidate_status != "accepted" || preparation_failure)).then_some(now);
        let compact_payload = committed && candidate_status != "accepted";
        prepared_rows.push(PreparedCandidateGateResolutionRow {
            effect_id: row.effect_id,
            status_before: row.status,
            updated_at_before: row.updated_at,
            payload_sha256_before: row.payload_sha256,
            payload_identity_sha256_before: row.payload_identity_sha256,
            terminal_committed_at_before: row.terminal_committed_at,
            status_after,
            accepted_candidate_id,
            next_run_at,
            completed_at,
            last_error_code: preparation_failure
                .then_some("terminal_effect_preparation_failed".to_owned()),
            last_error_message: preparation_failure
                .then_some("post-turn hook preparation failed before durable execution".to_owned()),
            compact_payload,
            compacted_payload_sha256: compact_payload.then(|| compacted_payload_sha256.clone()),
        });
    }
    Ok(PreparedCandidateGateResolution {
        candidate_id: candidate_id.to_owned(),
        thread_id: thread_id.to_owned(),
        turn_id: turn_id.to_owned(),
        resolved_at: now,
        requires_fence: true,
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
    if current_effect_ids.len() > MAX_EFFECTS_PER_TURN || current_effect_ids != prepared_effect_ids
    {
        bail!(
            "candidate-gated native terminal effects changed before resolving candidate `{}`",
            prepared.candidate_id
        );
    }

    let mut resolved = 0_u64;
    for row in prepared.rows {
        let mut update = native_terminal_effect_outbox::Entity::update_many()
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
        if updated != 1 {
            bail!(
                "candidate-gated native terminal effect changed before resolving candidate `{}`",
                prepared.candidate_id
            );
        }
        resolved = resolved.saturating_add(updated);
    }
    Ok(resolved)
}

/// Standalone reconciliation wrapper. Transactional candidate paths prepare
/// on the reader and call `apply_prepared_gate_resolution` directly.
pub async fn resolve_gate_for_candidate<C: ConnectionTrait>(
    db: &C,
    candidate_id: &str,
    thread_id: &str,
    turn_id: &str,
    candidate_status: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let prepared = prepare_gate_resolution_for_candidate(
        db,
        candidate_id,
        thread_id,
        turn_id,
        candidate_status,
        now,
    )
    .await?;
    apply_prepared_gate_resolution(db, prepared).await
}

pub async fn reconcile_waiting_gates<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<u64> {
    let rows = native_terminal_effect_outbox::Entity::find()
        .filter(native_terminal_effect_outbox::Column::Status.eq(STATUS_WAITING_ACCEPTANCE))
        .order_by_asc(native_terminal_effect_outbox::Column::PreparedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to scan waiting terminal-effect gates")?;
    let mut resolved = 0_u64;
    for row in rows {
        let candidate = task_result_candidate::Entity::find()
            .filter(task_result_candidate::Column::ThreadId.eq(row.thread_id.clone()))
            .filter(task_result_candidate::Column::TurnId.eq(row.turn_id.clone()))
            .filter(task_result_candidate::Column::Status.is_in(terminal_candidate_statuses()))
            .order_by_desc(task_result_candidate::Column::UpdatedAt)
            .one(db)
            .await
            .context("failed to reconcile terminal-effect candidate gate")?;
        if let Some(candidate) = candidate {
            resolved = resolved.saturating_add(
                resolve_gate_for_candidate(
                    db,
                    candidate.id.as_str(),
                    candidate.thread_id.as_str(),
                    candidate.turn_id.as_str(),
                    candidate.status.as_str(),
                    now,
                )
                .await?,
            );
        }
    }
    Ok(resolved)
}

pub async fn claim_due<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    claim_expires_at: DateTimeWithTimeZone,
    limit: u64,
    claim_token_factory: impl Fn() -> String,
) -> Result<Vec<ClaimedNativeTerminalEffect>> {
    let due = Condition::all()
        .add(native_terminal_effect_outbox::Column::Status.is_in([STATUS_READY, STATUS_RETRY_WAIT]))
        .add(native_terminal_effect_outbox::Column::NextRunAt.lte(now));
    let expired = Condition::all()
        .add(native_terminal_effect_outbox::Column::Status.eq(STATUS_RUNNING))
        .add(native_terminal_effect_outbox::Column::ClaimExpiresAt.lte(now));

    native_terminal_effect_outbox::Entity::update_many()
        .col_expr(
            native_terminal_effect_outbox::Column::Status,
            Expr::value(STATUS_UNRESOLVED.to_owned()),
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
        )
        .col_expr(
            native_terminal_effect_outbox::Column::CompletedAt,
            Expr::value(Some(now)),
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
            native_terminal_effect_outbox::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(Condition::any().add(due.clone()).add(expired.clone()))
        .filter(
            Expr::col(native_terminal_effect_outbox::Column::AttemptCount).gte(Expr::col(
                native_terminal_effect_outbox::Column::MaxAttempts,
            )),
        )
        .exec(db)
        .await
        .context("failed to terminalize exhausted native terminal effects")?;

    let candidates = native_terminal_effect_outbox::Entity::find()
        .filter(Condition::any().add(due.clone()).add(expired.clone()))
        .filter(
            Expr::col(native_terminal_effect_outbox::Column::AttemptCount).lt(Expr::col(
                native_terminal_effect_outbox::Column::MaxAttempts,
            )),
        )
        .order_by_asc(native_terminal_effect_outbox::Column::NextRunAt)
        .order_by_asc(native_terminal_effect_outbox::Column::PreparedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due native terminal effects")?;
    let mut claimed = Vec::new();
    for row in candidates {
        let token = claim_token_factory();
        let updated = native_terminal_effect_outbox::Entity::update_many()
            .col_expr(
                native_terminal_effect_outbox::Column::Status,
                Expr::value(STATUS_RUNNING.to_owned()),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::AttemptCount,
                Expr::col(native_terminal_effect_outbox::Column::AttemptCount).add(1),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::ClaimToken,
                Expr::value(Some(token.clone())),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::ClaimExpiresAt,
                Expr::value(Some(claim_expires_at)),
            )
            .col_expr(
                native_terminal_effect_outbox::Column::UpdatedAt,
                Expr::value(now),
            )
            .filter(native_terminal_effect_outbox::Column::EffectId.eq(row.effect_id.clone()))
            .filter(Condition::any().add(due.clone()).add(expired.clone()))
            .filter(
                Expr::col(native_terminal_effect_outbox::Column::AttemptCount).lt(Expr::col(
                    native_terminal_effect_outbox::Column::MaxAttempts,
                )),
            )
            .exec(db)
            .await
            .context("failed to claim native terminal effect")?
            .rows_affected;
        if updated == 1
            && let Some(row) = native_terminal_effect_outbox::Entity::find_by_id(row.effect_id)
                .one(db)
                .await
                .context("failed to reload claimed native terminal effect")?
        {
            claimed.push(ClaimedNativeTerminalEffect {
                row,
                claim_token: token,
            });
        }
    }
    Ok(claimed)
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
                bail!("native terminal-effect handler checkpoint exceeds its durable byte limit");
            }
            if payload_sha256_hex(checkpoint.as_str()) != expected_sha256 {
                bail!("native terminal-effect handler checkpoint hash mismatch");
            }
            Ok(Some(checkpoint))
        }
        _ => bail!("native terminal-effect handler checkpoint is incomplete"),
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

async fn candidate_gate_state<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<CandidateGateState> {
    let row = task_result_candidate::Entity::find()
        .filter(task_result_candidate::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(task_result_candidate::Column::TurnId.eq(turn_id.to_owned()))
        .filter(task_result_candidate::Column::Status.is_in(terminal_candidate_statuses()))
        .order_by_desc(task_result_candidate::Column::UpdatedAt)
        .one(db)
        .await
        .context("failed to resolve terminal-effect task-result gate")?;
    match row {
        Some(row) if row.status == "accepted" => Ok(CandidateGateState::Accepted(row.id)),
        Some(_) => Ok(CandidateGateState::Rejected),
        None => Ok(CandidateGateState::Waiting),
    }
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
