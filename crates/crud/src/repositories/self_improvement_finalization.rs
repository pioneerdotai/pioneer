use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use pioneer_entity::{
    agent_skill, agent_skill_version, self_improvement_run, self_improvement_workspace_state,
};
use pioneer_protocol::{ThreadSidebarVisibility, TurnKind, TurnOrigin, TurnStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, QueryFilter,
    Statement,
};

use super::agent_skill::{
    CreateAgentSkillMutation, NewAgentSkill, NewAgentSkillVersion, RollbackAgentSkillMutation,
    RollbackAgentSkillMutationResult, UpdateAgentSkillMutation, UpdateAgentSkillMutationResult,
    apply_create_in_caller_transaction, apply_rollback_in_caller_transaction,
    apply_update_in_caller_transaction, prepare_agent_skill_version,
};
use super::self_improvement_run::STATUS_COMPLETED;
use super::self_improvement_source_turn::{source_access_classes, source_origins};
use crate::{
    AcceptedAgentSkillCreate, FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult,
    SelfImprovementFinalOutcome, SelfImprovementFinalizationConflict,
    SelfImprovementNoChangeReason,
};

const RESULT_SUMMARY_MAX_BYTES: usize = 16 * 1024;
const MAX_REASON_CODES: usize = 8;
const MAX_REASON_CODE_CHARS: usize = 64;
const MAX_SOURCE_TURN_IDS: usize = 1_024;

#[derive(Clone)]
pub struct PreparedSelfImprovementFinalization {
    input: FinalizeSelfImprovementRunInput,
    outcome: PreparedFinalOutcome,
}

#[derive(Clone)]
enum PreparedFinalOutcome {
    AcceptedCreate {
        mutation: CreateAgentSkillMutation,
        source_check: Option<PreparedSourceTurnCheck>,
        source_turn_ids_json: String,
        applied_summary: String,
        duplicate_fingerprint_summary: String,
    },
    AcceptedUpdate {
        mutation: UpdateAgentSkillMutation,
        source_check: Option<PreparedSourceTurnCheck>,
        source_turn_ids_json: String,
        applied_summary: String,
        no_change_summaries: HashMap<&'static str, String>,
    },
    AcceptedRollback {
        mutation: RollbackAgentSkillMutation,
        source_check: Option<PreparedSourceTurnCheck>,
        applied_summary: String,
        no_change_summaries: HashMap<&'static str, String>,
    },
    NoChange {
        summary: String,
    },
}

#[derive(Clone)]
struct PreparedSourceTurnCheck {
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    expected_count: i64,
    statement: Statement,
}

impl PreparedSourceTurnCheck {
    async fn matches<C: ConnectionTrait>(self, db: &C) -> Result<bool> {
        let row = db
            .query_one_raw(self.statement)
            .await
            .context("failed to verify cited Agent skill source provenance")?
            .context("Agent skill source provenance query returned no aggregate row")?;
        let matched_count: i64 = row
            .try_get("", "matched_count")
            .context("failed to decode cited Agent skill source count")?;
        let new_anchor_count: i64 = row
            .try_get("", "new_anchor_count")
            .context("failed to decode cited Agent skill new-anchor count")?;
        Ok(matched_count == self.expected_count && new_anchor_count > 0)
    }
}

pub async fn prepare<C: ConnectionTrait>(
    db: &C,
    input: FinalizeSelfImprovementRunInput,
) -> Result<PreparedSelfImprovementFinalization> {
    validate_input(&input)?;
    let frozen_bounds = self_improvement_run::Entity::find_by_id(input.fence.run_id.clone())
        .filter(self_improvement_run::Column::WorkspaceId.eq(input.fence.workspace_id.clone()))
        .one(db)
        .await
        .context("failed to prepare self-improvement finalization source bounds")?
        .map(|run| (run.source_lower_exclusive, run.source_upper_inclusive));

    let outcome = match &input.outcome {
        SelfImprovementFinalOutcome::AcceptedCreate(create) => {
            let version = prepare_agent_skill_version(NewAgentSkillVersion {
                id: create.version_id.clone(),
                skill_id: create.skill_id.clone(),
                version_number: 1,
                source_run_id: Some(input.fence.run_id.clone()),
                parent_version_id: None,
                candidate_key: create.candidate_key.clone(),
                display_name: create.display_name.clone(),
                skill_markdown: create.skill_markdown.clone(),
                instruction_body: create.instruction_body.clone(),
                when_to_use: create.when_to_use.clone(),
                when_not_to_use: create.when_not_to_use.clone(),
                fingerprint: create.fingerprint.clone(),
                source_turn_ids: create.source_turn_ids.clone(),
            })?;
            let source_turn_ids_json = version.source_turn_ids_json().to_owned();
            PreparedFinalOutcome::AcceptedCreate {
                mutation: CreateAgentSkillMutation {
                    skill: NewAgentSkill {
                        skill_id: create.skill_id.clone(),
                        workspace_id: input.fence.workspace_id.clone(),
                        slug: create.slug.clone(),
                    },
                    version,
                },
                source_check: prepare_source_turn_check(
                    input.fence.workspace_id.as_str(),
                    create.source_turn_ids.as_slice(),
                    frozen_bounds,
                )?,
                source_turn_ids_json,
                applied_summary: applied_summary(
                    create.candidate_key.as_str(),
                    create.source_turn_ids.len(),
                )?,
                duplicate_fingerprint_summary: no_change_summary(
                    SelfImprovementNoChangeReason::HostValidationRejected,
                    &["duplicate_fingerprint"],
                    Some("create"),
                    Some(create.candidate_key.as_str()),
                    Some(create.fingerprint.as_str()),
                )?,
            }
        }
        SelfImprovementFinalOutcome::AcceptedUpdate(update) => {
            let version = prepare_agent_skill_version(NewAgentSkillVersion {
                id: update.version_id.clone(),
                skill_id: update.skill_id.clone(),
                version_number: update.version_number,
                source_run_id: Some(input.fence.run_id.clone()),
                parent_version_id: Some(update.expected_active_version_id.clone()),
                candidate_key: update.candidate_key.clone(),
                display_name: update.display_name.clone(),
                skill_markdown: update.skill_markdown.clone(),
                instruction_body: update.instruction_body.clone(),
                when_to_use: update.when_to_use.clone(),
                when_not_to_use: update.when_not_to_use.clone(),
                fingerprint: update.fingerprint.clone(),
                source_turn_ids: update.source_turn_ids.clone(),
            })?;
            let source_turn_ids_json = version.source_turn_ids_json().to_owned();
            let reason_codes = [
                "current_active_fingerprint",
                "historical_fingerprint",
                "exact_parent_requires_rollback",
                "update_identity_or_lineage_invalid",
                "update_target_not_found",
                "update_slug_changed",
            ];
            PreparedFinalOutcome::AcceptedUpdate {
                mutation: UpdateAgentSkillMutation {
                    workspace_id: input.fence.workspace_id.clone(),
                    skill_id: update.skill_id.clone(),
                    expected_active_version_id: update.expected_active_version_id.clone(),
                    expected_slug: update.slug.clone(),
                    version,
                },
                source_check: prepare_source_turn_check(
                    input.fence.workspace_id.as_str(),
                    update.source_turn_ids.as_slice(),
                    frozen_bounds,
                )?,
                source_turn_ids_json,
                applied_summary: applied_summary(
                    update.candidate_key.as_str(),
                    update.source_turn_ids.len(),
                )?,
                no_change_summaries: prepare_rejection_summaries(
                    &reason_codes,
                    "update",
                    update.candidate_key.as_str(),
                    Some(update.fingerprint.as_str()),
                )?,
            }
        }
        SelfImprovementFinalOutcome::AcceptedRollback(rollback) => {
            let reason_codes = [
                "rollback_identity_invalid",
                "rollback_target_not_found",
                "rollback_target_not_exact_parent",
                "rollback_parent_not_owned",
            ];
            PreparedFinalOutcome::AcceptedRollback {
                mutation: RollbackAgentSkillMutation {
                    workspace_id: input.fence.workspace_id.clone(),
                    skill_id: rollback.skill_id.clone(),
                    expected_active_version_id: rollback.expected_active_version_id.clone(),
                    target_parent_version_id: rollback.target_parent_version_id.clone(),
                },
                source_check: prepare_source_turn_check(
                    input.fence.workspace_id.as_str(),
                    rollback.source_turn_ids.as_slice(),
                    frozen_bounds,
                )?,
                applied_summary: applied_summary(
                    rollback.candidate_key.as_str(),
                    rollback.source_turn_ids.len(),
                )?,
                no_change_summaries: prepare_rejection_summaries(
                    &reason_codes,
                    "rollback",
                    rollback.candidate_key.as_str(),
                    None,
                )?,
            }
        }
        SelfImprovementFinalOutcome::NoChange {
            reason,
            reason_codes,
        } => PreparedFinalOutcome::NoChange {
            summary: no_change_summary(*reason, reason_codes, None, None, None)?,
        },
    };
    Ok(PreparedSelfImprovementFinalization { input, outcome })
}

pub fn validate_input(input: &FinalizeSelfImprovementRunInput) -> Result<()> {
    let fence = &input.fence;
    if fence.run_id.trim().is_empty()
        || fence.workspace_id.trim().is_empty()
        || fence.claim_token.trim().is_empty()
        || fence.claimed_by.trim().is_empty()
        || fence.activation_epoch <= 0
        || fence.source_lower_exclusive < 0
    {
        bail!("self-improvement finalization fence is incomplete");
    }
    for (name, value) in [
        (
            "learner_provider",
            input.authority.learner_provider.as_str(),
        ),
        ("learner_model", input.authority.learner_model.as_str()),
        (
            "reviewer_provider",
            input.authority.reviewer_provider.as_str(),
        ),
        ("reviewer_model", input.authority.reviewer_model.as_str()),
        (
            "pipeline_contract_version",
            input.authority.pipeline_contract_version.as_str(),
        ),
    ] {
        if value.trim().is_empty() || value != value.trim() {
            bail!("self-improvement finalization authority {name} is invalid");
        }
    }
    match &input.outcome {
        SelfImprovementFinalOutcome::AcceptedCreate(create) => {
            if create.version_id.len() != pioneer_protocol::SKILL_ID_LEN
                || !create
                    .version_id
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
                || create.slug.trim().is_empty()
                || create.slug != create.slug.trim()
                || create.source_turn_ids.len() < 2
            {
                bail!("accepted Agent skill create identity or evidence is invalid");
            }
            validate_source_turn_ids(create.source_turn_ids.as_slice())?;
            for (name, value) in [
                ("candidate_key", create.candidate_key.as_str()),
                ("display_name", create.display_name.as_str()),
                ("skill_markdown", create.skill_markdown.as_str()),
                ("instruction_body", create.instruction_body.as_str()),
                ("when_to_use", create.when_to_use.as_str()),
                ("when_not_to_use", create.when_not_to_use.as_str()),
                ("fingerprint", create.fingerprint.as_str()),
            ] {
                if value.trim().is_empty() {
                    bail!("accepted Agent skill create {name} must not be empty");
                }
            }
        }
        SelfImprovementFinalOutcome::AcceptedUpdate(update) => {
            validate_accepted_version_fields(
                update.version_id.as_str(),
                update.slug.as_str(),
                update.candidate_key.as_str(),
                update.display_name.as_str(),
                update.skill_markdown.as_str(),
                update.instruction_body.as_str(),
                update.when_to_use.as_str(),
                update.when_not_to_use.as_str(),
                update.fingerprint.as_str(),
                update.source_turn_ids.as_slice(),
            )?;
            if update.expected_active_version_id.trim().is_empty() || update.version_number < 2 {
                bail!("accepted Agent skill update target or version number is invalid");
            }
        }
        SelfImprovementFinalOutcome::AcceptedRollback(rollback) => {
            if rollback.expected_active_version_id.trim().is_empty()
                || rollback.target_parent_version_id.trim().is_empty()
                || rollback.expected_active_version_id == rollback.target_parent_version_id
                || rollback.candidate_key.trim().is_empty()
                || rollback.source_turn_ids.is_empty()
            {
                bail!("accepted Agent skill rollback identity or evidence is invalid");
            }
            validate_source_turn_ids(rollback.source_turn_ids.as_slice())?;
        }
        SelfImprovementFinalOutcome::NoChange { reason_codes, .. } => {
            if reason_codes.len() > MAX_REASON_CODES
                || reason_codes.iter().any(|reason_code| {
                    reason_code.is_empty()
                        || reason_code.chars().count() > MAX_REASON_CODE_CHARS
                        || !reason_code.chars().all(|character| {
                            character.is_ascii_lowercase()
                                || character.is_ascii_digit()
                                || character == '_'
                        })
                })
            {
                bail!("self-improvement no-change reason codes are invalid");
            }
            let mut distinct = reason_codes.clone();
            distinct.sort();
            distinct.dedup();
            if distinct.len() != reason_codes.len() {
                bail!("self-improvement no-change reason codes must be distinct");
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_accepted_version_fields(
    version_id: &str,
    slug: &str,
    candidate_key: &str,
    display_name: &str,
    skill_markdown: &str,
    instruction_body: &str,
    when_to_use: &str,
    when_not_to_use: &str,
    fingerprint: &str,
    source_turn_ids: &[String],
) -> Result<()> {
    if version_id.len() != pioneer_protocol::SKILL_ID_LEN
        || !version_id
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
        || slug.trim().is_empty()
        || slug != slug.trim()
        || source_turn_ids.is_empty()
    {
        bail!("accepted Agent skill version identity or evidence is invalid");
    }
    for (name, value) in [
        ("candidate_key", candidate_key),
        ("display_name", display_name),
        ("skill_markdown", skill_markdown),
        ("instruction_body", instruction_body),
        ("when_to_use", when_to_use),
        ("when_not_to_use", when_not_to_use),
        ("fingerprint", fingerprint),
    ] {
        if value.trim().is_empty() {
            bail!("accepted Agent skill version {name} must not be empty");
        }
    }
    validate_source_turn_ids(source_turn_ids)
}

fn validate_source_turn_ids(source_turn_ids: &[String]) -> Result<()> {
    if source_turn_ids.is_empty()
        || source_turn_ids.len() > MAX_SOURCE_TURN_IDS
        || source_turn_ids
            .iter()
            .any(|source_id| source_id.trim().is_empty() || source_id != source_id.trim())
    {
        bail!("accepted Agent skill source turn identities are invalid");
    }
    let mut sorted = source_turn_ids.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.len() != source_turn_ids.len() {
        bail!("accepted Agent skill source turn identities must be distinct");
    }
    Ok(())
}

fn prepare_source_turn_check(
    workspace_id: &str,
    source_turn_ids: &[String],
    frozen_bounds: Option<(i64, i64)>,
) -> Result<Option<PreparedSourceTurnCheck>> {
    let Some((source_lower_exclusive, source_upper_inclusive)) = frozen_bounds else {
        return Ok(None);
    };
    let expected_count = i64::try_from(source_turn_ids.len())
        .context("accepted Agent skill source turn count exceeds durable integer range")?;
    let placeholders = std::iter::repeat_n("?", source_turn_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT COUNT(*) AS matched_count, \
         COALESCE(SUM(CASE WHEN source.id > ? AND source.id <= ? THEN 1 ELSE 0 END), 0) \
             AS new_anchor_count \
         FROM self_improvement_source_turn AS source \
         INNER JOIN turn AS cited_turn \
             ON cited_turn.id = source.turn_id \
            AND cited_turn.thread_id = source.thread_id \
         INNER JOIN thread AS source_thread \
             ON source_thread.id = cited_turn.thread_id \
            AND source_thread.workspace_id = source.workspace_id \
         WHERE source.workspace_id = ? \
           AND source.turn_id IN ({placeholders}) \
           AND cited_turn.status = ? \
           AND cited_turn.turn_kind = ? \
           AND cited_turn.origin = ? \
           AND source_thread.access_class IN (?, ?) \
           AND source_thread.sidebar_visibility = ? \
           AND source_thread.origin_kind IN (?, ?, ?)"
    );
    let mut values = Vec::with_capacity(source_turn_ids.len() + 12);
    values.push(source_lower_exclusive.into());
    values.push(source_upper_inclusive.into());
    values.push(workspace_id.to_owned().into());
    values.extend(source_turn_ids.iter().cloned().map(Into::into));
    values.push(crate::convention::turn_status_to_db(TurnStatus::Completed).into());
    values.push(crate::convention::turn_kind_to_db(TurnKind::Conversation).into());
    values.push(crate::convention::turn_origin_to_db(TurnOrigin::User).into());
    values.extend(source_access_classes().into_iter().map(Into::into));
    values.push(
        crate::convention::thread_sidebar_visibility_to_db(ThreadSidebarVisibility::Visible).into(),
    );
    values.extend(source_origins().into_iter().map(Into::into));
    Ok(Some(PreparedSourceTurnCheck {
        source_lower_exclusive,
        source_upper_inclusive,
        expected_count,
        statement: Statement::from_sql_and_values(DatabaseBackend::Sqlite, sql, values),
    }))
}

fn applied_summary(candidate_key: &str, source_turn_count: usize) -> Result<String> {
    bounded_summary(serde_json::json!({
        "candidateKey": candidate_key,
        "sourceTurnCount": source_turn_count,
    }))
}

fn no_change_summary<S: AsRef<str>>(
    reason: SelfImprovementNoChangeReason,
    reason_codes: &[S],
    attempted_action: Option<&str>,
    candidate_key: Option<&str>,
    fingerprint: Option<&str>,
) -> Result<String> {
    let reason_codes = reason_codes.iter().map(AsRef::as_ref).collect::<Vec<_>>();
    bounded_summary(serde_json::json!({
        "reason": reason.as_str(),
        "reasonCodes": reason_codes,
        "attemptedAction": attempted_action,
        "candidateKey": candidate_key,
        "fingerprint": fingerprint,
    }))
}

fn prepare_rejection_summaries(
    reason_codes: &[&'static str],
    attempted_action: &'static str,
    candidate_key: &str,
    fingerprint: Option<&str>,
) -> Result<HashMap<&'static str, String>> {
    reason_codes
        .iter()
        .copied()
        .map(|reason_code| {
            no_change_summary(
                SelfImprovementNoChangeReason::HostValidationRejected,
                &[reason_code],
                Some(attempted_action),
                Some(candidate_key),
                fingerprint,
            )
            .map(|summary| (reason_code, summary))
        })
        .collect()
}

fn prepared_rejection_summary<'a>(
    summaries: &'a HashMap<&'static str, String>,
    reason_code: &str,
) -> Result<&'a str> {
    summaries
        .get(reason_code)
        .map(String::as_str)
        .with_context(|| {
            format!("self-improvement rejection `{reason_code}` has no prepared result summary")
        })
}

pub async fn finalize(
    db: &DatabaseTransaction,
    prepared: PreparedSelfImprovementFinalization,
    now: DateTimeWithTimeZone,
) -> Result<FinalizeSelfImprovementRunResult> {
    let PreparedSelfImprovementFinalization { input, outcome } = prepared;
    let Some(run) = self_improvement_run::Entity::find_by_id(input.fence.run_id.clone())
        .filter(self_improvement_run::Column::WorkspaceId.eq(input.fence.workspace_id.clone()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load self-improvement run `{}` for finalization",
                input.fence.run_id
            )
        })?
    else {
        return Ok(FinalizeSelfImprovementRunResult::Stale);
    };
    if run.status == STATUS_COMPLETED {
        return Ok(
            if completed_run_matches(db, &run, &input.outcome, &outcome).await? {
                FinalizeSelfImprovementRunResult::AlreadyFinalized
            } else {
                FinalizeSelfImprovementRunResult::Stale
            },
        );
    }
    if !run_matches_fence(&run, &input, now) {
        return Ok(FinalizeSelfImprovementRunResult::Stale);
    }
    if !super::self_improvement_run::workspace_matches_fence(db, &input.fence).await? {
        return Ok(FinalizeSelfImprovementRunResult::Stale);
    }

    match (&input.outcome, outcome) {
        (
            SelfImprovementFinalOutcome::AcceptedCreate(create),
            PreparedFinalOutcome::AcceptedCreate {
                mutation,
                source_check,
                source_turn_ids_json: _,
                applied_summary,
                duplicate_fingerprint_summary,
            },
        ) => {
            let Some(source_check) = source_check else {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            };
            if source_check.source_lower_exclusive != run.source_lower_exclusive
                || source_check.source_upper_inclusive != run.source_upper_inclusive
                || !source_check.matches(db).await?
            {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            }
            if let Some(conflict) = create_conflict(db, &input, create).await? {
                if conflict == SelfImprovementFinalizationConflict::Fingerprint {
                    return finish_no_change(
                        db,
                        &input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        duplicate_fingerprint_summary.as_str(),
                        now,
                    )
                    .await;
                }
                return Ok(FinalizeSelfImprovementRunResult::Conflict(conflict));
            }
            if !complete_run(
                db,
                &input,
                "applied",
                Some("create"),
                Some(create.skill_id.as_str()),
                None,
                Some(create.version_id.as_str()),
                applied_summary.as_str(),
                now,
            )
            .await?
            {
                bail!("self-improvement run fence changed during finalization");
            }
            if !advance_cursor(
                db,
                input.fence.workspace_id.as_str(),
                input.fence.activation_epoch,
                input.fence.source_lower_exclusive,
                run.source_upper_inclusive,
                now,
            )
            .await?
            {
                bail!("self-improvement workspace cursor changed during finalization");
            }
            apply_create_in_caller_transaction(db, mutation, now).await?;
            Ok(FinalizeSelfImprovementRunResult::Applied {
                skill_id: create.skill_id.clone(),
                version_id: create.version_id.clone(),
            })
        }
        (
            SelfImprovementFinalOutcome::AcceptedUpdate(update),
            PreparedFinalOutcome::AcceptedUpdate {
                mutation,
                source_check,
                source_turn_ids_json: _,
                applied_summary,
                no_change_summaries,
            },
        ) => {
            let Some(source_check) = source_check else {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            };
            if source_check.source_lower_exclusive != run.source_lower_exclusive
                || source_check.source_upper_inclusive != run.source_upper_inclusive
                || !source_check.matches(db).await?
            {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            }
            match apply_update_in_caller_transaction(db, mutation, now).await? {
                UpdateAgentSkillMutationResult::Applied {
                    previous_version_id,
                    resulting_version_id,
                } => {
                    if !complete_run(
                        db,
                        &input,
                        "applied",
                        Some("update"),
                        Some(update.skill_id.as_str()),
                        Some(previous_version_id.as_str()),
                        Some(resulting_version_id.as_str()),
                        applied_summary.as_str(),
                        now,
                    )
                    .await?
                    {
                        bail!("self-improvement run fence changed during update finalization");
                    }
                    if !advance_cursor(
                        db,
                        input.fence.workspace_id.as_str(),
                        input.fence.activation_epoch,
                        input.fence.source_lower_exclusive,
                        run.source_upper_inclusive,
                        now,
                    )
                    .await?
                    {
                        bail!("self-improvement workspace cursor changed during update");
                    }
                    Ok(FinalizeSelfImprovementRunResult::Applied {
                        skill_id: update.skill_id.clone(),
                        version_id: resulting_version_id,
                    })
                }
                UpdateAgentSkillMutationResult::CurrentFingerprintNoChange => {
                    finish_no_change(
                        db,
                        &input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        prepared_rejection_summary(
                            &no_change_summaries,
                            "current_active_fingerprint",
                        )?,
                        now,
                    )
                    .await
                }
                UpdateAgentSkillMutationResult::HistoricalFingerprintNoChange { .. } => {
                    finish_no_change(
                        db,
                        &input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        prepared_rejection_summary(&no_change_summaries, "historical_fingerprint")?,
                        now,
                    )
                    .await
                }
                UpdateAgentSkillMutationResult::ExactParentFingerprintRequiresRollback {
                    ..
                } => {
                    finish_no_change(
                        db,
                        &input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        prepared_rejection_summary(
                            &no_change_summaries,
                            "exact_parent_requires_rollback",
                        )?,
                        now,
                    )
                    .await
                }
                UpdateAgentSkillMutationResult::StaleActive => {
                    Ok(FinalizeSelfImprovementRunResult::Stale)
                }
                UpdateAgentSkillMutationResult::Rejected(reason_code) => {
                    finish_no_change(
                        db,
                        &input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        prepared_rejection_summary(&no_change_summaries, reason_code)?,
                        now,
                    )
                    .await
                }
            }
        }
        (
            SelfImprovementFinalOutcome::AcceptedRollback(rollback),
            PreparedFinalOutcome::AcceptedRollback {
                mutation,
                source_check,
                applied_summary,
                no_change_summaries,
            },
        ) => {
            let Some(source_check) = source_check else {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            };
            if source_check.source_lower_exclusive != run.source_lower_exclusive
                || source_check.source_upper_inclusive != run.source_upper_inclusive
                || !source_check.matches(db).await?
            {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            }
            match apply_rollback_in_caller_transaction(db, mutation, now).await? {
                RollbackAgentSkillMutationResult::Applied {
                    previous_version_id,
                    resulting_version_id,
                } => {
                    if !complete_run(
                        db,
                        &input,
                        "applied",
                        Some("rollback"),
                        Some(rollback.skill_id.as_str()),
                        Some(previous_version_id.as_str()),
                        Some(resulting_version_id.as_str()),
                        applied_summary.as_str(),
                        now,
                    )
                    .await?
                    {
                        bail!("self-improvement run fence changed during rollback finalization");
                    }
                    if !advance_cursor(
                        db,
                        input.fence.workspace_id.as_str(),
                        input.fence.activation_epoch,
                        input.fence.source_lower_exclusive,
                        run.source_upper_inclusive,
                        now,
                    )
                    .await?
                    {
                        bail!("self-improvement workspace cursor changed during rollback");
                    }
                    Ok(FinalizeSelfImprovementRunResult::Applied {
                        skill_id: rollback.skill_id.clone(),
                        version_id: resulting_version_id,
                    })
                }
                RollbackAgentSkillMutationResult::StaleActive => {
                    Ok(FinalizeSelfImprovementRunResult::Stale)
                }
                RollbackAgentSkillMutationResult::Rejected(reason_code) => {
                    finish_no_change(
                        db,
                        &input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        prepared_rejection_summary(&no_change_summaries, reason_code)?,
                        now,
                    )
                    .await
                }
            }
        }
        (
            SelfImprovementFinalOutcome::NoChange { reason, .. },
            PreparedFinalOutcome::NoChange { summary },
        ) => {
            finish_no_change(
                db,
                &input,
                run.source_upper_inclusive,
                *reason,
                summary.as_str(),
                now,
            )
            .await
        }
        _ => bail!("prepared self-improvement outcome does not match its validated input"),
    }
}

fn run_matches_fence(
    run: &self_improvement_run::Model,
    input: &FinalizeSelfImprovementRunInput,
    now: DateTimeWithTimeZone,
) -> bool {
    input.authority.effective_enabled
        && super::self_improvement_run::model_matches_fence(run, &input.fence, now)
        && run.learner_provider == input.authority.learner_provider
        && run.learner_model == input.authority.learner_model
        && run.learner_reasoning_effort == input.authority.learner_reasoning_effort
        && run.reviewer_provider == input.authority.reviewer_provider
        && run.reviewer_model == input.authority.reviewer_model
        && run.reviewer_reasoning_effort == input.authority.reviewer_reasoning_effort
        && run.pipeline_contract_version == input.authority.pipeline_contract_version
}

async fn completed_run_matches<C: ConnectionTrait>(
    db: &C,
    run: &self_improvement_run::Model,
    outcome: &SelfImprovementFinalOutcome,
    prepared: &PreparedFinalOutcome,
) -> Result<bool> {
    match (outcome, prepared) {
        (
            SelfImprovementFinalOutcome::AcceptedCreate(create),
            PreparedFinalOutcome::AcceptedCreate {
                source_turn_ids_json,
                duplicate_fingerprint_summary,
                ..
            },
        ) => {
            if run.outcome.as_deref() == Some("no_change") {
                return Ok(run.applied_action.is_none()
                    && run.result_summary.as_deref()
                        == Some(duplicate_fingerprint_summary.as_str()));
            }
            if !(run.outcome.as_deref() == Some("applied")
                && run.applied_action.as_deref() == Some("create")
                && run.skill_id.as_deref() == Some(create.skill_id.as_str())
                && run.resulting_version_id.as_deref() == Some(create.version_id.as_str()))
            {
                return Ok(false);
            }
            let version = agent_skill_version::Entity::find_by_id(create.version_id.clone())
                .one(db)
                .await
                .context("failed to verify idempotent Agent skill create version replay")?;
            let Some(version) = version else {
                return Ok(false);
            };
            let skill = agent_skill::Entity::find_by_id(create.skill_id.as_str().to_owned())
                .filter(agent_skill::Column::WorkspaceId.eq(run.workspace_id.clone()))
                .one(db)
                .await
                .context("failed to verify idempotent Agent skill create identity replay")?;
            let Some(skill) = skill else {
                return Ok(false);
            };
            Ok(skill.slug == create.slug
                && version.skill_id == create.skill_id.as_str()
                && version.version_number == 1
                && version.source_run_id.as_deref() == Some(run.id.as_str())
                && version.parent_version_id.is_none()
                && version.candidate_key == create.candidate_key
                && version.display_name == create.display_name
                && version.skill_markdown == create.skill_markdown
                && version.instruction_body == create.instruction_body
                && version.when_to_use == create.when_to_use
                && version.when_not_to_use == create.when_not_to_use
                && version.fingerprint == create.fingerprint
                && version.source_turn_ids_json == *source_turn_ids_json)
        }
        (
            SelfImprovementFinalOutcome::AcceptedUpdate(update),
            PreparedFinalOutcome::AcceptedUpdate {
                source_turn_ids_json,
                no_change_summaries,
                ..
            },
        ) => {
            if run.outcome.as_deref() == Some("no_change") {
                return Ok(run.applied_action.is_none()
                    && run.result_summary.as_deref().is_some_and(|summary| {
                        no_change_summaries
                            .values()
                            .any(|prepared| prepared == summary)
                    }));
            }
            if !(run.outcome.as_deref() == Some("applied")
                && run.applied_action.as_deref() == Some("update")
                && run.skill_id.as_deref() == Some(update.skill_id.as_str())
                && run.previous_version_id.as_deref()
                    == Some(update.expected_active_version_id.as_str())
                && run.resulting_version_id.as_deref() == Some(update.version_id.as_str()))
            {
                return Ok(false);
            }
            let version = agent_skill_version::Entity::find_by_id(update.version_id.clone())
                .one(db)
                .await
                .context("failed to verify idempotent Agent skill update version replay")?;
            let Some(version) = version else {
                return Ok(false);
            };
            let skill = agent_skill::Entity::find_by_id(update.skill_id.as_str().to_owned())
                .filter(agent_skill::Column::WorkspaceId.eq(run.workspace_id.clone()))
                .one(db)
                .await
                .context("failed to verify idempotent Agent skill update identity replay")?;
            let Some(skill) = skill else {
                return Ok(false);
            };
            Ok(skill.slug == update.slug
                && version.skill_id == update.skill_id.as_str()
                && version.version_number == update.version_number
                && version.source_run_id.as_deref() == Some(run.id.as_str())
                && version.parent_version_id.as_deref()
                    == Some(update.expected_active_version_id.as_str())
                && version.candidate_key == update.candidate_key
                && version.display_name == update.display_name
                && version.skill_markdown == update.skill_markdown
                && version.instruction_body == update.instruction_body
                && version.when_to_use == update.when_to_use
                && version.when_not_to_use == update.when_not_to_use
                && version.fingerprint == update.fingerprint
                && version.source_turn_ids_json == *source_turn_ids_json)
        }
        (
            SelfImprovementFinalOutcome::AcceptedRollback(rollback),
            PreparedFinalOutcome::AcceptedRollback {
                no_change_summaries,
                ..
            },
        ) => {
            if run.outcome.as_deref() == Some("no_change") {
                return Ok(run.applied_action.is_none()
                    && run.result_summary.as_deref().is_some_and(|summary| {
                        no_change_summaries
                            .values()
                            .any(|prepared| prepared == summary)
                    }));
            }
            if !(run.outcome.as_deref() == Some("applied")
                && run.applied_action.as_deref() == Some("rollback")
                && run.skill_id.as_deref() == Some(rollback.skill_id.as_str())
                && run.previous_version_id.as_deref()
                    == Some(rollback.expected_active_version_id.as_str())
                && run.resulting_version_id.as_deref()
                    == Some(rollback.target_parent_version_id.as_str()))
            {
                return Ok(false);
            }
            let target =
                agent_skill_version::Entity::find_by_id(rollback.target_parent_version_id.clone())
                    .filter(
                        agent_skill_version::Column::SkillId
                            .eq(rollback.skill_id.as_str().to_owned()),
                    )
                    .one(db)
                    .await
                    .context("failed to verify idempotent Agent skill rollback target replay")?;
            Ok(target.is_some())
        }
        (
            SelfImprovementFinalOutcome::NoChange { .. },
            PreparedFinalOutcome::NoChange { summary },
        ) => Ok(run.outcome.as_deref() == Some("no_change")
            && run.applied_action.is_none()
            && run.result_summary.as_deref() == Some(summary.as_str())),
        _ => Ok(false),
    }
}

async fn create_conflict<C: ConnectionTrait>(
    db: &C,
    input: &FinalizeSelfImprovementRunInput,
    create: &AcceptedAgentSkillCreate,
) -> Result<Option<SelfImprovementFinalizationConflict>> {
    if agent_skill::Entity::find_by_id(create.skill_id.as_str().to_owned())
        .one(db)
        .await
        .context("failed to check Agent skill identity conflict")?
        .is_some()
    {
        return Ok(Some(SelfImprovementFinalizationConflict::SkillIdentity));
    }
    if agent_skill_version::Entity::find_by_id(create.version_id.clone())
        .one(db)
        .await
        .context("failed to check Agent skill version identity conflict")?
        .is_some()
    {
        return Ok(Some(SelfImprovementFinalizationConflict::VersionIdentity));
    }
    if agent_skill::Entity::find()
        .filter(agent_skill::Column::WorkspaceId.eq(input.fence.workspace_id.clone()))
        .filter(agent_skill::Column::Slug.eq(create.slug.clone()))
        .one(db)
        .await
        .context("failed to check Agent skill slug conflict")?
        .is_some()
    {
        return Ok(Some(SelfImprovementFinalizationConflict::Slug));
    }
    if db
        .query_one_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "SELECT 1 AS present \
             FROM agent_skill_version AS version \
             INNER JOIN agent_skill AS skill ON skill.id = version.skill_id \
             WHERE skill.workspace_id = ? AND version.fingerprint = ? \
             LIMIT 1",
            [
                input.fence.workspace_id.clone().into(),
                create.fingerprint.clone().into(),
            ],
        ))
        .await
        .context("failed to check Agent skill fingerprint conflict")?
        .is_some()
    {
        return Ok(Some(SelfImprovementFinalizationConflict::Fingerprint));
    }
    if agent_skill_version::Entity::find()
        .filter(agent_skill_version::Column::SourceRunId.eq(input.fence.run_id.clone()))
        .filter(agent_skill_version::Column::CandidateKey.eq(create.candidate_key.clone()))
        .one(db)
        .await
        .context("failed to check Agent skill candidate conflict")?
        .is_some()
    {
        return Ok(Some(SelfImprovementFinalizationConflict::Candidate));
    }
    Ok(None)
}

async fn finish_no_change(
    db: &DatabaseTransaction,
    input: &FinalizeSelfImprovementRunInput,
    source_upper_inclusive: i64,
    reason: SelfImprovementNoChangeReason,
    result_summary: &str,
    now: DateTimeWithTimeZone,
) -> Result<FinalizeSelfImprovementRunResult> {
    if !complete_run(
        db,
        input,
        "no_change",
        None,
        None,
        None,
        None,
        result_summary,
        now,
    )
    .await?
    {
        bail!("self-improvement run fence changed during no-change finalization");
    }
    if !advance_cursor(
        db,
        input.fence.workspace_id.as_str(),
        input.fence.activation_epoch,
        input.fence.source_lower_exclusive,
        source_upper_inclusive,
        now,
    )
    .await?
    {
        bail!("self-improvement workspace cursor changed during no-change finalization");
    }
    Ok(FinalizeSelfImprovementRunResult::NoChange { reason })
}

#[allow(clippy::too_many_arguments)]
async fn complete_run<C: ConnectionTrait>(
    db: &C,
    input: &FinalizeSelfImprovementRunInput,
    outcome: &str,
    applied_action: Option<&str>,
    skill_id: Option<&str>,
    previous_version_id: Option<&str>,
    resulting_version_id: Option<&str>,
    result_summary: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    Ok(self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(STATUS_COMPLETED.to_owned()),
        )
        .col_expr(
            self_improvement_run::Column::Outcome,
            Expr::value(Some(outcome.to_owned())),
        )
        .col_expr(
            self_improvement_run::Column::AppliedAction,
            Expr::value(applied_action.map(str::to_owned)),
        )
        .col_expr(
            self_improvement_run::Column::SkillId,
            Expr::value(skill_id.map(str::to_owned)),
        )
        .col_expr(
            self_improvement_run::Column::PreviousVersionId,
            Expr::value(previous_version_id.map(str::to_owned)),
        )
        .col_expr(
            self_improvement_run::Column::ResultingVersionId,
            Expr::value(resulting_version_id.map(str::to_owned)),
        )
        .col_expr(
            self_improvement_run::Column::ResultSummary,
            Expr::value(Some(result_summary.to_owned())),
        )
        .col_expr(
            self_improvement_run::Column::AnalysisCursorJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::AnalysisDigestJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::ClaimToken,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::ClaimedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::LeaseExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            self_improvement_run::Column::NextAttemptAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(super::self_improvement_run::fence_condition(
            &input.fence,
            now,
        ))
        .exec(db)
        .await
        .context("failed to complete fenced self-improvement run")?
        .rows_affected
        == 1)
}

async fn advance_cursor<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    activation_epoch: i64,
    expected_cursor: i64,
    new_cursor: i64,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    Ok(self_improvement_workspace_state::Entity::update_many()
        .col_expr(
            self_improvement_workspace_state::Column::CursorSourceId,
            Expr::value(new_cursor),
        )
        .col_expr(
            self_improvement_workspace_state::Column::UpdatedAt,
            Expr::value(now),
        )
        .filter(self_improvement_workspace_state::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_workspace_state::Column::ActivationEpoch.eq(activation_epoch))
        .filter(self_improvement_workspace_state::Column::CursorSourceId.eq(expected_cursor))
        .filter(self_improvement_workspace_state::Column::EffectiveEnabledAt.is_not_null())
        .exec(db)
        .await
        .context("failed to advance self-improvement workspace cursor")?
        .rows_affected
        == 1)
}

fn bounded_summary(value: serde_json::Value) -> Result<String> {
    let encoded = serde_json::to_string(&value)
        .context("failed to encode self-improvement result summary")?;
    if encoded.len() > RESULT_SUMMARY_MAX_BYTES {
        bail!("self-improvement result summary exceeds its persistence limit");
    }
    Ok(encoded)
}
