use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use pioneer_entity::{
    agent_skill, agent_skill_version, self_improvement_run, self_improvement_source_turn,
    self_improvement_workspace_state, thread, turn,
};
use pioneer_protocol::{
    ThreadOriginKind, ThreadSidebarVisibility, TurnKind, TurnOrigin, TurnStatus,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter};

use super::agent_skill::{
    CreateAgentSkillMutation, NewAgentSkill, NewAgentSkillVersion, RollbackAgentSkillMutation,
    RollbackAgentSkillMutationResult, UpdateAgentSkillMutation, UpdateAgentSkillMutationResult,
    apply_create_in_caller_transaction, apply_rollback_in_caller_transaction,
    apply_update_in_caller_transaction,
};
use super::membership::{PersistedThreadAccessClass, persisted_thread_access_class_to_db};
use super::self_improvement_run::STATUS_COMPLETED;
use crate::convention::{
    thread_origin_kind_from_db, thread_sidebar_visibility_from_db, turn_kind_from_db,
    turn_origin_from_db, turn_status_from_db,
};
use crate::{
    AcceptedAgentSkillCreate, FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult,
    SelfImprovementFinalOutcome, SelfImprovementFinalizationConflict,
    SelfImprovementNoChangeReason,
};

const RESULT_SUMMARY_MAX_BYTES: usize = 16 * 1024;
const MAX_REASON_CODES: usize = 8;
const MAX_REASON_CODE_CHARS: usize = 64;

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
    Ok(())
}

pub async fn finalize(
    db: &DatabaseTransaction,
    input: &FinalizeSelfImprovementRunInput,
    now: DateTimeWithTimeZone,
) -> Result<FinalizeSelfImprovementRunResult> {
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
        return Ok(if completed_run_matches(db, &run, &input.outcome).await? {
            FinalizeSelfImprovementRunResult::AlreadyFinalized
        } else {
            FinalizeSelfImprovementRunResult::Stale
        });
    }
    if !run_matches_fence(&run, input, now) {
        return Ok(FinalizeSelfImprovementRunResult::Stale);
    }
    if !super::self_improvement_run::workspace_matches_fence(db, &input.fence).await? {
        return Ok(FinalizeSelfImprovementRunResult::Stale);
    }

    match &input.outcome {
        SelfImprovementFinalOutcome::AcceptedCreate(create) => {
            if !source_turns_match_frozen_range(
                db,
                input.fence.workspace_id.as_str(),
                run.source_lower_exclusive,
                run.source_upper_inclusive,
                create.source_turn_ids.as_slice(),
            )
            .await?
            {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            }
            if let Some(conflict) = create_conflict(db, input, create).await? {
                if conflict == SelfImprovementFinalizationConflict::Fingerprint {
                    return finish_no_change(
                        db,
                        input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        vec!["duplicate_fingerprint".to_owned()],
                        Some("create"),
                        Some(create.candidate_key.as_str()),
                        Some(create.fingerprint.as_str()),
                        now,
                    )
                    .await;
                }
                return Ok(FinalizeSelfImprovementRunResult::Conflict(conflict));
            }
            let result_summary = bounded_summary(serde_json::json!({
                "candidateKey": create.candidate_key,
                "sourceTurnCount": create.source_turn_ids.len(),
            }))?;
            if !complete_run(
                db,
                input,
                "applied",
                Some("create"),
                Some(create.skill_id.as_str()),
                None,
                Some(create.version_id.as_str()),
                result_summary.as_str(),
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
            apply_create_in_caller_transaction(
                db,
                CreateAgentSkillMutation {
                    skill: NewAgentSkill {
                        skill_id: create.skill_id.clone(),
                        workspace_id: input.fence.workspace_id.clone(),
                        slug: create.slug.clone(),
                    },
                    version: NewAgentSkillVersion {
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
                    },
                },
                now,
            )
            .await?;
            Ok(FinalizeSelfImprovementRunResult::Applied {
                skill_id: create.skill_id.clone(),
                version_id: create.version_id.clone(),
            })
        }
        SelfImprovementFinalOutcome::AcceptedUpdate(update) => {
            if !source_turns_match_frozen_range(
                db,
                input.fence.workspace_id.as_str(),
                run.source_lower_exclusive,
                run.source_upper_inclusive,
                update.source_turn_ids.as_slice(),
            )
            .await?
            {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            }
            let mutation = apply_update_in_caller_transaction(
                db,
                UpdateAgentSkillMutation {
                    workspace_id: input.fence.workspace_id.clone(),
                    skill_id: update.skill_id.clone(),
                    expected_active_version_id: update.expected_active_version_id.clone(),
                    expected_slug: update.slug.clone(),
                    version: NewAgentSkillVersion {
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
                    },
                },
                now,
            )
            .await?;
            match mutation {
                UpdateAgentSkillMutationResult::Applied {
                    previous_version_id,
                    resulting_version_id,
                } => {
                    let result_summary = bounded_summary(serde_json::json!({
                        "candidateKey": update.candidate_key,
                        "sourceTurnCount": update.source_turn_ids.len(),
                    }))?;
                    if !complete_run(
                        db,
                        input,
                        "applied",
                        Some("update"),
                        Some(update.skill_id.as_str()),
                        Some(previous_version_id.as_str()),
                        Some(resulting_version_id.as_str()),
                        result_summary.as_str(),
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
                        input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        vec!["current_active_fingerprint".to_owned()],
                        Some("update"),
                        Some(update.candidate_key.as_str()),
                        Some(update.fingerprint.as_str()),
                        now,
                    )
                    .await
                }
                UpdateAgentSkillMutationResult::HistoricalFingerprintNoChange { .. } => {
                    finish_no_change(
                        db,
                        input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        vec!["historical_fingerprint".to_owned()],
                        Some("update"),
                        Some(update.candidate_key.as_str()),
                        Some(update.fingerprint.as_str()),
                        now,
                    )
                    .await
                }
                UpdateAgentSkillMutationResult::ExactParentFingerprintRequiresRollback {
                    ..
                } => {
                    finish_no_change(
                        db,
                        input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        vec!["exact_parent_requires_rollback".to_owned()],
                        Some("update"),
                        Some(update.candidate_key.as_str()),
                        Some(update.fingerprint.as_str()),
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
                        input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        vec![reason_code.to_owned()],
                        Some("update"),
                        Some(update.candidate_key.as_str()),
                        Some(update.fingerprint.as_str()),
                        now,
                    )
                    .await
                }
            }
        }
        SelfImprovementFinalOutcome::AcceptedRollback(rollback) => {
            if !source_turns_match_frozen_range(
                db,
                input.fence.workspace_id.as_str(),
                run.source_lower_exclusive,
                run.source_upper_inclusive,
                rollback.source_turn_ids.as_slice(),
            )
            .await?
            {
                return Ok(FinalizeSelfImprovementRunResult::Stale);
            }
            match apply_rollback_in_caller_transaction(
                db,
                RollbackAgentSkillMutation {
                    workspace_id: input.fence.workspace_id.clone(),
                    skill_id: rollback.skill_id.clone(),
                    expected_active_version_id: rollback.expected_active_version_id.clone(),
                    target_parent_version_id: rollback.target_parent_version_id.clone(),
                },
                now,
            )
            .await?
            {
                RollbackAgentSkillMutationResult::Applied {
                    previous_version_id,
                    resulting_version_id,
                } => {
                    let result_summary = bounded_summary(serde_json::json!({
                        "candidateKey": rollback.candidate_key,
                        "sourceTurnCount": rollback.source_turn_ids.len(),
                    }))?;
                    if !complete_run(
                        db,
                        input,
                        "applied",
                        Some("rollback"),
                        Some(rollback.skill_id.as_str()),
                        Some(previous_version_id.as_str()),
                        Some(resulting_version_id.as_str()),
                        result_summary.as_str(),
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
                        input,
                        run.source_upper_inclusive,
                        SelfImprovementNoChangeReason::HostValidationRejected,
                        vec![reason_code.to_owned()],
                        Some("rollback"),
                        Some(rollback.candidate_key.as_str()),
                        None,
                        now,
                    )
                    .await
                }
            }
        }
        SelfImprovementFinalOutcome::NoChange {
            reason,
            reason_codes,
        } => {
            finish_no_change(
                db,
                input,
                run.source_upper_inclusive,
                *reason,
                reason_codes.clone(),
                None,
                None,
                None,
                now,
            )
            .await
        }
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
        && run.reviewer_provider == input.authority.reviewer_provider
        && run.reviewer_model == input.authority.reviewer_model
        && run.pipeline_contract_version == input.authority.pipeline_contract_version
}

async fn completed_run_matches<C: ConnectionTrait>(
    db: &C,
    run: &self_improvement_run::Model,
    outcome: &SelfImprovementFinalOutcome,
) -> Result<bool> {
    match outcome {
        SelfImprovementFinalOutcome::AcceptedCreate(create) => {
            if run.outcome.as_deref() == Some("no_change") {
                return Ok(completed_attempt_matches(
                    run,
                    "create",
                    create.candidate_key.as_str(),
                    Some(create.fingerprint.as_str()),
                ));
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
            let source_turn_ids =
                serde_json::from_str::<Vec<String>>(version.source_turn_ids_json.as_str())
                    .context("failed to decode idempotent Agent skill create evidence replay")?;
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
                && source_turn_ids == create.source_turn_ids)
        }
        SelfImprovementFinalOutcome::AcceptedUpdate(update) => {
            if run.outcome.as_deref() == Some("no_change") {
                return Ok(completed_attempt_matches(
                    run,
                    "update",
                    update.candidate_key.as_str(),
                    Some(update.fingerprint.as_str()),
                ));
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
            let source_turn_ids =
                serde_json::from_str::<Vec<String>>(version.source_turn_ids_json.as_str())
                    .context("failed to decode idempotent Agent skill update evidence replay")?;
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
                && source_turn_ids == update.source_turn_ids)
        }
        SelfImprovementFinalOutcome::AcceptedRollback(rollback) => {
            if run.outcome.as_deref() == Some("no_change") {
                return Ok(completed_attempt_matches(
                    run,
                    "rollback",
                    rollback.candidate_key.as_str(),
                    None,
                ));
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
        SelfImprovementFinalOutcome::NoChange {
            reason,
            reason_codes,
        } => {
            if run.outcome.as_deref() != Some("no_change") || run.applied_action.is_some() {
                return Ok(false);
            }
            let summary = run
                .result_summary
                .as_deref()
                .and_then(|summary| serde_json::from_str::<serde_json::Value>(summary).ok());
            Ok(summary.is_some_and(|summary| {
                summary.get("reason").and_then(serde_json::Value::as_str) == Some(reason.as_str())
                    && summary
                        .get("reasonCodes")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|stored| {
                            stored
                                .iter()
                                .map(|value| value.as_str())
                                .eq(reason_codes.iter().map(|value| Some(value.as_str())))
                        })
            }))
        }
    }
}

fn completed_attempt_matches(
    run: &self_improvement_run::Model,
    attempted_action: &str,
    candidate_key: &str,
    fingerprint: Option<&str>,
) -> bool {
    if run.applied_action.is_some() {
        return false;
    }
    run.result_summary
        .as_deref()
        .and_then(|summary| serde_json::from_str::<serde_json::Value>(summary).ok())
        .is_some_and(|summary| {
            summary
                .get("attemptedAction")
                .and_then(serde_json::Value::as_str)
                == Some(attempted_action)
                && summary
                    .get("candidateKey")
                    .and_then(serde_json::Value::as_str)
                    == Some(candidate_key)
                && match fingerprint {
                    Some(fingerprint) => {
                        summary
                            .get("fingerprint")
                            .and_then(serde_json::Value::as_str)
                            == Some(fingerprint)
                    }
                    None => true,
                }
        })
}

async fn source_turns_match_frozen_range<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    source_lower_exclusive: i64,
    source_upper_inclusive: i64,
    source_turn_ids: &[String],
) -> Result<bool> {
    let ids = source_turn_ids.iter().cloned().collect::<HashSet<_>>();
    if ids.len() != source_turn_ids.len() {
        return Ok(false);
    }
    let turns = turn::Entity::find()
        .filter(turn::Column::Id.is_in(source_turn_ids.to_vec()))
        .all(db)
        .await
        .context("failed to verify cited Agent skill turns")?;
    if turns.len() != source_turn_ids.len() {
        return Ok(false);
    }
    if turns.iter().any(|turn| {
        turn_status_from_db(turn.status.as_str()) != Some(TurnStatus::Completed)
            || turn_kind_from_db(turn.turn_kind.as_str()) != Some(TurnKind::Conversation)
            || turn_origin_from_db(turn.origin.as_str()) != Some(TurnOrigin::User)
    }) {
        return Ok(false);
    }
    let thread_ids = turns
        .iter()
        .map(|turn| turn.thread_id.clone())
        .collect::<HashSet<_>>();
    let workspace_threads = thread::Entity::find()
        .filter(thread::Column::Id.is_in(thread_ids.iter().cloned().collect::<Vec<_>>()))
        .filter(thread::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(
            thread::Column::AccessClass.eq(persisted_thread_access_class_to_db(
                PersistedThreadAccessClass::Workspace,
            )),
        )
        .all(db)
        .await
        .context("failed to verify cited Agent skill workspace ownership")?;
    if workspace_threads.len() != thread_ids.len() {
        return Ok(false);
    }
    if workspace_threads.iter().any(|source_thread| {
        thread_sidebar_visibility_from_db(source_thread.sidebar_visibility.as_str())
            != Some(ThreadSidebarVisibility::Visible)
            || !thread_origin_kind_from_db(source_thread.origin_kind.as_str()).is_some_and(
                |origin| {
                    matches!(
                        origin,
                        ThreadOriginKind::Collaborative
                            | ThreadOriginKind::DirectMessage
                            | ThreadOriginKind::User
                    )
                },
            )
    }) {
        return Ok(false);
    }
    let source_rows = self_improvement_source_turn::Entity::find()
        .filter(self_improvement_source_turn::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_source_turn::Column::TurnId.is_in(source_turn_ids.to_vec()))
        .all(db)
        .await
        .context("failed to verify complete Agent skill source provenance")?;
    if source_rows.len() != source_turn_ids.len() {
        return Ok(false);
    }
    let source_thread_by_turn = source_rows
        .iter()
        .map(|source| (source.turn_id.as_str(), source.thread_id.as_str()))
        .collect::<std::collections::HashMap<_, _>>();
    if turns.iter().any(|turn| {
        source_thread_by_turn.get(turn.id.as_str()).copied() != Some(turn.thread_id.as_str())
    }) {
        return Ok(false);
    }
    let new_anchor_count = source_rows
        .iter()
        .filter(|source| source.id > source_lower_exclusive && source.id <= source_upper_inclusive)
        .count();
    // At least one cited source must come from this immutable run range. All
    // other cited context must still have a complete same-workspace ledger
    // identity and a currently workspace-visible parent.
    Ok(new_anchor_count > 0)
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
    let workspace_skills = agent_skill::Entity::find()
        .filter(agent_skill::Column::WorkspaceId.eq(input.fence.workspace_id.clone()))
        .all(db)
        .await
        .context("failed to load Agent skill identities for fingerprint check")?;
    let workspace_skill_ids = workspace_skills
        .into_iter()
        .map(|skill| skill.id)
        .collect::<Vec<_>>();
    if !workspace_skill_ids.is_empty()
        && agent_skill_version::Entity::find()
            .filter(agent_skill_version::Column::SkillId.is_in(workspace_skill_ids))
            .filter(agent_skill_version::Column::Fingerprint.eq(create.fingerprint.clone()))
            .one(db)
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

#[allow(clippy::too_many_arguments)]
async fn finish_no_change(
    db: &DatabaseTransaction,
    input: &FinalizeSelfImprovementRunInput,
    source_upper_inclusive: i64,
    reason: SelfImprovementNoChangeReason,
    reason_codes: Vec<String>,
    attempted_action: Option<&str>,
    candidate_key: Option<&str>,
    fingerprint: Option<&str>,
    now: DateTimeWithTimeZone,
) -> Result<FinalizeSelfImprovementRunResult> {
    let result_summary = bounded_summary(serde_json::json!({
        "reason": reason.as_str(),
        "reasonCodes": reason_codes,
        "attemptedAction": attempted_action,
        "candidateKey": candidate_key,
        "fingerprint": fingerprint,
    }))?;
    if !complete_run(
        db,
        input,
        "no_change",
        None,
        None,
        None,
        None,
        result_summary.as_str(),
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
