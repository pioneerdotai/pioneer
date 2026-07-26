use anyhow::{Context, Result, bail};
use pioneer_entity::{self_improvement_run, self_improvement_workspace_state};
use sea_orm::ExprTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict, Query, SelectStatement};
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::{
    NewSelfImprovementRun, SelfImprovementFinalizationAuthority, SelfImprovementRunFence,
    SelfImprovementRunMutationResult, SelfImprovementRunRecord,
};

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_RUNNING: &str = "running";
pub const STATUS_COMPLETED: &str = "completed";
pub const STATUS_FAILED: &str = "failed";
pub const STATUS_CANCELLED: &str = "cancelled";

pub const ANALYSIS_CURSOR_MAX_BYTES: usize = 64 * 1024;
pub const ANALYSIS_DIGEST_MAX_BYTES: usize = 1024 * 1024;
pub const LAST_ERROR_MAX_BYTES: usize = 1_024;

pub async fn find_by_id<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    run_id: &str,
) -> Result<Option<self_improvement_run::Model>> {
    self_improvement_run::Entity::find_by_id(run_id.to_owned())
        .filter(self_improvement_run::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to load self-improvement run `{run_id}` for workspace `{workspace_id}`")
        })
}

pub async fn find_daily<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    activation_epoch: i64,
    scheduled_date_utc: &str,
) -> Result<Option<self_improvement_run::Model>> {
    self_improvement_run::Entity::find()
        .filter(self_improvement_run::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_run::Column::ActivationEpoch.eq(activation_epoch))
        .filter(self_improvement_run::Column::ScheduledDateUtc.eq(scheduled_date_utc.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load daily self-improvement run for workspace `{workspace_id}` epoch \
                 `{activation_epoch}` date `{scheduled_date_utc}`"
            )
        })
}

pub async fn find_oldest_unresolved<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    activation_epoch: i64,
) -> Result<Option<self_improvement_run::Model>> {
    self_improvement_run::Entity::find()
        .filter(self_improvement_run::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_run::Column::ActivationEpoch.eq(activation_epoch))
        .filter(
            self_improvement_run::Column::Status
                .is_not_in([STATUS_COMPLETED.to_owned(), STATUS_CANCELLED.to_owned()]),
        )
        .order_by_asc(self_improvement_run::Column::CreatedAt)
        .order_by_asc(self_improvement_run::Column::Id)
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to load oldest unresolved self-improvement run for workspace \
                 `{workspace_id}` epoch `{activation_epoch}`"
            )
        })
}

pub async fn find_earliest_retry_at<C: ConnectionTrait>(
    db: &C,
) -> Result<Option<DateTimeWithTimeZone>> {
    let pending_retry = self_improvement_run::Entity::find()
        .filter(self_improvement_run::Column::Status.eq(STATUS_PENDING))
        .filter(self_improvement_run::Column::NextAttemptAt.is_not_null())
        .order_by_asc(self_improvement_run::Column::NextAttemptAt)
        .order_by_asc(self_improvement_run::Column::Id)
        .one(db)
        .await
        .context("failed to load earliest pending self-improvement retry")?
        .and_then(|run| run.next_attempt_at);
    let running_reclaim = self_improvement_run::Entity::find()
        .filter(self_improvement_run::Column::Status.eq(STATUS_RUNNING))
        .filter(self_improvement_run::Column::LeaseExpiresAt.is_not_null())
        .order_by_asc(self_improvement_run::Column::LeaseExpiresAt)
        .order_by_asc(self_improvement_run::Column::Id)
        .one(db)
        .await
        .context("failed to load earliest self-improvement lease expiry")?
        .and_then(|run| run.lease_expires_at);

    Ok([pending_retry, running_reclaim].into_iter().flatten().min())
}

pub async fn insert_pending<C: ConnectionTrait>(
    db: &C,
    run_id: &str,
    input: &NewSelfImprovementRun,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    validate_new_run(input)?;

    self_improvement_run::Entity::insert(self_improvement_run::ActiveModel {
        id: Set(run_id.to_owned()),
        workspace_id: Set(input.workspace_id.clone()),
        activation_epoch: Set(input.activation_epoch),
        scheduled_date_utc: Set(input.scheduled_date_utc.clone()),
        source_lower_exclusive: Set(input.source_lower_exclusive),
        source_upper_inclusive: Set(input.source_upper_inclusive),
        status: Set(STATUS_PENDING.to_owned()),
        claim_token: Set(None),
        claimed_by: Set(None),
        lease_expires_at: Set(None),
        attempt_count: Set(0),
        next_attempt_at: Set(Some(now)),
        learner_provider: Set(input.learner_provider.clone()),
        learner_model: Set(input.learner_model.clone()),
        reviewer_provider: Set(input.reviewer_provider.clone()),
        reviewer_model: Set(input.reviewer_model.clone()),
        pipeline_contract_version: Set(input.pipeline_contract_version.clone()),
        analysis_cursor_json: Set(None),
        analysis_digest_json: Set(None),
        outcome: Set(None),
        applied_action: Set(None),
        skill_id: Set(None),
        previous_version_id: Set(None),
        resulting_version_id: Set(None),
        result_summary: Set(None),
        last_error: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            self_improvement_run::Column::WorkspaceId,
            self_improvement_run::Column::ActivationEpoch,
            self_improvement_run::Column::ScheduledDateUtc,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert daily self-improvement run for workspace `{}` epoch `{}` date `{}`",
            input.workspace_id, input.activation_epoch, input.scheduled_date_utc
        )
    })?;
    Ok(())
}

pub async fn claim_available<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    run_id: &str,
    activation_epoch: i64,
    claim_token: &str,
    claimed_by: &str,
    now: DateTimeWithTimeZone,
    lease_expires_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let available = Condition::any()
        .add(
            Condition::all()
                .add(self_improvement_run::Column::Status.eq(STATUS_PENDING))
                .add(self_improvement_run::Column::ClaimToken.is_null())
                .add(self_improvement_run::Column::ClaimedBy.is_null())
                .add(self_improvement_run::Column::LeaseExpiresAt.is_null())
                .add(self_improvement_run::Column::NextAttemptAt.lte(now)),
        )
        .add(
            Condition::all()
                .add(self_improvement_run::Column::Status.eq(STATUS_RUNNING))
                .add(self_improvement_run::Column::LeaseExpiresAt.lte(now)),
        );
    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(STATUS_RUNNING.to_owned()),
        )
        .col_expr(
            self_improvement_run::Column::ClaimToken,
            Expr::value(Some(claim_token.to_owned())),
        )
        .col_expr(
            self_improvement_run::Column::ClaimedBy,
            Expr::value(Some(claimed_by.to_owned())),
        )
        .col_expr(
            self_improvement_run::Column::LeaseExpiresAt,
            Expr::value(Some(lease_expires_at)),
        )
        .col_expr(
            self_improvement_run::Column::AttemptCount,
            Expr::col(self_improvement_run::Column::AttemptCount).add(1),
        )
        .col_expr(
            self_improvement_run::Column::NextAttemptAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            self_improvement_run::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(self_improvement_run::Column::Id.eq(run_id.to_owned()))
        .filter(self_improvement_run::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_run::Column::ActivationEpoch.eq(activation_epoch))
        .filter(available)
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to claim self-improvement run `{run_id}` for workspace `{workspace_id}`"
            )
        })?
        .rows_affected
        == 1;
    Ok(affected)
}

pub async fn requeue_failed<C: ConnectionTrait>(
    db: &C,
    run: &SelfImprovementRunRecord,
    now: DateTimeWithTimeZone,
) -> Result<SelfImprovementRunMutationResult> {
    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(STATUS_PENDING.to_owned()),
        )
        .col_expr(
            self_improvement_run::Column::NextAttemptAt,
            Expr::value(Some(now)),
        )
        .col_expr(self_improvement_run::Column::AttemptCount, Expr::value(0))
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(self_improvement_run::Column::Id.eq(run.id.clone()))
        .filter(self_improvement_run::Column::WorkspaceId.eq(run.workspace_id.clone()))
        .filter(
            self_improvement_run::Column::WorkspaceId.in_subquery(active_workspace_subquery(
                run.workspace_id.as_str(),
                run.activation_epoch,
                run.source_lower_exclusive,
            )),
        )
        .filter(self_improvement_run::Column::ActivationEpoch.eq(run.activation_epoch))
        .filter(self_improvement_run::Column::SourceLowerExclusive.eq(run.source_lower_exclusive))
        .filter(self_improvement_run::Column::SourceUpperInclusive.eq(run.source_upper_inclusive))
        .filter(self_improvement_run::Column::Status.eq(STATUS_FAILED))
        .filter(self_improvement_run::Column::ClaimToken.is_null())
        .filter(self_improvement_run::Column::ClaimedBy.is_null())
        .filter(self_improvement_run::Column::LeaseExpiresAt.is_null())
        .filter(self_improvement_run::Column::LearnerProvider.eq(run.learner_provider.clone()))
        .filter(self_improvement_run::Column::LearnerModel.eq(run.learner_model.clone()))
        .filter(self_improvement_run::Column::ReviewerProvider.eq(run.reviewer_provider.clone()))
        .filter(self_improvement_run::Column::ReviewerModel.eq(run.reviewer_model.clone()))
        .filter(
            self_improvement_run::Column::PipelineContractVersion
                .eq(run.pipeline_contract_version.clone()),
        )
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to requeue self-improvement run `{}` for next daily wake",
                run.id
            )
        })?
        .rows_affected;
    Ok(if affected == 1 {
        SelfImprovementRunMutationResult::Applied
    } else {
        SelfImprovementRunMutationResult::LostAuthority
    })
}

pub async fn reset_unfinished_authority<C: ConnectionTrait>(
    db: &C,
    run: &SelfImprovementRunRecord,
    authority: &SelfImprovementFinalizationAuthority,
    now: DateTimeWithTimeZone,
) -> Result<SelfImprovementRunMutationResult> {
    validate_reset_authority(authority)?;
    if !matches!(
        run.status.as_str(),
        STATUS_PENDING | STATUS_RUNNING | STATUS_FAILED
    ) {
        return Ok(SelfImprovementRunMutationResult::LostAuthority);
    }
    if run.learner_provider == authority.learner_provider
        && run.learner_model == authority.learner_model
        && run.reviewer_provider == authority.reviewer_provider
        && run.reviewer_model == authority.reviewer_model
        && run.pipeline_contract_version == authority.pipeline_contract_version
    {
        bail!(
            "self-improvement run `{}` authority reset requires a model or contract mismatch",
            run.id
        );
    }

    let mut expected = Condition::all()
        .add(self_improvement_run::Column::Id.eq(run.id.clone()))
        .add(self_improvement_run::Column::WorkspaceId.eq(run.workspace_id.clone()))
        .add(
            self_improvement_run::Column::WorkspaceId.in_subquery(active_workspace_subquery(
                run.workspace_id.as_str(),
                run.activation_epoch,
                run.source_lower_exclusive,
            )),
        )
        .add(self_improvement_run::Column::ActivationEpoch.eq(run.activation_epoch))
        .add(self_improvement_run::Column::SourceLowerExclusive.eq(run.source_lower_exclusive))
        .add(self_improvement_run::Column::SourceUpperInclusive.eq(run.source_upper_inclusive))
        .add(self_improvement_run::Column::Status.eq(run.status.clone()))
        .add(self_improvement_run::Column::LearnerProvider.eq(run.learner_provider.clone()))
        .add(self_improvement_run::Column::LearnerModel.eq(run.learner_model.clone()))
        .add(self_improvement_run::Column::ReviewerProvider.eq(run.reviewer_provider.clone()))
        .add(self_improvement_run::Column::ReviewerModel.eq(run.reviewer_model.clone()))
        .add(
            self_improvement_run::Column::PipelineContractVersion
                .eq(run.pipeline_contract_version.clone()),
        );
    expected = match run.claim_token.as_ref() {
        Some(claim_token) => {
            expected.add(self_improvement_run::Column::ClaimToken.eq(claim_token.clone()))
        }
        None => expected.add(self_improvement_run::Column::ClaimToken.is_null()),
    };
    expected = match run.claimed_by.as_ref() {
        Some(claimed_by) => {
            expected.add(self_improvement_run::Column::ClaimedBy.eq(claimed_by.clone()))
        }
        None => expected.add(self_improvement_run::Column::ClaimedBy.is_null()),
    };

    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(STATUS_PENDING.to_owned()),
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
        .col_expr(self_improvement_run::Column::AttemptCount, Expr::value(0))
        .col_expr(
            self_improvement_run::Column::NextAttemptAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            self_improvement_run::Column::LearnerProvider,
            Expr::value(authority.learner_provider.clone()),
        )
        .col_expr(
            self_improvement_run::Column::LearnerModel,
            Expr::value(authority.learner_model.clone()),
        )
        .col_expr(
            self_improvement_run::Column::ReviewerProvider,
            Expr::value(authority.reviewer_provider.clone()),
        )
        .col_expr(
            self_improvement_run::Column::ReviewerModel,
            Expr::value(authority.reviewer_model.clone()),
        )
        .col_expr(
            self_improvement_run::Column::PipelineContractVersion,
            Expr::value(authority.pipeline_contract_version.clone()),
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
            self_improvement_run::Column::Outcome,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::AppliedAction,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::SkillId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::PreviousVersionId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::ResultingVersionId,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::ResultSummary,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(expected)
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to reset self-improvement run `{}` for model or contract change",
                run.id
            )
        })?
        .rows_affected;
    Ok(if affected == 1 {
        SelfImprovementRunMutationResult::Applied
    } else {
        SelfImprovementRunMutationResult::LostAuthority
    })
}

pub async fn workspace_matches_fence<C: ConnectionTrait>(
    db: &C,
    fence: &SelfImprovementRunFence,
) -> Result<bool> {
    self_improvement_workspace_state::Entity::find_by_id(fence.workspace_id.clone())
        .filter(
            self_improvement_workspace_state::Column::ActivationEpoch.eq(fence.activation_epoch),
        )
        .filter(
            self_improvement_workspace_state::Column::CursorSourceId
                .eq(fence.source_lower_exclusive),
        )
        .filter(self_improvement_workspace_state::Column::EffectiveEnabledAt.is_not_null())
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify self-improvement workspace fence for run `{}`",
                fence.run_id
            )
        })
        .map(|state| state.is_some())
}

pub fn fence_condition(fence: &SelfImprovementRunFence, now: DateTimeWithTimeZone) -> Condition {
    Condition::all()
        .add(self_improvement_run::Column::Id.eq(fence.run_id.clone()))
        .add(self_improvement_run::Column::WorkspaceId.eq(fence.workspace_id.clone()))
        .add(
            self_improvement_run::Column::WorkspaceId.in_subquery(active_workspace_subquery(
                fence.workspace_id.as_str(),
                fence.activation_epoch,
                fence.source_lower_exclusive,
            )),
        )
        .add(self_improvement_run::Column::ActivationEpoch.eq(fence.activation_epoch))
        .add(self_improvement_run::Column::SourceLowerExclusive.eq(fence.source_lower_exclusive))
        .add(self_improvement_run::Column::SourceUpperInclusive.eq(fence.source_upper_inclusive))
        .add(self_improvement_run::Column::Status.eq(STATUS_RUNNING))
        .add(self_improvement_run::Column::ClaimToken.eq(fence.claim_token.clone()))
        .add(self_improvement_run::Column::ClaimedBy.eq(fence.claimed_by.clone()))
        .add(self_improvement_run::Column::LearnerProvider.eq(fence.learner_provider.clone()))
        .add(self_improvement_run::Column::LearnerModel.eq(fence.learner_model.clone()))
        .add(self_improvement_run::Column::ReviewerProvider.eq(fence.reviewer_provider.clone()))
        .add(self_improvement_run::Column::ReviewerModel.eq(fence.reviewer_model.clone()))
        .add(
            self_improvement_run::Column::PipelineContractVersion
                .eq(fence.pipeline_contract_version.clone()),
        )
        .add(self_improvement_run::Column::LeaseExpiresAt.gt(now))
}

fn active_workspace_subquery(
    workspace_id: &str,
    activation_epoch: i64,
    cursor_source_id: i64,
) -> SelectStatement {
    Query::select()
        .column(self_improvement_workspace_state::Column::WorkspaceId)
        .from(self_improvement_workspace_state::Entity)
        .and_where(
            self_improvement_workspace_state::Column::WorkspaceId.eq(workspace_id.to_owned()),
        )
        .and_where(self_improvement_workspace_state::Column::ActivationEpoch.eq(activation_epoch))
        .and_where(self_improvement_workspace_state::Column::CursorSourceId.eq(cursor_source_id))
        .and_where(self_improvement_workspace_state::Column::EffectiveEnabledAt.is_not_null())
        .to_owned()
}

pub fn model_matches_fence(
    run: &self_improvement_run::Model,
    fence: &SelfImprovementRunFence,
    now: DateTimeWithTimeZone,
) -> bool {
    run.id == fence.run_id
        && run.workspace_id == fence.workspace_id
        && run.activation_epoch == fence.activation_epoch
        && run.source_lower_exclusive == fence.source_lower_exclusive
        && run.source_upper_inclusive == fence.source_upper_inclusive
        && run.status == STATUS_RUNNING
        && run.claim_token.as_deref() == Some(fence.claim_token.as_str())
        && run.claimed_by.as_deref() == Some(fence.claimed_by.as_str())
        && run
            .lease_expires_at
            .as_ref()
            .is_some_and(|expires| expires > &now)
        && run.learner_provider == fence.learner_provider
        && run.learner_model == fence.learner_model
        && run.reviewer_provider == fence.reviewer_provider
        && run.reviewer_model == fence.reviewer_model
        && run.pipeline_contract_version == fence.pipeline_contract_version
}

pub async fn heartbeat<C: ConnectionTrait>(
    db: &C,
    fence: &SelfImprovementRunFence,
    now: DateTimeWithTimeZone,
    lease_expires_at: DateTimeWithTimeZone,
) -> Result<SelfImprovementRunMutationResult> {
    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::LeaseExpiresAt,
            Expr::value(Some(lease_expires_at)),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(fence_condition(fence, now))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to heartbeat self-improvement run `{}`",
                fence.run_id
            )
        })?
        .rows_affected;
    Ok(if affected == 1 {
        SelfImprovementRunMutationResult::Applied
    } else {
        SelfImprovementRunMutationResult::LostAuthority
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn transition_claimed<C: ConnectionTrait>(
    db: &C,
    fence: &SelfImprovementRunFence,
    status: &str,
    next_attempt_at: Option<DateTimeWithTimeZone>,
    last_error: &str,
    now: DateTimeWithTimeZone,
) -> Result<SelfImprovementRunMutationResult> {
    if status != STATUS_PENDING && status != STATUS_FAILED && status != STATUS_CANCELLED {
        bail!("invalid claimed self-improvement run transition target `{status}`");
    }
    let last_error = validate_persisted_error(last_error)?;
    if status == STATUS_PENDING && next_attempt_at.is_none() {
        bail!("pending self-improvement run transition requires next_attempt_at");
    }
    if status != STATUS_PENDING && next_attempt_at.is_some() {
        bail!("terminal self-improvement run transition cannot have next_attempt_at");
    }
    let mut update = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(status.to_owned()),
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
            Expr::value(next_attempt_at),
        )
        .col_expr(
            self_improvement_run::Column::LastError,
            Expr::value(Some(last_error.to_owned())),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now));
    if status == STATUS_CANCELLED {
        update = update
            .col_expr(
                self_improvement_run::Column::AnalysisCursorJson,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                self_improvement_run::Column::AnalysisDigestJson,
                Expr::value(Option::<String>::None),
            );
    }
    let affected = update
        .filter(fence_condition(fence, now))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to transition claimed self-improvement run `{}` to `{status}`",
                fence.run_id
            )
        })?
        .rows_affected;
    Ok(if affected == 1 {
        SelfImprovementRunMutationResult::Applied
    } else {
        SelfImprovementRunMutationResult::LostAuthority
    })
}

pub async fn yield_claimed<C: ConnectionTrait>(
    db: &C,
    fence: &SelfImprovementRunFence,
    next_attempt_at: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<SelfImprovementRunMutationResult> {
    if next_attempt_at <= now {
        bail!("self-improvement budget yield must schedule a future wake");
    }
    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(STATUS_PENDING.to_owned()),
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
            Expr::value(Some(next_attempt_at)),
        )
        .col_expr(self_improvement_run::Column::AttemptCount, Expr::value(0))
        .col_expr(
            self_improvement_run::Column::LastError,
            Expr::value(Option::<String>::None),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(fence_condition(fence, now))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to yield self-improvement run `{}` after wake budget",
                fence.run_id
            )
        })?
        .rows_affected;
    Ok(if affected == 1 {
        SelfImprovementRunMutationResult::Applied
    } else {
        SelfImprovementRunMutationResult::LostAuthority
    })
}

pub async fn cancel_unfinished_for_workspace<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    reason: &str,
    now: DateTimeWithTimeZone,
) -> Result<u64> {
    let reason = validate_persisted_error(reason)?;
    let disabled = self_improvement_workspace_state::Entity::find_by_id(workspace_id.to_owned())
        .filter(self_improvement_workspace_state::Column::EffectiveEnabledAt.is_null())
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to verify disabled self-improvement workspace `{workspace_id}` before \
                 invalidation"
            )
        })?
        .is_some();
    if !disabled {
        bail!(
            "unfinished self-improvement runs can only be invalidated after workspace \
             `{workspace_id}` is disabled"
        );
    }

    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::Status,
            Expr::value(STATUS_CANCELLED.to_owned()),
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
        .col_expr(
            self_improvement_run::Column::LastError,
            Expr::value(Some(reason.to_owned())),
        )
        .col_expr(
            self_improvement_run::Column::AnalysisCursorJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            self_improvement_run::Column::AnalysisDigestJson,
            Expr::value(Option::<String>::None),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(self_improvement_run::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .filter(self_improvement_run::Column::Status.is_in([
            STATUS_PENDING.to_owned(),
            STATUS_RUNNING.to_owned(),
            STATUS_FAILED.to_owned(),
        ]))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to cancel unfinished self-improvement runs for workspace `{workspace_id}`"
            )
        })?
        .rows_affected;

    Ok(affected)
}

fn validate_reset_authority(authority: &SelfImprovementFinalizationAuthority) -> Result<()> {
    if !authority.effective_enabled {
        bail!("self-improvement run authority reset requires effective enabled Settings");
    }
    for (name, value) in [
        ("learner_provider", authority.learner_provider.as_str()),
        ("learner_model", authority.learner_model.as_str()),
        ("reviewer_provider", authority.reviewer_provider.as_str()),
        ("reviewer_model", authority.reviewer_model.as_str()),
        (
            "pipeline_contract_version",
            authority.pipeline_contract_version.as_str(),
        ),
    ] {
        if value.trim().is_empty() || value != value.trim() {
            bail!("self-improvement run reset authority {name} is invalid");
        }
    }
    Ok(())
}

fn validate_persisted_error(value: &str) -> Result<&str> {
    let value = value.trim();
    if value.is_empty() {
        bail!("self-improvement persisted error must not be empty");
    }
    if value.len() > LAST_ERROR_MAX_BYTES {
        bail!(
            "self-improvement persisted error exceeds its {}-byte limit",
            LAST_ERROR_MAX_BYTES
        );
    }
    if value.chars().any(char::is_control) {
        bail!("self-improvement persisted error must be one safe line");
    }
    Ok(value)
}

pub async fn save_checkpoint<C: ConnectionTrait>(
    db: &C,
    fence: &SelfImprovementRunFence,
    analysis_cursor_json: &str,
    analysis_digest_json: &str,
    now: DateTimeWithTimeZone,
) -> Result<SelfImprovementRunMutationResult> {
    validate_bounded_json(
        "analysis_cursor_json",
        analysis_cursor_json,
        ANALYSIS_CURSOR_MAX_BYTES,
    )?;
    validate_bounded_json(
        "analysis_digest_json",
        analysis_digest_json,
        ANALYSIS_DIGEST_MAX_BYTES,
    )?;

    let affected = self_improvement_run::Entity::update_many()
        .col_expr(
            self_improvement_run::Column::AnalysisCursorJson,
            Expr::value(Some(analysis_cursor_json.to_owned())),
        )
        .col_expr(
            self_improvement_run::Column::AnalysisDigestJson,
            Expr::value(Some(analysis_digest_json.to_owned())),
        )
        .col_expr(self_improvement_run::Column::UpdatedAt, Expr::value(now))
        .filter(fence_condition(fence, now))
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to save self-improvement checkpoint for run `{}`",
                fence.run_id
            )
        })?
        .rows_affected;

    Ok(if affected == 1 {
        SelfImprovementRunMutationResult::Applied
    } else {
        SelfImprovementRunMutationResult::LostAuthority
    })
}

pub fn validate_new_run(input: &NewSelfImprovementRun) -> Result<()> {
    if input.workspace_id.trim().is_empty() {
        bail!("self-improvement run workspace_id must not be empty");
    }
    if input.activation_epoch <= 0 {
        bail!("self-improvement run activation_epoch must be positive");
    }
    if input.source_lower_exclusive < 0
        || input.source_upper_inclusive <= input.source_lower_exclusive
    {
        bail!("self-improvement run source range must be non-empty and monotonic");
    }
    if !valid_utc_date(input.scheduled_date_utc.as_str()) {
        bail!("self-improvement scheduled_date_utc must use YYYY-MM-DD");
    }
    for (name, value) in [
        ("learner_provider", input.learner_provider.as_str()),
        ("learner_model", input.learner_model.as_str()),
        ("reviewer_provider", input.reviewer_provider.as_str()),
        ("reviewer_model", input.reviewer_model.as_str()),
        (
            "pipeline_contract_version",
            input.pipeline_contract_version.as_str(),
        ),
    ] {
        if value.trim().is_empty() || value != value.trim() {
            bail!("self-improvement run {name} must be non-empty and normalized");
        }
    }
    Ok(())
}

fn valid_utc_date(value: &str) -> bool {
    value.len() == 10 && chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok()
}

fn validate_bounded_json(name: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.len() > max_bytes {
        bail!("{name} exceeds its {max_bytes}-byte persistence limit");
    }
    serde_json::from_str::<serde_json::Value>(value)
        .with_context(|| format!("{name} must contain valid JSON"))?;
    Ok(())
}

pub fn record_from_model(model: self_improvement_run::Model) -> SelfImprovementRunRecord {
    SelfImprovementRunRecord {
        id: model.id,
        workspace_id: model.workspace_id,
        activation_epoch: model.activation_epoch,
        scheduled_date_utc: model.scheduled_date_utc,
        source_lower_exclusive: model.source_lower_exclusive,
        source_upper_inclusive: model.source_upper_inclusive,
        status: model.status,
        claim_token: model.claim_token,
        claimed_by: model.claimed_by,
        lease_expires_at_unix: model.lease_expires_at.map(|value| value.timestamp()),
        attempt_count: model.attempt_count,
        next_attempt_at_unix: model.next_attempt_at.map(|value| value.timestamp()),
        learner_provider: model.learner_provider,
        learner_model: model.learner_model,
        reviewer_provider: model.reviewer_provider,
        reviewer_model: model.reviewer_model,
        pipeline_contract_version: model.pipeline_contract_version,
        analysis_cursor_json: model.analysis_cursor_json,
        analysis_digest_json: model.analysis_digest_json,
        outcome: model.outcome,
        applied_action: model.applied_action,
        skill_id: model.skill_id,
        previous_version_id: model.previous_version_id,
        resulting_version_id: model.resulting_version_id,
        result_summary: model.result_summary,
        last_error: model.last_error,
        created_at_unix: model.created_at.timestamp(),
        updated_at_unix: model.updated_at.timestamp(),
    }
}

#[cfg(test)]
mod tests {
    use super::{LAST_ERROR_MAX_BYTES, validate_persisted_error};

    #[test]
    fn persisted_error_is_bounded_and_single_line() {
        assert_eq!(
            validate_persisted_error(" provider_transport:chunk_failed ")
                .expect("safe error code must normalize"),
            "provider_transport:chunk_failed"
        );
        assert!(validate_persisted_error("").is_err());
        assert!(validate_persisted_error("provider\nsecret").is_err());
        assert!(validate_persisted_error(&"x".repeat(LAST_ERROR_MAX_BYTES + 1)).is_err());
    }
}
