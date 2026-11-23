//! Native CLI attempt and execution segment persistence.
use anyhow::{Context, Result, anyhow};
use pioneer_entity::{turn_cli_runtime_attempt, turn_cli_runtime_execution_segment};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimeTurnAttemptStatus {
    Starting,
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl CliRuntimeTurnAttemptStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Starting | Self::Running)
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "starting" => Ok(Self::Starting),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(anyhow!("unknown CLI runtime turn attempt status `{other}`")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeTurnAttemptRecord {
    pub id: String,
    pub turn_id: String,
    pub attempt_index: u32,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_turn_id: Option<String>,
    pub recovery_job_id: Option<String>,
    pub recovery_attempt_id: Option<String>,
    pub recovery_confirmed_at: Option<DateTimeWithTimeZone>,
    pub execution_window_index: Option<u32>,
    pub status: CliRuntimeTurnAttemptStatus,
    pub failure_reason: Option<String>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeTurnAttempt {
    pub id: String,
    pub turn_id: String,
    pub attempt_index: u32,
    pub runtime_id: String,
    pub runtime_kind: String,
    pub native_thread_id: String,
    pub native_turn_id: Option<String>,
    pub recovery_job_id: Option<String>,
    pub recovery_attempt_id: Option<String>,
    pub execution_window_index: Option<u32>,
    pub status: CliRuntimeTurnAttemptStatus,
    pub failure_reason: Option<String>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliRuntimeExecutionSegmentStatus {
    Running,
    Completed,
    Failed,
    Interrupted,
}

impl CliRuntimeExecutionSegmentStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }

    fn from_db(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(anyhow!(
                "unknown CLI runtime execution segment status `{other}`"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliRuntimeExecutionSegmentRecord {
    pub id: String,
    pub attempt_id: String,
    pub turn_id: String,
    pub segment_index: u32,
    pub runtime_id: String,
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub status: CliRuntimeExecutionSegmentStatus,
    pub failure_reason: Option<String>,
    pub started_at: DateTimeWithTimeZone,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCliRuntimeExecutionSegment {
    pub id: String,
    pub attempt_id: String,
    pub turn_id: String,
    pub segment_index: u32,
    pub runtime_id: String,
    pub native_thread_id: String,
    pub native_turn_id: String,
    pub status: CliRuntimeExecutionSegmentStatus,
    pub failure_reason: Option<String>,
    pub started_at: DateTimeWithTimeZone,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

pub async fn create_turn_attempt<C: ConnectionTrait>(
    db: &C,
    attempt: NewCliRuntimeTurnAttempt,
) -> Result<CliRuntimeTurnAttemptRecord> {
    validate_new_turn_attempt(&attempt)?;
    let id = attempt.id.clone();
    turn_cli_runtime_attempt::Entity::insert(active_turn_attempt_from_new(attempt))
        .exec(db)
        .await
        .context("failed to insert CLI runtime turn attempt")?;

    find_turn_attempt(db, id.as_str())
        .await?
        .context("inserted CLI runtime turn attempt is missing")
}

pub async fn find_turn_attempt<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime turn attempt")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn find_turn_attempt_by_native_turn<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find()
        .filter(turn_cli_runtime_attempt::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(turn_cli_runtime_attempt::Column::NativeTurnId.eq(native_turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query CLI runtime turn attempt by native turn")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn find_turn_attempt_by_recovery_attempt<C: ConnectionTrait>(
    db: &C,
    recovery_attempt_id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find()
        .filter(
            turn_cli_runtime_attempt::Column::RecoveryAttemptId.eq(recovery_attempt_id.to_owned()),
        )
        .one(db)
        .await
        .context("failed to query CLI runtime turn attempt by recovery attempt")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn latest_turn_attempt<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
    turn_cli_runtime_attempt::Entity::find()
        .filter(turn_cli_runtime_attempt::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_cli_runtime_attempt::Column::AttemptIndex)
        .one(db)
        .await
        .context("failed to query latest CLI runtime turn attempt")?
        .map(turn_attempt_record_from_model)
        .transpose()
}

pub async fn mark_turn_attempt_running<C: ConnectionTrait>(
    db: &C,
    id: &str,
    native_turn_id: &str,
    started_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = turn_cli_runtime_attempt::Entity::update_many()
        .col_expr(
            turn_cli_runtime_attempt::Column::NativeTurnId,
            sea_orm::sea_query::Expr::value(Some(native_turn_id.to_owned())),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::Status,
            sea_orm::sea_query::Expr::value(CliRuntimeTurnAttemptStatus::Running.as_str()),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::StartedAt,
            sea_orm::sea_query::Expr::value(Some(started_at)),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(started_at),
        )
        .filter(turn_cli_runtime_attempt::Column::Id.eq(id.to_owned()))
        .filter(
            turn_cli_runtime_attempt::Column::Status
                .eq(CliRuntimeTurnAttemptStatus::Starting.as_str()),
        )
        .exec(db)
        .await
        .context("failed to mark CLI runtime turn attempt running")?;
    Ok(result.rows_affected == 1)
}

pub async fn mark_turn_attempt_terminal<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: CliRuntimeTurnAttemptStatus,
    failure_reason: Option<String>,
    completed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if status.is_active() {
        return Err(anyhow!(
            "CLI runtime terminal attempt status cannot be `{}`",
            status.as_str()
        ));
    }
    let result = turn_cli_runtime_attempt::Entity::update_many()
        .col_expr(
            turn_cli_runtime_attempt::Column::Status,
            sea_orm::sea_query::Expr::value(status.as_str()),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::FailureReason,
            sea_orm::sea_query::Expr::value(failure_reason.clone()),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::CompletedAt,
            sea_orm::sea_query::Expr::value(Some(completed_at)),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(completed_at),
        )
        .filter(turn_cli_runtime_attempt::Column::Id.eq(id.to_owned()))
        .filter(turn_cli_runtime_attempt::Column::Status.is_in([
            CliRuntimeTurnAttemptStatus::Starting.as_str(),
            CliRuntimeTurnAttemptStatus::Running.as_str(),
        ]))
        .exec(db)
        .await
        .context("failed to mark CLI runtime turn attempt terminal")?;
    if result.rows_affected == 1 {
        let segment_status = match status {
            CliRuntimeTurnAttemptStatus::Completed => CliRuntimeExecutionSegmentStatus::Completed,
            CliRuntimeTurnAttemptStatus::Failed => CliRuntimeExecutionSegmentStatus::Failed,
            CliRuntimeTurnAttemptStatus::Interrupted => {
                CliRuntimeExecutionSegmentStatus::Interrupted
            }
            CliRuntimeTurnAttemptStatus::Starting | CliRuntimeTurnAttemptStatus::Running => {
                unreachable!("active attempt status was rejected above")
            }
        };
        turn_cli_runtime_execution_segment::Entity::update_many()
            .col_expr(
                turn_cli_runtime_execution_segment::Column::Status,
                sea_orm::sea_query::Expr::value(segment_status.as_str()),
            )
            .col_expr(
                turn_cli_runtime_execution_segment::Column::FailureReason,
                sea_orm::sea_query::Expr::value(failure_reason),
            )
            .col_expr(
                turn_cli_runtime_execution_segment::Column::CompletedAt,
                sea_orm::sea_query::Expr::value(Some(completed_at)),
            )
            .col_expr(
                turn_cli_runtime_execution_segment::Column::UpdatedAt,
                sea_orm::sea_query::Expr::value(completed_at),
            )
            .filter(turn_cli_runtime_execution_segment::Column::AttemptId.eq(id.to_owned()))
            .filter(
                turn_cli_runtime_execution_segment::Column::Status
                    .eq(CliRuntimeExecutionSegmentStatus::Running.as_str()),
            )
            .exec(db)
            .await
            .context("failed to terminalize active CLI runtime execution segments")?;
    }
    Ok(result.rows_affected == 1)
}

pub async fn mark_turn_attempt_recovery_confirmed<C: ConnectionTrait>(
    db: &C,
    id: &str,
    recovery_attempt_id: &str,
    confirmed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = turn_cli_runtime_attempt::Entity::update_many()
        .col_expr(
            turn_cli_runtime_attempt::Column::RecoveryConfirmedAt,
            sea_orm::sea_query::Expr::value(Some(confirmed_at)),
        )
        .col_expr(
            turn_cli_runtime_attempt::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(confirmed_at),
        )
        .filter(turn_cli_runtime_attempt::Column::Id.eq(id.to_owned()))
        .filter(
            turn_cli_runtime_attempt::Column::RecoveryAttemptId.eq(recovery_attempt_id.to_owned()),
        )
        .filter(turn_cli_runtime_attempt::Column::RecoveryConfirmedAt.is_null())
        .exec(db)
        .await
        .context("failed to mark CLI runtime recovery confirmed")?;
    Ok(result.rows_affected == 1)
}

pub async fn create_execution_segment<C: ConnectionTrait>(
    db: &C,
    segment: NewCliRuntimeExecutionSegment,
) -> Result<CliRuntimeExecutionSegmentRecord> {
    validate_new_execution_segment(&segment)?;
    let id = segment.id.clone();
    turn_cli_runtime_execution_segment::Entity::insert(active_execution_segment_from_new(segment))
        .exec(db)
        .await
        .context("failed to insert CLI runtime execution segment")?;
    find_execution_segment(db, id.as_str())
        .await?
        .context("inserted CLI runtime execution segment is missing")
}

pub async fn find_execution_segment<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find_by_id(id.to_owned())
        .one(db)
        .await
        .context("failed to query CLI runtime execution segment")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn find_execution_segment_by_native_turn<C: ConnectionTrait>(
    db: &C,
    runtime_id: &str,
    native_turn_id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find()
        .filter(turn_cli_runtime_execution_segment::Column::RuntimeId.eq(runtime_id.to_owned()))
        .filter(
            turn_cli_runtime_execution_segment::Column::NativeTurnId.eq(native_turn_id.to_owned()),
        )
        .one(db)
        .await
        .context("failed to query CLI runtime execution segment by native turn")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn latest_execution_segment_for_attempt<C: ConnectionTrait>(
    db: &C,
    attempt_id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find()
        .filter(turn_cli_runtime_execution_segment::Column::AttemptId.eq(attempt_id.to_owned()))
        .order_by_desc(turn_cli_runtime_execution_segment::Column::SegmentIndex)
        .one(db)
        .await
        .context("failed to query latest CLI runtime execution segment")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn latest_running_execution_segment_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
    turn_cli_runtime_execution_segment::Entity::find()
        .filter(turn_cli_runtime_execution_segment::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            turn_cli_runtime_execution_segment::Column::Status
                .eq(CliRuntimeExecutionSegmentStatus::Running.as_str()),
        )
        .order_by_desc(turn_cli_runtime_execution_segment::Column::SegmentIndex)
        .one(db)
        .await
        .context("failed to query running CLI runtime execution segment")?
        .map(execution_segment_record_from_model)
        .transpose()
}

pub async fn mark_execution_segment_terminal<C: ConnectionTrait>(
    db: &C,
    id: &str,
    status: CliRuntimeExecutionSegmentStatus,
    failure_reason: Option<String>,
    completed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if !status.is_terminal() {
        return Err(anyhow!(
            "CLI runtime terminal execution segment status cannot be `{}`",
            status.as_str()
        ));
    }
    let result = turn_cli_runtime_execution_segment::Entity::update_many()
        .col_expr(
            turn_cli_runtime_execution_segment::Column::Status,
            sea_orm::sea_query::Expr::value(status.as_str()),
        )
        .col_expr(
            turn_cli_runtime_execution_segment::Column::FailureReason,
            sea_orm::sea_query::Expr::value(failure_reason),
        )
        .col_expr(
            turn_cli_runtime_execution_segment::Column::CompletedAt,
            sea_orm::sea_query::Expr::value(Some(completed_at)),
        )
        .col_expr(
            turn_cli_runtime_execution_segment::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(completed_at),
        )
        .filter(turn_cli_runtime_execution_segment::Column::Id.eq(id.to_owned()))
        .filter(
            turn_cli_runtime_execution_segment::Column::Status
                .eq(CliRuntimeExecutionSegmentStatus::Running.as_str()),
        )
        .exec(db)
        .await
        .context("failed to mark CLI runtime execution segment terminal")?;
    Ok(result.rows_affected == 1)
}

fn active_turn_attempt_from_new(
    attempt: NewCliRuntimeTurnAttempt,
) -> turn_cli_runtime_attempt::ActiveModel {
    turn_cli_runtime_attempt::ActiveModel {
        id: Set(attempt.id),
        turn_id: Set(attempt.turn_id),
        attempt_index: Set(i64::from(attempt.attempt_index)),
        runtime_id: Set(attempt.runtime_id),
        runtime_kind: Set(attempt.runtime_kind),
        native_thread_id: Set(attempt.native_thread_id),
        native_turn_id: Set(attempt.native_turn_id),
        recovery_job_id: Set(attempt.recovery_job_id),
        recovery_attempt_id: Set(attempt.recovery_attempt_id),
        recovery_confirmed_at: Set(None),
        execution_window_index: Set(attempt.execution_window_index.map(i64::from)),
        status: Set(attempt.status.as_str().to_owned()),
        failure_reason: Set(attempt.failure_reason),
        started_at: Set(attempt.started_at),
        completed_at: Set(attempt.completed_at),
        created_at: Set(attempt.created_at),
        updated_at: Set(attempt.updated_at),
    }
}

fn active_execution_segment_from_new(
    segment: NewCliRuntimeExecutionSegment,
) -> turn_cli_runtime_execution_segment::ActiveModel {
    turn_cli_runtime_execution_segment::ActiveModel {
        id: Set(segment.id),
        attempt_id: Set(segment.attempt_id),
        turn_id: Set(segment.turn_id),
        segment_index: Set(i64::from(segment.segment_index)),
        runtime_id: Set(segment.runtime_id),
        native_thread_id: Set(segment.native_thread_id),
        native_turn_id: Set(segment.native_turn_id),
        status: Set(segment.status.as_str().to_owned()),
        failure_reason: Set(segment.failure_reason),
        started_at: Set(segment.started_at),
        completed_at: Set(segment.completed_at),
        created_at: Set(segment.created_at),
        updated_at: Set(segment.updated_at),
    }
}

fn validate_new_turn_attempt(attempt: &NewCliRuntimeTurnAttempt) -> Result<()> {
    if attempt.attempt_index == 0 {
        return Err(anyhow!("CLI runtime turn attempt index must be positive"));
    }
    if attempt.execution_window_index == Some(0) {
        return Err(anyhow!(
            "CLI runtime turn attempt execution window index must be positive"
        ));
    }
    if attempt.recovery_job_id.is_some() != attempt.recovery_attempt_id.is_some() {
        return Err(anyhow!(
            "CLI runtime turn attempt recovery job and attempt ids must both be present or absent"
        ));
    }
    if attempt.status == CliRuntimeTurnAttemptStatus::Running
        && attempt.native_turn_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(anyhow!(
            "running CLI runtime turn attempt must have a native turn id"
        ));
    }
    if attempt.status == CliRuntimeTurnAttemptStatus::Starting && attempt.native_turn_id.is_some() {
        return Err(anyhow!(
            "starting CLI runtime turn attempt cannot have a native turn id"
        ));
    }
    Ok(())
}

fn validate_new_execution_segment(segment: &NewCliRuntimeExecutionSegment) -> Result<()> {
    for (label, value) in [
        ("id", segment.id.as_str()),
        ("attempt_id", segment.attempt_id.as_str()),
        ("turn_id", segment.turn_id.as_str()),
        ("runtime_id", segment.runtime_id.as_str()),
        ("native_thread_id", segment.native_thread_id.as_str()),
        ("native_turn_id", segment.native_turn_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(anyhow!(
                "CLI runtime execution segment `{label}` cannot be empty"
            ));
        }
    }
    if segment.segment_index == 0 {
        return Err(anyhow!(
            "CLI runtime execution segment index must be positive"
        ));
    }
    if segment.status != CliRuntimeExecutionSegmentStatus::Running
        || segment.completed_at.is_some()
        || segment.failure_reason.is_some()
    {
        return Err(anyhow!(
            "new CLI runtime execution segment must be running and non-terminal"
        ));
    }
    Ok(())
}

fn turn_attempt_record_from_model(
    model: turn_cli_runtime_attempt::Model,
) -> Result<CliRuntimeTurnAttemptRecord> {
    Ok(CliRuntimeTurnAttemptRecord {
        id: model.id,
        turn_id: model.turn_id,
        attempt_index: u32::try_from(model.attempt_index)
            .context("CLI runtime turn attempt index is outside u32")?,
        runtime_id: model.runtime_id,
        runtime_kind: model.runtime_kind,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        recovery_job_id: model.recovery_job_id,
        recovery_attempt_id: model.recovery_attempt_id,
        recovery_confirmed_at: model.recovery_confirmed_at,
        execution_window_index: model
            .execution_window_index
            .map(u32::try_from)
            .transpose()
            .context("CLI runtime execution window index is outside u32")?,
        status: CliRuntimeTurnAttemptStatus::from_db(model.status.as_str())?,
        failure_reason: model.failure_reason,
        started_at: model.started_at,
        completed_at: model.completed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

fn execution_segment_record_from_model(
    model: turn_cli_runtime_execution_segment::Model,
) -> Result<CliRuntimeExecutionSegmentRecord> {
    Ok(CliRuntimeExecutionSegmentRecord {
        id: model.id,
        attempt_id: model.attempt_id,
        turn_id: model.turn_id,
        segment_index: u32::try_from(model.segment_index)
            .context("CLI runtime execution segment index is outside u32 range")?,
        runtime_id: model.runtime_id,
        native_thread_id: model.native_thread_id,
        native_turn_id: model.native_turn_id,
        status: CliRuntimeExecutionSegmentStatus::from_db(model.status.as_str())?,
        failure_reason: model.failure_reason,
        started_at: model.started_at,
        completed_at: model.completed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_postgres_test::with_database;
    use sea_orm::TransactionTrait;

    #[tokio::test]
    async fn cli_attempt_and_active_segment_commit_or_roll_back_together() -> Result<()> {
        with_database(async |fixture| {
            let db = fixture.connection();
            let now = crate::time::unix_to_datetime(1_700_000_000);
            let attempt = create_turn_attempt(
                db,
                NewCliRuntimeTurnAttempt {
                    id: "attempt".to_owned(),
                    turn_id: "turn".to_owned(),
                    attempt_index: 1,
                    runtime_id: "runtime".to_owned(),
                    runtime_kind: "codex".to_owned(),
                    native_thread_id: "native-thread".to_owned(),
                    native_turn_id: None,
                    recovery_job_id: None,
                    recovery_attempt_id: None,
                    execution_window_index: Some(1),
                    status: CliRuntimeTurnAttemptStatus::Starting,
                    failure_reason: None,
                    started_at: None,
                    completed_at: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
            assert!(mark_turn_attempt_running(db, &attempt.id, "native-turn", now).await?);
            assert!(!mark_turn_attempt_running(db, &attempt.id, "stale-replay", now).await?);
            assert_eq!(
                find_turn_attempt_by_native_turn(db, "runtime", "native-turn")
                    .await?
                    .unwrap()
                    .id,
                attempt.id,
            );
            let segment = create_execution_segment(
                db,
                NewCliRuntimeExecutionSegment {
                    id: "segment".to_owned(),
                    attempt_id: attempt.id.clone(),
                    turn_id: attempt.turn_id.clone(),
                    segment_index: 1,
                    runtime_id: attempt.runtime_id.clone(),
                    native_thread_id: attempt.native_thread_id.clone(),
                    native_turn_id: "segment-native-turn".to_owned(),
                    status: CliRuntimeExecutionSegmentStatus::Running,
                    failure_reason: None,
                    started_at: now,
                    completed_at: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
            let done = now + chrono::Duration::seconds(1);
            for commit in [false, true] {
                let tx = db.begin().await?;
                assert!(
                    mark_turn_attempt_terminal(
                        &tx,
                        &attempt.id,
                        CliRuntimeTurnAttemptStatus::Failed,
                        Some("test failure".to_owned()),
                        done
                    )
                    .await?
                );
                let updated_attempt = find_turn_attempt(&tx, &attempt.id).await?.unwrap();
                let updated_segment = find_execution_segment(&tx, &segment.id).await?.unwrap();
                assert_eq!(updated_attempt.status, CliRuntimeTurnAttemptStatus::Failed);
                assert_eq!(
                    updated_segment.status,
                    CliRuntimeExecutionSegmentStatus::Failed
                );
                assert_eq!(
                    updated_attempt.failure_reason,
                    updated_segment.failure_reason
                );
                assert_eq!(updated_segment.completed_at, Some(done));
                if commit {
                    tx.commit().await?;
                    assert_eq!(
                        find_execution_segment(db, &segment.id).await?,
                        Some(updated_segment)
                    );
                } else {
                    tx.rollback().await?;
                    assert_eq!(
                        find_execution_segment(db, &segment.id).await?,
                        Some(segment.clone())
                    );
                    assert_eq!(
                        find_turn_attempt(db, &attempt.id).await?.unwrap().status,
                        CliRuntimeTurnAttemptStatus::Running
                    );
                }
            }
            Ok(())
        })
        .await
    }
}
