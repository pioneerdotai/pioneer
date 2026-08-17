use anyhow::{Context, Result, bail};
use pioneer_entity::turn_execution;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnExecutorKind {
    NativeAgent,
    ApiProvider,
    CliRuntime,
    AcpRuntime,
}

impl TurnExecutorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeAgent => "native_agent",
            Self::ApiProvider => "api_provider",
            Self::CliRuntime => "cli_runtime",
            Self::AcpRuntime => "acp_runtime",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "native_agent" => Some(Self::NativeAgent),
            "api_provider" => Some(Self::ApiProvider),
            "cli_runtime" => Some(Self::CliRuntime),
            "acp_runtime" => Some(Self::AcpRuntime),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnExecutionStatus {
    Queued,
    Starting,
    Running,
    Recovering,
    Completed,
    Failed,
    Interrupted,
    Blocked,
}

impl TurnExecutionStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Recovering => "recovering",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::Blocked => "blocked",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "recovering" => Some(Self::Recovering),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "interrupted" => Some(Self::Interrupted),
            "blocked" => Some(Self::Blocked),
            _ => None,
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Starting | Self::Running | Self::Recovering
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewTurnExecution {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub executor_kind: TurnExecutorKind,
    pub executor_key: Option<String>,
    pub status: TurnExecutionStatus,
    pub owner_id: String,
    pub lease_until: DateTimeWithTimeZone,
    pub created_at: DateTimeWithTimeZone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnExecutionRecord {
    pub turn_id: String,
    pub thread_id: String,
    pub workspace_id: String,
    pub executor_kind: TurnExecutorKind,
    pub executor_key: Option<String>,
    pub status: TurnExecutionStatus,
    pub owner_id: String,
    pub owner_generation: u64,
    pub lease_until: DateTimeWithTimeZone,
    pub heartbeat_at: DateTimeWithTimeZone,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

pub async fn insert_immutable<C: ConnectionTrait>(
    db: &C,
    execution: NewTurnExecution,
) -> Result<TurnExecutionRecord> {
    validate_new(&execution)?;
    let expected = execution.clone();
    let started_at =
        (execution.status == TurnExecutionStatus::Running).then_some(execution.created_at);
    turn_execution::Entity::insert(turn_execution::ActiveModel {
        turn_id: Set(execution.turn_id.clone()),
        thread_id: Set(execution.thread_id),
        workspace_id: Set(execution.workspace_id),
        executor_kind: Set(execution.executor_kind.as_str().to_owned()),
        executor_key: Set(execution.executor_key),
        status: Set(execution.status.as_str().to_owned()),
        owner_id: Set(execution.owner_id),
        owner_generation: Set(1),
        lease_until: Set(execution.lease_until),
        heartbeat_at: Set(execution.created_at),
        started_at: Set(started_at),
        completed_at: Set(None),
        created_at: Set(execution.created_at),
        updated_at: Set(execution.created_at),
    })
    .on_conflict(
        OnConflict::column(turn_execution::Column::TurnId)
            .do_nothing()
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to insert immutable Turn execution ownership")?;

    let persisted = find(db, expected.turn_id.as_str())
        .await?
        .context("Turn execution ownership is missing after insert")?;
    ensure_same_immutable(&persisted, &expected)?;
    Ok(persisted)
}

pub async fn find<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<TurnExecutionRecord>> {
    turn_execution::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query Turn execution ownership")?
        .map(record_from_model)
        .transpose()
}

pub async fn heartbeat_owner<C: ConnectionTrait>(
    db: &C,
    owner_id: &str,
    heartbeat_at: DateTimeWithTimeZone,
    lease_until: DateTimeWithTimeZone,
) -> Result<u64> {
    let result = turn_execution::Entity::update_many()
        .filter(turn_execution::Column::OwnerId.eq(owner_id.to_owned()))
        .filter(turn_execution::Column::Status.is_in([
            TurnExecutionStatus::Queued.as_str(),
            TurnExecutionStatus::Starting.as_str(),
            TurnExecutionStatus::Running.as_str(),
            TurnExecutionStatus::Recovering.as_str(),
        ]))
        .col_expr(
            turn_execution::Column::HeartbeatAt,
            Expr::value(heartbeat_at),
        )
        .col_expr(turn_execution::Column::LeaseUntil, Expr::value(lease_until))
        .col_expr(turn_execution::Column::UpdatedAt, Expr::value(heartbeat_at))
        .exec(db)
        .await
        .context("failed to heartbeat owned Turn executions")?;
    Ok(result.rows_affected)
}

pub async fn mark_running_owned<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    owner_id: &str,
    started_at: DateTimeWithTimeZone,
    lease_until: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = turn_execution::Entity::update_many()
        .filter(turn_execution::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_execution::Column::OwnerId.eq(owner_id.to_owned()))
        .filter(turn_execution::Column::Status.is_in([
            TurnExecutionStatus::Queued.as_str(),
            TurnExecutionStatus::Starting.as_str(),
            TurnExecutionStatus::Running.as_str(),
            TurnExecutionStatus::Recovering.as_str(),
        ]))
        .col_expr(
            turn_execution::Column::Status,
            Expr::value(TurnExecutionStatus::Running.as_str()),
        )
        .col_expr(
            turn_execution::Column::StartedAt,
            Expr::value(Some(started_at)),
        )
        .col_expr(turn_execution::Column::HeartbeatAt, Expr::value(started_at))
        .col_expr(turn_execution::Column::LeaseUntil, Expr::value(lease_until))
        .col_expr(turn_execution::Column::UpdatedAt, Expr::value(started_at))
        .exec(db)
        .await
        .context("failed to mark owned Turn execution running")?;
    Ok(result.rows_affected == 1)
}

pub async fn mark_terminal<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: TurnExecutionStatus,
    completed_at: DateTimeWithTimeZone,
) -> Result<bool> {
    if status.is_active() {
        bail!(
            "Turn execution terminal state cannot be `{}`",
            status.as_str()
        );
    }
    let result = turn_execution::Entity::update_many()
        .filter(turn_execution::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_execution::Column::Status.is_in([
            TurnExecutionStatus::Queued.as_str(),
            TurnExecutionStatus::Starting.as_str(),
            TurnExecutionStatus::Running.as_str(),
            TurnExecutionStatus::Recovering.as_str(),
        ]))
        .col_expr(turn_execution::Column::Status, Expr::value(status.as_str()))
        .col_expr(
            turn_execution::Column::CompletedAt,
            Expr::value(Some(completed_at)),
        )
        .col_expr(
            turn_execution::Column::LeaseUntil,
            Expr::value(completed_at),
        )
        .col_expr(
            turn_execution::Column::HeartbeatAt,
            Expr::value(completed_at),
        )
        .col_expr(turn_execution::Column::UpdatedAt, Expr::value(completed_at))
        .exec(db)
        .await
        .context("failed to terminalize Turn execution ownership")?;
    Ok(result.rows_affected == 1)
}

pub async fn list_expired_foreign_active<C: ConnectionTrait>(
    db: &C,
    current_owner_id: &str,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<TurnExecutionRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    turn_execution::Entity::find()
        .filter(turn_execution::Column::OwnerId.ne(current_owner_id.to_owned()))
        .filter(turn_execution::Column::LeaseUntil.lte(now))
        .filter(turn_execution::Column::Status.is_in([
            TurnExecutionStatus::Queued.as_str(),
            TurnExecutionStatus::Starting.as_str(),
            TurnExecutionStatus::Running.as_str(),
            TurnExecutionStatus::Recovering.as_str(),
        ]))
        .order_by_asc(turn_execution::Column::LeaseUntil)
        .order_by_asc(turn_execution::Column::TurnId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list expired foreign Turn executions")?
        .into_iter()
        .map(record_from_model)
        .collect()
}

pub async fn list_owned_recovering<C: ConnectionTrait>(
    db: &C,
    owner_id: &str,
    limit: u64,
) -> Result<Vec<TurnExecutionRecord>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    turn_execution::Entity::find()
        .filter(turn_execution::Column::OwnerId.eq(owner_id.to_owned()))
        .filter(turn_execution::Column::Status.eq(TurnExecutionStatus::Recovering.as_str()))
        .order_by_asc(turn_execution::Column::UpdatedAt)
        .order_by_asc(turn_execution::Column::TurnId)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list owned recovering Turn executions")?
        .into_iter()
        .map(record_from_model)
        .collect()
}

pub async fn claim_expired<C: ConnectionTrait>(
    db: &C,
    expected: &TurnExecutionRecord,
    new_owner_id: &str,
    claimed_at: DateTimeWithTimeZone,
    lease_until: DateTimeWithTimeZone,
) -> Result<Option<TurnExecutionRecord>> {
    let next_generation = expected
        .owner_generation
        .checked_add(1)
        .context("Turn execution owner generation overflow")?;
    let next_generation = i64::try_from(next_generation)
        .context("Turn execution owner generation exceeds SQLite INTEGER")?;
    let expected_generation = i64::try_from(expected.owner_generation)
        .context("Turn execution owner generation exceeds SQLite INTEGER")?;
    let result = turn_execution::Entity::update_many()
        .filter(turn_execution::Column::TurnId.eq(expected.turn_id.clone()))
        .filter(turn_execution::Column::OwnerId.eq(expected.owner_id.clone()))
        .filter(turn_execution::Column::OwnerGeneration.eq(expected_generation))
        .filter(turn_execution::Column::LeaseUntil.lte(claimed_at))
        .filter(turn_execution::Column::Status.eq(expected.status.as_str()))
        .col_expr(
            turn_execution::Column::Status,
            Expr::value(TurnExecutionStatus::Recovering.as_str()),
        )
        .col_expr(
            turn_execution::Column::OwnerId,
            Expr::value(new_owner_id.to_owned()),
        )
        .col_expr(
            turn_execution::Column::OwnerGeneration,
            Expr::value(next_generation),
        )
        .col_expr(turn_execution::Column::HeartbeatAt, Expr::value(claimed_at))
        .col_expr(turn_execution::Column::LeaseUntil, Expr::value(lease_until))
        .col_expr(turn_execution::Column::UpdatedAt, Expr::value(claimed_at))
        .exec(db)
        .await
        .context("failed to claim expired Turn execution ownership")?;
    if result.rows_affected == 0 {
        return Ok(None);
    }
    find(db, expected.turn_id.as_str()).await
}

pub async fn reacquire_blocked<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    owner_id: &str,
    resumed_at: DateTimeWithTimeZone,
    lease_until: DateTimeWithTimeZone,
) -> Result<bool> {
    let Some(current) = find(db, turn_id).await? else {
        return Ok(true);
    };
    if current.status != TurnExecutionStatus::Blocked {
        return Ok(false);
    }
    let current_generation = i64::try_from(current.owner_generation)
        .context("Turn execution owner generation exceeds SQLite INTEGER")?;
    let next_generation = current_generation
        .checked_add(1)
        .context("Turn execution owner generation overflow")?;
    let result = turn_execution::Entity::update_many()
        .filter(turn_execution::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_execution::Column::Status.eq(TurnExecutionStatus::Blocked.as_str()))
        .filter(turn_execution::Column::OwnerGeneration.eq(current_generation))
        .col_expr(
            turn_execution::Column::Status,
            Expr::value(TurnExecutionStatus::Starting.as_str()),
        )
        .col_expr(
            turn_execution::Column::OwnerId,
            Expr::value(owner_id.to_owned()),
        )
        .col_expr(
            turn_execution::Column::OwnerGeneration,
            Expr::value(next_generation),
        )
        .col_expr(turn_execution::Column::HeartbeatAt, Expr::value(resumed_at))
        .col_expr(turn_execution::Column::LeaseUntil, Expr::value(lease_until))
        .col_expr(
            turn_execution::Column::CompletedAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(turn_execution::Column::UpdatedAt, Expr::value(resumed_at))
        .exec(db)
        .await
        .context("failed to reacquire blocked Turn execution")?;
    Ok(result.rows_affected == 1)
}

fn validate_new(execution: &NewTurnExecution) -> Result<()> {
    for (name, value) in [
        ("turn_id", execution.turn_id.as_str()),
        ("thread_id", execution.thread_id.as_str()),
        ("workspace_id", execution.workspace_id.as_str()),
        ("owner_id", execution.owner_id.as_str()),
    ] {
        if value.trim().is_empty() {
            bail!("Turn execution `{name}` cannot be empty");
        }
    }
    if !execution.status.is_active() || execution.status == TurnExecutionStatus::Recovering {
        bail!("new Turn execution must use queued, starting, or running state");
    }
    if execution.lease_until <= execution.created_at {
        bail!("new Turn execution lease must expire after creation");
    }
    Ok(())
}

fn ensure_same_immutable(
    persisted: &TurnExecutionRecord,
    expected: &NewTurnExecution,
) -> Result<()> {
    if persisted.turn_id != expected.turn_id
        || persisted.thread_id != expected.thread_id
        || persisted.workspace_id != expected.workspace_id
        || persisted.executor_kind != expected.executor_kind
        || persisted.executor_key != expected.executor_key
    {
        bail!("Turn execution ownership conflicts with its immutable admission");
    }
    Ok(())
}

fn record_from_model(model: turn_execution::Model) -> Result<TurnExecutionRecord> {
    Ok(TurnExecutionRecord {
        turn_id: model.turn_id,
        thread_id: model.thread_id,
        workspace_id: model.workspace_id,
        executor_kind: TurnExecutorKind::parse(model.executor_kind.as_str())
            .with_context(|| format!("unknown Turn executor kind `{}`", model.executor_kind))?,
        executor_key: model.executor_key,
        status: TurnExecutionStatus::parse(model.status.as_str())
            .with_context(|| format!("unknown Turn execution status `{}`", model.status))?,
        owner_id: model.owner_id,
        owner_generation: u64::try_from(model.owner_generation)
            .context("Turn execution owner generation is negative")?,
        lease_until: model.lease_until,
        heartbeat_at: model.heartbeat_at,
        started_at: model.started_at,
        completed_at: model.completed_at,
        created_at: model.created_at,
        updated_at: model.updated_at,
    })
}
