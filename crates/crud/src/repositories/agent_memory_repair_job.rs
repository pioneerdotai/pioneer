use anyhow::{Context, Result};
use pioneer_entity::agent_memory_repair_job;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    DB_ID_LEN, MEMORY_REPAIR_JOB_STATUS_COMPLETED, MEMORY_REPAIR_JOB_STATUS_FAILED,
    MEMORY_REPAIR_JOB_STATUS_PENDING, MEMORY_REPAIR_JOB_STATUS_RUNNING, memory_scope_kind_to_db,
};
use crate::memory::NewAgentMemoryRepairJob;
use crate::util::unix_to_datetime;

pub async fn enqueue_repair_job<C: ConnectionTrait>(
    db: &C,
    job: NewAgentMemoryRepairJob,
    now: DateTimeWithTimeZone,
) -> Result<agent_memory_repair_job::Model> {
    if let Some(existing) = find_open_repair_job(db, &job).await? {
        return Ok(existing);
    }

    let id = pioneer_protocol::generate_id(DB_ID_LEN);
    agent_memory_repair_job::Entity::insert(agent_memory_repair_job::ActiveModel {
        id: Set(id.clone()),
        job_kind: Set(job.job_kind.clone()),
        status: Set(MEMORY_REPAIR_JOB_STATUS_PENDING.to_owned()),
        workspace_id: Set(job.workspace_id),
        scope_kind: Set(job
            .scope_kind
            .map(memory_scope_kind_to_db)
            .map(str::to_owned)),
        scope_key_hash: Set(job.scope_key_hash),
        memory_id: Set(job.memory_id),
        capsule_id: Set(job.capsule_id),
        priority: Set(job.priority),
        attempts: Set(0),
        max_attempts: Set(i64::from(job.max_attempts)),
        locked_by: Set(None),
        lock_expires_at: Set(None),
        scheduled_at: Set(unix_to_datetime(job.scheduled_at_unix)),
        started_at: Set(None),
        completed_at: Set(None),
        last_error: Set(None),
        payload_json: Set(job.payload_json),
        result_json: Set(None),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to enqueue memory repair job `{}`", job.job_kind))?;

    agent_memory_repair_job::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload memory repair job")?
        .context("inserted memory repair job missing")
}

async fn find_open_repair_job<C: ConnectionTrait>(
    db: &C,
    job: &NewAgentMemoryRepairJob,
) -> Result<Option<agent_memory_repair_job::Model>> {
    let mut query = agent_memory_repair_job::Entity::find()
        .filter(agent_memory_repair_job::Column::JobKind.eq(job.job_kind.clone()))
        .filter(agent_memory_repair_job::Column::Status.is_in([
            MEMORY_REPAIR_JOB_STATUS_PENDING.to_owned(),
            MEMORY_REPAIR_JOB_STATUS_RUNNING.to_owned(),
        ]));

    query = match job.memory_id.as_deref() {
        Some(memory_id) => query.filter(agent_memory_repair_job::Column::MemoryId.eq(memory_id)),
        None => query.filter(agent_memory_repair_job::Column::MemoryId.is_null()),
    };
    query = match job.capsule_id.as_deref() {
        Some(capsule_id) => query.filter(agent_memory_repair_job::Column::CapsuleId.eq(capsule_id)),
        None => query.filter(agent_memory_repair_job::Column::CapsuleId.is_null()),
    };

    query
        .order_by_desc(agent_memory_repair_job::Column::CreatedAt)
        .one(db)
        .await
        .context("failed to find open memory repair job")
}

pub async fn claim_due_repair_jobs<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    lock_expires_at: DateTimeWithTimeZone,
    locked_by: &str,
    limit: u64,
) -> Result<Vec<agent_memory_repair_job::Model>> {
    let candidates = agent_memory_repair_job::Entity::find()
        .filter(agent_memory_repair_job::Column::Status.eq(MEMORY_REPAIR_JOB_STATUS_PENDING))
        .filter(agent_memory_repair_job::Column::ScheduledAt.lte(now))
        .filter(
            Condition::any()
                .add(agent_memory_repair_job::Column::LockExpiresAt.is_null())
                .add(agent_memory_repair_job::Column::LockExpiresAt.lte(now)),
        )
        .order_by_desc(agent_memory_repair_job::Column::Priority)
        .order_by_asc(agent_memory_repair_job::Column::ScheduledAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list due memory repair jobs")?;

    let mut claimed = Vec::new();
    for candidate in candidates {
        let affected = agent_memory_repair_job::Entity::update_many()
            .col_expr(
                agent_memory_repair_job::Column::Status,
                Expr::value(MEMORY_REPAIR_JOB_STATUS_RUNNING.to_owned()),
            )
            .col_expr(
                agent_memory_repair_job::Column::LockedBy,
                Expr::value(Some(locked_by.to_owned())),
            )
            .col_expr(
                agent_memory_repair_job::Column::LockExpiresAt,
                Expr::value(Some(lock_expires_at)),
            )
            .col_expr(
                agent_memory_repair_job::Column::StartedAt,
                Expr::value(Some(now)),
            )
            .col_expr(agent_memory_repair_job::Column::UpdatedAt, Expr::value(now))
            .filter(agent_memory_repair_job::Column::Id.eq(candidate.id.clone()))
            .filter(agent_memory_repair_job::Column::Status.eq(MEMORY_REPAIR_JOB_STATUS_PENDING))
            .exec(db)
            .await
            .with_context(|| format!("failed to claim memory repair job `{}`", candidate.id))?
            .rows_affected;

        if affected == 0 {
            continue;
        }
        if let Some(row) = agent_memory_repair_job::Entity::find_by_id(candidate.id)
            .one(db)
            .await
            .context("failed to reload claimed memory repair job")?
        {
            claimed.push(row);
        }
    }

    Ok(claimed)
}

pub async fn mark_repair_job_running<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    locked_by: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory_repair_job::Model>> {
    let affected = agent_memory_repair_job::Entity::update_many()
        .col_expr(
            agent_memory_repair_job::Column::Status,
            Expr::value(MEMORY_REPAIR_JOB_STATUS_RUNNING.to_owned()),
        )
        .col_expr(
            agent_memory_repair_job::Column::LockedBy,
            Expr::value(Some(locked_by.to_owned())),
        )
        .col_expr(
            agent_memory_repair_job::Column::StartedAt,
            Expr::value(Some(now)),
        )
        .col_expr(agent_memory_repair_job::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory_repair_job::Column::Id.eq(job_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark memory repair job `{job_id}` running"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_repair_job_by_id(db, job_id).await
}

pub async fn mark_repair_job_completed<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    locked_by: &str,
    result_json: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory_repair_job::Model>> {
    let affected = agent_memory_repair_job::Entity::update_many()
        .col_expr(
            agent_memory_repair_job::Column::Status,
            Expr::value(MEMORY_REPAIR_JOB_STATUS_COMPLETED.to_owned()),
        )
        .col_expr(
            agent_memory_repair_job::Column::LockedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            agent_memory_repair_job::Column::LockExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            agent_memory_repair_job::Column::CompletedAt,
            Expr::value(Some(now)),
        )
        .col_expr(
            agent_memory_repair_job::Column::ResultJson,
            Expr::value(result_json),
        )
        .col_expr(agent_memory_repair_job::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory_repair_job::Column::Id.eq(job_id.to_owned()))
        .filter(agent_memory_repair_job::Column::Status.eq(MEMORY_REPAIR_JOB_STATUS_RUNNING))
        .filter(agent_memory_repair_job::Column::LockedBy.eq(locked_by.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to complete memory repair job `{job_id}`"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_repair_job_by_id(db, job_id).await
}

pub async fn mark_repair_job_failed<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    locked_by: &str,
    last_error: String,
    retry_at: Option<DateTimeWithTimeZone>,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory_repair_job::Model>> {
    let Some(job) = find_repair_job_by_id(db, job_id).await? else {
        return Ok(None);
    };
    if job.status != MEMORY_REPAIR_JOB_STATUS_RUNNING || job.locked_by.as_deref() != Some(locked_by)
    {
        return Ok(None);
    }

    let attempts = job.attempts.saturating_add(1);
    let terminal = attempts >= job.max_attempts;
    let next_status = if terminal {
        MEMORY_REPAIR_JOB_STATUS_FAILED
    } else {
        MEMORY_REPAIR_JOB_STATUS_PENDING
    };

    agent_memory_repair_job::Entity::update_many()
        .col_expr(
            agent_memory_repair_job::Column::Status,
            Expr::value(next_status.to_owned()),
        )
        .col_expr(
            agent_memory_repair_job::Column::Attempts,
            Expr::value(attempts),
        )
        .col_expr(
            agent_memory_repair_job::Column::LastError,
            Expr::value(Some(last_error)),
        )
        .col_expr(
            agent_memory_repair_job::Column::ScheduledAt,
            Expr::value(retry_at.unwrap_or(now)),
        )
        .col_expr(
            agent_memory_repair_job::Column::LockedBy,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            agent_memory_repair_job::Column::LockExpiresAt,
            Expr::value(Option::<DateTimeWithTimeZone>::None),
        )
        .col_expr(
            agent_memory_repair_job::Column::CompletedAt,
            Expr::value(terminal.then_some(now)),
        )
        .col_expr(agent_memory_repair_job::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory_repair_job::Column::Id.eq(job_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to fail memory repair job `{job_id}`"))?;

    find_repair_job_by_id(db, job_id).await
}

pub async fn find_repair_job_by_id<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> Result<Option<agent_memory_repair_job::Model>> {
    agent_memory_repair_job::Entity::find_by_id(job_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find memory repair job `{job_id}`"))
}

pub async fn list_repair_jobs_for_memory<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_repair_job::Model>> {
    agent_memory_repair_job::Entity::find()
        .filter(agent_memory_repair_job::Column::MemoryId.eq(memory_id.to_owned()))
        .order_by_desc(agent_memory_repair_job::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory repair jobs for `{memory_id}`"))
}
