use anyhow::{Context, Result};
use pioneer_entity::{agent_execution_resource_state, agent_work_queue};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{Condition, ConnectionTrait, DbBackend, EntityTrait, Statement};

/// Select one eligible branch per active graph before limiting the global page.
pub async fn candidates<C: ConnectionTrait>(
    db: &C,
    now: DateTimeWithTimeZone,
    limit: u64,
) -> Result<Vec<String>> {
    let limit = i64::try_from(limit).context("agent promotion limit exceeds bigint")?;
    // SeaORM has no Entity projection for ranks across two windowed CTEs.
    db.query_all_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "WITH branch_heads AS (\
               SELECT q.id, q.root_execution_id, q.branch_key, q.created_at, \
                      q.enqueue_sequence, branch.last_scheduled_sequence, \
                      ROW_NUMBER() OVER (\
                        PARTITION BY q.root_execution_id, q.branch_key \
                        ORDER BY q.created_at, q.enqueue_sequence, q.id\
                      ) AS branch_rank \
               FROM agent_work_queue q \
               JOIN agent_work_branch_schedule branch \
                 ON branch.root_execution_id = q.root_execution_id \
                AND branch.branch_key = q.branch_key \
               WHERE q.state = 'queued' \
                 AND (q.eligible_at IS NULL OR q.eligible_at <= $1)\
             ), root_heads AS (\
               SELECT id, root_execution_id, created_at, enqueue_sequence, \
                      ROW_NUMBER() OVER (\
                        PARTITION BY root_execution_id \
                        ORDER BY last_scheduled_sequence, \
                                 created_at, enqueue_sequence, id\
                      ) AS root_rank \
               FROM branch_heads WHERE branch_rank = 1\
             ) \
             SELECT candidate.id FROM root_heads candidate \
             JOIN agent_work_resource_scope scope \
               ON scope.root_execution_id = candidate.root_execution_id \
             WHERE candidate.root_rank = 1 AND scope.status = 'active' \
               AND (SELECT COUNT(*) FROM agent_running_permit permit \
                    WHERE permit.root_execution_id = candidate.root_execution_id \
                      AND permit.status = 'held') < scope.max_concurrency \
             ORDER BY scope.last_scheduled_sequence, \
                      candidate.created_at, candidate.enqueue_sequence, candidate.id \
             LIMIT $2",
        [now.into(), limit.into()],
    ))
    .await
    .context("failed to list queued agent domain executions")?
    .into_iter()
    .map(|row| row.try_get("", "id").map_err(Into::into))
    .collect()
}

pub async fn load_fair_queued_candidate<C: ConnectionTrait>(
    db: &C,
    root_execution_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_work_queue::Model>> {
    // The first window ranks the eligible members of each branch, so LIMIT
    // cannot hide the oldest branch. Keep its computed alias inside the CTE.
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "WITH branch_heads AS (\
               SELECT q.id, q.branch_key, q.created_at, q.enqueue_sequence, \
                      branch.last_scheduled_sequence, \
                      ROW_NUMBER() OVER (\
                        PARTITION BY q.branch_key \
                        ORDER BY q.created_at, q.enqueue_sequence, q.id\
                      ) AS branch_rank \
               FROM agent_work_queue q \
               JOIN agent_work_branch_schedule branch \
                 ON branch.root_execution_id = q.root_execution_id \
                AND branch.branch_key = q.branch_key \
               WHERE q.root_execution_id = $1 AND q.state = 'queued' \
                 AND (q.eligible_at IS NULL OR q.eligible_at <= $2)\
             ) \
             SELECT id FROM branch_heads WHERE branch_rank = 1 \
             ORDER BY last_scheduled_sequence, \
                      created_at, enqueue_sequence, id LIMIT 1",
            [root_execution_id.into(), now.into()],
        ))
        .await
        .context("failed to select fair queued branch candidate")?;
    let Some(row) = row else { return Ok(None) };
    let id: String = row.try_get("", "id")?;
    agent_work_queue::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to load fair queued branch candidate")
}

pub fn extend_idle_deadline(proposed: DateTimeWithTimeZone) -> Expr {
    use agent_execution_resource_state::Column;
    let hard = Expr::col(Column::HardDeadline);
    let idle = Expr::col(Column::IdleDeadline);
    Expr::case(hard.clone().lte(proposed), hard)
        .case(
            Condition::any()
                .add(idle.clone().is_null())
                .add(idle.clone().lt(proposed)),
            proposed,
        )
        .finally(idle)
        .into()
}
