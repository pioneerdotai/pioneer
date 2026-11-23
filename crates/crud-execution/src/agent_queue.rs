use anyhow::{Context, Result, bail};
use pioneer_entity::{
    agent_execution_resource_state, agent_work_branch_schedule, agent_work_queue,
    agent_work_resource_scope, agent_work_scheduler_state,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, ExprTrait, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait,
    QueryFilter, Set, Statement,
};

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

pub async fn next_agent_schedule_sequence(
    db: &DatabaseTransaction,
    now: DateTimeWithTimeZone,
) -> Result<i64> {
    const GLOBAL_SCHEDULER: &str = "global";
    agent_work_scheduler_state::Entity::insert(agent_work_scheduler_state::ActiveModel {
        scheduler_key: Set(GLOBAL_SCHEDULER.to_owned()),
        schedule_generation: Set(0),
        updated_at: Set(now.clone()),
    })
    .on_conflict(
        OnConflict::column(agent_work_scheduler_state::Column::SchedulerKey)
            .do_nothing()
            .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to seed global Agent work scheduler cursor")?;
    let updated = agent_work_scheduler_state::Entity::update_many()
        .col_expr(
            agent_work_scheduler_state::Column::ScheduleGeneration,
            sea_orm::sea_query::Expr::col(agent_work_scheduler_state::Column::ScheduleGeneration)
                .add(1),
        )
        .col_expr(
            agent_work_scheduler_state::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_work_scheduler_state::Column::SchedulerKey.eq(GLOBAL_SCHEDULER))
        .filter(agent_work_scheduler_state::Column::ScheduleGeneration.lt(i64::MAX))
        .exec_with_returning(db)
        .await
        .context("failed to advance global Agent work scheduler cursor")?;
    updated
        .into_iter()
        .next()
        .map(|state| state.schedule_generation)
        .context("global Agent work scheduler generation is exhausted")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentQueueEntryInput {
    pub id: String,
    pub root_execution_id: String,
    pub execution_id: String,
    pub attempt_generation: i64,
    pub branch_key: String,
    pub enqueue_sequence: i64,
    pub eligible_at: Option<DateTimeWithTimeZone>,
    pub now: DateTimeWithTimeZone,
}

/// Publish or verify the immutable queue entry inside the caller's transaction.
pub async fn enqueue_agent_execution<C: ConnectionTrait>(
    db: &C,
    input: &AgentQueueEntryInput,
) -> Result<(agent_work_queue::Model, bool)> {
    ensure_agent_branch_schedule(
        db,
        input.root_execution_id.as_str(),
        input.branch_key.as_str(),
        input.now.clone(),
    )
    .await?;
    let inserted = agent_work_queue::Entity::insert(agent_work_queue::ActiveModel {
        id: Set(input.id.clone()),
        root_execution_id: Set(input.root_execution_id.clone()),
        execution_id: Set(input.execution_id.clone()),
        attempt_generation: Set(input.attempt_generation),
        branch_key: Set(input.branch_key.clone()),
        enqueue_sequence: Set(input.enqueue_sequence),
        state: Set("queued".to_owned()),
        eligible_at: Set(input.eligible_at),
        claim_token: Set(None),
        created_at: Set(input.now),
        updated_at: Set(input.now),
    })
    .on_conflict(
        OnConflict::columns([
            agent_work_queue::Column::RootExecutionId,
            agent_work_queue::Column::ExecutionId,
            agent_work_queue::Column::AttemptGeneration,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to enqueue agent execution")?;
    let queue = agent_work_queue::Entity::find()
        .filter(agent_work_queue::Column::RootExecutionId.eq(input.root_execution_id.clone()))
        .filter(agent_work_queue::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(agent_work_queue::Column::AttemptGeneration.eq(input.attempt_generation))
        .one(db)
        .await
        .context("failed to reload agent work queue entry")?
        .context("agent work queue entry disappeared after idempotent insert")?;
    if queue.id != input.id
        || queue.branch_key != input.branch_key
        || queue.enqueue_sequence != input.enqueue_sequence
        || queue.eligible_at != input.eligible_at
    {
        bail!("agent work queue entry was reused with different immutable facts");
    }
    Ok((queue, inserted > 0))
}

pub async fn ensure_agent_branch_schedule<C: ConnectionTrait>(
    db: &C,
    root_execution_id: &str,
    branch_key: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    if branch_key.trim().is_empty() || branch_key.len() > 255 {
        bail!("agent work branch key is invalid");
    }
    agent_work_branch_schedule::Entity::insert(agent_work_branch_schedule::ActiveModel {
        root_execution_id: Set(root_execution_id.to_owned()),
        branch_key: Set(branch_key.to_owned()),
        last_scheduled_sequence: Set(0),
        last_scheduled_at: Set(None),
        created_at: Set(now.clone()),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            agent_work_branch_schedule::Column::RootExecutionId,
            agent_work_branch_schedule::Column::BranchKey,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist agent work branch schedule")?;
    Ok(())
}

pub async fn mark_agent_branch_scheduled(
    db: &DatabaseTransaction,
    root_execution_id: &str,
    branch_key: &str,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    ensure_agent_branch_schedule(db, root_execution_id, branch_key, now.clone()).await?;
    let sequence = next_agent_schedule_sequence(db, now.clone()).await?;
    let updated = agent_work_branch_schedule::Entity::update_many()
        .col_expr(
            agent_work_branch_schedule::Column::LastScheduledSequence,
            sea_orm::sea_query::Expr::value(sequence),
        )
        .col_expr(
            agent_work_branch_schedule::Column::LastScheduledAt,
            sea_orm::sea_query::Expr::value(Some(now.clone())),
        )
        .col_expr(
            agent_work_branch_schedule::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(
            agent_work_branch_schedule::Column::RootExecutionId.eq(root_execution_id.to_owned()),
        )
        .filter(agent_work_branch_schedule::Column::BranchKey.eq(branch_key.to_owned()))
        .exec(db)
        .await
        .context("failed to advance agent work branch scheduling cursor")?;
    if updated.rows_affected != 1 {
        bail!("agent work branch schedule disappeared during admission");
    }
    let updated = agent_work_resource_scope::Entity::update_many()
        .col_expr(
            agent_work_resource_scope::Column::LastScheduledSequence,
            sea_orm::sea_query::Expr::value(sequence),
        )
        .col_expr(
            agent_work_resource_scope::Column::LastScheduledAt,
            sea_orm::sea_query::Expr::value(Some(now.clone())),
        )
        .col_expr(
            agent_work_resource_scope::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(now),
        )
        .filter(agent_work_resource_scope::Column::RootExecutionId.eq(root_execution_id.to_owned()))
        .filter(agent_work_resource_scope::Column::Status.eq("active"))
        .exec(db)
        .await
        .context("failed to advance root work-graph scheduling cursor")?;
    if updated.rows_affected != 1 {
        bail!("agent work root scope changed during scheduling");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::test_support::{EXECUTION_ID, insert_agent_execution, seed_agent_execution};
    use sea_orm::ActiveModelTrait;

    #[pioneer_postgres_test::postgres_test]
    async fn durable_fair_queue_alternates_sibling_branches_across_transactions() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        seed_agent_execution(db).await;
        let now = chrono::Utc::now().fixed_offset();
        let root_id = EXECUTION_ID;
        agent_work_resource_scope::ActiveModel {
            root_execution_id: Set(root_id.to_owned()),
            scope_generation: Set(1),
            max_concurrency: Set(1),
            max_queue_depth: Set(8),
            max_depth: Set(8),
            max_fan_out: Set(8),
            max_total_nodes: Set(64),
            aggregate_usage_json: Set("{}".to_owned()),
            queue_generation: Set(4),
            last_scheduled_sequence: Set(0),
            last_scheduled_at: Set(None),
            status: Set("active".to_owned()),
            created_at: Set(now),
            updated_at: Set(now),
        }
        .insert(db)
        .await
        .unwrap();
        for (sequence, id, execution_id, branch_key) in [
            (1, "queue-a-1", "execution-a-1", "branch-a"),
            (2, "queue-b-1", "execution-b-1", "branch-b"),
            (3, "queue-a-2", "execution-a-2", "branch-a"),
            (4, "queue-b-2", "execution-b-2", "branch-b"),
        ] {
            let execution_id = crate::canonical_id::canonical_agent_id('E', execution_id);
            insert_agent_execution(db, &execution_id).await;
            enqueue_agent_execution(
                db,
                &AgentQueueEntryInput {
                    id: id.to_owned(),
                    root_execution_id: root_id.to_owned(),
                    execution_id: execution_id.to_owned(),
                    attempt_generation: 1,
                    branch_key: branch_key.to_owned(),
                    enqueue_sequence: sequence,
                    eligible_at: None,
                    now,
                },
            )
            .await
            .unwrap();
        }

        let mut selected = Vec::new();
        for _ in 0..4 {
            // A fresh transaction models the scheduler losing every in-memory
            // local between promotions. Only durable branch/global cursors
            // may determine the next sibling.
            let transaction = db.begin().await.unwrap();
            let candidate = load_fair_queued_candidate(&transaction, root_id, now)
                .await
                .unwrap()
                .unwrap();
            selected.push(candidate.id.clone());
            let claimed = agent_work_queue::Entity::update_many()
                .col_expr(
                    agent_work_queue::Column::State,
                    sea_orm::sea_query::Expr::value("claimed"),
                )
                .filter(agent_work_queue::Column::Id.eq(candidate.id))
                .filter(agent_work_queue::Column::State.eq("queued"))
                .exec(&transaction)
                .await
                .unwrap();
            assert_eq!(claimed.rows_affected, 1);
            mark_agent_branch_scheduled(&transaction, root_id, candidate.branch_key.as_str(), now)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
        assert_eq!(
            selected,
            ["queue-a-1", "queue-b-1", "queue-a-2", "queue-b-2"]
        );
    }

    use super::*;
    use sea_orm::TransactionTrait;
    #[pioneer_postgres_test::postgres_test]
    async fn scheduler_cursor_is_monotonic_even_when_time_does_not_advance() {
        let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
        let db = fixture.connection();
        let now = chrono::Utc::now().fixed_offset();
        let transaction = db.begin().await.unwrap();
        let first = next_agent_schedule_sequence(&transaction, now.clone())
            .await
            .unwrap();
        let second = next_agent_schedule_sequence(&transaction, now)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 2);

        let persisted = agent_work_scheduler_state::Entity::find_by_id("global")
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.schedule_generation, 2);
    }
}
